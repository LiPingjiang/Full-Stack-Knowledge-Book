# 3.3 附录：线上 OOM / CPU 飙高实战排查

> 配套正文 [3.3 JVM 运行时 · 考点 6](./03-JVM运行时.md#考点-6线上-oom--cpu-飙高怎么排查实战加分项最能拉开差距)。
> 正文给的是「排查思路链路图」，本文给**可复现的示例代码 + 一条条能照着敲的命令**——把理论变成肌肉记忆。

排查这类问题的本质是两步：**先复现/锁定现象，再用工具把现象映射回代码行**。下面分别用一段能跑出问题的代码，配一套完整命令演示。

---

## 一、内存泄漏 / OOM

### 1. 复现代码：一个典型的「静态集合只加不删」泄漏

最常见的内存泄漏不是「忘了 new 的对象没回收」，而是**对象被一个长生命周期的容器一直引用着，GC 不敢回收**。下面用静态 `List` 模拟：

```java
import java.util.ArrayList;
import java.util.List;

/**
 * 运行参数（故意把堆设小，快速触发 OOM 并自动 dump）：
 * java -Xms64m -Xmx64m \
 *      -XX:+HeapDumpOnOutOfMemoryError \
 *      -XX:HeapDumpPath=./heap.hprof \
 *      OOMDemo
 */
public class OOMDemo {

    // 静态容器：生命周期跟整个应用一样长，里面的对象永远不会被回收
    private static final List<byte[]> LEAK = new ArrayList<>();

    public static void main(String[] args) throws InterruptedException {
        int i = 0;
        while (true) {
            // 每次塞一个 1MB 的数组，且只加不删 —— 内存只涨不降
            LEAK.add(new byte[1024 * 1024]);
            System.out.println("已泄漏 " + (++i) + " MB");
            Thread.sleep(50);
        }
    }
}
```

跑起来很快就会抛 `java.lang.OutOfMemoryError: Java heap space`，并因为加了 `-XX:+HeapDumpOnOutOfMemoryError` 自动在当前目录生成 `heap.hprof`。

> **真实业务里的同款根因**：静态 `Map` 当缓存却不设淘汰、`ThreadLocal` 用完不 `remove()`（线程池里线程复用，TL 永不释放）、监听器/回调注册了不注销、连接/流未 `close()`。现象都一样：某类对象越积越多。

### 2. 排查命令（照着敲）

第一步，找到 Java 进程 PID：

```bash
jps -l                # 列出所有 Java 进程及主类
# 或 jps -lvm          # 额外打印启动参数
# 输出示例： 12345 OOMDemo
```

第二步，先看 GC 和各内存区，判断「是不是真的内存问题、是哪个区」：

```bash
# 每 1000ms 打一行，看 FullGC（FGC）次数是否飞涨、老年代（O）是否一直逼近 100%
jstat -gcutil 12345 1000

# 字段速记：
#   E=Eden  O=老年代  M=Metaspace  YGC/YGCT=YoungGC次数/耗时  FGC/FGCT=FullGC次数/耗时
# 典型泄漏信号：O 一直接近 100%，FGC 频繁但回收不下去 → 内存被钉死了
```

第三步，dump 堆快照（也可以等它自动 dump，但主动 dump 能在没崩之前抓现场）：

```bash
# live 表示先触发一次 FullGC 只 dump 存活对象，文件更小更干净
jmap -dump:live,format=b,file=heap.hprof 12345

# 看一眼对象直方图，快速知道是哪个类的实例最多（不用打开 MAT 就有线索）
jmap -histo:live 12345 | head -20
# 输出大致：
#  num   #instances  #bytes  class name
#    1       1024     ...     [B            <-- byte[] 占了绝大部分，正好对上我们的泄漏
```

第四步，用 MAT / JProfiler / VisualVM 打开 `heap.hprof` 做深度分析：

- 看 **Dominator Tree**（支配树）：谁「独占」了最多内存，排第一的往往就是泄漏根。
- 看 **GC Roots 引用链**：右键泄漏对象 → `Path to GC Roots`，一直能看到 `OOMDemo.LEAK` 这个静态字段死死拽着它们。
- MAT 的 **Leak Suspects** 报告会直接帮你点名嫌疑对象。

第五步，顺引用链回到代码 → 找到那个「只加不删」的静态集合 → 加淘汰策略 / 及时移除 / 改用弱引用缓存。

> **排查口诀**：`jps` 找进程 → `jstat -gcutil` 确认是内存问题且老年代回收不掉 → `jmap -dump` 抓现场 → `MAT 看 Dominator Tree + Path to GC Roots` 锁定泄漏对象和引用链。

---

## 二、CPU 飙高

### 1. 复现代码：一个空转的死循环线程

CPU 飙高的经典场景是某个线程**疯狂空转**（死循环、正则回溯、自旋锁、频繁计算）。下面用一个什么都不干的 `while(true)` 模拟：

```java
/**
 * 直接 java CPUDemo 运行即可，会有一个线程把单核吃满
 */
public class CPUDemo {

    public static void main(String[] args) {
        // 起一个会被我们抓到的「坏线程」
        Thread bad = new Thread(() -> {
            long x = 0;
            while (true) {       // 死循环空转，单核 CPU 直接 100%
                x++;
            }
        }, "busy-loop-thread");  // 起个好认的名字，jstack 里一眼看到
        bad.start();

        // 主线程正常待命，方便观察
        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
            }
        }
    }
}
```

运行后用 `top` 就能看到这个 Java 进程 CPU 接近 100%。

> **真实业务里的同款根因**：死循环 / `HashMap` 多线程并发导致的成环死循环（JDK7）/ 正则灾难性回溯 / 锁竞争激烈线程疯狂自旋 / 频繁 Full GC 导致 GC 线程占满 CPU。

### 2. 排查命令（照着敲）—— 经典三连 `top -Hp → printf → jstack`

第一步，找到 CPU 高的**进程**：

```bash
top                   # 找到 %CPU 最高的 Java 进程，记下 PID，比如 12345
```

第二步，钻到**线程**级别，找到是哪个线程在烧 CPU：

```bash
# -H 显示线程，-p 指定进程；找到 %CPU 最高的那一行，记下它的线程号（PID 列），比如 12360
top -Hp 12345
```

第三步，把线程号转成 16 进制（因为 `jstack` 里线程 id 是 16 进制）：

```bash
printf '0x%x\n' 12360
# 输出： 0x304c
```

第四步，抓线程栈，用 16 进制线程号精确定位到代码行：

```bash
# nid 就是上一步的 16 进制线程号；-A 30 多打 30 行栈，看到完整调用链
jstack 12345 | grep '0x304c' -A 30

# 你会看到类似：
# "busy-loop-thread" #11 prio=5 ... nid=0x304c runnable [0x...]
#    java.lang.Thread.State: RUNNABLE
#        at CPUDemo.lambda$main$0(CPUDemo.java:14)   <-- 精确到死循环那一行
```

栈顶那一行 `CPUDemo.java:14` 就是元凶——回到代码改掉死循环 / 加退出条件 / 修正锁逻辑即可。

> **排查口诀**：`top` 找进程 → `top -Hp pid` 找线程 → `printf '0x%x'` 转 16 进制 → `jstack pid | grep 16进制nid -A 30` 落到代码行。能完整说出这套组合拳，面试官立刻知道你真排查过线上问题。

---

## 三、更快的姿势：Arthas（线上首选）

上面是「原生 JDK 工具链」，胜在任何环境都有。但线上排查更推荐阿里开源的 **Arthas**，不用重启、不用改参数、交互式：

```bash
# 一行启动，自动列出 Java 进程让你选
curl -O https://arthas.aliyun.com/arthas-boot.jar
java -jar arthas-boot.jar

# 进入后常用命令：
dashboard            # 实时面板：线程/内存/GC 一屏看全
thread -n 3          # 直接列出最忙的 3 个线程及其栈（CPU 飙高一键定位，省去 top→printf→jstack）
thread -b            # 找出当前阻塞其他线程的「罪魁」线程（排查死锁/锁竞争）
heapdump heap.hprof  # 等价 jmap dump
jad 全类名            # 反编译线上类，确认跑的代码版本对不对
watch 类 方法 '{params,returnObj}'  # 不重启就观察方法入参/返回值，定位逻辑 bug
```

> **一句话**：原生工具链（`jps/jstat/jmap/jstack/jinfo`）要会，能体现你懂底层；Arthas 要熟，能体现你真在线上救过火。两者结合是 Java 工程师排查问题的标配武器。

---

## 四、命令速查表

| 场景 | 命令 | 作用 |
| --- | --- | --- |
| 找 Java 进程 | `jps -lvm` | 列出 PID + 主类 + 启动参数 |
| 看 GC/各内存区 | `jstat -gcutil pid 1000` | 每秒打印 GC 频率、各区占用 |
| 看对象直方图 | `jmap -histo:live pid \| head` | 快速看哪类实例最多 |
| dump 堆 | `jmap -dump:live,format=b,file=heap.hprof pid` | 抓堆快照给 MAT 分析 |
| OOM 自动 dump | `-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=...` | 启动参数，崩时自动留现场 |
| 找高 CPU 线程 | `top -Hp pid` | 线程级 CPU 占用 |
| 线程号转 16 进制 | `printf '0x%x\n' tid` | 配合 jstack |
| 抓线程栈 | `jstack pid \| grep 0xXXX -A 30` | 定位到具体代码行 |
| 看/改运行时参数 | `jinfo -flag 名 pid` | 查看 JVM 参数 |
| 线上一键排查 | `arthas: dashboard / thread -n 3 / thread -b` | 不重启、交互式 |

---

[← 返回 3.3 JVM 运行时](./03-JVM运行时.md)
