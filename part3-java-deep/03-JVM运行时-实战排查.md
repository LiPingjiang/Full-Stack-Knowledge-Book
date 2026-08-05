# 3.3 附录：线上 OOM / CPU 飙高 / 锁竞争实战排查

> 配套正文 [3.3 JVM 运行时 · 考点 6](./03-JVM运行时.md#考点-6线上-oom-cpu-飙高怎么排查实战加分项最能拉开差距)。
> 正文给的是「排查思路链路图」，本文给**可复现的示例代码 + 一条条能照着敲的命令**——把理论变成肌肉记忆。

排查这类问题的本质是两步：**先复现/锁定现象，再用工具把现象映射回代码行**。下面分别用一段能跑出问题的代码，配一套完整命令演示。

---

## 〇、先把示例编译运行起来

下面两段示例都是单文件，编译运行就两步：`javac` 编译 → `java` 运行。

```bash
javac OOMDemo.java        # 编译，生成 OOMDemo.class
java OOMDemo              # 运行
```

> **文件名必须和 public 类名完全一致**（大小写也要对）：类是 `public class OOMDemo`，文件就得叫 `OOMDemo.java`，否则报「类名与文件名不匹配」。

> **JDK 11+ 可一步到位**：`java OOMDemo.java`（源码启动器，免显式编译）。JDK 8 不支持，老老实实两步。

### JDK 8 编译中文注释报错？——编码坑

如果你的代码里有**中文注释**，JDK 8 用 `javac` 编译可能报：

```
error: unmappable character for encoding ASCII
```

原因：JDK 8 的 `javac` **默认用系统平台编码读源文件**，当系统 locale 是空（`LANG=""`）或非 UTF-8 时，会把 UTF-8 的中文当 ASCII 解析而失败。两种解法：

```bash
# 解法一（临时）：编译时显式指定 UTF-8
javac -encoding UTF-8 OOMDemo.java

# 解法二（一劳永逸）：设好系统 locale，写进 ~/.zshrc
export LANG=en_US.UTF-8
export LC_ALL=en_US.UTF-8
```

> 查当前编码环境：`locale | grep -i lang`。看到 `LANG=`（空）就是它在捣乱。

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

> ⚠️ **重要认知：OOM 之后一般就用不上 `jps` 了。** 抛出 `OutOfMemoryError` 后主线程异常终止，**整个 JVM 进程随即退出**，`jps` 自然列不出它（你只会看到 `jps` 命令自己）。所以下面第二、三步的 `jstat`/`jmap` 都需要**进程还活着**——属于「实时排查」。
> 真实 OOM 往往是**事后排查**：进程已崩，你只能靠它崩溃前留下的现场——也就是 `-XX:+HeapDumpOnOutOfMemoryError` 自动生成的 `heap.hprof`（见第四步直接拿文件分析）。
> 想做实时分析，要么**趁它涨内存还没崩时**另开终端执行下面命令，要么把堆调大、拉长复现时间窗口。

第二步（进程存活时）先看 GC 和各内存区，判断「是不是真的内存问题、是哪个区」：

```bash
# 每 1000ms 打一行，看 FullGC（FGC）次数是否飞涨、老年代（O）是否一直逼近 100%
jstat -gcutil 12345 1000

# 字段速记：
#   E=Eden  O=老年代  M=Metaspace  YGC/YGCT=YoungGC次数/耗时  FGC/FGCT=FullGC次数/耗时
# 典型泄漏信号：O 一直接近 100%，FGC 频繁但回收不下去 → 内存被钉死了
```

> 📷 **真实输出**（跑 `OOMDemo` 时的 `jstat -gcutil`）：可以看到 `O`（老年代）一路飙到接近 `100`、`FGC`（FullGC 次数）不停增长但内存压不下去——这就是典型的「内存被钉死、GC 救不回来」的泄漏信号。
>
> ![jstat-gcutil 真实输出：老年代逼近100、FGC频繁](https://github.com/user-attachments/assets/e16ea828-b02f-478f-bf85-478188174d3f)

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

> 📷 **真实输出**（跑 `OOMDemo` 时的 `jmap -histo:live`）：第一行 `[B`（即 `byte[]`，`[` 是数组、`B` 是 byte）就把堆吃光了，`#bytes` 是第二名的几千倍——一眼锁定「是 `byte[]` 在泄漏」。`#instances` 数量正好对应代码里 `new byte[1024*1024]` 被调用的次数（每个 1MB）。
>
> ![jmap-histo 真实输出1：byte数组占据绝大部分内存](https://github.com/user-attachments/assets/5988a716-8a41-457d-bf8d-657785b2f750)
>
> 换个进程再抓一次，结论一致——`[B` 以约 446MB（`467697016` 字节）、454 个实例稳居第一，第二名 `[C` 才 9 万字节，相差 5000 多倍：
>
> ![jmap-histo 真实输出2：byte数组 446MB-454个实例](https://github.com/user-attachments/assets/08b869cc-4155-496f-8016-a7a07676d582)
>
> **数组类型符号速记**：`[B`=`byte[]`、`[C`=`char[]`、`[I`=`int[]`、`[Ljava.lang.Object;`=`Object[]`（`[` 表数组，后面是元素类型）。

第四步，分析 `heap.hprof`。**有图形界面**就用 MAT / JProfiler / VisualVM 打开：

- 看 **Dominator Tree**（支配树）：谁「独占」了最多内存，排第一的往往就是泄漏根。
- 看 **GC Roots 引用链**：右键泄漏对象 → `Path to GC Roots`，一直能看到 `OOMDemo.LEAK` 这个静态字段死死拽着它们。
- MAT 的 **Leak Suspects** 报告会直接帮你点名嫌疑对象。

**没有图形界面**（服务器 / sandbox / 远程终端）怎么办？JDK 8 自带过 `jhat`，可把 hprof 解析成本地网页：

```bash
jhat -port 7000 heap.hprof
# 看到 "Server is ready." 后访问：
#   http://localhost:7000/histo/   对象直方图，[B 一般排最前
# jhat 还支持 OQL（Object Query Language）检索对象
```

> ⚠️ **真实踩坑：`jhat: command not found` / `no such file or directory`。** `jhat` **在 JDK 9 就被移除了**，高版本 JDK 根本没有它。更隐蔽的是「**多 JDK 版本错位**」：
> ```bash
> java -version              # 显示 1.8.0_201（/usr/bin/java 软链指向的）
> /usr/libexec/java_home     # 却返回 .../openjdk-24.0.1（按最高版本挑）
> ```
> 这时 `$(/usr/libexec/java_home)/bin/jhat` 指向 JDK 24，自然找不到。要用 JDK 8 的 jhat 得显式指版本：
> ```bash
> $(/usr/libexec/java_home -v 1.8)/bin/jhat -port 7000 heap.hprof
> ```
> 一劳永逸：在 `~/.zshrc` 固定 `export JAVA_HOME=$(/usr/libexec/java_home -v 1.8)` 和 `export PATH="$JAVA_HOME/bin:$PATH"`，让 `java`/`jmap`/`jhat` 都来自同一个 JDK，不再错位。

> 说实话别在 jhat 上耗——它弱、且 JDK 9+ 已删。**最省事还是趁进程活着 `jmap -histo:live <pid> | head -20` 直接看直方图**，一眼锁定 `[B`。

#### 最后一公里：histo 只到「类」，怎么定位到「哪行代码」？

`jmap -histo` 有个天花板：它只告诉你「**是什么类**在泄漏」（你已得到答案：`byte[]`），但 `byte[]` 太普遍，**它不告诉你「谁在 new、从哪行代码来」**。要落到代码行，有两条路：

**路 A —— 谁在「持有」（找泄漏首选）：MAT 引用链。** dump 后用 MAT 打开，对 `byte[]` 右键 `Merge Shortest Paths to GC Roots`（排除弱/软引用），引用链会一直指到 `OOMDemo.LEAK` 这个 static 字段——这就是「谁拽着它们不放」。

**路 B —— 谁在「分配」（分配热点）：async-profiler 火焰图。** 想知道「到底哪行代码在疯狂 new byte[]」，用开源的 [async-profiler](https://github.com/async-profiler/async-profiler) 做内存分配采样，直接给出**带代码行号**的火焰图：

```bash
# 1) 下载（免费开源，Apache 2.0；macOS 为例，版本号以官方 release 为准）
curl -L -o async-profiler.zip \
  https://github.com/async-profiler/async-profiler/releases/download/v4.0/async-profiler-4.0-macos.zip
unzip async-profiler.zip && cd async-profiler-4.0-macos

# 2) macOS 第一次跑要解除 Gatekeeper 隔离，否则报"无法验证开发者"
xattr -dr com.apple.quarantine .

# 3) 对运行中的进程做 alloc 采样 30 秒，输出火焰图（macOS 上 attach 通常需 sudo）
sudo ./bin/asprof -e alloc -d 30 -f alloc.html <PID>
```

打开 `alloc.html`，火焰图**最宽**的那一栏点进去，直接看到分配热点代码行：

```
OOMDemo.main(OOMDemo.java:36)   ← new byte[1024*1024] 那一行
```

> **分工记牢**：`jmap -histo` 回答「**是什么对象**在泄漏」→ MAT 引用链回答「**谁持有**它」→ async-profiler 回答「**哪行代码在分配**」。三者配合才能从「内存爆了」一路追到「就是这行代码」。
>
> 工具选型：**找泄漏（对象回收不掉）→ 优先 MAT 引用链**；**找分配热点（内存涨得快不一定泄漏）→ 优先 async-profiler `-e alloc`**；JDK 11+ 想用自带工具可考虑 JFR + JMC。

第五步，顺引用链/火焰图回到代码 → 找到那个「只加不删」的静态集合 → 加淘汰策略 / 及时移除 / 改用弱引用缓存。

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

## 三、锁竞争 / 线程池打满——RT 变高但 CPU 不忙

前两节的 OOM 和 CPU 飙高，共同特征是「机器资源确实被吃满了」，工具链的任务是揪出「谁在吃」。但线上还有一类更隐蔽的现象：**接口 RT（响应时间）明显变长，甚至超时，但 `top` 看 CPU 占用率一点都不高，机器看起来很闲**。这种情况的根因往往不是「线程在拼命算」，而是**线程在傻等**——要么在等一把锁，要么在线程池队列里排队，要么在等下游响应。用 CPU 飙高那套「找烧CPU的线程」的思路在这里会直接扑空，因为问题线程根本不怎么消耗 CPU，需要换一套完全不同的排查工具和视角。

### 1. 复现代码：一把锁把大部分线程堵死

下面模拟一个典型场景：多个请求线程抢同一把锁去更新一个共享的本地缓存（这正是 [11.2 高并发中台架构全景](../part11-case-tag-strategy-platform/02-高并发中台架构全景.md) 里提到的本地缓存更新场景），持锁的线程里故意 `sleep` 模拟一次稍慢的操作，让其余线程排队等待：

```java
import java.util.concurrent.CountDownLatch;

/**
 * 模拟：20 个"请求线程"抢同一把锁去更新共享缓存，
 * 每次持锁操作耗时 500ms（模拟一次不该放在锁里的慢操作），
 * 观察大部分线程会长时间处于 BLOCKED 状态排队等锁。
 */
public class LockContentionDemo {

    // 共享资源，所有线程抢的就是这把锁
    private static final Object CACHE_LOCK = new Object();
    private static int cacheVersion = 0;

    public static void main(String[] args) throws InterruptedException {
        int threadCount = 20;
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            Thread t = new Thread(() -> {
                synchronized (CACHE_LOCK) {          // 抢锁
                    try {
                        // 模拟一次不该放在临界区里的慢操作（比如序列化整个大对象）
                        Thread.sleep(500);
                        cacheVersion++;
                    } catch (InterruptedException ignored) {
                    }
                }
                latch.countDown();
            }, "request-thread-" + i);               // 起个好认的名字，方便 jstack 里识别
            t.start();
        }

        latch.await();
        System.out.println("全部完成，最终版本号 = " + cacheVersion);
    }
}
```

这段代码跑起来，`top` 看 CPU 占用几乎不会有明显异常（毕竟线程大部分时间在 `sleep`/等锁，不消耗 CPU），但如果把这 20 个线程模拟成 20 个并发请求，最后一个请求线程要等前面 19 个各自持锁 500ms 才能轮到自己，相当于**整体 RT 被线性拉长到接近 10 秒**——这正是「CPU 不忙但 RT 很高」这类问题的典型样子。

> **真实业务里的同款根因**：本地缓存/共享 Map 更新时加了粒度过粗的锁（比如整个方法加锁，而不是只锁真正需要保护的那几行）；把远程调用（RPC/Redis/DB 查询）放在了 `synchronized` 块里，导致持锁时间被下游延迟拖长；用了 `Collections.synchronizedMap` 这类粗粒度同步容器在高并发下退化成串行访问。

### 2. 排查命令（照着敲）—— 用 jstack 找 BLOCKED 线程和持锁人

第一步，确认现象：`top` CPU 不高，但接口 RT 确实变长了（业务监控/APM 上能看到），初步怀疑不是计算密集型问题。

第二步，直接抓全部线程栈，重点看 `BLOCKED` 状态的线程数量：

```bash
jstack <pid> > threads.txt

# 统计各状态线程数量，一眼看出是不是大面积 BLOCKED
grep "java.lang.Thread.State" threads.txt | sort | uniq -c
# 典型输出（锁竞争场景）：
#   18 java.lang.Thread.State: BLOCKED (on object monitor)
#    1 java.lang.Thread.State: RUNNABLE
#    1 java.lang.Thread.State: TIMED_WAITING
```

如果看到大量线程都是 `BLOCKED (on object monitor)`，基本可以确认是**锁竞争**，而不是 CPU 计算问题——这一步就是「CPU 飙高」和「锁竞争」两类问题在排查上的第一个分岔点。

第三步，`jstack` 的输出里，`BLOCKED` 线程会直接告诉你**它在等哪把锁、这把锁当前被谁拿着**：

```bash
grep -A 5 "request-thread" threads.txt | head -40

# 你会看到类似：
# "request-thread-5" #16 ... 
#    java.lang.Thread.State: BLOCKED (on object monitor)
#         at LockContentionDemo.lambda$main$0(LockContentionDemo.java:18)
#         - waiting to lock <0x000000076b5a1234> (a java.lang.Object)
#         - locked <0x000000076b5a5678> (a java.util.concurrent.CountDownLatch)
#
# "request-thread-2" #13 ...
#    java.lang.Thread.State: TIMED_WAITING (sleeping)
#         at java.lang.Thread.sleep(Native Method)
#         at LockContentionDemo.lambda$main$0(LockContentionDemo.java:16)
#         - locked <0x000000076b5a1234> (a java.lang.Object)   ← 这个线程正拿着大家在等的锁
```

关键字是 `waiting to lock <地址>` 和 `locked <地址>`——**地址相同**说明大家都在等同一把锁；反过来搜这个地址找到哪个线程写的是 `locked` 而不是 `waiting to lock`，那个线程就是「正在持锁、让别人干等」的元凶，顺着它的栈往下看，就能定位到具体是哪一行代码把锁占了这么久（这里就是 `LockContentionDemo.java:16` 的 `Thread.sleep(500)`）。

> **一个更快的姿势**：如果是真正的死锁（互相等对方的锁，谁都拿不到），`jstack` 输出末尾会直接打印一段 `Found one Java-level deadlock:`，把死锁的两个线程和它们互相等待的锁直接列出来，不需要你手动比对地址——但**锁竞争（大量线程排队等同一把锁，但最终都能拿到）不是死锁**，`jstack` 不会自动帮你标出来，需要按上面的方法手动比对 `waiting to lock` 和 `locked` 的地址。

第四步，用 Arthas 更快定位（省去手动比对地址）：

```bash
thread -b      # 直接找出「当前阻塞其他线程的罪魁线程」，锁竞争/死锁排查专用，一步到位
```

### 3. 另一种「不忙但慢」：线程池排队，而不是锁竞争

如果 `jstack` 里没看到大量 `BLOCKED`，而是看到大量业务线程处于 `WAITING`/`TIMED_WAITING` 卡在线程池的任务队列上（比如栈里出现 `java.util.concurrent.ThreadPoolExecutor$Worker.run` 或者 `LinkedBlockingQueue.take`），说明瓶颈不是锁，而是**线程池被打满，请求在队列里排队**，这是高并发场景下 RT 劣化的另一大常见根因。

排查线程池状态，优先用监控面板看趋势（而不是登机器执行一次性命令看瞬时值）：

- 如果线程池指标已经上报到 Prometheus/Grafana：直接看 `活跃线程数`、`队列排队任务数`、`已完成任务数` 这几条曲线是否和 QPS 上涨、RT 劣化的时间点吻合。
- 如果是 Spring Boot 应用且开了 Actuator：`/actuator/metrics/tomcat.threads.busy`、`/actuator/metrics/tomcat.threads.current` 能直接看到 Tomcat 内置线程池的繁忙情况。
- 如果是自定义 `ThreadPoolExecutor` 且没有提前埋点监控，临时排查可以在代码里加一行日志定时打印 `getActiveCount()` / `getQueue().size()` / `getPoolSize()`，或者用 Arthas 的 `watch` 命令不重启直接观察这几个方法的返回值：

```bash
# 不重启服务，直接观察线程池对象某个方法的返回值
watch com.xxx.TagQueryService threadPoolExecutor '{target.getActiveCount(), target.getQueue().size()}'
```

如果观察到 `getActiveCount()` 长期等于线程池最大线程数（线程全忙）、`getQueue().size()` 持续增长，说明线程池确实被打满了，请求在队列里等待的时间直接计入了总 RT，但这部分等待时间完全不消耗 CPU，这也是为什么 `top` 看起来"很闲"却依然超时——**这正是本节开头强调的核心认知：RT 高不等于 CPU 忙，也可能是线程在排队或等锁**。

> **排查口诀**：先看 `top` CPU 是否真的高——高就走「CPU 飙高」那条路（`top -Hp → jstack`）；不高但 RT 依然差，就抓 `jstack` 统计 `BLOCKED` 线程数量，大量 `BLOCKED` 走「锁竞争」这条路（比对 `waiting to lock`/`locked` 地址，或直接 `thread -b`）；如果 `BLOCKED` 不多但线程都卡在队列相关的栈帧上，走「线程池打满」这条路（看线程池的活跃数/队列长度监控）。三条路径分工明确，不要一上来就无脑上火焰图——火焰图擅长发现「CPU 确实在忙着算什么」，但对「线程在傻等」这类问题基本看不出名堂。

---

## 四、更快的姿势：Arthas（线上首选）

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

## 五、命令速查表

| 场景 | 命令 | 作用 |
| --- | --- | --- |
| 编译（含中文注释） | `javac -encoding UTF-8 X.java` | JDK 8 避免 ASCII 编码报错 |
| 找 Java 进程 | `jps -lvm` | 列出 PID + 主类 + 启动参数（**OOM 崩溃后看不到**） |
| 看 GC/各内存区 | `jstat -gcutil pid 1000` | 每秒打印 GC 频率、各区占用 |
| 看对象直方图 | `jmap -histo:live pid \| head` | 快速看哪类实例最多 |
| dump 堆 | `jmap -dump:live,format=b,file=heap.hprof pid` | 抓堆快照给 MAT 分析 |
| 无界面分析 hprof | `jhat -port 7000 heap.hprof` | 仅 JDK 8（**JDK 9+ 已移除**），指版本用 `$(/usr/libexec/java_home -v 1.8)/bin/jhat` |
| 定位分配代码行 | `asprof -e alloc -d 30 -f a.html pid` | async-profiler（开源）火焰图，看哪行在 new |
| OOM 自动 dump | `-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=...` | 启动参数，崩时自动留现场 |
| 找高 CPU 线程 | `top -Hp pid` | 线程级 CPU 占用 |
| 线程号转 16 进制 | `printf '0x%x\n' tid` | 配合 jstack |
| 抓线程栈 | `jstack pid \| grep 0xXXX -A 30` | 定位到具体代码行 |
| 看/改运行时参数 | `jinfo -flag 名 pid` | 查看 JVM 参数 |
| 统计线程状态分布 | `jstack pid \| grep "java.lang.Thread.State" \| sort \| uniq -c` | 一眼看是否大面积 `BLOCKED`，锁竞争排查第一步 |
| 找持锁/等锁线程 | `jstack pid \| grep -A 5 "request-thread"` 后比对 `waiting to lock`/`locked` 地址 | 定位「谁在等」「谁在拿」这把锁 |
| 看线程池活跃/排队数 | Arthas `watch 类 字段 '{target.getActiveCount(), target.getQueue().size()}'` | 免重启看线程池是否被打满 |
| 线上一键排查 | `arthas: dashboard / thread -n 3 / thread -b` | 不重启、交互式，`thread -b` 专治锁竞争/死锁 |

---

[← 返回 3.3 JVM 运行时](./03-JVM运行时.md)
