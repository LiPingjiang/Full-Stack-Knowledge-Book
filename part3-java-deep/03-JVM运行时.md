# 3.3 JVM 运行时：你的代码从 `.java` 到运行经历了什么

> 你写了一辈子 Java，但你的代码到底跑在哪、内存怎么分、垃圾怎么回收、类怎么加载？
> 这一节把 JVM 这个「你天天用却看不见的运行时」讲清楚，它是后面对比 Go/Rust「无 VM、无 GC」的基准。

---

## 一、从源码到运行：一条完整的链路

先建立全局视角。你的 `HelloWorld.java` 到底经历了什么？

```mermaid
graph LR
    A[".java 源码"] -->|javac 编译| B[".class 字节码"]
    B -->|类加载器加载| C[JVM 方法区]
    C -->|解释执行 / JIT 编译| D[机器码运行]
    D -.热点代码.-> E[JIT 编译成本地码<br/>后续直接快速执行]

    style B fill:#fff4e1
    style E fill:#d4edda
```

关键点：

1. **`javac` 把源码编译成字节码（Bytecode，`.class`）**，不是机器码。字节码是 JVM 能懂的「中间语言」。
2. **JVM 加载字节码并执行**。一开始是**解释执行（Interpretation）**（一条条翻译），跑得慢。
3. **JIT（即时编译，Just-In-Time Compilation）** 发现「热点代码（Hotspot Code）」（被反复执行的方法）后，把它**编译成本地机器码**，后续直接快速执行。这是 Java「运行越久越快」的原因。

<details>
<summary><b>展开：解释执行和 JIT 都是跑机器码，到底差在哪？</b></summary>

先说清「本地机器码」这个词：它就是**原生机器码（native machine code，简称 native code）**——真实物理 CPU 直接能认、不再经过 JVM 这层抽象的指令。这里的「本地 / native」对照的是**「字节码」这种「虚拟机的码」**：字节码是给 JVM 这台虚拟机看的中间语言，而 native code 是落到真机、脱离虚拟机后直接执行的码。所以「本地机器码」「原生机器码」「native code」说的是同一个东西，后文统一这么理解即可。（顺带：JNI 是 Java **Native** Interface、GraalVM 的 AOT 产物叫 **Native** Image，这里的 native 全是「脱离 JVM 抽象、直接面向真机」之意。）

回到正题。解释执行和 JIT 编译，最终都是在本机 CPU 上执行 native code——这一点完全一致。它们的区别**不在「跑不跑机器码」，而在「字节码 → 机器指令的转换发生在什么时机、以什么粒度、转换结果有没有被保存复用」**。

**解释执行（Interpretation）——逐条翻译，即翻即丢，不为你的方法生成新机器码**

解释器内部，对每一条字节码指令（如 `iadd`、`getfield`）都预先写好了一段对应的处理逻辑——这些逻辑早在 JVM 自身被编译时就已经是机器码了，固化在解释器里。运行时它做的是：读一条字节码 → 跳去执行那段「早已存在的」处理片段 → 读下一条 → 再跳……

所以解释执行**不会针对你的业务方法生成一段新的、连续的机器码**，它只是反复复用 JVM 内部那些「每条指令的处理片段」。优点是没有编译开销、启动快；缺点是同一行代码循环一万次，就要重复「取字节码 → 指令分发 → 跳转」一万次，纯属重复劳动，所以慢。

**JIT 编译（Just-In-Time Compilation）——整段预译，缓存复用，生成全新机器码**

JIT 针对一整个热点方法，**当场生成一段全新的、连续优化过的本地机器码**，缓存进 Code Cache。之后再调用该方法，直接跳进这段机器码全速运行，**不再经过字节码逐条分发**。

而且关键在于「运行时」三个字：JIT 编译时能拿到程序实际跑出来的运行数据（哪个分支常走、哪个对象没逃逸、哪个虚方法实际只指向一个实现），据此做静态编译器做不到的激进优化——方法内联、逃逸分析、去虚化、冷分支剪枝等。所以热点代码被 JIT 编译后，有时甚至比 C/Go 这类提前编译（AOT）还快。代价是编译本身要耗 CPU 和内存，因此只有反复执行、值得回本的「热点」才会被编译。

**一句话对比**

| 维度 | 解释执行 | JIT 编译 |
|------|---------|---------|
| 转换时机 | 边执行边翻译，每次都翻 | 热点触发时一次性编译 |
| 转换粒度 | 一条字节码一条 | 整个方法 |
| 结果是否保存 | 不保存，即翻即丢 | 缓存进 Code Cache 复用 |
| 是否生成新机器码 | 否（复用 JVM 内置片段） | 是（生成专属优化机器码） |
| 启动速度 | 快（无编译开销） | 慢（要先编译预热） |
| 长跑性能 | 慢 | 快（运行越久越快） |
| 能否用运行时数据优化 | 否 | 能（这是 JIT 的杀手锏） |

> 打个比方：**解释执行像同声传译——演讲者说一句、翻译官当场翻一句，即时进行、说完就忘，下次再听到同样的话还得重翻一遍**；**JIT 像把一整首诗提前译好存稿——以后任何人再提到这首诗，直接拿出之前译好的稿子，不必重译**。两者说的都是「native code」这门语言，差别只是一个临场逐句口译不留稿、一个整段预译并缓存复用。
>
> JVM 实际是**两者混合**：方法刚开始一律解释执行（省去冷启动编译开销），执行次数超过阈值（默认约 1 万次，受 `-XX:CompileThreshold` 影响）才升级为 JIT 编译。HotSpot 这个名字，正是来源于「找出 Hotspot 热点代码再重点编译」这一策略。

</details>

> 这就是 Java「一次编写，到处运行」的根基：字节码与平台无关，只要目标机器有 JVM 就能跑。
> **对比钩子**：[Go 和 Rust](../part4-multilang-compare/01-高并发HTTP服务对比.md) 走的是另一条路——**直接编译成目标平台的机器码**，没有 VM、没有解释、没有 JIT 预热，启动即全速。代价是「编译产物与平台绑定」。这个根本差异，后面会反复影响性能、启动速度、部署形态的对比。

---

## 二、运行时内存结构：数据都放哪

JVM 运行时把内存划分为几块区域，理解它们是排查内存问题的基础：

```mermaid
graph TB
    subgraph JVM["JVM 运行时内存"]
        direction TB
        subgraph Private["线程私有（每个线程一份）"]
            direction LR
            Stack["虚拟机栈<br/>栈帧 / 局部变量 / 引用"]
            PC["程序计数器<br/>当前执行指令地址"]
            NativeStack["本地方法栈<br/>native 方法"]
        end
        subgraph Shared["线程共享"]
            direction TB
            subgraph Heap["堆 Heap（所有对象实例都在这里）"]
                direction LR
                Young["新生代<br/>新对象"] -->|多次 GC 后晋升| Old["老年代<br/>长寿对象"]
            end
            Meta["方法区 / 元空间<br/>类信息、常量、静态变量"]
        end
    end

    style Private fill:#e1f0ff,stroke:#4a90d9
    style Shared fill:#fff4e1,stroke:#e0a800
    style Heap fill:#fff9ef,stroke:#e0a800
    style Young fill:#d4edda,stroke:#28a745
    style Old fill:#f8d7da,stroke:#dc3545
    style Meta fill:#e2d9f3,stroke:#6f42c1
```

你最该记住的两个区域，以及它们的本质区别：

| 区域 | 存什么 | 线程共享? | 生命周期 |
|------|--------|----------|---------|
| **栈（Stack）** | 局部变量、方法调用帧、对象引用 | 每个线程私有 | 随方法调用/返回自动分配回收 |
| **堆（Heap）** | 所有对象实例（`new` 出来的） | 所有线程共享 | 由 GC 管理回收 |

一个关键认知：**`new` 出来的对象在堆上，但指向它的「引用」（变量）在栈上**。

```java
void method() {
    User u = new User();   // User 对象在【堆】，引用变量 u 在【栈】
}   // 方法结束，栈上的 u 自动消失；堆上的 User 对象等 GC 回收
```

> 这个「引用在栈、对象在堆，由 GC 回收」的模型，是 Java 内存管理的核心。
> **对比钩子**：[Rust](../part4-multilang-compare/04-Java到Rust.md) 用**所有权**机制，让对象在「拥有者离开作用域时立即确定性回收」，**没有 GC**；[Go](../part4-multilang-compare/03-Java到Go.md) 有 GC 但通过逃逸分析尽量把对象分配在栈上。这些设计差异，第四章会专门对比。

---

## 三、垃圾回收（GC）：自动内存管理的代价与红利

Java 最大的「红利」之一就是 **GC 自动回收不再使用的对象**，你不用像 C/C++ 那样手动 `free`。但红利背后有代价，理解它才能用好。

**GC 怎么判断对象「该回收」？** 主流是**可达性分析（Reachability Analysis）**：从一组「根对象」（GC Roots，如栈上的引用、静态变量）出发，能引用到的对象都「存活」，引用不到的就是垃圾。

```mermaid
graph LR
    Roots(["GC Roots<br/>栈引用 / 静态变量"])
    Roots --> A["对象 A"] --> B["对象 B"]
    Roots --> C["对象 C"]
    D["对象 D"] --> E["对象 E"]

    style Roots fill:#cfe2ff,stroke:#0d6efd,stroke-width:2px
    style A fill:#d4edda,stroke:#28a745
    style B fill:#d4edda,stroke:#28a745
    style C fill:#d4edda,stroke:#28a745
    style D fill:#f8d7da,stroke:#dc3545,stroke-dasharray:4
    style E fill:#f8d7da,stroke:#dc3545,stroke-dasharray:4
```

> 绿色（A/B/C）从 GC Roots 可达，**存活**；红色虚线（D/E）没有任何根能到达，即使它们互相引用，也是**不可达 → 回收**。

**分代回收（Generational Collection）**：基于「大部分对象朝生夕死」的经验，堆分为新生代（Young Generation，频繁、快速回收）和老年代（Old Generation，少回收、对象长寿）。新对象先进新生代，熬过多次 GC 后「晋升（Promotion）」到老年代。

**GC 的代价——STW（Stop The World，停顿世界）**：GC 工作时可能要**暂停所有应用线程**。这就是为什么你的服务偶尔会有「卡顿毛刺」。现代 GC（G1、ZGC、Shenandoah）拼命缩短 STW 时间：

| 收集器 | 特点 | 适用 |
|--------|------|------|
| Parallel GC | 吞吐优先，STW 较长 | 批处理、后台计算 |
| G1 GC | 平衡吞吐与停顿（JDK 9+ 默认） | 大多数在线服务 |
| ZGC / Shenandoah | 超低停顿（亚毫秒级） | 对延迟极敏感的服务 |

> **对比钩子**：GC 是「便利」和「不可预测的停顿」之间的权衡。[Rust](../part4-multilang-compare/04-Java到Rust.md) 选择**没有 GC**——靠所有权在编译期确定回收时机，因此**没有 STW、内存占用可预测**，代价是写代码时要满足借用检查器。[Go](../part4-multilang-compare/03-Java到Go.md) 选择**低延迟并发 GC**。这条「要不要 GC、要什么样的 GC」的分岔，是系统语言设计的核心抉择之一。

---

## 四、类加载机制：类是怎么进 JVM 的

JVM 不是一上来就加载所有类，而是**用到时才加载（懒加载，Lazy Loading）**。类加载经历：加载（Loading）→ 验证（Verification）→ 准备（Preparation）→ 解析（Resolution）→ 初始化（Initialization）。你最需要理解的是**双亲委派模型（Parent Delegation Model）**：

```mermaid
graph BT
    Custom["自定义类加载器"] -->|向上委派| App
    App["应用类加载器 Application<br/>加载你的 classpath 代码"] -->|向上委派| Platform
    Platform["扩展类加载器 Platform"] -->|向上委派| Bootstrap
    Bootstrap["启动类加载器 Bootstrap<br/>加载核心类库 java.*"]

    Bootstrap -.父加载不了再向下回退.-> Platform
    Platform -.->|回退| App
    App -.->|回退| Custom

    style Bootstrap fill:#f8d7da,stroke:#dc3545,stroke-width:2px
    style Platform fill:#fff4e1,stroke:#e0a800
    style App fill:#d4edda,stroke:#28a745
    style Custom fill:#cfe2ff,stroke:#0d6efd
```

**双亲委派（Parent Delegation）**的逻辑：一个类加载器（ClassLoader）收到加载请求，先**往上委派给父加载器（Parent ClassLoader）**，父加载器能加载就用父的，加载不了才自己来。

为什么这么设计？**为了安全和唯一性**。比如你写了个 `java.lang.String`，企图替换核心库——双亲委派会把它委派给启动类加载器，启动类加载器加载了官方的 `String`，你的山寨版根本没机会被加载。这保证了核心类不被篡改，且同一个类在 JVM 中**全局唯一**。

<details>
<summary><b>展开：为什么叫「双亲」？到底是哪「两个」亲？</b></summary>

**结论先行：「双亲委派」里的「双亲」其实是个翻译误会，原文 `Parent Delegation Model` 里只有一个 `Parent`（父），并不存在「两个父母」。准确叫法应是「父级委派模型」。**

拆开看这个误会是怎么来的：

- 英文原词是 **`Parent Delegation Model`**，`Parent` 是单数，指「（每一层各自的）父加载器」。
- 翻译成中文时，有人把 `Parent` 译成了「双亲」（中文里「双亲」=父母两人）。于是很多人就开始找「是哪两个加载器」，越想越绕——**这个问题本身是个伪命题**。

那为什么 `Parent` 容易被误读成「两个」？因为类加载器确实是一条**多层链**，每一层都有它的「上一层父加载器」：

```
自定义类加载器  ──父──>  应用类加载器  ──父──>  扩展类加载器  ──父──>  启动类加载器
```

这里的「父子」是**逐层单向**的关系（每个加载器只有一个直接父加载器），不是「一个孩子有爸和妈两个父母」。所以：

- ❌ 错误理解：双亲 = 某两个固定的加载器（比如「启动 + 应用」两个）。
- ✅ 正确理解：每个加载器都有**一个**父加载器，加载时**层层向上委派给父**，直到顶层启动类加载器；这条链上每一环的「父」都是单数。

> 补充一个实现细节：这里的「父子」**不是 Java 的继承关系（`extends`）**，而是通过 `ClassLoader` 内部的 `parent` 字段组合（composition）持有的——子加载器对象里有一个字段指向它的父加载器对象。所以严格说是「持有关系」而非「继承关系」。

一句话记住：**「双亲」是 `Parent`（父）的不准确翻译，实际只有「一个父」，是一条逐层向上的单父链，不存在「哪两个」。**

</details>

> 实战中你会在这些场景碰到类加载：Tomcat 的应用隔离、热部署、SPI 机制、各种「ClassNotFoundException / NoClassDefFoundError」排查。理解双亲委派，这些问题就有了分析框架。

---

## 五、给后端大脑的速查表

| 概念 | 一句话本质 | 实战意义 |
|------|-----------|---------|
| 字节码 + JVM | 平台无关的中间码 + 解释/JIT 执行 | 跨平台、运行越久越快 |
| 栈 | 局部变量、引用、调用帧，线程私有 | 方法结束自动回收 |
| 堆 | 所有对象实例，线程共享 | GC 管理，可能 OOM |
| GC | 自动回收不可达对象 | 省心，但有 STW 停顿 |
| 分代 | 新生代频繁快收，老年代少收 | 调优的基础概念 |
| 双亲委派（Parent Delegation） | 加载先往上委派父加载器 | 保证核心类安全与唯一 |

---

## 六、面试深度剖析：大厂高频考点

> JVM 是大厂面试的「硬通货」，尤其考察**线上问题排查能力**——这正是区分「背过八股」和「真扛过线上事故」的地方。下面按高频考点的追问链展开。

### 考点 1：内存区域哪些线程私有、哪些共享，哪些会 OOM/SOF（必考）

**面试官**：「JVM 内存分哪几块？哪些线程私有？哪些会发生什么错误？」

| 区域 | 线程私有/共享 | 可能的错误 |
|------|-------------|-----------|
| 程序计数器 | 私有 | **唯一不会 OOM** 的区域 |
| 虚拟机栈 | 私有 | 栈深度超限 → `StackOverflowError`；扩展失败 → OOM |
| 本地方法栈 | 私有 | 同上（native 方法） |
| 堆 | 共享 | `OutOfMemoryError: Java heap space`（最常见） |
| 方法区/元空间（Method Area / Metaspace） | 共享 | `OutOfMemoryError: Metaspace`（动态生成类过多） |

> **陷阱**：程序计数器是唯一不会 OOM 的区域，常被拿来考。元空间（JDK 8 用本地内存替代了永久代 PermGen）OOM 常见于大量动态生成类（CGLIB、反射、热部署）。

### 考点 2：对象创建的完整过程 + 内存分配

**面试官**：「`new 一个对象` 在 JVM 里经历了什么？」

```mermaid
graph TB
    S1["1.类加载检查<br/>类没加载先触发加载"] --> S2["2.分配内存<br/>指针碰撞 / 空闲列表（取决于堆是否规整）"]
    S2 --> S2a["并发安全：CAS 重试 或 TLAB<br/>线程本地分配缓冲，每线程一小块，无竞争"]
    S2a --> S3["3.初始化零值<br/>字段默认值 0 / null"]
    S3 --> S4["4.设置对象头<br/>Mark Word（hashCode / GC 分代年龄 / 锁状态）+ 类型指针"]
    S4 --> S5["5.执行 init 构造方法<br/>程序员定义的初始化"]

    style S1 fill:#cfe2ff,stroke:#0d6efd
    style S2 fill:#fff4e1,stroke:#e0a800
    style S2a fill:#fff9ef,stroke:#e0a800,stroke-dasharray:4
    style S5 fill:#d4edda,stroke:#28a745
```

> **追问：对象一定分配在堆上吗？** 不一定！**逃逸分析（Escape Analysis）**优化下，如果对象不会逃出方法作用域，JVM 可能做**栈上分配（Stack Allocation）**（随栈帧回收，无 GC 压力）或**标量替换（Scalar Replacement）**（把对象拆成基本类型直接放栈）。这是高级加分点，也呼应了第四章 [Go 的逃逸分析](../part4-multilang-compare/03-Java到Go.md)。

### 考点 3：GC 怎么判断对象存活 + 引用类型

**面试官**：「怎么判断对象该不该回收？为什么不用引用计数？」

- **不用引用计数（Reference Counting）**的原因：**无法解决循环引用（Circular Reference）**（A 引用 B，B 引用 A，互相引用计数都不为 0，但实际已无人使用，永不回收）。
- 用 **可达性分析（Reachability Analysis）**（GC Roots 出发，不可达即回收）。**GC Roots 包括**：虚拟机栈中引用的对象、静态变量引用的对象、常量引用、本地方法栈 JNI 引用、活跃线程。

**追问：四种引用强度？**

| 引用 | 回收时机 | 典型用途 |
|------|---------|---------|
| 强引用 | 永不回收（除非不可达） | 普通 `new` |
| 软引用 SoftReference | **内存不足时**回收 | 缓存（内存敏感） |
| 弱引用 WeakReference | **下次 GC 必回收** | `ThreadLocalMap` 的 key（[见 3.1](./01-并发体系.md)） |
| 虚引用 PhantomReference | 随时被回收，仅用于回收通知 | 堆外内存管理（如 DirectByteBuffer） |

### 考点 4：垃圾回收算法与三色标记（深挖）

**面试官**：「常见 GC 算法？CMS/G1 用的三色标记是怎么回事？漏标怎么解决？」

- 基础算法：**标记-清除（Mark-Sweep）**（产生碎片）、**标记-复制（Mark-Copy）**（新生代用，空间换效率）、**标记-整理（Mark-Compact）**（老年代用，无碎片但慢）。
- **三色标记（Tri-color Marking）**（并发标记的核心）：把对象标记为白（White，未访问）、灰（Gray，自己被访问、引用未扫完）、黑（Black，自己和引用都扫完）。
- **并发标记的问题**：标记过程中应用线程还在改引用，可能**漏标**（把存活对象当垃圾误回收）。解决方案：
  - **CMS 用增量更新（Incremental Update）**：记录新增的「黑→白」引用，重新标记时再扫。
  - **G1 用原始快照（SATB, Snapshot-At-The-Beginning）**：记录被删除的引用，保证标记开始时存活的对象都不被回收。

> 这道题能问到三色标记和 SATB/增量更新，基本是 P6/P7 级别的深度了。

### 考点 5：常见 GC 收集器与选型 + 调优参数

**面试官**：「线上服务你会怎么选 GC？常用调优参数有哪些？」

- **G1**（JDK 9+ 默认）：分 Region 管理，可预测停顿（`-XX:MaxGCPauseMillis`），适合大堆、在线服务。
- **ZGC / Shenandoah**：亚毫秒级停顿，适合对延迟极敏感、超大堆（几十上百 GB）。
- 关键参数（要能说出几个）：

```
-Xms4g -Xmx4g          # 初始/最大堆，生产建议设成相等避免动态扩容抖动
-Xmn2g                 # 新生代大小
-XX:MetaspaceSize=256m # 元空间
-XX:+UseG1GC           # 指定 G1
-XX:MaxGCPauseMillis=200          # G1 目标停顿
-XX:+HeapDumpOnOutOfMemoryError   # OOM 时自动 dump 堆，排查必备
-XX:HeapDumpPath=/path/dump.hprof
```

### 考点 6：线上 OOM / CPU 飙高怎么排查（实战加分项，最能拉开差距）

**面试官**：「线上一个服务内存涨爆了/CPU 100%，你怎么定位？」这是最体现实战的题，给出标准排查链：

**内存泄漏 / OOM 排查**：

```mermaid
graph TB
    M1["1. jstat -gcutil pid 1000<br/>看 GC 频率和各区占用，确认是否 FullGC 频繁"]
    M2["2. jmap -dump:live,format=b,file=heap.hprof pid<br/>dump 堆（或靠 -XX:+HeapDumpOnOutOfMemoryError 自动 dump）"]
    M3["3. 用 MAT / JProfiler 分析 hprof<br/>找 Dominator Tree 里最大的对象"]
    M4["4. 定位泄漏对象 → 找引用链 → 锁定代码"]
    M5(["常见根因：静态集合只加不删 / ThreadLocal 没 remove /<br/>连接未关闭 / 缓存无淘汰"])
    M1 --> M2 --> M3 --> M4 --> M5

    style M1 fill:#cfe2ff,stroke:#0d6efd
    style M4 fill:#fff4e1,stroke:#e0a800
    style M5 fill:#f8d7da,stroke:#dc3545
```

**CPU 飙高排查**：

```mermaid
graph TB
    C1["1. top -Hp pid<br/>找到占 CPU 最高的【线程】tid"]
    C2["2. printf 0x%x tid<br/>线程 id 转 16 进制"]
    C3["3. jstack pid 管道 grep 16进制tid -A 30<br/>在线程栈里定位到具体代码行"]
    C4(["常见根因：死循环 / 正则回溯 / 频繁 GC / 锁竞争自旋"])
    C1 --> C2 --> C3 --> C4

    style C1 fill:#cfe2ff,stroke:#0d6efd
    style C3 fill:#fff4e1,stroke:#e0a800
    style C4 fill:#f8d7da,stroke:#dc3545
```

> 能完整说出 `top -Hp → printf → jstack` 这套组合拳，面试官立刻知道你是真排查过线上问题的。这套工具链（jps/jstat/jmap/jstack/jinfo + Arthas）是 Java 工程师的必备武器。

### 考点 7：类加载与「破坏双亲委派」

**面试官**：「双亲委派讲了，那有没有打破它的场景？」

双亲委派不是铁律，有意被打破的经典场景：

- **JDBC（SPI 机制）**：`DriverManager`（核心类，由启动类加载器加载）要加载厂商的 `Driver` 实现（在 classpath，本该由应用类加载器加载）。靠**线程上下文类加载器（ContextClassLoader）** 反向委派给子加载器，打破了双亲委派。
- **Tomcat**：每个 webapp 有独立的类加载器，且**优先加载自己的类**（而非先委派父级），以实现应用间隔离 + 同一容器跑多个版本的库。
- **OSGi / 热部署**：用自定义类加载器实现模块化和热替换。

**追问：类初始化的时机？** 6 种主动引用会触发初始化：`new`、读写静态字段（非常量）、调用静态方法、反射、初始化子类时父类先初始化、作为程序入口的主类。被动引用（如引用静态常量 `final`、用数组定义类）不触发。

---

## 本章小结

- Java 代码经 `javac` 编成**字节码**，由 **JVM** 解释执行 + **JIT** 把热点编成机器码（运行越久越快）。
- 运行时内存核心是**栈**（局部变量/引用，线程私有，自动回收）和**堆**（对象实例，线程共享，GC 回收）。
- **GC** 用可达性分析 + 分代回收，自动管理内存，代价是 **STW 停顿**；G1/ZGC 致力于压低停顿。
- **类加载**用双亲委派模型保证核心类的安全与全局唯一。
- 这一整套「VM + GC + 类加载」是 Java 的运行时基石，也是第四章对比 [Go](../part4-multilang-compare/03-Java到Go.md)（编译机器码 + 并发 GC）和 [Rust](../part4-multilang-compare/04-Java到Rust.md)（编译机器码 + 无 GC + 所有权）的基准。

---

[← 上一节：3.2 内存模型 JMM](./02-内存模型JMM.md) | [下一节：3.4 类型系统 →](./04-类型系统.md)
