# 3.2 Java 内存模型（JMM）：为什么「看起来对」的并发代码会出错

> 这一节解决一个让无数后端工程师栽过跟头的问题：
> **为什么多线程下，明明逻辑没错的代码，却会得到诡异、不可复现的结果？**
> 答案藏在 Java 内存模型（JMM）里，它是理解一切并发安全的根基。

---

## 一、一个会出错的「正确」程序

先看这段代码。直觉上它绝对没问题——主线程把 `flag` 设为 `true`，子线程看到后就退出循环：

```java
public class Visibility {
    static boolean flag = false;    // 注意：没有 volatile

    public static void main(String[] args) throws InterruptedException {
        Thread t = new Thread(() -> {
            while (!flag) {
                // 空循环，等待 flag 变 true
            }
            System.out.println("看到 flag 变 true 了！");
        });
        t.start();

        Thread.sleep(1000);
        flag = true;    // 主线程修改 flag
        System.out.println("主线程已把 flag 设为 true");
    }
}
```

**实际运行结果**：很可能子线程**永远不退出**，那行「看到了」永远不打印。主线程改了 `flag`，子线程却「看不见」。

这违背了你的所有直觉。为什么？因为你脑子里的内存模型是错的——你以为「内存只有一份，谁改了大家立刻都看到」。**真实的硬件不是这样工作的。**

---

## 二、真相：硬件有多层缓存，没有「单一内存」

现代 CPU 为了性能，每个核心都有自己的**多级缓存（L1/L2/L3）**。线程读写变量时，往往操作的是**自己核心缓存里的副本**，而不是主内存。

```
   线程A(CPU核1)          线程B(CPU核2)
   ┌──────────┐          ┌──────────┐
   │ flag=true │          │ flag=false│  ← B 的缓存里还是旧值！
   │ (本地缓存) │          │ (本地缓存) │
   └─────┬────┘          └─────┬────┘
         │                     │
         └──────┬──────────────┘
                ▼
          ┌──────────┐
          │ flag=??? │   ← 主内存，何时同步不确定
          └──────────┘
```

于是问题来了：

- 线程 A 改了 `flag`，可能只改在自己的缓存里，**没及时刷回主内存**。
- 线程 B 一直读自己缓存里的旧值，**没去主内存重新加载**。

这叫**可见性问题（visibility）**：一个线程的修改，对另一个线程不可见。

更糟的是，编译器和 CPU 为了优化，还会**重排指令顺序（reordering）**，只要保证「单线程内看起来结果一致」即可。但多线程下，这种重排会让其他线程观察到「不可能的执行顺序」——这叫**有序性问题（ordering）**。

JMM（Java Memory Model）就是 Java 用来**规范「在什么条件下，一个线程的写对另一个线程可见、以什么顺序可见」**的一套规则。

---

## 三、三大问题：可见性、有序性、原子性

并发 bug 的根源可归为三类，JMM 围绕它们建立规则：

| 问题 | 含义 | 上面的例子 |
|------|------|-----------|
| **可见性** | 一个线程的修改，别的线程看不到 | `flag` 改了 B 看不见 |
| **有序性** | 指令被重排，观察到诡异顺序 | 初始化未完成却被别人用了 |
| **原子性** | 一个操作执行到一半被打断 | `count++` 实际是「读-改-写」三步 |

> 注意：`count++` 看着是一行，实际是「读取 count → 加 1 → 写回」三步，中间可能被其他线程插入，导致丢失更新。这是原子性问题，解法是 [3.1](./01-并发体系.md) 提到的 `AtomicInteger` 或加锁。

---

## 四、`volatile`：解决可见性与有序性

回到开头那个死循环的例子。修复方法极简单——给 `flag` 加 `volatile`：

```java
static volatile boolean flag = false;   // 加上 volatile，问题消失
```

`volatile` 做了两件事：

1. **保证可见性**：对 volatile 变量的写，**立即刷回主内存**；读，**直接从主内存加载**。线程间的修改互相可见。
2. **禁止重排序**：在 volatile 读写前后插入**内存屏障**，禁止编译器/CPU 把指令重排到屏障另一侧。

但要清醒认识 `volatile` 的边界——**它不保证原子性**：

```java
static volatile int count = 0;
count++;    // 错！volatile 保证不了这个，因为 ++ 是三步操作
            // 多线程下仍会丢失更新，要用 AtomicInteger 或锁
```

记住口诀：**`volatile` 管「看得见、不乱序」，不管「不被打断」。** 需要原子性，用 `Atomic*` 类或加锁。

---

## 五、happens-before：JMM 的核心法则

`volatile` 只是表象，JMM 的灵魂是 **happens-before 原则**。它定义了「如果操作 A happens-before 操作 B，那么 A 的结果对 B 可见，且 A 排在 B 前面」。

你需要记住的几条关键 happens-before 规则：

| 规则 | 含义 |
|------|------|
| **程序顺序规则** | 单线程内，前面的操作 happens-before 后面的 |
| **volatile 规则** | 对 volatile 的写 happens-before 后续对它的读 |
| **锁规则** | 解锁 happens-before 后续对同一把锁的加锁 |
| **线程启动规则** | `t.start()` happens-before 线程 t 内的任何操作 |
| **线程终止规则** | 线程内所有操作 happens-before 其他线程检测到它终止（`t.join()` 返回） |
| **传递性** | A hb B，B hb C，则 A hb C |

这套规则的价值在于：**它让你能推理「我的修改到底对哪个线程、在什么时候可见」**，而不必关心底层缓存和重排的细节。比如「锁规则」就解释了为什么 `synchronized` 块里改的数据，下一个拿到锁的线程一定能看到最新值——因为「解锁 hb 加锁」。

```java
synchronized (lock) {
    sharedData = 42;    // 这次修改
}   // 解锁

// 另一个线程
synchronized (lock) {   // 加锁，根据"锁规则"，一定能看到 sharedData = 42
    use(sharedData);
}
```

---

## 六、给后端大脑的实践准则

理论讲完，落到实战，记住这几条：

1. **多线程共享的「标志位」一律加 `volatile`**（如开关、状态标记）。
2. **计数器、累加器用 `Atomic*` 类**，别用 `volatile int` 然后 `++`。
3. **复杂的复合操作用锁（`synchronized` 或 `ReentrantLock`）**，靠「锁规则」同时拿到原子性 + 可见性 + 有序性。
4. **优先用 JUC 现成组件**（`ConcurrentHashMap` 等），它们内部已正确处理了 JMM 问题。
5. **不确定时，宁可加锁**——正确性永远比那一点性能重要。

---

## 七、埋给第四章的「钩子」

JMM 是 Java 应对「多核共享内存」复杂性的方案。它的复杂，恰恰来自「**多个线程共享可变内存**」这个根本模型。

> 这是巨大的对比伏笔：[Go](../concurrency-models/go-goroutine-csp.md) 的哲学是「**不要用共享内存来通信，要用通信来共享内存**」——尽量不共享可变状态，而用 channel 传递数据，从根上回避了 JMM 这类问题。而 [Rust](../concurrency-models/rust-async-tokio.md) 更激进，用**所有权和借用检查器在编译期**就禁止了数据竞争，让「JMM 式 bug」根本无法编译通过。
>
> 当你第四章看到这些时，会更深刻地理解：**JMM 的复杂，是「共享可变内存」这条路必然的代价；而其他语言选择了不同的路来回避这份复杂。**

---

## 八、面试深度剖析：大厂高频考点

> JMM 是面试官用来区分「会用并发」和「懂并发」的试金石。下面是高频考点的层层追问。

### 考点 1：双重检查锁（DCL）为什么必须加 volatile（经典必考）

**面试官**：「写个线程安全的单例，用双重检查锁。然后告诉我那个 `volatile` 能不能去掉。」

```java
public class Singleton {
    private static volatile Singleton instance;   // volatile 不能去掉！

    public static Singleton getInstance() {
        if (instance == null) {                   // 第一次检查（无锁，性能）
            synchronized (Singleton.class) {
                if (instance == null) {            // 第二次检查（持锁，正确性）
                    instance = new Singleton();    // 关键问题在这一行
                }
            }
        }
        return instance;
    }
}
```

**核心答案**：`new Singleton()` **不是原子操作**，它实际分三步：

```
1. 分配内存空间
2. 初始化对象（执行构造函数）
3. 把 instance 指向这块内存
```

JVM 可能**指令重排**成 `1 → 3 → 2`。此时：线程 A 执行到 3（instance 已非 null，但对象还没初始化完），线程 B 在第一次检查时看到 `instance != null`，直接返回了一个**「半初始化」的残缺对象**，使用时出错。

**`volatile` 的作用**：禁止 2、3 重排序，保证「对象完全初始化后」才赋值给 `instance`。这就是为什么 DCL 必须配 volatile。

> 这道题把本节的「有序性」「重排序」「volatile 禁止重排」三个知识点串成了一个完整闭环，是检验你是否真懂 JMM 的标准题。

### 考点 2：volatile 底层靠什么实现（内存屏障）

**面试官**：「volatile 禁止重排序、保证可见性，底层是怎么做到的？」

底层是 **内存屏障（Memory Barrier / Fence）**。编译器在 volatile 读写前后插入屏障指令：

- **volatile 写**之前插 `StoreStore` 屏障，之后插 `StoreLoad` 屏障 → 保证写之前的操作不会重排到写之后，且写完立即刷主内存。
- **volatile 读**之后插 `LoadLoad` 和 `LoadStore` 屏障 → 保证读之后的操作不会重排到读之前，且读时从主内存加载。

在 x86 上，volatile 写最终会编译出一条带 **`lock` 前缀**的指令，它会锁总线/缓存行并触发缓存一致性（让其他核心的缓存副本失效）。

**追问：缓存一致性协议是什么？** → **MESI 协议**（Modified / Exclusive / Shared / Invalid）。一个核心修改了缓存行，会通过总线通知其他核心把对应缓存行标记为 Invalid，它们下次读就得从主内存重新加载。这从硬件层面解释了 volatile 的可见性是怎么落地的。

### 考点 3：as-if-serial 与 happens-before 的关系

**面试官**：「既然有重排序，怎么保证单线程程序结果是对的？」

- **as-if-serial 语义**：不管怎么重排，**单线程**的执行结果不能被改变。编译器/CPU 只会重排「没有数据依赖」的指令（`a=1; b=2` 可以重排，`a=1; b=a+1` 不能）。所以单线程你永远感觉不到重排。
- **happens-before** 则是面向**多线程**的可见性保证。两者的关系：as-if-serial 保证单线程「看起来没乱序」，happens-before 保证多线程间「指定的操作可见且有序」。重排序的「坑」只在多线程、且没有 happens-before 约束时才暴露（如本节开头的 flag 例子）。

### 考点 4：volatile vs synchronized vs atomic 怎么选

**面试官**：「这三个都能解决并发问题，什么场景用哪个？」

| 场景 | 选择 | 原因 |
|------|------|------|
| 状态标志位（一写多读，如 `shutdown` 开关） | `volatile` | 只需可见性+有序性，无原子性需求，最轻量 |
| 计数器、累加（读-改-写） | `AtomicLong`/`LongAdder` | volatile 保证不了 `++` 的原子性；高并发计数用 `LongAdder` 更快 |
| 复合操作、临界区（多步骤要整体原子） | `synchronized`/`ReentrantLock` | 同时拿到原子性+可见性+有序性 |

> **陷阱题**：「`volatile int count; count++` 线程安全吗？」答案：**不安全**。`count++` 是「读-改-写」三步复合操作，volatile 只保证每一步的可见性，保证不了三步整体的原子性。这是本节口诀「看得见、不乱序，不管不被打断」的直接应用。

### 考点 5：手撕一个重排序导致出错的例子

**面试官**：「能举一个重排序真的导致 bug 的例子吗？」（除了 flag 死循环）

经典的「写-写、读-读」乱序：

```java
int a = 0;
boolean ready = false;     // 都没加 volatile

// 线程1
a = 1;          // (1)
ready = true;   // (2)  —— (1)(2) 无依赖，可能被重排成 (2)(1)

// 线程2
if (ready) {              // (3)
    System.out.println(a); // (4) 可能打印 0！
}
```

如果线程 1 的 (1)(2) 被重排，线程 2 可能看到 `ready=true` 但 `a` 还是 0。修复：给 `ready` 加 `volatile`，根据 happens-before 的 volatile 规则，(1) 一定 happens-before (4)，打印必为 1。

---

## 本章小结

- 并发 bug 的根源是硬件「多级缓存 + 指令重排」，导致**可见性、有序性、原子性**三大问题。
- **JMM** 是 Java 规范「线程间修改何时可见、以何顺序可见」的规则体系。
- **`volatile`** 保证可见性和禁止重排，但**不保证原子性**（口诀：看得见、不乱序，不管不被打断）。
- **happens-before** 是 JMM 的核心法则，让你能推理可见性（volatile 规则、锁规则等）。
- 实践上：标志位用 volatile、计数用 Atomic、复合操作用锁、优先用 JUC 组件。
- JMM 的复杂源于「共享可变内存」模型——这正是 Go（通信代替共享）和 Rust（编译期禁止数据竞争）要规避的，第四章见分晓。

---

[← 上一节：3.1 并发体系](./01-并发体系.md) | [下一节：3.3 JVM 运行时 →](./03-JVM运行时.md)
