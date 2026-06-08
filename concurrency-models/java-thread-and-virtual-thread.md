# 并发模型库 · Java 线程与虚拟线程

> 本篇深挖 Java 并发的底层机制，是 [3.1 Java 并发体系](../part3-java-deep/01-并发体系.md) 和 [4.1 高并发对比](../part4-multilang-compare/01-高并发HTTP服务对比.md) 的「钩子」目标。

---

## 一、平台线程：OS 线程的 1:1 映射

JDK 21 之前的 `Thread`（现在称为**平台线程，platform thread**），与操作系统内核线程是 **1:1 映射**——一个 Java 线程对应一个 OS 线程。

```
   Java 平台线程        OS 内核线程
   ┌──────────┐  1:1  ┌──────────┐
   │ Thread A  │ ────> │ kernel-1 │
   │ Thread B  │ ────> │ kernel-2 │
   └──────────┘       └──────────┘   ← OS 调度器负责切换
```

这个 1:1 模型决定了平台线程的全部特性：

- **栈固定**：默认每线程约 1MB 栈（`-Xss` 可调），且在创建时就分配。
- **调度在内核**：线程切换是内核态的上下文切换，要陷入内核、保存/恢复寄存器、可能刷 TLB，开销约几微秒。
- **数量受限**：受内存（每个 1MB）和内核调度能力限制，几千个就吃力，上万个基本不可行。
- **阻塞 = 浪费**：线程执行阻塞 IO 时被 OS 挂起，那 1MB 栈和内核资源就被占着干等。

---

## 二、虚拟线程：M:N 调度的革命

JDK 21 的**虚拟线程（virtual thread）** 引入了 **M:N 调度**——M 个虚拟线程被复用到 N 个平台线程（称为**载体线程，carrier thread**）上，N 通常等于 CPU 核数。

```
   M 个虚拟线程 (海量)         N 个载体线程 (少量, ≈核数)      OS 线程
   ┌─────────┐
   │ VT-1     │ ─┐
   │ VT-2     │  ├──挂载──> ┌──────────┐  1:1  ┌────────┐
   │ VT-3     │  │          │ carrier-1 │ ────> │ kernel │
   │ ...      │ ─┘          └──────────┘       └────────┘
   │ VT-100万 │ ──挂载──>   ┌──────────┐  1:1  ┌────────┐
   └─────────┘             │ carrier-2 │ ────> │ kernel │
                           └──────────┘       └────────┘
```

关键机制：

- **栈在堆上、按需增长**：虚拟线程的栈（称为 stack chunk）存在 JVM 堆里，几百字节起步，按需增长。所以能开几百万个。
- **JVM 用户态调度**：虚拟线程的调度由 JVM（一个 ForkJoinPool）在**用户态**完成，不陷入内核，切换成本极低。

---

## 三、核心魔法：挂载（mount）与卸载（unmount）

虚拟线程「阻塞自动让出」的秘密，在于 **mount/unmount 机制**：

1. 虚拟线程要运行时，**挂载（mount）** 到一个载体线程上执行。
2. 当它执行到**阻塞操作**（如阻塞 IO、`Thread.sleep`、加锁等待）时，JVM 把它的栈**保存到堆里**，并将它从载体线程**卸载（unmount）**。
3. 载体线程立刻空出来，去跑别的虚拟线程。
4. 等阻塞操作完成（IO 就绪），这个虚拟线程被重新调度，**挂载**回某个载体线程，从断点继续。

```
虚拟线程 VT 执行中...
     │
     ▼ 遇到阻塞 IO (如 socket.read)
  [unmount] ── VT 的栈存入堆，载体线程释放 ──> 载体线程去跑 VT-other
     │
     ▼ IO 完成
  [mount]   ── VT 重新挂载到某个载体线程，从断点继续 ──> 继续执行
```

**这就是「写起来像阻塞、跑起来像异步」的原理**：你写的 `socket.read()` 看似阻塞，但 JVM 在底层把「阻塞」转换成了「卸载-等待-重新挂载」，载体线程从不空转。这与 [Go goroutine](./go-goroutine-csp.md) 的调度思想高度一致。

---

## 四、坑：Pinning（钉住）

虚拟线程有一个要注意的陷阱——**pinning（钉住）**。在某些情况下，虚拟线程**无法从载体线程卸载**，即使它阻塞了也会一直占着载体线程：

主要发生在两种情况：

1. **在 `synchronized` 块/方法内阻塞**：JDK 21 中，持有 `synchronized` 监视器时阻塞会导致 pinning（这会削弱虚拟线程的优势）。
2. **调用 native 方法（JNI）时阻塞**。

```java
// JDK 21 中可能 pinning 的写法
synchronized (lock) {
    blockingIO();   // 在 synchronized 内阻塞 → 虚拟线程被钉住，载体线程被占
}

// 推荐改用 ReentrantLock，它对虚拟线程友好，阻塞时能正常卸载
lock.lock();
try {
    blockingIO();   // 不会 pinning
} finally {
    lock.unlock();
}
```

> 实践建议（JDK 21）：在虚拟线程密集的代码里，**用 `ReentrantLock` 替代 `synchronized`** 来避免 pinning。注意：JDK 24+（JEP 491）已解决了 `synchronized` 的 pinning 问题，但在过渡期了解这个坑很重要。

---

## 五、对照其他模型

| 维度 | Java 平台线程 | Java 虚拟线程 | 对照 |
|------|--------------|--------------|------|
| 映射 | 1:1 (OS 线程) | M:N (用户态调度) | 似 [Go goroutine](./go-goroutine-csp.md) |
| 栈 | 固定 1MB | 堆上按需增长 | 似 goroutine 的可增长栈 |
| 阻塞处理 | OS 挂起，浪费 | mount/unmount 自动让出 | 似 goroutine 让出 |
| 写法 | 同步阻塞 | 同步阻塞（不变！） | vs [Rust async](./rust-async-tokio.md) 的显式 await |

**最重要的对照**：虚拟线程和 [Go goroutine](./go-goroutine-csp.md) 在机制上几乎是同一套东西（用户态 M:N 调度 + 阻塞让出）。而与 [Rust/Node/Python 的 async](./rust-async-tokio.md) 相比，虚拟线程的最大优势是**不需要 `async/await` 关键字污染代码**——你的同步代码原封不动就能享受高并发，没有「函数染色（function coloring）」问题。

---

## 本篇小结

- 平台线程 = OS 线程 1:1 映射，1MB 栈、内核调度、阻塞浪费。
- 虚拟线程 = M:N 用户态调度，堆上按需栈、JVM 调度、阻塞自动让出（mount/unmount）。
- 「写起来像阻塞、跑起来像异步」的原理是 unmount/mount：阻塞时卸载栈、释放载体线程，就绪时重新挂载。
- 注意 JDK 21 的 **pinning** 坑：`synchronized` 内阻塞会钉住，用 `ReentrantLock` 规避。
- 虚拟线程与 [Go goroutine](./go-goroutine-csp.md) 同源，且无 [async](./rust-async-tokio.md) 的函数染色问题。

---

[← 返回并发模型库](./README.md) | 相关正文：[3.1 Java 并发](../part3-java-deep/01-并发体系.md) · [4.1 高并发对比](../part4-multilang-compare/01-高并发HTTP服务对比.md)
