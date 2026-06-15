# 并发模型库 · Go goroutine 与 CSP

> 本篇深挖 Go 并发的底层机制，是 [3.1](../part3-java-deep/01-并发体系.md)、[3.2 JMM](../part3-java-deep/02-内存模型JMM.md)、[4.3 Java→Go](../part4-multilang-compare/03-Java到Go.md) 的「钩子」目标。

---

## 一、goroutine：比线程轻两个数量级

goroutine 是 Go 运行时管理的轻量级执行单元。和 [Java 虚拟线程](./java-thread-and-virtual-thread.md) 同属「用户态调度的轻量协程」流派，特性几乎一致：

- **初始栈仅 ~2KB**（对比平台线程 1MB，轻了约 500 倍），按需增长/收缩。
- **由 Go 运行时在用户态调度**，不陷入内核。
- 开**几十万到上百万个**都没问题。

```go
go func() {           // go 关键字启动 goroutine，就这么轻
    doWork()
}()
```

与 Java 虚拟线程最大的不同：goroutine 是 Go **从 2009 年第一天就内置**的核心特性，整个标准库和生态都围绕它构建；而 Java 虚拟线程是 2023 年才补上的，需要时间让生态适配。

---

## 二、GMP 调度模型：Go 并发的心脏

Go 运行时用 **GMP 模型** 实现 M:N 调度。理解这三个角色是理解 Go 并发的关键：

```
   G (goroutine)        P (processor, 逻辑处理器)      M (machine, OS 线程)
   ┌────┐               ┌──────────────┐              ┌────────┐
   │ G1 │ ─放入本地队列─> │ P (含本地G队列) │ ─绑定──>      │ M (OS) │ ─> CPU核
   │ G2 │               │  [G1][G2][G3] │              └────────┘
   │ G3 │               └──────────────┘
   │... │               ┌──────────────┐              ┌────────┐
   │ Gn │               │ P2 [G4][G5]  │ ─绑定──>      │ M2(OS) │ ─> CPU核
   └────┘               └──────────────┘              └────────┘
                              ▲
                    全局队列 + work-stealing (P 空了去偷别人的 G)
```

| 角色 | 全称 | 作用 |
|------|------|------|
| **G** | goroutine | 要执行的任务（含自己的栈和上下文） |
| **M** | machine | OS 线程，真正在 CPU 上跑代码的载体 |
| **P** | processor | 逻辑处理器，持有一个本地 G 队列，是 G 和 M 之间的「调度上下文」。数量 = `GOMAXPROCS`（默认核数） |

调度逻辑：

1. 每个 **P** 维护一个**本地 goroutine 队列**。
2. **M** 必须绑定一个 **P** 才能执行 G（P 提供调度所需的资源）。
3. M 从绑定的 P 的本地队列取 G 来跑。
4. **work-stealing（工作窃取）**：某个 P 的队列空了，它会去**偷**其他 P 队列里的 G，保证负载均衡、CPU 不闲置。
5. 当一个 G 执行**阻塞系统调用**时，M 会和 P **解绑**，P 转交给另一个 M 继续跑剩下的 G——**阻塞的 M 不拖累其他 goroutine**。

> **和 Java 网络 IO 模型的呼应**：goroutine 让你用「同步阻塞」的写法（`conn.Read()` 直接写）却拿到高并发——这和 [Java 虚拟线程](./java-thread-and-virtual-thread.md) 的「同步写法 + 异步吞吐」是同一招（运行时在阻塞时把执行单元从 OS 线程卸载）。Go 网络 IO 底层走的也是 epoll/kqueue（Go runtime 内置 netpoller），本质就是 [3.6 Java 网络 IO 模型](../part3-java-deep/06-网络IO模型.md) 里讲的 **Reactor 模式**——只不过 Go 把它藏进 runtime，让你写起来像 BIO，跑起来像 NIO。

这套 GMP 设计精妙地平衡了「调度开销低」和「负载均衡」，是 Go 高并发性能的根基。

---

## 三、抢占式调度：避免「霸占」

早期 Go（1.14 前）是**协作式调度**——goroutine 要主动在函数调用等「安全点」让出。问题是：如果一个 goroutine 跑一个没有函数调用的死循环（如纯计算），它会**永远霸占** M，别的 goroutine 饿死。

Go 1.14 引入**基于信号的抢占式调度**：运行时可以通过发信号**强制中断**一个跑太久的 goroutine，让出 CPU 给别人。这让 Go 的调度更公平、更健壮，接近 OS 级别的抢占能力。

---

## 四、channel：CSP 的载体

兑现 [3.2 JMM](../part3-java-deep/02-内存模型JMM.md) 的对比钩子。Go 的并发哲学是 **CSP（Communicating Sequential Processes）**——goroutine 之间不共享内存，而是**通过 channel 传递数据来通信**。

channel 底层是一个**带锁的环形缓冲队列 + 等待 goroutine 队列**：

```go
ch := make(chan int, 3)   // 带缓冲的 channel，容量 3

ch <- 1                   // 发送：若缓冲满则发送方 goroutine 阻塞（进等待队列）
x := <-ch                 // 接收：若缓冲空则接收方 goroutine 阻塞
```

```
   发送 goroutine                channel                  接收 goroutine
   ┌──────────┐    ch<-data   ┌──────────────┐  <-ch   ┌──────────┐
   │ producer  │ ───────────> │ [缓冲区][锁]   │ ──────> │ consumer  │
   └──────────┘               │ sendq/recvq   │         └──────────┘
                              │ (等待队列)     │
                              └──────────────┘
```

关键点：**channel 操作天然是 goroutine 安全的**，且 channel 的发送/接收建立了 happens-before 关系（类似 [3.2](../part3-java-deep/02-内存模型JMM.md) 讲的 Java happens-before）。所以「用 channel 通信」从根上**回避了共享可变内存的数据竞争**——这正是 Go 对 [3.2 JMM](../part3-java-deep/02-内存模型JMM.md) 复杂性的「绕开式」回答。

> Go 也有 `sync.Mutex`、`sync/atomic` 等共享内存原语，且 Go 也有自己的内存模型（Go Memory Model）规范 happens-before。但 channel 是它推崇的「Go 风格」。

---

## 五、select：多路复用

`select` 让一个 goroutine 同时等待多个 channel 操作，谁先就绪就执行谁（类似 Linux 的 `select/epoll` 思想）：

```go
select {
case msg := <-ch1:
    handle(msg)
case ch2 <- data:
    // 发送成功
case <-time.After(time.Second):
    // 超时控制：1 秒没有任何 channel 就绪就走这里
}
```

这是 Go 并发编程的利器，常用于超时控制、多任务协调、优雅退出。

---

## 六、对照其他模型

| 维度 | Go goroutine | 对照 |
|------|-------------|------|
| 调度 | GMP M:N 用户态 + work-stealing | 似 [Java 虚拟线程](./java-thread-and-virtual-thread.md) |
| 栈 | 2KB 起按需增长 | 似虚拟线程堆栈 |
| 通信 | channel (CSP) | vs Java 共享内存+锁 ([3.2](../part3-java-deep/02-内存模型JMM.md)) |
| 写法 | 同步阻塞 | vs [Rust/Node async](./rust-async-tokio.md) 显式 await |
| 底层 IO | runtime netpoller (epoll/kqueue) | 同 [3.6 Java NIO](../part3-java-deep/06-网络IO模型.md) 的 Reactor，藏进 runtime |
| 内置时间 | 2009 一等公民 | vs Java 2023 才补 |

---

## 本篇小结

- goroutine = 2KB 起的轻量协程，与 [Java 虚拟线程](./java-thread-and-virtual-thread.md) 同属「用户态轻量协程」流派，但 Go 从第一天就内置。
- **GMP 模型**（G 任务 / M 系统线程 / P 调度上下文）+ work-stealing 是 Go 高并发的心脏；阻塞系统调用时 M 与 P 解绑，不拖累他人。
- Go 1.14+ 的**抢占式调度**避免单个 goroutine 霸占 CPU。
- **channel + CSP** 是 Go 的并发哲学：用通信代替共享内存，从根上回避 [3.2 JMM](../part3-java-deep/02-内存模型JMM.md) 式的数据竞争；`select` 实现多路复用。

---

[← 返回并发模型库](./README.md) | 相关正文：[3.1 Java 并发](../part3-java-deep/01-并发体系.md) · [4.3 Java→Go](../part4-multilang-compare/03-Java到Go.md)
