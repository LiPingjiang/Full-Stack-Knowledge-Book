# 并发模型库 · Rust async 与 Tokio

> 本篇深挖 Rust 异步并发的底层机制，是 [3.1](../part3-java-deep/01-并发体系.md)、[3.2 JMM](../part3-java-deep/02-内存模型JMM.md)、[4.4 Java→Rust](../part4-multilang-compare/04-Java到Rust.md) 的「钩子」目标。

---

## 一、Rust 的并发是「无栈协程」

和 [Java 虚拟线程](./java-thread-and-virtual-thread.md)、[Go goroutine](./go-goroutine-csp.md) 的「有栈协程」（每个协程有自己的栈，能在任意点挂起）不同，Rust 的 async 是**无栈协程（stackless coroutine）**：

- **无栈**：async 函数不分配独立的栈，而是被编译器转换成一个**状态机**，所有局部状态保存在这个状态机结构体里。
- **只能在 `.await` 点挂起**：不像有栈协程能在任意函数调用深处挂起，无栈协程只能在显式的 `.await` 点让出。

这是 Rust「零成本抽象」哲学（[4.4](../part4-multilang-compare/04-Java到Rust.md)）的体现——无栈协程内存开销极小（就一个状态机结构体），没有为每个协程预留栈空间。

---

## 二、Future：一个可轮询的状态机

Rust 异步的核心抽象是 **`Future`** trait——代表「一个未来会完成的计算」：

```rust
trait Future {
    type Output;
    // 被反复"轮询"，要么返回 Ready(结果)，要么返回 Pending(还没好)
    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output>;
}

enum Poll<T> {
    Ready(T),    // 完成了，带结果
    Pending,     // 还没好，稍后再 poll 我
}
```

关键认知：**Rust 的 Future 是「惰性」的——创建出来不会自动执行，必须有运行时反复 `poll` 它，它才推进。** 这和 Java 的 `CompletableFuture`（创建即开始执行）相反。

---

## 三、async/await 如何编译成状态机

这是理解 Rust 异步的钥匙。当你写：

```rust
async fn fetch_user(id: u32) -> User {
    let conn = connect().await;        // 让出点 1
    let data = conn.query(id).await;   // 让出点 2
    parse(data)
}
```

编译器把它**转换成一个状态机枚举**，每个 `.await` 是一个状态：

```rust
// 编译器生成的伪代码（概念示意）
enum FetchUserStateMachine {
    Start { id: u32 },                  // 初始状态
    WaitingConnect { id: u32, fut: ConnectFuture },   // 卡在 await 1
    WaitingQuery { conn: Conn, fut: QueryFuture },    // 卡在 await 2
    Done,
}
// poll() 被调用时，根据当前状态推进到下一个状态，到 await 点就返回 Pending
```

```
poll #1 ──> Start ──> 推进到 connect().await ──> Pending (连接还没好)
poll #2 ──> WaitingConnect ──> 连接好了 ──> 推进到 query().await ──> Pending
poll #3 ──> WaitingQuery ──> 数据回来了 ──> parse ──> Ready(user)
```

**这就是「无栈」的含义**：函数的执行进度和局部变量，全都编码进这个状态机结构体里，不需要保留一个独立的调用栈。每次 `poll` 从上次的状态继续。

---

## 四、Tokio 运行时：谁来 poll 这些 Future

Future 是惰性的，需要**运行时（runtime）** 来驱动。Rust 标准库**不自带异步运行时**（这又是「零成本、不强加」的哲学），最主流的运行时是 **Tokio**。

Tokio 做的事：

1. **执行器（Executor）**：管理一堆 task（被 spawn 的顶层 Future），反复 poll 它们推进。
2. **Reactor（基于 epoll/kqueue/IOCP）**：监听 IO 事件。当一个 Future 因为等 IO 返回 `Pending` 时，Tokio 把它「挂起」，去 poll 别的 task。
3. **Waker**：IO 就绪时，操作系统通知 Reactor，Reactor 通过 **Waker** 唤醒对应的 task，把它重新放入待 poll 队列。

```
┌─────────────────────────────────────────────┐
│             Tokio 运行时                       │
│                                               │
│  Executor (少量工作线程, ≈核数)                  │
│   ┌──poll──> Task A ──Pending──> 挂起          │
│   ├──poll──> Task B ──Ready────> 完成          │
│   └──poll──> Task C ...                        │
│                  ▲                            │
│                  │ Waker 唤醒                  │
│  Reactor (epoll) ─┘                           │
│   监听 IO 就绪事件 ──> 调用对应 Task 的 Waker     │
└─────────────────────────────────────────────┘
```

> 注意这个「task 排队 + Waker 唤醒」的结构，和 [3.1 Java AQS](../part3-java-deep/01-并发体系.md) 的「等待队列 + 唤醒」在思想上异曲同工——都是「资源没就绪就挂起排队，就绪了再唤醒」。并发的底层智慧是相通的。

---

## 五、为何 Rust async 性能极致

- **零成本抽象**：状态机在编译期生成，运行时没有额外的调度对象开销，没有 GC（[4.4 所有权](../part4-multilang-compare/04-Java到Rust.md)）。
- **无栈省内存**：每个 task 只占一个状态机结构体大小，可以开海量 task。
- **基于 epoll 的高效 IO 多路复用**：和 [Node 的 libuv](./nodejs-eventloop.md) 底层一个思路。
- **借用检查器保证无数据竞争**（[3.2 JMM](../part3-java-deep/02-内存模型JMM.md) 的钩子）：编译期就杜绝了并发 bug，是「无畏并发」。

代价：心智负担最高。你要理解 `Future`、`Pin`、`Send`/`Sync`、生命周期，还有「**函数染色（function coloring）**」问题——async 函数只能被 async 函数调用，污染会传染整条调用链。这正是 [Java 虚拟线程](./java-thread-and-virtual-thread.md) 相对 async 的优势（虚拟线程无染色）。

---

## 六、对照其他模型

| 维度 | Rust async | 对照 |
|------|-----------|------|
| 协程类型 | 无栈（状态机） | vs [Java虚拟线程](./java-thread-and-virtual-thread.md)/[Go](./go-goroutine-csp.md) 有栈 |
| 让出点 | 显式 `.await` | vs 虚拟线程/goroutine 自动让出 |
| 运行时 | 需第三方(Tokio) | vs Go/Java 内置 |
| 底层 IO | epoll/kqueue | 同 [Node libuv](./nodejs-eventloop.md) |
| 性能/内存 | 极致 | 最强 |
| 心智负担 | 最高(染色/Pin/生命周期) | 最难 |

---

## 本篇小结

- Rust async 是**无栈协程**：async 函数被编译成**状态机**，局部状态编码进结构体，只能在 `.await` 点让出。
- **Future** 是惰性的可轮询状态机（`poll` 返回 `Ready`/`Pending`），与 Java `CompletableFuture` 的「即时执行」相反。
- **Tokio** 提供执行器 + Reactor(epoll) + Waker，驱动 Future 推进；其「排队 + 唤醒」思想与 [Java AQS](../part3-java-deep/01-并发体系.md) 相通。
- 性能极致（零成本、无栈、无 GC、编译期无数据竞争），代价是最高心智负担和**函数染色**问题（[Java 虚拟线程](./java-thread-and-virtual-thread.md) 无此问题）。

---

[← 返回并发模型库](./README.md) | 相关正文：[3.1 Java 并发](../part3-java-deep/01-并发体系.md) · [4.4 Java→Rust](../part4-multilang-compare/04-Java到Rust.md)
