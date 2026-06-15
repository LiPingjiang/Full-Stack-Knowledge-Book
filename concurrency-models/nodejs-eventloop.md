# 并发模型库 · Node.js 事件循环与 libuv

> 本篇深挖 Node.js 单线程并发的底层机制，是 [2.1 三件套](../part2-frontend-core/01-三件套速成.md)、[4.2 Java→JS/TS](../part4-multilang-compare/02-Java到JS-TS.md) 的「钩子」目标。

---

## 一、一句话颠覆 Java 工程师的直觉

**Node.js 用「单线程 + 事件循环」处理高并发 IO。**

对一个习惯了「一请求一线程、线程池扛并发」（[3.1](../part3-java-deep/01-并发体系.md)）的 Java 工程师，这句话很反直觉：单线程怎么扛得住上万并发连接？

答案：**Node 的单线程只负责执行 JS 代码，真正的 IO（网络、文件）交给底层的 libuv 线程池和操作系统异步机制去做。** JS 主线程从不阻塞在 IO 上等待，它只是「下发任务 + 处理回调」。

类比 Java：就像你不在业务线程里 `socket.read()` 死等，而是全用 NIO 的 `Selector` + 回调。Node 把这套「Reactor 模式」做成了语言的默认形态。

<details>
<summary><b>展开：那「对应的 Java 形态」到底是什么？</b></summary>

一句话：**Node 的「单线程 EventLoop + Reactor」，对应的就是 Java NIO 的 `Selector` + 事件循环——只不过 Java 把它当成「需要你手动搭的高级工具」，而 Node 把它当成「语言唯一形态」。**

**① 底层是同一套东西。** Node 的 EventLoop 底层是 libuv，封装了 OS 的 `epoll`(Linux)/`kqueue`(macOS)/IOCP(Windows)；Java NIO 的 `Selector` 封装的是**完全相同**的 `epoll`/`kqueue`。从操作系统视角看，两者就是同一个机制：一个线程用 `select()` 同时盯着上万个 fd，谁就绪处理谁。这就是 **Reactor 模式**。

**② 区别在「谁是默认形态」。** 这才是关键差异：

| | Node | Java |
|---|------|------|
| 语言默认形态 | **单线程 EventLoop（强制）**，你写的代码默认就跑在循环里 | **「一连接一线程」的阻塞 BIO**（`new Thread` + `inputStream.read()` 死等） |
| Reactor 怎么来 | 内置，唯一姿势 | NIO Selector 是 JDK 1.4 才补的**可选 API**，要手写 `Selector.open()`、注册 `SelectionKey`、自己写分发循环（很繁琐） |

换句话说：你在 Java 里**默认拿到的是 BIO**，想要 Node 那种事件循环，得自己切换到 NIO 并手动搭。

**③ 所以真正「对应 Node 形态」的，是 NIO 之上的框架。** 因为裸写 NIO Selector 太痛苦，Java 生态把它封装成了框架——最典型的是 **Netty**：它的 `EventLoopGroup`/`EventLoop` 几乎就是 Node EventLoop 的 Java 翻版（一个线程绑定多个 Channel、事件驱动、回调式 Handler 链）。Spring WebFlux、Vert.x 也是这一脉。**可以说：Node 默认就给你的，Java 要靠 Netty 这类框架才能得到。**

**④ 但 Java 还有另一条路：虚拟线程（JDK 21）。** Node 解决「异步麻烦」只有一招——async/await。Java 则在 Reactor 之外又给了第二条路：**虚拟线程**让你用「同步阻塞」的写法（`read()` 直接阻塞），底层 JVM 自动把阻塞的虚拟线程从 OS 线程上卸载，效果上达到 NIO 的高并发。相当于**「用同步的代码长相，拿到异步的吞吐」**——恰好是 Node [回调地狱](../part3-java-deep/06-网络IO模型.md#六回调地狱异步模型绕不开的痛以及-java-怎么躲过它)的反面解法。

> **一图总结**：OS 底层两者用的是同一套 epoll/kqueue；分歧在上层——Node 选了「单线程 EventLoop + async/await」作为唯一形态；Java 默认给你阻塞 BIO，要 Reactor 得用 Netty，或者干脆用虚拟线程「用同步写法拿异步吞吐」。BIO/NIO/AIO/Reactor/Netty 的完整拆解，见 [3.6 Java 网络 IO 模型](../part3-java-deep/06-网络IO模型.md)；虚拟线程见 [3.1 并发体系](../part3-java-deep/01-并发体系.md)。

</details>

---

## 二、libuv：Node 的并发引擎

```
┌──────────────────────────────────────────┐
│  JS 主线程 (单线程, V8 引擎执行你的代码)        │
│   ↓ 发起 IO (fs.readFile / http 请求)        │
├──────────────────────────────────────────┤
│              libuv (C 库)                   │
│   ┌─────────────────────────────────┐      │
│   │ 网络 IO: epoll/kqueue/IOCP        │  ← OS 异步, 不占线程
│   │ 文件 IO / DNS / CPU 密集: 线程池    │  ← 默认 4 线程
│   └─────────────────────────────────┘      │
│   IO 完成 ──> 把回调放进事件循环队列            │
├──────────────────────────────────────────┤
│  事件循环 ──> 取出回调 ──> 丢回 JS 主线程执行     │
└──────────────────────────────────────────┘
```

- **网络 IO**：走操作系统的 epoll/kqueue/IOCP，**不占用线程**，所以能扛海量连接（C10K）。这和 [Rust Tokio](./rust-async-tokio.md) 底层是同一套 OS 机制。
- **文件 IO、DNS、CPU 密集任务**：交给 libuv 的**线程池**（默认 4 个线程，可调 `UV_THREADPOOL_SIZE`）。

---

## 三、事件循环的六个阶段

Node 事件循环不是一个简单队列，而是按固定顺序轮转的**六个阶段（phase）**：

```
   ┌──> timers          (执行到期的 setTimeout/setInterval 回调)
   │      ↓
   │    pending callbacks (执行系统操作回调, 如 TCP 错误)
   │      ↓
   │    idle, prepare    (内部使用)
   │      ↓
   │    poll             (核心: 等待并取出 IO 事件回调)
   │      ↓
   │    check            (执行 setImmediate 回调)
   │      ↓
   └──  close callbacks  (执行 close 事件回调, 如 socket.on('close'))
```

每轮循环（tick）走一遍这六个阶段，`poll` 阶段是核心——如果没有定时器和 setImmediate，它会停在这里等待 IO 事件。

---

## 四、宏任务 vs 微任务：面试高频

JS 的异步回调分两类，**微任务优先级高于宏任务**：

| 类型 | 代表 | 执行时机 |
|------|------|---------|
| 宏任务（macrotask） | setTimeout、setImmediate、IO 回调 | 事件循环各阶段 |
| 微任务（microtask） | Promise.then、queueMicrotask、process.nextTick | **每个宏任务执行完立刻清空整个微任务队列** |

关键规则：**每执行完一个宏任务，就把当前积累的所有微任务全部执行完，再进入下一个宏任务/下一阶段。** 在 Node 里 `process.nextTick` 优先级又高于 Promise 微任务。

```js
console.log('1');                          // 同步
setTimeout(() => console.log('2'), 0);     // 宏任务
Promise.resolve().then(() => console.log('3')); // 微任务
console.log('4');                          // 同步
// 输出: 1 4 3 2
// 同步代码先跑(1,4) → 清空微任务(3) → 下一轮宏任务(2)
```

> 这套「微任务插队」机制，对应 [2.1 三件套](../part2-frontend-core/01-三件套速成.md) 里讲的事件循环模型，是前端面试的标配考点。

---

## 五、单线程的代价与边界

- **致命弱点：CPU 密集任务会阻塞整个事件循环。** 一个 `while` 死循环或大计算会卡住所有请求——因为只有一个 JS 线程。这是 Java 线程池模型（[3.1](../part3-java-deep/01-并发体系.md)）天然没有的问题。
- **解法**：CPU 密集任务用 `worker_threads`（真多线程）或拆成多进程（`cluster`）。
- **多核利用**：单个 Node 进程用不满多核，生产环境用 `cluster` 或 PM2 起多个进程占满 CPU。

---

## 六、对照其他模型

| 维度 | Node.js | 对照 |
|------|---------|------|
| 并发模型 | 单线程事件循环 | vs [Java](./java-thread-and-virtual-thread.md) 多线程 |
| 网络 IO | epoll/kqueue(不占线程) | 同 [Rust Tokio](./rust-async-tokio.md) |
| CPU 密集 | 阻塞循环(需 worker) | vs [Go](./go-goroutine-csp.md)/Java 多核并行 |
| 让出方式 | 回调/Promise/async | vs [虚拟线程](./java-thread-and-virtual-thread.md) 同步写法 |
| 多核 | 需多进程 | vs Go/Java 单进程吃满 |

---

## 本篇小结

- Node 是**单线程事件循环**：JS 主线程只执行代码，IO 交给 **libuv**（网络走 epoll 不占线程，文件/CPU 走 4 线程池）。
- 事件循环有**六个阶段**轮转，核心是 `poll` 阶段等待 IO。
- **微任务（Promise/nextTick）优先级高于宏任务（setTimeout/IO）**：每个宏任务后清空整个微任务队列——前端面试高频。
- 弱点：**CPU 密集任务阻塞整个循环**，需 `worker_threads`/`cluster`；多核需多进程。底层 IO 机制与 [Rust Tokio](./rust-async-tokio.md) 相同，但心智模型与 [Java 多线程](./java-thread-and-virtual-thread.md) 截然不同。

---

[← 返回并发模型库](./README.md) | 相关正文：[2.1 三件套](../part2-frontend-core/01-三件套速成.md) · [4.2 Java→JS/TS](../part4-multilang-compare/02-Java到JS-TS.md)
