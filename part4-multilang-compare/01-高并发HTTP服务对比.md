# 4.1 高并发 HTTP 服务对比：同一个需求，五种语言

> 这是本章的**主线实验**。我们定一个具体、真实的需求，然后用 Java、Go、Rust、Node、Python 五种语言各实现一遍，**放在一起对比**。
> 这是理解「不同语言并发模型」最直观的方式——看同一个问题被五种思路解决。

---

## 实验设定：一个统一的需求

为了公平对比，五种实现都满足同一个需求：

> **一个 HTTP 服务，提供 `GET /user/{id}` 接口。每个请求要：(1) 查一次数据库（模拟为 100ms 的 IO 等待）；(2) 返回 JSON。要求能扛住一万个并发连接。**

这个需求是**典型的 IO 密集型高并发服务**——绝大多数后端服务都是这个形态。关键挑战在于：**一万个并发请求，大部分时间都在「等数据库返回」（IO 阻塞），如何高效利用这段等待时间？**

不同语言的并发模型，本质就是对「**如何处理 IO 等待**」给出的不同答案。带着这个问题往下看。

---

## 一、Java（虚拟线程版）：写起来像阻塞，跑起来像异步

基于 [3.1](../part3-java-deep/01-并发体系.md) 讲的 JDK 21 虚拟线程：

```java
// Java 21 + 虚拟线程，用 Spring Boot 或纯 HttpServer 演示核心思路
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public class UserServer {
    public static void main(String[] args) throws Exception {
        var server = HttpServer.create(new InetSocketAddress(8080), 0);

        // 关键：用"每请求一个虚拟线程"的执行器
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());

        server.createContext("/user/", exchange -> {
            String id = exchange.getRequestURI().getPath().substring(6);

            // 直接写"阻塞式"代码！查数据库就直接等
            // 虚拟线程会在阻塞时自动让出底层 OS 线程，毫无浪费
            String user = queryDatabase(id);   // 阻塞 100ms

            byte[] resp = ("{\"id\":\"" + id + "\",\"name\":\"" + user + "\"}").getBytes();
            exchange.sendResponseHeaders(200, resp.length);
            exchange.getResponseBody().write(resp);
            exchange.close();
        });
        server.start();
    }

    static String queryDatabase(String id) throws InterruptedException {
        Thread.sleep(100);   // 模拟数据库 IO（这个"阻塞"在虚拟线程里不浪费 OS 线程）
        return "user-" + id;
    }
}
```

**精髓**：代码是**最直白的同步阻塞写法**（`Thread.sleep` 模拟阻塞查库），但因为跑在虚拟线程上，一万个并发请求 = 一万个虚拟线程，它们阻塞时自动让出少数几个 OS 线程，内存和调度毫无压力。**这是 Java 21 之后高并发的推荐写法**：心智负担最低，性能却很好。

> 深入虚拟线程调度细节 → [Java 线程与虚拟线程](../concurrency-models/java-thread-and-virtual-thread.md)

---

## 二、Go：goroutine + 天生为并发而生

```go
package main

import (
    "fmt"
    "net/http"
    "strings"
    "time"
)

func userHandler(w http.ResponseWriter, r *http.Request) {
    id := strings.TrimPrefix(r.URL.Path, "/user/")

    // 同样是直白的阻塞写法，查数据库直接等
    user := queryDatabase(id) // 阻塞 100ms

    // Go 的 http 服务器为【每个请求自动开一个 goroutine】
    // goroutine 阻塞时，Go 运行时自动调度其他 goroutine 上场
    fmt.Fprintf(w, `{"id":"%s","name":"%s"}`, id, user)
}

func queryDatabase(id string) string {
    time.Sleep(100 * time.Millisecond) // 模拟 IO
    return "user-" + id
}

func main() {
    http.HandleFunc("/user/", userHandler)
    http.ListenAndServe(":8080", nil) // 一行启动，自动 goroutine-per-request
}
```

**精髓**：Go 的 `net/http` **天生为每个请求开一个 goroutine**，你完全不用操心并发。goroutine 极轻量（几 KB 栈起步，不是 Java 传统线程的 1MB），开几十万个都行。和 Java 虚拟线程惊人地相似——**都是「用户态调度的轻量协程 + 阻塞自动让出」**。区别是 Go 从第一天（2009）就是这个模型，而 Java 直到 2023 的虚拟线程才补上。

> Go 的 goroutine 与 CSP 模型细节 → [Go goroutine 与 CSP](../concurrency-models/go-goroutine-csp.md)
> 完整对比 Go 的语言哲学 → [4.3 Java → Go](./03-Java到Go.md)

---

## 三、Rust：async/await + 零成本抽象，极致性能

```rust
// 使用 tokio + axum，Rust 生态主流的异步 Web 栈
use axum::{routing::get, Router, extract::Path};
use std::time::Duration;

async fn user_handler(Path(id): Path<String>) -> String {
    // async 函数：遇到 .await 就"让出"，不阻塞线程
    let user = query_database(&id).await; // 异步等待 100ms
    format!(r#"{{"id":"{}","name":"{}"}}"#, id, user)
}

async fn query_database(id: &str) -> String {
    // 异步 sleep：让出执行权给其他任务，不占线程
    tokio::time::sleep(Duration::from_millis(100)).await;
    format!("user-{}", id)
}

#[tokio::main]
async fn main() {
    let app = Router::new().route("/user/{id}", get(user_handler));
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
```

**精髓**：Rust 用 **`async/await`** 显式标注「这里可能让出」。注意所有 IO 都要 `.await`，编译器把整个 async 函数转成一个**状态机**，由 Tokio 运行时调度。Rust 没有 GC、没有 VM，编译成原生机器码，**性能通常是这五者里最强的、内存占用最低**。代价是：你要写 `async`、处理生命周期、满足借用检查器——心智负担最高。

> Rust async 与 Tokio 运行时原理 → [Rust async 与 Tokio](../concurrency-models/rust-async-tokio.md)
> 完整对比 Rust 的所有权与设计 → [4.4 Java → Rust](./04-Java到Rust.md)

---

## 四、Node.js：单线程事件循环，IO 密集型的天选

```javascript
// Node.js，原生 http 模块
import http from 'node:http';

function queryDatabase(id) {
  // 模拟异步 IO，返回 Promise（真实场景就是数据库驱动的异步 API）
  return new Promise((resolve) => {
    setTimeout(() => resolve(`user-${id}`), 100);
  });
}

const server = http.createServer(async (req, res) => {
  const id = req.url.replace('/user/', '');

  // await 时，单线程不会"干等"，而是去处理其他请求的事件
  const user = await queryDatabase(id);

  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ id, name: user }));
});

server.listen(8080); // 单进程单线程，却能扛大量并发 IO
```

**精髓**：Node 是**单线程**的（一个主线程跑 JS），靠**事件循环 + 非阻塞 IO** 实现高并发。秘诀在于：当 `await queryDatabase` 时，这一个线程**不会傻等**，而是立刻去处理其他请求的事件，等数据库结果回来了再回来继续。呼应 [1.1](../part1-mindset-shift/01-从请求响应到用户交互.md) 讲的事件循环——这正是前端那套机制在后端的应用。**对 IO 密集型场景，单线程事件循环出奇地高效**（没有线程切换开销）。但 CPU 密集型任务会卡死这唯一的线程。

> Node 事件循环与 libuv 原理 → [Node.js 事件循环](../concurrency-models/nodejs-eventloop.md)
> 完整对比 JS/TS → [4.2 Java → JS/TS](./02-Java到JS-TS.md)

---

## 五、Python：asyncio 异步，以及 GIL 的阴影

```python
# Python，使用 asyncio + FastAPI（现代异步 Web 框架）
import asyncio
from fastapi import FastAPI

app = FastAPI()

async def query_database(id: str) -> str:
    # 异步 sleep：await 时让出，事件循环去处理别的请求
    await asyncio.sleep(0.1)  # 模拟 IO
    return f"user-{id}"

@app.get("/user/{id}")
async def user_handler(id: str):
    user = await query_database(id)  # 异步等待，不阻塞事件循环
    return {"id": id, "name": user}

# 用 uvicorn 启动：uvicorn main:app --port 8080
```

**精髓**：Python 的 `asyncio` 与 Node 思路几乎一样——**单线程事件循环 + async/await**。这套对 IO 密集型很有效。但 Python 有个绕不开的东西——**GIL（全局解释器锁）**：CPython 同一时刻只允许一个线程执行 Python 字节码，导致**多线程无法真正并行利用多核 CPU**。所以 Python 的高并发靠 asyncio（IO 密集）或多进程（CPU 密集），而不是多线程。

> Python GIL 与 asyncio 真相 → [Python GIL 与 asyncio](../concurrency-models/python-gil-asyncio.md)
> 完整对比 Python → [4.5 Java → Python](./05-Java到Python.md)

---

## 六、五种实现的横向对比

把五种实现拉到同一张表里，差异一目了然：

| 维度 | Java(虚拟线程) | Go | Rust | Node | Python |
|------|--------------|-----|------|------|--------|
| 并发模型 | 虚拟线程(用户态协程) | goroutine(CSP) | async状态机 | 单线程事件循环 | 单线程事件循环 |
| 处理 IO 等待 | 阻塞自动让出 OS 线程 | 阻塞自动让出 | `.await` 让出 | 事件回调 | `await` 让出 |
| 代码写法 | 同步阻塞(最直白) | 同步阻塞(最直白) | 显式 async | 显式 async | 显式 async |
| 利用多核 | ✅ 自动 | ✅ 自动 | ✅ 自动 | ❌ 单线程(靠多进程) | ❌ GIL(靠多进程) |
| 性能/内存 | 高/中 | 高/低 | 极高/极低 | 中高/中 | 中/中 |
| 心智负担 | 低 | 低 | 高 | 中 | 中 |
| 启动速度 | 较慢(JVM预热) | 极快 | 极快 | 快 | 快 |

**关键洞察**：

1. **「同步写法 + 自动让出」是趋势**。Java 虚拟线程和 Go goroutine 殊途同归——让你用最简单的阻塞写法，享受高并发。这是体验最好的方向。
2. **「显式 async/await」是另一条路**。Rust/Node/Python 要你显式标注让出点，多一份心智负担，但换来对调度的精确控制（Rust 还换来极致性能）。
3. **GIL 和单线程是约束**。Node 和 Python 单线程模型对 IO 密集型很香，但都靠多进程才能吃满多核。
4. **没有银弹**。Rust 性能最强但最难写，Go 平衡性最好，Java 生态最成熟，Node 适合 IO 密集 + 全栈统一语言，Python 适合快速开发 + AI 生态。

---

## 七、回到那个核心问题

实验开头我们问：**「一万个并发请求大部分时间在等 IO，如何高效利用等待时间？」** 现在你有了五个答案：

- **Java/Go**：开一万个轻量协程，谁阻塞了就把底层线程让给别人。
- **Rust/Node/Python**：单/少线程跑事件循环，遇到 `await` 就切去处理别的事件。

**本质上它们做的是同一件事——「别让线程在等 IO 时空转」**——只是实现机制和心智模型不同。理解了这一点，你就抓住了所有并发模型的牛鼻子。各语言并发模型的深层原理，我们在[并发模型引用库](../concurrency-models/README.md)里逐一展开。

---

## 本章小结

- 用「一万并发的 `GET /user/{id}`」这个统一需求，对比了五种语言的并发实现。
- **核心问题永远是「如何高效利用 IO 等待时间」**，五种语言给出两类答案：轻量协程自动让出（Java/Go）vs 事件循环显式 await（Rust/Node/Python）。
- 选型没有银弹：Rust 极致性能、Go 最佳平衡、Java 生态成熟、Node 全栈统一、Python 快速开发。详见 [4.7 选型决策树](./07-语言选型决策树.md)。
- 每种语言的并发细节，跳转[并发模型引用库](../concurrency-models/README.md)深读；每种语言的全面对比，见后续各专章。

---

[← 返回第四章导读](./README.md) | [下一节：4.2 Java → JS/TS →](./02-Java到JS-TS.md)
