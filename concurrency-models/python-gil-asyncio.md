# 并发模型库 · Python GIL 与 asyncio

> 本篇深挖 Python 并发的底层机制，是 [4.5 Java→Python](../part4-multilang-compare/05-Java到Python.md) 的「钩子」目标。对数据研发工程师尤其重要——你天天写 Python，但可能没真正搞懂为什么它「多线程不快」。

---

## 一、GIL 是什么：一把全局的大锁

**GIL（Global Interpreter Lock，全局解释器锁）** 是 CPython 解释器里的一把全局互斥锁：**任意时刻，只有一个线程能执行 Python 字节码。**

对一个习惯 [Java 真并行多线程](./java-thread-and-virtual-thread.md) 的工程师，这是最大的认知冲击：

> 在 CPython 里，哪怕你开 8 个线程跑在 8 核机器上，**同一时刻也只有 1 个线程在执行 Python 代码**。多线程无法利用多核做 CPU 并行计算。

```
Java 多线程 (真并行):              Python 多线程 (GIL 串行):
核心1: 线程A ████████             核心1: 线程A ██░░██░░██  ┐
核心2: 线程B ████████             核心2: 线程B ░░██░░██░░  ├ 抢同一把GIL
核心3: 线程C ████████             核心3: 线程C ░░░░░░░░░░  ┘ 轮流跑
   → 吃满多核                        → 实际只用了1核的算力
```

---

## 二、GIL 的来龙去脉：为什么会有它

GIL 不是设计失误，而是历史权衡：

- **简化内存管理**：CPython 用**引用计数**做垃圾回收。如果多线程并发修改引用计数，需要给每个对象加锁——开销巨大。GIL 用「一把大锁锁全部」简单粗暴地避免了这个问题。
- **C 扩展兼容**：早期大量 C 扩展（NumPy 等的底层）假设了 GIL 的存在，不需要自己处理线程安全。
- **单线程性能好**：没有细粒度锁的开销，单线程程序跑得快。

代价就是：牺牲了多线程的 CPU 并行能力。

---

## 三、关键：GIL 什么时候会释放

GIL 不是「一个线程从头霸占到尾」，它会在以下时机释放，让别的线程有机会跑：

1. **IO 操作时**：当线程发起 IO（网络、文件、`time.sleep`），它会**主动释放 GIL**，等 IO 时别的线程可以跑。
2. **执行一定字节码后**：解释器定期（默认每 5ms，`sys.setswitchinterval`）强制切换线程。
3. **调用释放 GIL 的 C 扩展时**：NumPy/Pandas 的底层大量计算是 C 实现的，会**主动释放 GIL**——这就是为什么数据研发用 NumPy 做向量化计算能「绕开」GIL 跑满多核。

**核心结论**：

| 任务类型 | 多线程有用吗？ | 原因 |
|---------|--------------|------|
| IO 密集（爬虫、网络请求） | ✅ 有用 | IO 时释放 GIL，线程能并发等待 |
| CPU 密集（纯 Python 计算） | ❌ 没用 | 抢同一把 GIL，反而更慢 |
| CPU 密集（NumPy/Pandas） | ✅ 有用 | C 层释放 GIL，真并行 |

---

## 四、asyncio：单线程协程，绕开 GIL 的另一条路

既然 IO 密集场景里线程切换有开销，Python 提供了 **asyncio**——**单线程 + 事件循环 + 协程**，思路和 [Node.js 事件循环](./nodejs-eventloop.md) 几乎一模一样：

```python
import asyncio

async def fetch(url):
    print(f"开始 {url}")
    await asyncio.sleep(1)   # 让出点: 等待时切去执行别的协程
    print(f"完成 {url}")
    return url

async def main():
    # 三个任务并发执行, 总耗时约 1 秒而非 3 秒
    await asyncio.gather(fetch("a"), fetch("b"), fetch("c"))

asyncio.run(main())
```

- `async def` 定义协程，`await` 是让出点（和 [Rust .await](./rust-async-tokio.md) 概念一致）。
- 单线程内通过事件循环调度，遇到 `await` 让出，去跑别的协程。
- 因为是单线程，**没有 GIL 争抢、没有锁**，IO 并发效率高。

> asyncio 同样有 [Rust](./rust-async-tokio.md) 提到的「**函数染色**」问题：`await` 只能在 `async` 函数里用，会传染整条调用链。这是协程模型的通病，[Java 虚拟线程](./java-thread-and-virtual-thread.md) 用「同步写法」避开了。

---

## 五、CPU 密集怎么办：多进程绕开 GIL

GIL 是「解释器级」的锁，每个进程有自己独立的解释器和 GIL。所以**多进程能真正利用多核**：

```python
from multiprocessing import Pool

def heavy_compute(n):
    return sum(i * i for i in range(n))

if __name__ == "__main__":
    with Pool(4) as pool:   # 4 个进程, 各有独立 GIL, 真并行
        results = pool.map(heavy_compute, [10**7] * 4)
```

代价：进程比线程重（独立内存空间），进程间通信（IPC）需要序列化，开销大。

**Python 并发选型口诀**：IO 密集用 `asyncio`/线程，CPU 密集用 `multiprocessing`/NumPy。

---

## 六、PEP 703：无 GIL 的未来

GIL 一直是 Python 的痛点。**PEP 703** 提出了「可选关闭 GIL」的方案（free-threaded CPython），在 Python 3.13 中作为实验性特性引入（`--disable-gil` 构建）。未来 Python 可能逐步走向无 GIL，真正支持多线程并行。但这是个漫长的迁移过程（C 扩展兼容、单线程性能回退等问题），到本书知识截止时仍处于早期阶段。

---

## 七、对照其他模型

| 维度 | Python | 对照 |
|------|--------|------|
| 多线程并行 | ❌ 受 GIL 限制 | vs [Java](./java-thread-and-virtual-thread.md)/[Go](./go-goroutine-csp.md) 真并行 |
| IO 并发 | asyncio(单线程协程) | 同 [Node](./nodejs-eventloop.md) |
| CPU 并行 | 靠多进程/NumPy | vs Java/Go 直接多线程 |
| 让出点 | 显式 `await` | 同 [Rust](./rust-async-tokio.md)/Node |
| 心智负担 | 中(要懂 GIL 边界) | — |

---

## 本篇小结

- **GIL** 让 CPython 同一时刻只能跑一个线程的字节码——多线程**无法做 CPU 并行**，这是与 [Java 多线程](./java-thread-and-virtual-thread.md) 最大的区别。
- GIL 在 **IO 操作**、**定时切换**、**C 扩展（NumPy）** 时释放——所以 IO 密集和 NumPy 计算多线程仍有用，纯 Python CPU 计算没用。
- **asyncio** 是单线程协程事件循环，思路同 [Node](./nodejs-eventloop.md)，IO 并发高效，但有函数染色问题。
- CPU 密集靠 **multiprocessing**（每进程独立 GIL）真并行。**PEP 703** 正在推进无 GIL 的未来。

---

[← 返回并发模型库](./README.md) | 相关正文：[4.5 Java→Python](../part4-multilang-compare/05-Java到Python.md)
