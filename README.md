# 全栈开发大全 · 数据后端转全栈视角

> 一本写给「数据研发 / Java 后端工程师」的全栈进阶手册。
>
> 不把你当白纸，而是站在你**已经精通**的强类型、JVM、线程池、SQL、分布式系统之上，
> 用你熟悉的概念去类比和拆解前端世界与多语言生态，帮你**丝滑过渡**到全栈。

---

## 这本书为谁而写

如果你符合下面的画像，这本书就是为你量身定制的：

- 写了多年 Java（Spring Boot / MyBatis / 线程池 / JUC），习惯了强类型和编译期检查；
- 做过数据研发（Hive / Spark / Flink / SQL），擅长用「表结构 + 数据流」思考问题；
- 熟悉分布式系统、高并发、服务治理，但**前端基本是盲区**；
- 想在 AI Coding 时代成为能独立交付完整产品的全栈工程师，而不只是「后端 + 调接口」。

我们的核心信念是：**你缺的不是智商，而是一套从已知映射到未知的「翻译词典」。** 这本书就是那本词典。

---

## 全书地图

全书由四个主体部分 + 一个「并发模型引用库」组成。各部分之间通过超链接交叉串联，遇到深水区会用钩子链接把你引向专门的深挖章节。

### 第一部分 · 后端转前端的思维模式转变 〔核心重点 ①〕

这是全书的灵魂。转前端最难的从来不是 API 语法，而是**心智模型的重装**。

- [本部分导读](./part1-mindset-shift/README.md)
- [1.1 从「请求-响应」到「用户交互」：同步思维 → 事件驱动思维](./part1-mindset-shift/01-从请求响应到用户交互.md)
- [1.2 从「无状态」到「有状态」：后端的无状态信仰 vs 前端的状态管理](./part1-mindset-shift/02-从无状态到有状态.md)
- [1.3 从「强类型」到「类型光谱」：静态 / 动态类型的心智模型](./part1-mindset-shift/03-从强类型到类型光谱.md)
- [1.4 从「单一运行时」到「多运行时」：JVM vs 浏览器 / Node / Deno](./part1-mindset-shift/04-从单一运行时到多运行时.md)
- [1.5 从「数据建模」到「UI 建模」：表结构思维 → 组件与状态建模](./part1-mindset-shift/05-从数据建模到UI建模.md)

### 第二部分 · 从 Java 延伸到多语言

用 Java 当锚点，逐一拆解迁移到其他语言的关键认知差。

- [本部分导读](./part2-java-to-multilang/README.md)
- [2.1 Java → JavaScript / TypeScript：最关键的一跳](./part2-java-to-multilang/01-Java到JS-TS.md)
- [2.2 Java → Go：后端同温层的迁移](./part2-java-to-multilang/02-Java到Go.md)
- [2.3 Java → Rust：所有权与系统级编程](./part2-java-to-multilang/03-Java到Rust.md)
- [2.4 Java → Python：数据研发老朋友的再认识](./part2-java-to-multilang/04-Java到Python.md)
- [2.5 语言选型决策树：什么场景该用哪门语言](./part2-java-to-multilang/05-语言选型决策树.md)

### 第三部分 · 同一功能的多语言对比 〔核心重点 ②〕

以**高并发**为主线案例，把同一个功能用不同语言实现，横向对比。遇到各语言的并发模型，用超链接「干镰刀」跳转到下方的并发模型引用库深挖。

- [本部分导读](./part3-same-feature-compare/README.md)
- [3.1 高并发 HTTP 服务：Java / Go / Rust / Node / Python 五语言对比](./part3-same-feature-compare/01-高并发HTTP服务对比.md)
- [3.2 并发数据处理：批量任务的五语言写法对比](./part3-same-feature-compare/02-并发数据处理对比.md)
- [3.3 错误处理：异常 vs 返回值 vs Result 的哲学差异](./part3-same-feature-compare/03-错误处理对比.md)

### 并发模型引用库 〔被第三部分超链接引用〕

每门语言的并发模型独立成篇，作为「深挖锚点」。第三部分的对比章节会大量链接到这里。

- [引用库导读](./concurrency-models/README.md)
- [Java 并发模型：线程池 + JUC + 虚拟线程（Loom）](./concurrency-models/java-thread-and-virtual-thread.md)
- [Go 并发模型：Goroutine + Channel（CSP）](./concurrency-models/go-goroutine-csp.md)
- [Rust 并发模型：async/await + Tokio](./concurrency-models/rust-async-tokio.md)
- [Node.js 并发模型：事件循环 + libuv](./concurrency-models/nodejs-eventloop.md)
- [Python 并发模型：GIL + asyncio + 多进程](./concurrency-models/python-gil-asyncio.md)

### 第四部分 · AI Coding 驱动的全栈学习方法论

在 AI Coding 时代，学一门新语言的方式已经变了。这一部分讲如何用 AI 把学习曲线压平。

- [本部分导读](./part4-ai-coding-method/README.md)
- [4.1 用 AI 快速进入陌生语言：从「查文档」到「对话式学习」](./part4-ai-coding-method/01-用AI快速进入陌生语言.md)
- [4.2 AI 辅助的全栈工作流：前后端一把梭](./part4-ai-coding-method/02-AI辅助的全栈工作流.md)
- [4.3 验证与避坑：AI 生成代码的信任边界](./part4-ai-coding-method/03-验证与避坑.md)

---

## 如何阅读

推荐两种路径：

**路径 A · 按部就班（推荐首次阅读）**：第一部分 → 第二部分 → 第三部分（遇到并发模型链接就跳进引用库）→ 第四部分。第一部分的思维转变是地基，后面所有内容都建立在它之上。

**路径 B · 问题驱动（带着具体问题查阅）**：直接从第三部分的对比案例切入，遇到不懂的语言特性顺着超链接回溯到第二部分的语言迁移章节，或并发模型引用库。

全书所有 `.md` 文件之间用相对路径超链接互相串联，你可以在任意支持 Markdown 渲染的编辑器（VS Code、Typora、Obsidian）或 Git 平台中点击跳转，像在网页里冲浪一样阅读。

---

## 贯穿全书的一个隐喻

> **后端是「一栋楼的地基和管道」，前端是「住在楼里的人的生活」。**
>
> 你过去关心的是：水管会不会爆、承重够不够、并发上万人入住时会不会塌。
> 现在你要额外关心：开灯的开关好不好按、家具摆得顺不顺手、人在屋里走动时灯光会不会跟着变。
>
> 这两套关注点都重要，全栈就是同时住进地基和客厅。

接下来从 [第一部分 · 思维模式转变](./part1-mindset-shift/README.md) 开始。
