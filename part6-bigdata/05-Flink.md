# 6.5 Flink——实时计算引擎

> **一句话定位**：Spark 是"用批处理的思路模拟流"（微批次），Flink 是"原生流处理引擎"——每条数据到达即处理，毫秒级延迟。它是实时大盘、实时风控、实时推荐等场景的首选引擎，也是目前实时计算领域的事实标准。

---

## 一、流处理的核心挑战

实时计算和批处理最大的区别是：**数据没有尽头**。批处理有明确的输入边界（昨天的日志文件），处理完就结束。流处理面对的是源源不断涌入的事件，永远不会"处理完"。

这带来三个独特挑战：

| 挑战 | 含义 | 举例 |
|------|------|------|
| **乱序** | 事件到达顺序 ≠ 事件发生顺序 | 用户 10:00:01 的点击事件在 10:00:05 才到达 Flink |
| **延迟** | 事件可能在很久之后才到达 | 移动端断网恢复后批量上报 |
| **窗口** | 无限流上怎么做聚合？需要把流切成有限的"窗口" | "最近 5 分钟的订单总额" |

Flink 的核心价值就是提供了一套完整的机制来应对这三个挑战。

---

## 二、核心概念

### 2.1 事件时间 vs 处理时间

| 时间语义 | 含义 | 优劣 |
|---------|------|------|
| **Event Time（事件时间）** | 事件实际发生的时间（嵌在数据中） | 结果准确，但需要处理乱序和延迟 |
| **Processing Time（处理时间）** | 事件到达 Flink 的时间 | 最简单、延迟最低，但结果受处理速度影响 |

> **生产经验**：大多数场景用 Event Time。只有对延迟极其敏感且允许结果不精确的场景（如实时监控告警）才用 Processing Time。

### 2.2 Watermark（水位线）——解决乱序

Watermark 是 Flink 用来衡量"事件时间推进到哪了"的机制。它本质是一个时间戳，含义是：**所有时间戳 ≤ Watermark 的事件都已经到达**。

```
数据流：[10:01, 10:03, 10:02, 10:05, 10:04]  ← 乱序到达
Watermark 策略：允许 2 秒乱序

当收到 10:05 的事件时，生成 Watermark = 10:05 - 2s = 10:03
意味着 Flink 认为 10:03 之前的事件都到了，可以触发 10:00-10:03 窗口的计算
```

如果 10:03 之后还来了一个 10:02 的事件——这就是**迟到数据**。Flink 可以配置丢弃、放入侧输出流（Side Output）、或允许窗口更新。

#### Watermark 生成逻辑：maxTimestamp 只增不减

上游生成 Watermark 的逻辑是：维护一个 `maxTimestamp`（已见过的最大时间戳），每次有数据进来，取 `max(maxTimestamp, 当前数据时间戳)` 更新它，然后定期发出 `Watermark(maxTimestamp - allowedLateness)`。

当乱序数据 10:02 在 10:05 之后到达时，数据和 Watermark 走两条独立的逻辑：

```
数据 10:05 到达 → maxTimestamp = max(-∞, 10:05) = 10:05 → 发出 Watermark(10:05 - 2s) = 10:03
数据 10:02 到达 → maxTimestamp = max(10:05, 10:02) = 10:05 → maxTimestamp 不变, 不发新的 Watermark
```

数据 10:02 本身照常往下游流——它是一条普通数据记录，该处理还是处理。但因为它比 maxTimestamp 小，不会更新 maxTimestamp，也就不会产生更小的 Watermark。**数据可以下发，但水位线不会回退。**

这个 10:02 的数据流到下游后，下游算子发现它的时间戳 < 当前 Watermark（10:03），判定为迟到数据，走迟到处理逻辑（丢弃、侧输出、或更新已关闭的窗口）。

#### Watermark 不是数据字段，是流里的特殊记录

Watermark 不是加在每条数据上的新字段，而是数据流里穿插的一种**特殊记录**。可以把它想象成一条管道，里面流着两种类型的元素：

```
[数据] [数据] [数据] [Watermark: 10:03] [数据] [数据] [Watermark: 10:05] [数据] ...
```

它们共享同一条通道，混在一起往下游传。算子处理时会判断元素类型：普通数据走业务逻辑，Watermark 走时间推进逻辑（更新时钟、触发 Timer、关闭窗口等）。

这一点解释了几个关键问题：

Watermark 能跨算子传播——因为它是流里的元素，上游生成后自然跟着数据往下游流，下游收到就能用，不需要每条数据额外携带。

Watermark 不影响数据序列化结构——你的 POJO、Row、JSON 里都没有 Watermark 字段，它是 Flink 运行时层面的东西，对用户数据模型完全透明。

SQL 里 `WATERMARK FOR click_time AS click_time - INTERVAL '5' SECOND` 也不是给表加了列，而是告诉 Flink："请根据 click_time 的值，定期往流里插入 Watermark 记录。"

#### 每个 Task 维护一个事件时间时钟

事件时间时钟不是 per executor 的，而是 **per Task（并行子任务）**。比如并行度为 4，一个算子有 4 个 Task，每个 Task 各自维护独立的事件时间时钟，互不影响。

时钟由收到的 **Watermark 记录**驱动前进（不是直接根据数据时间戳），规则如下：

单输入：收到更大的 Watermark 就推进，取最新值。Watermark 不能倒退。

多输入（如 KeyedCoProcessFunction 有两个输入流）：每个输入流各自维护收到的最新 Watermark，Task 的时钟取**两者中的最小值**。任何一个输入流的 Watermark 没推进，整体时钟就不动。这就是多流"等待"机制的实现方式。

为什么取 min 而不是 max？因为水位线的语义是"这个时间之前的数据都到了"。多个输入流各自有各自的进度，只有当**所有**输入都认为自己推进到了某个时间，才能说"这个时间之前所有流的数据都齐了"。取 max 意味着 A 说到 10:08 了但 B 才到 10:06，10:06 到 10:08 之间 B 的数据可能还没到，提前推进就会漏数据。取 min 就是取"最保守"的时间——被最慢的那个输入拖住，宁可等也不能漏。

```
当前 inputA=10:08, inputB=10:06

取 max → 时钟 = 10:08  ← 激进：B 在 10:06~10:08 的数据可能还没到，会漏
取 min → 时钟 = 10:06  ← 保守：两个流都保证到 10:06，安全
```

Watermark 记录本身不携带"来源流 ID"之类的字段——它只包含一个时间戳。下游 Task 能区分 Watermark 来自哪个输入，靠的是物理通道：每个输入是一条独立的物理通道，Task 内部为每个输入分别维护一个 watermark 值。Watermark 从哪条通道进来，就更新那个输入对应的值。

```
Watermark 记录结构：{ timestamp: 10:06 }，没有别的字段

Task 内部维护：
  input A 的最新 watermark: 10:08   ← 从通道 A 收到的
  input B 的最新 watermark: 10:06   ← 从通道 B 收到的
  当前时钟 = min(10:08, 10:06) = 10:06
```

#### Watermark 实时处理，单调递增不回退

Watermark 的处理是实时的——每收到一条 Watermark，立刻更新该输入通道的值并重新算 min，没有攒批、没有窗口。而且**单个输入通道的 watermark 只增不减**：收到更大的才更新，比当前值小的直接丢弃。因此整体时钟（min）也一定是单调递增的，永远不会回退。

```
时刻1: A 发来 WM=10:03, B 还没发 → inputA=10:03, inputB=-∞, 时钟=min=−∞
时刻2: B 发来 WM=10:04            → inputA=10:03, inputB=10:04, 时钟=min=10:03
时刻3: A 发来 WM=10:05            → inputA=10:05, inputB=10:04, 时钟=min=10:04
时刻4: B 发来 WM=10:06            → inputA=10:05, inputB=10:06, 时钟=min=10:05
时刻5: A 异常发来 WM=10:01        → 10:01 < 10:05, 丢弃, inputA 保持 10:05
```

每个输入只增不减，min 也只增不减，但增长速度被最慢的那个输入拖住。

#### Watermark 不仅驱动窗口

窗口是 Watermark 最常见的消费者，但不是唯一的。Watermark 是整个事件时间体系的"全局时钟指针"，以下机制都订阅这个时钟：

窗口：Watermark 推进到窗口结束时间时，触发计算并关闭窗口。

Event Time Timer：用 `ProcessFunction` 注册的事件时间定时器（如"在事件时间 10:10 时执行某段逻辑"），靠 Watermark 推进来触发，与窗口无关。

Interval Join / 时间维表关联：两个流做时间窗口 join 时，Watermark 决定何时可以认为某时间段数据"齐了"，可以开始 join 并清理过期状态。这里没有传统意义上的窗口，但依然依赖 Watermark。

State 清理：某些算子基于事件时间清理过期状态（如"保留最近 1 小时的状态"），这个"1 小时"按 Watermark 推进来算。

#### Watermark 的代码示例

DataStream API 中手动生成 Watermark：

```java
DataStream<Event> stream = env
    .addSource(new MySource())
    // 为流分配 Watermark 策略：最大乱序 2 秒
    .assignTimestampsAndWatermarks(
        WatermarkStrategy
            .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
            .withTimestampAssigner((event, ts) -> event.getTimestamp())
    );
```

`forBoundedOutOfOrderness` 的内部逻辑就是：记录已见过的最大时间戳 `maxTimestamp`，定期发出 `Watermark(maxTimestamp - 2s)`。这个 Watermark 作为特殊记录混在数据流里往下游传。

多输入场景下（如 Interval Join），每个流各自走自己的 Watermark 策略，各自发送自己的 Watermark，下游算子取最小值推进：

```java
// 两个流各自分配 Watermark 策略
DataStream<Order> orders = env.addSource(...)
    .assignTimestampsAndWatermarks(
        WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(3))
            .withTimestampAssigner((o, ts) -> o.getOrderTime()));

DataStream<Payment> payments = env.addSource(...)
    .assignTimestampsAndWatermarks(
        WatermarkStrategy.<Payment>forBoundedOutOfOrderness(Duration.ofSeconds(5))
            .withTimestampAssigner((p, ts) -> p.getPayTime()));

// Interval Join：下游算子取两个流 Watermark 的最小值作为事件时间时钟
orders
    .keyBy(Order::getOrderId)
    .intervalJoin(payments.keyBy(Payment::getOrderId))
    .between(Time.seconds(-10), Time.seconds(10))
    .process(new OrderPaymentJoinFunc());
```

### 2.3 窗口（Window）

窗口把无限流切成有限的数据集来做聚合。Flink 支持四种窗口：

| 窗口类型 | 含义 | 示例 |
|---------|------|------|
| **滚动窗口（Tumbling）** | 固定大小、不重叠 | 每 5 分钟统计一次 PV |
| **滑动窗口（Sliding）** | 固定大小、可重叠 | 窗口 10 分钟、每 1 分钟滑动一次 |
| **会话窗口（Session）** | 按活跃间隔动态切分 | 用户 30 分钟无操作则关闭会话 |
| **全局窗口（Global）** | 所有数据在一个窗口，需自定义触发器 | 特殊场景 |

#### 滚动窗口（Tumbling Window）

**SQL**：

```sql
-- 统计每 5 分钟每个商品的订单数
SELECT
  product_id,
  TUMBLE_START(order_time, INTERVAL '5' MINUTE) AS window_start,
  TUMBLE_END(order_time, INTERVAL '5' MINUTE) AS window_end,
  COUNT(*) AS order_cnt
FROM orders
GROUP BY
  product_id,
  TUMBLE(order_time, INTERVAL '5' MINUTE);
```

**Java DataStream**：

```java
DataStream<Order> orders = env.addSource(new KafkaSource<>())
    .assignTimestampsAndWatermarks(
        WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(5))
            .withTimestampAssigner((o, ts) -> o.getOrderTime()));

orders
    .keyBy(Order::getProductId)
    .window(TumblingEventTimeWindows.of(Time.minutes(5)))  // 5 分钟滚动窗口
    .aggregate(new OrderCountAggregate())  // 自定义聚合函数
    .addSink(new KafkaSink<>());
```

#### 滑动窗口（Sliding Window）

**SQL**：

```sql
-- 每 1 分钟统计一次最近 10 分钟的订单数
SELECT
  product_id,
  HOP_START(order_time, INTERVAL '1' MINUTE, INTERVAL '10' MINUTE) AS window_start,
  HOP_END(order_time, INTERVAL '1' MINUTE, INTERVAL '10' MINUTE) AS window_end,
  COUNT(*) AS order_cnt
FROM orders
GROUP BY
  product_id,
  HOP(order_time, INTERVAL '1' MINUTE, INTERVAL '10' MINUTE);
-- 语法：HOP(timeCol, slideSize, windowSize)
```

**Java DataStream**：

```java
orders
    .keyBy(Order::getProductId)
    .window(SlidingEventTimeWindows.of(
        Time.minutes(10),   // 窗口大小
        Time.minutes(1)   // 滑动步长
    ))
    .aggregate(new OrderCountAggregate())
    .addSink(new KafkaSink<>());
```

#### 会话窗口（Session Window）

**SQL**：

```sql
-- 统计用户会话，30 分钟无操作则关闭会话
SELECT
  user_id,
  SESSION_START(visit_time, INTERVAL '30' MINUTE) AS session_start,
  SESSION_END(visit_time, INTERVAL '30' MINUTE) AS session_end,
  COUNT(*) AS pv
FROM user_visits
GROUP BY
  user_id,
  SESSION(visit_time, INTERVAL '30' MINUTE);
```

**Java DataStream**：

```java
orders
    .keyBy(Order::getUserId)
    .window(EventTimeSessionWindows.withDynamicGap(
        (Order o) -> Time.minutes(30)  // 动态间隔，也可固定
    ))
    // 或固定间隔：.window(EventTimeSessionWindows.withGap(Time.minutes(30)))
    .aggregate(new SessionAggregate())
    .addSink(new KafkaSink<>());
```

#### 累积窗口（Cumulative Window）

**SQL**（Flink 1.13+）：

```sql
-- 每天从 00:00 开始，每 1 小时输出一次累计结果（截至当前小时）
SELECT
  product_id,
  CUMULATE_START(order_time, INTERVAL '1' HOUR, INTERVAL '1' DAY) AS window_start,
  CUMULATE_END(order_time, INTERVAL '1' HOUR, INTERVAL '1' DAY) AS window_end,
  COUNT(*) AS pv
FROM orders
GROUP BY
  product_id,
  CUMULATE(order_time, INTERVAL '1' HOUR, INTERVAL '1' DAY);
-- 语法：CUMULATE(timeCol, step, maxSize)
--   step    = INTERVAL '1' HOUR  → 每隔 1 小时输出一次累计结果
--   maxSize = INTERVAL '1' DAY   → 累计窗口的总跨度（00:00 ~ 24:00）
-- 产生的窗口：[00:00,01:00), [00:00,02:00), [00:00,03:00), ... [00:00,24:00)
```

**Java DataStream**（Flink 1.13+ 无原生 Cumulative Window API，需用 ProcessFunction + Timer 模拟）：

```java
// 用 KeyedProcessFunction 实现累积窗口
orders
    .keyBy(Order::getProductId)
    .process(new KeyedProcessFunction<String, Order, Result>() {
        private ValueState<Long> pvState;
        private ValueState<Long> lastOutputHour;

        @Override
        public void open(Configuration parameters) {
            pvState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("pv", Long.class));
            lastOutputHour = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lastHour", Long.class));
        }

        @Override
        public void processElement(Order order, Context ctx, Collector<Result> out) throws Exception {
            long currentPv = pvState.value() == null ? 0 : pvState.value();
            pvState.update(currentPv + 1);

            // 注册当天每小时结束的 Timer
            long eventTime = order.getOrderTime();
            long hourEnd = (eventTime / 3600000 + 1) * 3600000; // 下一小时的边界
            ctx.timerService().registerEventTimeTimer(hourEnd);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Result> out) throws Exception {
            long pv = pvState.value() == null ? 0 : pvState.value();
            out.collect(new Result(ctx.getCurrentKey(), timestamp, pv));
        }
    });
```

#### 全局窗口（Global Window）+ 自定义触发器

**SQL**：SQL 层无全局窗口概念，需用 `GROUP BY` 无窗口聚合配合 `EMIT` 策略。

**Java DataStream**：

```java
// 全局窗口 + 自定义触发器：每 100 条数据或 1 分钟触发一次计算
orders
    .keyBy(Order::getProductId)
    .window(GlobalWindows.create())
    .trigger(CountTrigger.of(100))  // 每 100 条触发
    // 或组合触发器：.trigger(CountTrigger.of(100).or(ProcessingTimeTrigger.of(Time.minutes(1))))
    .aggregate(new OrderCountAggregate())
    .addSink(new KafkaSink<>());
```

---

## 三、架构

```mermaid
graph TD
    A["JobManager（主节点）<br/>接收 Job → 生成执行图 → 调度 Task → 协调 Checkpoint"] --> B["TaskManager 1<br/>执行 Task（含多个 Slot）"]
    A --> C["TaskManager 2<br/>执行 Task"]
    A --> D["TaskManager N<br/>执行 Task"]
    
    E["Checkpoint<br/>State 快照 → 持久化到 HDFS/S3"] -.定期触发.-> A
```

| 组件 | 职责 | 类比 |
|------|------|------|
| **JobManager** | 接收用户 Job，生成执行图（JobGraph → ExecutionGraph），调度 Task，协调 Checkpoint | 类似 Spark Driver |
| **TaskManager** | 执行具体的算子逻辑，每个 TaskManager 有多个 **Slot**（执行线程） | 类似 Spark Executor |
| **Slot** | TaskManager 中的资源单元（内存隔离），一个 Slot 运行一条算子链 | 类似 Spark 的 Task |

---

## 四、容错机制——Checkpoint + Exactly-Once

### 4.1 Checkpoint（检查点）

#### Chandy-Lamport 分布式快照算法

Flink 的容错核心是 **Chandy-Lamport 分布式快照算法**。这个算法解决的问题是：在一个持续流动的分布式数据流中，如何在不暂停计算的情况下，给整个系统拍一个一致的"全局快照"。

核心思路是引入 **Marker（标记消息）**——在 Chandy-Lamport 原始论文中叫 marker，Flink 里叫 **Barrier（屏障）**。它是一种特殊的控制消息，混在数据流里跟普通数据一起传输，思路跟 Watermark 类似。

算法流程：

1. **发起**：协调器（Flink 中是 JobManager）决定做快照，向所有 Source 注入一条 Barrier N。
2. **Source 快照**：Source 收到 Barrier N 后，记录自己的状态（如 Kafka 的 offset），然后把 Barrier N 往下游发。
3. **中游对齐**：当一个算子有多个输入时，收到第一个输入的 Barrier N 后，**暂停**该输入的数据处理（先缓冲起来），等所有输入都收到 Barrier N——这叫 **Barrier 对齐**。对齐后，算子保存自己的 State，然后把 Barrier N 往下游发。
4. **完成**：所有算子都保存完 State 后，JobManager 确认这次快照成功。

```
Source → [Barrier N] → Map → [Barrier N] → Aggregate → [Barrier N] → Sink
                           ↓ 对齐 Barrier 时保存快照
                      State Snapshot → HDFS
```

关键点在于 Barrier 把数据流切成了"快照前"和"快照后"两部分。一个算子在 Barrier N 之前处理的数据都属于快照 N 的状态，之后的数据属于下一个快照。因为所有算子都按同一个 Barrier 做切分，所以全局状态是一致的。

#### Watermark 与 Barrier 的区别

两者都是混在数据流里的特殊记录，但触发机制、语义和作用完全不同：

| 维度 | Watermark | Barrier |
|------|-----------|---------|
| **触发方式** | Source 根据数据时间戳**自主生成**，数据驱动 | JobManager 通过 RPC**统一注入**，定时触发（按 checkpoint interval） |
| **语义** | 所有时间戳 ≤ Watermark 的事件都已到达 | 标志快照边界，Barrier 之前的数据属于快照 N，之后属于快照 N+1 |
| **多输入处理** | 取**min**（最小值），推进事件时间时钟 | 取**对齐**（等所有输入都收到），然后保存快照 |
| **方向** | 从 Source 往下游流，驱动时间推进 | 从 Source 往下游流，协调全局快照 |
| **作用** | 驱动窗口触发、Event Time Timer、Interval Join、State 清理 | 驱动 Checkpoint 状态快照 |
| **是否可丢弃** | 比当前值小的会被丢弃（只增不减） | 不能丢弃，必须严格按编号顺序处理 |
| **生成位置** | Source 或任何能提取时间戳的算子 | 仅由 JobManager 注入，从 Source 开始 |

一句话总结：**Watermark 管"时间到了没"，Barrier 管"快照切在哪"**。Watermark 推进事件时间，决定业务逻辑何时触发；Barrier 推进快照进度，决定容错状态何时保存。两者互不干扰，各自在数据流里独立传播。

**Barrier 对齐的代价**：多输入算子需要对齐 Barrier，对齐期间会暂停处理较快输入的数据（缓冲起来），这会引入短暂延迟。Flink 还提供了 **非对齐 Checkpoint（Unaligned Checkpoint）** 选项：不做对齐，直接把缓冲区里的数据也存进快照。这样更快，但快照更大。适合反压严重、对齐时间过长的场景。

#### 非对齐 Checkpoint（Unaligned Checkpoint）的精确原理

传统对齐 Checkpoint 在反压严重时，对齐时间会无限拉长（因为下游处理不过来，上游数据堆积，Barrier 迟迟无法对齐），最终导致 Checkpoint 超时。Flink 1.11+ 引入的 Unaligned Checkpoint 解决了这个问题：

- 当某个输入流的 Barrier 到达时，算子**不需要等待其他输入流**，而是立即将当前所有输入缓冲区中 barrier 之后的数据、输出缓冲区中待发送的数据，连同当前算子状态一起存入快照。
- 恢复时，这些被快照的缓冲数据会重新注入到算子的输入队列中重新计算。关键是：**算子状态本身只包含 barrier 之前的数据处理结果**，barrier 之后的数据只是被"缓存"在快照里，恢复时重新消费，所以不会重复也不会丢失。
- 优点：Checkpoint 不受反压影响，对齐时间几乎为零。
- 缺点：快照需要额外保存缓冲数据，体积更大；恢复时需要重新注入缓冲数据，启动时间稍长。

Flink 1.11+ 可以通过配置 `execution.checkpointing.unaligned: true` 启用，也可以配置 `alignment-timeout`（默认 0ms，即立即降级），当对齐时间超过阈值时自动降级为 Unaligned Checkpoint。

#### 生产环境 Checkpoint 配置

| 配置参数 | 含义 | 生产建议 |
|---------|------|---------|
| `checkpoint.interval` | 两次 Checkpoint 的间隔 | 通常 1~10 分钟，太小会拖垮系统，太大会增加恢复时数据重放量 |
| `checkpoint.timeout` | 单次 Checkpoint 的超时时间 | 通常设为 interval 的 2~3 倍，超时则失败 |
| `max-concurrent-checkpoints` | 最大并发 Checkpoint 数 | 默认 1；如果单次 Checkpoint 耗时 > interval，建议增加，但不宜过大 |
| `min-pause-between-checkpoints` | 两次 Checkpoint 之间的最小间隔 | 防止 Checkpoint 过于密集，给 Task 留出处理时间 |
| `execution.checkpointing.unaligned` | 是否启用非对齐 Checkpoint | 反压严重时启用；正常场景建议保持对齐（默认 false） |
| `execution.checkpointing.max-aligned-checkpoint-size` | 对齐 Checkpoint 的最大缓冲数据量 | 控制 Unaligned Checkpoint 的内存上限 |
| `state.checkpoints.dir` | Checkpoint 存储路径（HDFS/S3） | 生产必选，取消作业后如未配置 `externalized-checkpoint` 则数据丢失 |
| `execution.checkpointing.externalized-checkpoint-retention` | 作业取消后是否保留 Checkpoint | 生产必须设为 `RETAIN_ON_CANCELLATION`，否则取消作业后无法恢复 |

**关于 `externalized-checkpoint` 的坑**：默认情况下，作业取消（cancel）时 Flink 会自动删除对应的 Checkpoint 数据。如果后续想从该 Checkpoint 恢复（如回滚版本），会发现数据已丢失。生产环境必须配置 `execution.checkpointing.externalized-checkpoint-retention: RETAIN_ON_CANCELLATION`，这样 Checkpoint 才会保留。

任务失败时，从最近一次成功的 Checkpoint 恢复 State，数据源（如 Kafka）从对应 offset 重放，实现 **Exactly-Once** 语义。


### 4.2 端到端 Exactly-Once 语义

#### Checkpoint 与两阶段提交（2PC）的关系

首先需要澄清一个常见误区：**Checkpoint 本身只保证 Flink 内部算子状态的 exactly-once**——即算子状态在故障恢复后不丢不重。但 Checkpoint 不负责 Sink 写入外部系统（如 Kafka、MySQL）的一致性。如果 Sink 在 Checkpoint 完成后、数据真正写入外部系统前崩溃，外部系统仍可能丢失或重复数据。

**两阶段提交（2PC）解决的是 Sink 端与外部系统的一致性**。它不是在替代 Checkpoint，而是在 Checkpoint 保证内部一致性的基础上，**额外保证外部系统的一致性**。两者的关系是协作：Checkpoint 负责"算子状态不丢不重"，2PC 负责"外部写入不丢不重"。只有 Sink 需要事务时，才需要 2PC；Source 端只需要支持按 offset 重放（不需要 2PC）。

Exactly-Once 不是 Flink 单方面保证的，而是整个数据管道所有组件一致性的**木桶效应**——整个端到端一致性级别取决于所有组件中最弱的一环。具体可以划分为三个层面：

| 层面 | 要求 | 实现方式 |
|------|------|---------|
| **内部保证** | Flink 自身不丢不重 | Checkpoint 机制 + Barrier 对齐 |
| **Source 端** | 故障后可重放，不丢数据 | 数据源支持按 offset / 位置回溯重读（Kafka Consumer 保存 offset） |
| **Sink 端** | 故障恢复时不重复写入外部系统 | 幂等（Idempotent）写入 或 事务性（Transactional）写入 |

Flink 通过**两阶段提交（2PC）**协议与支持事务的 Sink（如 Kafka 0.11+）配合，实现真正的端到端 Exactly-Once。

#### 4.2.1 幂等写入（Idempotent Writes）

幂等操作是指重复执行多次，只导致一次结果更改。如果 Sink 系统天然支持幂等（如按主键去重的数据库、按 doc id 索引的 Elasticsearch），可以用 **at-least-once + 下游幂等** 替代 2PC，性能更好。典型做法是：为每条数据生成唯一标识（如业务主键），Sink 写入时冲突即覆盖或忽略。

#### 4.2.2 事务写入

事务写入的核心思想是：**构建的事务对应着 Checkpoint，等到 Checkpoint 真正完成时，才把所有对应结果写入 Sink 系统**。DataStream API 提供了 `GenericWriteAheadSink`（预写日志 WAL）和 `TwoPhaseCommitSinkFunction`（两阶段提交）两种事务性写入模板。

##### 预写日志（WAL）

把结果数据先作为 State 缓存，收到 Checkpoint 完成通知后，一次性批量写入 Sink。简单通用，但延迟较大，且外部系统需要支持批量写入。

##### 两阶段提交（2PC）

两阶段提交协议（Two-Phase Commit）是分布式系统中协调多节点事务一致性的经典算法。在 Flink 中，它被用来协调 Sink 与 Checkpoint 的提交节奏：

**第一阶段（预提交）**：当 Checkpoint 启动时，JobManager 向数据流注入 Barrier。Sink 收到 Barrier 后，将当前事务中的数据写入外部系统（如 Kafka），但**不提交**——此时数据处于预提交状态，对外部消费者不可见（Kafka 的 `read_committed` 隔离级别下）。然后 Sink 将当前事务 ID 等状态保存到 Checkpoint。

**第二阶段（正式提交）**：当所有算子的 Checkpoint 都成功完成后，JobManager 向所有 Task 发送确认通知。Sink 收到通知后，调用外部系统的事务提交（如 `producer.commitTransaction()`），数据才真正变为可见和可消费。

如果 Checkpoint 失败或作业崩溃，Flink 从最近一次成功的 Checkpoint 恢复，Sink 会重新执行 `commit()`（因为 Checkpoint 中已记录待提交的事务信息）。这要求**提交操作必须是幂等的**——Kafka 在相同事务 ID 下重复调用 `commitTransaction` 是安全的。

#### 4.2.3 两阶段提交的完整时序

以 Flink + Kafka 为例，端到端 exactly-once 的时序如下：

```
1. 数据持续流入，Kafka Producer 在事务中写入数据（预提交，未 commit）
2. JobManager 注入 Barrier N
3. Source 收到 Barrier，保存 offset 到状态后端，向下游传递 Barrier
4. Sink 收到 Barrier N，将当前事务数据 flush 到 Kafka，但不 commit
5. Sink 开启新事务（用于 Checkpoint N+1 的数据），将 Barrier 继续向下传递
6. 所有算子完成状态快照，JobManager 确认 Checkpoint N 成功
7. Sink 收到 Checkpoint 完成通知，正式提交旧事务（commitTransaction）
8. Kafka 中该事务的数据变为可见，消费者可以读取
```

关键点：预提交阶段的数据已写入 Kafka 的日志，但消费者通过 `read_committed` 隔离级别看不到；只有 `commitTransaction` 后，事务内消息才从 `uncommitted` 变为 `committed`。

#### 4.2.4 2PC 对外部 Sink 系统的要求

并非所有 Sink 都支持 2PC，需要满足以下条件：

- 外部系统必须提供**事务支持**（或 Sink 能模拟事务）。
- 在 Checkpoint 间隔期间内，能**开启事务并接受数据写入**。
- 在收到 Checkpoint 完成通知前，事务必须处于**"等待提交"**状态。若事务超时关闭（如 Kafka 事务超时），未提交数据会丢失。
- Sink 必须能在**进程失败后恢复事务**（通过 Checkpoint 中的事务状态）。
- **提交事务必须是幂等操作**（支持重复提交相同事务）。

#### 4.2.5 Flink + Kafka 端到端 Exactly-Once

Flink + Kafka 是经典的端到端 exactly-once 数据管道（Kafka 进、Kafka 出）。各组件的职责如下：

| 组件 | 保证机制 |
|------|---------|
| **Flink 内部** | Checkpoint 保存算子状态，故障时从 State Backend 恢复 |
| **Kafka Source** | Consumer 将 offset 作为 State 保存，恢复时按 offset 重放，不丢数据 |
| **Kafka Sink** | Producer 启用事务（`transactional.id`），通过 2PC 与 Flink Checkpoint 配合，不重复写入 |
| **Kafka Consumer（下游）** | 必须设置 `isolation.level=read_committed`，否则可能读取到被 abort 的事务数据，破坏端到端一致性 |

**Kafka Producer 的事务代码示例**：

```java
Properties props = new Properties();
props.put("bootstrap.servers", "kafka:9092");
props.put("transactional.id", "my-producer-id"); // 启用事务，必须设置唯一事务 ID
props.put("enable.idempotence", "true");         // 开启幂等性

KafkaProducer<String, String> producer = new KafkaProducer<>(props);
producer.initTransactions();
producer.beginTransaction();
producer.send(new ProducerRecord<>("topic", "key", "value"));
producer.commitTransaction(); // 或 abortTransaction()
```

#### 4.2.6 生产环境配置与常见踩坑

**事务超时与 Checkpoint 间隔的匹配**

Kafka 的 `transaction.timeout.ms` 默认是 60 秒。如果 Flink 的 Checkpoint 间隔（`checkpoint.interval`）设置得太大（比如 5 分钟），在 Checkpoint 完成前 Kafka 事务可能已超时并被 Coordinator 中止，导致数据丢失。

**解决方法**：确保 Kafka 的事务超时 > Flink 的 Checkpoint 间隔 + 最大容忍时间。Flink 的 `KafkaSink` 提供了 `transaction.timeout.ms` 配置，应与 Kafka Broker 端的 `transaction.max.timeout.ms` 相匹配。

```java
// Flink KafkaSink 事务超时配置（应小于 Kafka broker 的 transaction.max.timeout.ms）
KafkaSink<String> sink = KafkaSink.<String>builder()
    .setBootstrapServers("kafka:9092")
    .setRecordSerializer(...)
    .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
    .setProperty("transaction.timeout.ms", "900000") // 15 分钟，需匹配 broker 配置
    .build();
```

**其他常见踩坑**：

- 下游消费者未设置 `read_committed`：即使 Flink 做到了 exactly-once，下游消费者用 `read_uncommitted` 仍会读到未提交或被 abort 的数据。
- `transactional.id` 冲突：同一个 `transactional.id` 不能同时被两个 Producer 实例使用，否则 Kafka 会报错。Flink 通过 `subtaskIndex` 来生成唯一的事务 ID，确保每个并行子任务互不冲突。
- 事务协调器故障：Kafka 的 Transaction Coordinator 负责管理事务状态，如果 Coordinator 发生故障转移，正在等待提交的 2PC 事务可能会超时，需要合理设置超时参数。

#### 4.2.7 API 演进：FlinkKafkaProducer 与 KafkaSink

Flink 的 Kafka Sink API 经历了演进：

- **Flink 1.4~1.14**：使用 `FlinkKafkaProducer`，需继承 `TwoPhaseCommitSinkFunction` 实现自定义 2PC Sink。这是较老的 API，已在 Flink 1.15 起标记为 deprecated。
- **Flink 1.15+**：推荐使用 `KafkaSink`，内置 `EXACTLY_ONCE` / `AT_LEAST_ONCE` / `NONE` 三种投递保证，通过 `DeliveryGuarantee` 枚举直接配置，无需手动实现 2PC 接口。

```java
// Flink 1.15+ 推荐使用的新 API
KafkaSink<String> sink = KafkaSink.<String>builder()
    .setBootstrapServers("kafka:9092")
    .setRecordSerializer(KafkaRecordSerializationSchema.builder()
        .setTopic("output-topic")
        .setValueSerializationSchema(new SimpleStringSchema())
        .build())
    .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
    .build();

stream.sinkTo(sink);
```

#### 4.2.8 2PC 与 at-least-once + 幂等的权衡

| 方案 | 优点 | 缺点 | 适用场景 |
|------|------|------|---------|
| **2PC** | 严格 exactly-once，不依赖下游去重 | 有性能开销（事务协调、屏障同步、超时限制），Checkpoint 间隔不能太大 | 金融交易、对账、不允许重复的关键业务 |
| **at-least-once + 幂等** | 性能更好，无事务超时限制，实现简单 | 需要 Sink 系统或业务层支持幂等 | 日志写入、指标上报、下游支持主键去重 |

如果下游系统天然支持幂等（如按主键更新的 MySQL、按 doc id 写入的 Elasticsearch），**at-least-once + 幂等** 通常是更轻量、更可靠的选择。只有在下游无法幂等、且业务对重复零容忍时，才必须使用 2PC。


---

## 五、状态管理

### 5.1 State 分类

一个 Flink Job 通常由 Source → Transformation → Sink 组成，每个算子在一个或多个 Task 中并行运行。State 就是流处理过程中需要"记住"的数据快照，既包括业务数据（如累加计数），也包括元数据（如 Kafka Consumer 的 offset）。

| 类型 | 作用域 | 典型场景 | 缩放行为 |
|------|--------|---------|---------|
| **Keyed State** | 每个 key 一份，仅在 KeyedStream 上可用 | 按 key 聚合的计数、求和、窗口状态 | 按 Key Group 重新分配 |
| **Operator State** | 每个 Sub-Task（并行实例）一份 | Kafka Source 的 offset 列表、Broadcast State | 按 List 重新分配 |

Keyed State 可以理解为"分布式 Map"——从每条记录中提取 key，状态按 key 隔离存储。常见的 Keyed State 类型：ValueState（单值）、ListState（列表）、MapState（映射）、ReducingState（聚合）、AggregatingState（聚合输出不同类型）。

Operator State 常见类型：ListState（如 Kafka Source 每个 Sub-Task 维护自己消费的 partition offset 列表）、BroadcastState（广播状态，所有 Sub-Task 持有完整副本）。

#### Broadcast State 详解

Broadcast State 用来解决"**一条主数据流需要动态参照另一份配置/规则**"的场景。典型例子：实时订单流（每秒几万条）需要根据运营后台动态下发的欺诈检测规则来判断是否可疑。规则是动态变化的，不能写死在代码里。

做法是把规则流通过 `broadcast()` 广播出去，每个处理订单的 Sub-Task 都会收到**完整的一份规则副本**，存在本地的 Broadcast State 里。订单数据来了直接从本地状态读规则做判断，不需要查外部系统。

```java
// 1. 定义 Broadcast State 的描述符
MapStateDescriptor<String, Rule> ruleDescriptor =
    new MapStateDescriptor<>("rules", String.class, Rule.class);

// 2. 规则流广播出去
BroadcastStream<Rule> broadcastRules = ruleStream.broadcast(ruleDescriptor);

// 3. 订单流连接广播流，用 BroadcastProcessFunction 处理
orderStream
    .connect(broadcastRules)
    .process(new BroadcastProcessFunction<Order, Rule, Alert>() {

        @Override
        public void processElement(Order order, ReadOnlyContext ctx, Collector<Alert> out) {
            // 处理订单：从 Broadcast State 读取当前规则（只读）
            ReadOnlyBroadcastState<String, Rule> state =
                ctx.getBroadcastState(ruleDescriptor);
            Rule rule = state.get(order.getCategory());
            if (rule != null && rule.matches(order)) {
                out.collect(new Alert(order, "命中规则: " + rule.getName()));
            }
        }

        @Override
        public void processBroadcastElement(Rule rule, Context ctx, Collector<Alert> out) {
            // 处理规则更新：写入 Broadcast State（所有 Sub-Task 都会执行）
            BroadcastState<String, Rule> state = ctx.getBroadcastState(ruleDescriptor);
            state.put(rule.getCategory(), rule);
        }
    });
```

**为什么不直接查 Redis/MySQL？** 每条订单都查一次外部系统，QPS 太高会把外部系统打挂，而且有网络延迟。Broadcast State 把规则存在本地内存，读取是纯本地操作，零延迟、零外部依赖。

**Checkpoint 行为**：每个 Sub-Task 持有完全相同的规则副本，Checkpoint 时各自保存一份完整副本。恢复时不管并行度怎么变，每个 Sub-Task 直接拿到完整副本，不需要像 Keyed State 那样做 Key Group 重分配。

**典型使用场景**：动态规则引擎（风控/反欺诈）、AB 实验配置下发、黑名单/白名单实时更新、维度表小表广播 Join。

**限制**：Broadcast State 适合数据量小、更新频率低的配置数据。如果广播的数据量很大（如百万级维度表），每个 Sub-Task 都持有完整副本，内存开销会很大，应改用 Async I/O 查外部存储。

### 5.1.1 State TTL（状态过期清理）

Keyed State 可以设置 TTL（Time-To-Live），让状态在一定时间后自动清理，防止状态无限增长撑爆磁盘/内存。这对累积型统计（如"最近 24 小时的用户点击数"）非常关键——如果不清理，State 会永远增长，最终因 Checkpoint 过大或 RocksDB 文件过多而失败。

```java
StateTtlConfig ttlConfig = StateTtlConfig
    .newBuilder(Time.hours(24))          // 状态存活 24 小时
    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)  // 每次读写都刷新过期时间
    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)  // 过期后不可见
    .build();

ValueStateDescriptor<String> descriptor = new ValueStateDescriptor<>("userState", String.class);
descriptor.enableTimeToLive(ttlConfig);
ValueState<String> state = getRuntimeContext().getState(descriptor);
```

**清理策略**：

- **惰性清理（Lazy Cleanup）**：读取时判断是否过期，过期则删除。不会主动扫描，但可能留下"僵尸状态"（如果某 key 再也不被访问，就永远不会被清理）。
- **RocksDB Compaction 时清理**：RocksDB 做后台 Compaction 时，会检查并删除过期的状态。需要依赖 RocksDB 的 Compaction 频率，不是实时的。
- **全量快照清理**：在生成 Checkpoint 快照时，清理过期状态。恢复后不会再有过期数据，但 Checkpoint 文件本身仍包含过期数据（只是恢复后会被清理）。

**实践建议**：生产环境必须根据业务语义设置 TTL，避免 State 无限膨胀。例如，按用户会话统计时，会话超时后相关状态就应清理。TTL 时间应略大于业务最大窗口或超时时间。

### 5.2 为算子设置 UID——Savepoint 恢复的关键

Flink 在恢复 State 时，通过 **UID** 将 Savepoint 中的状态映射到对应的算子。UID 和状态唯一绑定。

默认情况下，Flink 通过遍历 JobGraph 并 hash 算子属性自动生成 UID。这很方便但**非常脆弱**——任何对 JobGraph 的改动（加算子、改顺序、调并行度）都可能导致 UID 变化，进而导致状态无法恢复。

```java
// 强烈建议：给所有有状态的算子手动指定 UID（包括 Source 和 Sink）
env.addSource(new MySource()).uid("my-source")
    .keyBy(anInt -> 0)
    .map(new MyStatefulFunction()).uid("my-map")
    .addSink(new DiscardingSink<>()).uid("my-sink");
```

**实践原则**：有状态的算子必须设 UID，无状态的算子设了也没坏处。如果通过 SQL 层或解析器间接生成 Flink Job，要确保解析器能生成稳定的 UID，否则修改 SQL 后 Savepoint 恢复会大面积丢失状态。

### 5.3 State 存储后端

以最常用的 **RocksDBStateBackend** 为例，状态数据的流动分为三层：

```
用户代码 → 本地 RocksDB 文件（实时读写） → HDFS/S3（Checkpoint 异步同步）
```

- 用户代码产生的 State 实时存储在 TaskManager 本地的 RocksDB 文件中，100% 本地性，不需要网络传输。
- Checkpoint 触发时，RocksDB 的增量快照异步同步到远端分布式文件系统（HDFS/S3）。
- 各 Sub-Task 只负责自己所属的那部分状态，不需要互相传输，也不频繁读写 HDFS。
- 作业重启时，从 HDFS 取回 State 数据到本地 RocksDB，恢复现场。

#### RocksDB 增量 Checkpoint 原理

RocksDB 采用 LSM-Tree 结构：新数据先写入内存 MemTable，MemTable 满了之后刷盘生成不可变的 SST 文件（Sorted String Table）。已存在的 SST 文件不会被修改（只能被合并/删除）。

基于这个特性，增量 Checkpoint 的工作方式是：

- **全量 Checkpoint**：将 RocksDB 当前所有的 SST 文件全部上传到 HDFS/S3。
- **增量 Checkpoint**：只上传**自上次 Checkpoint 以来新增或修改的 SST 文件**。因为旧的 SST 文件一旦生成就不会改变，所以不需要重复上传。
- 每次 Checkpoint 只记录一个"文件清单"（manifest），包含哪些 SST 文件属于该次快照。恢复时按照清单从 HDFS 拉取所有涉及的 SST 文件重建本地状态。

**优点与代价**：增量 Checkpoint 大幅减少了上传的数据量，缩短了 Checkpoint 时间。但代价是状态历史链（SST 文件链）会越来越长，恢复时需要回溯并下载多个增量文件，全量恢复时间可能更长。因此生产环境需要定期做全量 Checkpoint（Flink 会自动做全量对齐，也可以通过配置触发）。

#### LSM-Tree 的三种放大效应

RocksDB 底层的 LSM-Tree（Log-Structured Merge-Tree）虽然写入性能极高（顺序写），但存在三种放大效应，直接影响 Flink 大 State 场景的稳定性：

**写放大（Write Amplification）**

用户写入 1 条数据，实际磁盘写入量可能是 10~30 倍。原因是 LSM-Tree 的分层 Compaction 机制：

```
用户写入数据
    ↓
MemTable（内存，几十 MB）
    ↓ 满了刷盘（第 1 次写磁盘）
L0（SST 文件，直接落盘）
    ↓ L0 文件数量达到阈值，触发 Compaction
L1（SST 文件，有序，与 L0 合并）  ← 第 2 次写磁盘
    ↓ L1 大小达到阈值，触发 Compaction
L2（SST 文件，有序，与 L1 合并）  ← 第 3 次写磁盘
    ↓
L3 ... LN（每层大小是上一层的 10 倍）← 每层都会再写一次
```

每层 Compaction 时，RocksDB 需要**读取当前层和下一层的 SST 文件，合并排序后写回下一层**。一条数据从 L0 一路被"搬运"到 LN，每经过一层就被读写一次。假设有 6 层，写放大就是 6 倍；如果层间大小比为 10，Level Compaction 的理论写放大约为 `10 × (层数 - 1)`。

**写放大对 Flink 的影响**：如果 Flink 算子每秒更新 10 万条 State，实际磁盘写入量可能是每秒 100 万~300 万条的 I/O 量。HDD 的随机写 IOPS 只有几百，根本扛不住；即使 SSD 也需要关注磁盘写入寿命（TBW）。

**读放大（Read Amplification）**

读取一条数据时，最坏情况下需要从 L0 到 LN 逐层查找。每层可能需要读取一个 SST 文件的 Index Block + Data Block。L0 的文件之间没有排序，需要全部扫描。读放大 = 需要读取的 SST 文件数。

RocksDB 通过 **Bloom Filter** 缓解读放大：每个 SST 文件附带一个 Bloom Filter，可以快速判断"这个 key 肯定不在这个文件里"，跳过不必要的磁盘读取。Flink 默认启用了 Bloom Filter（`state.backend.rocksdb.bloom-filter.per-key-bits`）。

**空间放大（Space Amplification）**

同一个 key 可能在多层中都有记录（旧值在低层，新值在高层），直到 Compaction 合并后才会清除旧值。空间放大 = 磁盘实际占用 / 有效数据量。

**两种 Compaction 策略对比**：

| 策略 | 写放大 | 读放大 | 空间放大 | 适用场景 |
|------|--------|--------|---------|---------|
| **Size-Tiered**（默认） | 低（每层内合并，层间不交叉） | 高（L0 文件多，无序） | 高（同 key 多副本） | 写多读少 |
| **Level Compaction** | 高（层间合并，写入量大） | 低（每层有序，Bloom Filter 有效） | 低（及时清理旧值） | 读写均衡、大 State |

**Flink 生产优化建议**：

```yaml
# flink-conf.yaml 中的 RocksDB 调优
state.backend.rocksdb.predefined-options: FLASH_SSD_OPTIMIZED  # SSD 优化预设，使用 Level Compaction
state.backend.rocksdb.memory.managed: true                     # 使用 Flink 托管内存，避免 OOM
state.backend.rocksdb.writebuffer.size: 64mb                   # 单个 MemTable 大小（默认 64MB）
state.backend.rocksdb.writebuffer.count: 3                     # MemTable 数量（默认 2）
state.backend.rocksdb.block.cache-size: 256mb                  # Block Cache 大小（读缓存）
```

- 使用 SSD 是第一优先级，可以把写放大的 I/O 代价降低一个数量级。
- 大 State 场景下切换为 Level Compaction（`FLASH_SSD_OPTIMIZED` 预设），用更高的写放大换取更低的读放大和空间放大。
- 增大 MemTable 大小和数量，减少刷盘频率，让 Compaction 更高效（但会占用更多托管内存）。

#### 增量 vs 全量 Checkpoint 的选择

增量和全量 Checkpoint 是可以选择的，但**只有 RocksDBStateBackend 才支持增量**，MemoryStateBackend 和 FsStateBackend 只能全量（因为它们的状态在内存中，没有 SST 文件的"不可变"特性可利用）。

**配置方式**：

```java
// 方式一：代码中直接配置（推荐）
// 构造参数 true = 启用增量 Checkpoint
env.setStateBackend(new EmbeddedRocksDBStateBackend(true));

// 方式二：flink-conf.yaml 全局配置
// state.backend: rocksdb
// state.backend.incremental: true    # 启用增量（默认 false）
```

**如何选择**：

| 模式 | 快照速度 | 快照大小 | 恢复速度 | 适用场景 |
|------|---------|---------|---------|---------|
| **全量** | 慢（每次上传全部 SST 文件） | 大（完整状态） | 快（只下载一份完整快照） | 状态小（< 1GB）、恢复速度优先 |
| **增量** | 快（只上传新增 SST 文件） | 小（仅增量部分） | 可能较慢（需回溯增量链） | 状态大（GB~TB 级）、生产首选 |

**生产建议**：状态超过几百 MB 就应该开启增量 Checkpoint。Flink 内部会自动管理增量链，当链太长时自动做一次全量基线对齐，不需要手动干预。如果对恢复时间非常敏感（如金融场景），可以在低峰期手动触发一次 Savepoint（全量快照），保证恢复时不需要回溯太长的增量链。

| State Backend | 存储位置 | 适用场景 | 特点 |
|---------------|---------|---------|------|
| **MemoryStateBackend** | 纯内存（Java 堆） | 验证、测试 | 不推荐生产，State 大小受 JVM 堆限制 |
| **FsStateBackend** | 内存 + 文件（HDFS/S3） | 中小规模 State | State 在内存中，Checkpoint 时写文件 |
| **RocksDBStateBackend** | 本地 RocksDB 文件 + HDFS/S3 | 大规模 State（生产首选） | 支持增量 Checkpoint，State 大小不受 JVM 堆限制 |

### 5.4 State 重分布——改变并行度时的状态分配

Flink 不支持运行时动态改变并行度，必须先停止作业，修改并行度后从 Savepoint 恢复。改变并行度后，State 怎么分配给新的 Sub-Task？

**Operator State 的重分布**：

- **ListState**：将所有 Sub-Task 的 List 取出合并，然后按元素个数均匀分配给新的 Sub-Task。
- **UnionListState**：将所有 List 拼接起来，不做划分，直接完整分发给每个新的 Sub-Task（由用户自行处理）。
- **BroadcastState**：直接复制到所有新的 Sub-Task（每个 Sub-Task 持有完整副本）。

**Keyed State 的重分布——Key Group 机制**：

如果没有状态，改变并行度只需要重新分配数据流即可。但 Keyed State 的状态数据存在 HDFS 里，并行度变化后需要决定哪些状态分给哪个 Sub-Task。

最朴素的想法是按 `hash(key) % newParallelism` 重新分配。但问题在于：Checkpoint 时状态是顺序写入文件的，恢复时需要随机读（HDFS 不带按 key 的索引），效率极低；而且重新分配后各 Sub-Task 处理的 key 可能和之前完全不同，本地性很差。

为解决这个问题，Flink 引入了 **Key Group（键组）**：

- Key Group 是 Keyed State 分配的**原子单位**，不能再细分。
- Key Group 的数量 = **最大并行度（maxParallelism）**，索引范围为 `[0, maxParallelism - 1]`。
- 每个 Sub-Task 处理一个或多个 Key Group。

**key 如何分配到 Key Group？** 对 key 做两重哈希（一次取 hashCode，一次做 MurmurHash）后对最大并行度取余：

```
keyGroupIndex = MathUtils.murmurHash(key.hashCode()) % maxParallelism
```

**Key Group 如何分配到 Sub-Task？** 由并行度、最大并行度和 Sub-Task 索引共同决定：

```
// 简化逻辑
int keyGroupsPerTask = maxParallelism / parallelism  // 均匀分配
startGroup = subTaskIndex * keyGroupsPerTask
endGroup = startGroup + keyGroupsPerTask - 1
```

例如，最大并行度 = 10，当前并行度从 3 改为 4：

```
并行度 = 3 时：
  Sub-Task 0 → Key Group [0, 1, 2, 3]    (4 个)
  Sub-Task 1 → Key Group [4, 5, 6]        (3 个)
  Sub-Task 2 → Key Group [7, 8, 9]        (3 个)

并行度 = 4 时（从 Savepoint 恢复）：
  Sub-Task 0 → Key Group [0, 1]           (2 个)
  Sub-Task 1 → Key Group [2, 3, 4]        (3 个)
  Sub-Task 2 → Key Group [5, 6, 7]        (3 个)
  Sub-Task 3 → Key Group [8, 9]           (2 个)
```

每个 Sub-Task 只需要从 Checkpoint 中读取自己负责的 Key Group 的数据，不需要读取整个文件，解决了随机读和本地性问题。

### 5.5 最大并行度（Max Parallelism）

Key Group 的数量 = 最大并行度，这意味着**当前并行度不能超过最大并行度**，否则有 Sub-Task 分不到 Key Group 变成空转，Flink 会直接报错：

```
Caused by: org.apache.flink.runtime.JobException: 
  Vertex Map's parallelism (4) is higher than the max parallelism (2).
  Please lower the parallelism or increase the max parallelism.
```

**最大并行度一旦设置就不可轻易修改**——因为 Key Group 数量变了，Checkpoint 中的状态快照无法映射到新的 Key Group，所有状态快照会失效。

**默认值规则**（未手动设置时）：

- 当并行度 < 128 时，最大并行度默认 = 128
- 当并行度 ≥ 128 时，最大并行度 = `parallelism + parallelism / 2`，上限为 32768（2^15）

**设置方式**：

```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.getConfig().setMaxParallelism(4);
```

**实践建议**：最大并行度应根据未来数据增量预估设置——当前并行度 ≤ 最大并行度，留出扩容空间。但不要设得过大，因为 Key Group 数量越多，状态元数据越大，Checkpoint 快照也随之增大，会降低性能。在满足业务需求的前提下设置尽可能小的最大并行度。

### 5.6 Savepoint 恢复规则

从 Savepoint 恢复时，Flink 按 UID 匹配算子状态。以下是各种代码变更的恢复情况：

| 变更类型 | 能否恢复 | 说明 |
|---------|---------|------|
| 算子顺序改变，UID 不变 | 可以 | 按 UID 匹配，与位置无关 |
| 新增无状态算子 | 可以 | 无状态不涉及恢复 |
| 新增有状态算子 | 可以（新算子无初始状态） | 不影响已有算子恢复 |
| 删除有状态算子 | 默认报错 | 需加 `-allowNonRestoredState`（`-n`）跳过 |
| UID 变了 | 恢复失败 | 找不到对应状态，最常见的事故 |
| 调整并行度 | 可以（≤ maxParallelism） | 按 Key Group 重新分配 |
| 修改最大并行度 | 状态失效 | Key Group 数量变化，无法映射 |

#### Savepoint 格式与兼容性

Flink 1.11+ 开始，Checkpoint 和 Savepoint 的格式已经统一（都基于 `canonical` 格式），两者可以互换使用。但它们的**默认保存路径**不同：Checkpoint 使用 `state.checkpoints.dir`，Savepoint 使用 `state.savepoints.dir`。

从 Savepoint 恢复时，如果算子拓扑发生变化（如删除有状态算子），需要加 `--allowNonRestoredState`（或 `-n`）跳过缺失的状态，否则恢复会报错。新增有状态算子不影响恢复（新算子从空状态启动）。

```bash
# 从 Savepoint 恢复，允许跳过缺失的状态
flink run -s hdfs://path/to/savepoint -n -c com.example.MyJob my-job.jar
```

### 5.7 大 State 处理策略

生产环境中，State 可能膨胀到几十 GB 甚至几百 GB（如全量用户画像、长时间窗口聚合），直接决定作业能否稳定运行。以下是处理大 State 的核心策略：

#### 1. 选择 RocksDBStateBackend（生产唯一选择）

MemoryStateBackend 和 FsStateBackend 都将 State 放在 JVM 堆内存中，受 JVM 堆大小限制，且大堆会导致 GC 停顿和 OOM。RocksDBStateBackend 将 State 放在本地磁盘（RocksDB 文件），不受 JVM 堆限制，是唯一适合大 State 的生产方案。

```java
// 配置 RocksDBStateBackend，Checkpoint 保存到 HDFS
env.setStateBackend(new EmbeddedRocksDBStateBackend());
env.getCheckpointConfig().setCheckpointStorage("hdfs://namenode:8020/flink/checkpoints");
```

**RocksDB 理论上能用满磁盘吗？** 理论上 RocksDB 的数据存在本地磁盘，不受 JVM 堆限制，上限就是磁盘容量。但实际生产中有三个约束让它远远达不到"用满磁盘"：

- **托管内存（Managed Memory）限制**：RocksDB 的 MemTable（写缓冲）、Block Cache（读缓存）、Index/Filter 都在内存中，Flink 通过 `taskmanager.memory.managed.fraction`（默认 0.4，即 Task 总内存的 40%）控制上限。内存太小会导致 RocksDB 频繁刷盘和 Compaction，磁盘 I/O 成为瓶颈，吞吐量急剧下降。
- **Checkpoint 上传瓶颈**：本地 State 越大，Checkpoint 时需要上传到 HDFS/S3 的数据越多（即使是增量也有增量链回溯），如果超过 `checkpoint.timeout` 就会失败。Checkpoint 反复失败意味着作业无法容错恢复。
- **磁盘 I/O 写放大**：RocksDB 的 LSM-Tree 存在写放大问题——实际写入磁盘的数据量可能是用户写入的 10~30 倍（因为多层 Compaction）。如果用的是 HDD 而不是 SSD，State 几十 GB 就可能把磁盘 I/O 打满。

所以实际上限不是磁盘容量，而是"Checkpoint 能在超时时间内完成"和"磁盘 I/O 不成为瓶颈"这两个约束中更紧的那个。**生产建议**：单个 TaskManager 的 State 尽量控制在几十 GB 以内，超过则通过增加并行度分散 State。磁盘务必使用 SSD，HDD 在大 State 场景下性能完全不可接受。

#### 2. 增量 Checkpoint + 定期全量对齐

增量 Checkpoint 只上传新增 SST 文件，大幅减少上传数据量。但增量链过长会导致恢复变慢。Flink 会自动在增量链达到一定长度时做全量对齐，也可以通过 `state.backend.incremental` 配置开启/关闭。保持增量开启（默认），无需特殊处理。

#### 3. State TTL——必须设置

大 State 最常见的根源是"只写不删"——数据源源不断流入，但过期数据永远不清理。必须根据业务语义设置 TTL：

- 用户会话统计：TTL = 会话最大超时时间 + 缓冲
- 7 日留存窗口：TTL = 7 天 + 1 天
- 实时去重：TTL = 业务允许的重复时间窗口（如 24 小时）

```java
StateTtlConfig ttl = StateTtlConfig.newBuilder(Time.days(7))
    .setUpdateType(OnCreateAndWrite)
    .setStateVisibility(NeverReturnExpired)
    .cleanupIncrementally(10, true)  // 增量清理，每 10 条读取触发一次清理
    .build();
```

#### 4. 状态拆分：MapState 替代 ValueState<Set> / ValueState<List>

当需要存储"某个 key 关联的大量子项"时，不要用一个 ValueState 存一个大集合，而是用 MapState：

```java
// 错误：ValueState<Set<String>> 存所有用户，集合越来越大
ValueState<Set<String>> allUsersState = getRuntimeContext().getState(
    new ValueStateDescriptor<>("users", TypeInformation.of(new TypeHint<Set<String>>() {})));

// 正确：MapState<userId, Boolean> 按需存，配合 TTL 自动清理
MapState<String, Boolean> userMapState = getRuntimeContext().getMapState(
    new MapStateDescriptor<>("users", String.class, Boolean.class));
```

MapState 的每个 entry 可以独立设置 TTL（RocksDB 中每个 entry 对应独立的 key），过期后 RocksDB Compaction 可以逐条清理。而 ValueState 的 TTL 只能整体过期，无法细粒度清理。

#### 5. 状态压缩与序列化优化

- **使用 Protobuf / Avro 代替 Java 原生序列化**：原生序列化体积大、速度慢，且对 Schema 变化敏感。Protobuf 可以将状态体积减少 50% 以上。
- **状态类型注册器（TypeSerializer）**：自定义 POJO 时，确保 Flink 能推断出 TypeSerializer，避免 fallback 到 Kryo（Kryo 兼容性差，版本升级可能破坏序列化）。
- **禁用泛型快照（禁用 GenericTypeInfo）**：尽量用 `TypeHint` 或 `TypeInformation` 显式声明类型，避免 Flink 使用 Kryo 作为 fallback。

```java
// 显式声明类型，避免 Kryo
TypeInformation<MyState> stateType = TypeInformation.of(new TypeHint<MyState>() {});
ValueStateDescriptor<MyState> descriptor = new ValueStateDescriptor<>("state", stateType);
```

#### 6. 大状态排查与监控

| 监控指标 | 含义 | 告警阈值 |
|---------|------|---------|
| `rocksdb_memtable_flush_duration` | MemTable 刷盘耗时 | > 5s 告警，可能写放大严重 |
| `rocksdb_compaction_times` | Compaction 次数 | 突增告警，可能 SST 文件过多 |
| `rocksdb_num_immutable_mem_tables` | 不可变 MemTable 数量 | > 3 告警，写入压力过大 |
| `checkpoint_duration` | Checkpoint 总耗时 | > interval 的 50% 告警 |
| `checkpoint_state_size` | 单次 Checkpoint 状态大小 | 持续增长且无收敛趋势告警 |
| `numRecordsInPerSecond` / `numRecordsOutPerSecond` | 输入/输出速率 | 反压时 `out` 远小于 `in` |

**排查方法**：Flink Web UI → Task Metrics → 按 Sub-Task 查看 `rocksdb_estimate_live_data_size`，如果某个 Sub-Task 的 RocksDB 数据量远大于其他，说明数据倾斜或 Key 设计不合理。

#### 7. 状态清理策略优化

RocksDB 默认的 Compaction 策略是 Size-Tiered，大 State 下建议改为 **Level Compaction**（通过 `state.backend.rocksdb.predefined-options` 配置为 `FLASH_SSD_OPTIMIZED`），减少读放大和写放大：

```java
// 配置 RocksDB 预定义选项（优化 SSD 场景）
env.setStateBackend(new EmbeddedRocksDBStateBackend());
DefaultConfigurableOptionsFactory optionsFactory = new DefaultConfigurableOptionsFactory();
optionsFactory.setRocksDBOptions("state.backend.rocksdb.predefined-options", "FLASH_SSD_OPTIMIZED");
optionsFactory.setRocksDBOptions("state.backend.rocksdb.memory.managed", "true");
env.setStateBackend(new EmbeddedRocksDBStateBackend(true, optionsFactory));
```

#### 8. 大状态作业的扩容与缩容

- **扩容**：增加并行度（不超过 maxParallelism），Key Group 重新分配，各 Sub-Task 负载降低。但 Savepoint 恢复时，每个 Sub-Task 需要从 HDFS 拉取属于自己的 Key Group 数据，首次启动可能较慢。
- **缩容**：减少并行度，多个 Sub-Task 的状态合并到更少的 Sub-Task。注意：缩容后每个 Sub-Task 的 State 变大，需要确保本地磁盘和内存足够。
- **分区重设计**：如果某些 Key 的数据量远大于其他（如"匿名用户" key 占 80%），考虑重新设计 key（如加随机后缀打散，或拆分冷热 key）。

---

## 六、Flink SQL


Flink 也支持 SQL 接口，语法和 Hive SQL / 标准 SQL 类似，但增加了流处理专用语法：

```sql
-- 创建 Kafka 数据源表
CREATE TABLE user_clicks (
    user_id BIGINT,
    url STRING,
    click_time TIMESTAMP(3),
    WATERMARK FOR click_time AS click_time - INTERVAL '5' SECOND  -- Watermark
) WITH (
    'connector' = 'kafka',
    'topic' = 'clicks',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format' = 'json'
);

-- 每 10 分钟统计每个用户的点击次数（滚动窗口）
SELECT 
    user_id,
    TUMBLE_START(click_time, INTERVAL '10' MINUTE) as window_start,
    COUNT(*) as click_count
FROM user_clicks
GROUP BY user_id, TUMBLE(click_time, INTERVAL '10' MINUTE);
```

---

## 七、面试深度剖析

### 考点 1：Flink vs Spark Streaming

> **面试官**：「Flink 和 Spark Streaming 有什么区别？」

核心区别是计算模型：Spark Streaming（包括 Structured Streaming）是**微批次**——把流按时间间隔切成小批次，每个批次当作 RDD/DataFrame 处理，延迟在秒级。Flink 是**逐条处理**——每条事件到达即计算，延迟在毫秒级。Flink 在事件时间处理、Watermark、Exactly-Once 语义上更成熟。

### 考点 2：Checkpoint 和 Savepoint 的区别

> **面试官**：「Checkpoint 和 Savepoint 有什么区别？」

Checkpoint 是 Flink 自动定期触发的快照，用于故障恢复，生命周期和任务绑定（任务取消后可配置保留或删除）。Savepoint 是用户手动触发的快照，用于有计划的停机（升级代码、调整并行度）和迁移，永久保留。两者的格式和恢复机制相同。

### 考点 3：Watermark 怎么工作

> **面试官**：「如果数据严重乱序，Watermark 怎么设置？」

Watermark 的延迟容忍时间需要在"结果准确性"和"延迟"之间权衡。设得太小，迟到数据被丢弃，结果不准；设得太大，窗口迟迟不触发，延迟高。实践中先统计数据的乱序程度（P99 延迟），把 Watermark 设为 P99 值。严重迟到的数据用 Side Output 收集后异步修正。

### 考点 4：反压（Backpressure）

> **面试官**：「Flink 处理不过来怎么办？」

Flink 有天然的反压机制：下游算子处理不过来时，它的输入缓冲区满了，上游算子的输出缓冲区也随之满了，一层层传导到 Source，Source 自动降低读取速率。不需要额外配置。反压持续过久说明需要加资源（增加并行度）或优化算子逻辑。

### 考点 5：Key Group 和最大并行度

> **面试官**：「Flink 改并行度时状态怎么恢复？Key Group 是什么？」

Flink 用 Key Group 作为 Keyed State 重分配的原子单位。Key Group 数量 = 最大并行度，key 通过 `murmurHash(key.hashCode()) % maxParallelism` 分配到某个 Key Group，每个 Sub-Task 负责一段连续的 Key Group 范围。改变并行度时，Key Group 的归属重新划分，但 Key Group 到 key 的映射不变，所以状态能正确恢复。最大并行度一旦设定不能随意改，否则 Key Group 数量变化导致状态失效。默认值：并行度 < 128 时取 128，否则取 `parallelism + parallelism / 2`。

### 考点 6：Savepoint 恢复的坑

> **面试官**：「修改 Flink 作业代码后从 Savepoint 恢复，有哪些注意事项？」

最关键的是 UID——不手动指定 UID 时，Flink 自动 hash 生成，代码任何改动都可能导致 UID 变化，状态恢复失败。所以有状态的算子必须手动 `.uid()`。其他规则：新增无状态算子不影响恢复；删除有状态算子需加 `-n` 跳过；调整并行度可以恢复（≤ maxParallelism）；修改最大并行度会导致状态失效。

### 考点 7：Watermark 与 Barrier 的区别

> **面试官**：「Watermark 和 Barrier 有什么区别？」

两者都是混在数据流里的特殊记录，但本质不同。Watermark 是 Source 根据数据时间戳自主生成的，驱动事件时间推进，决定窗口何时触发、Timer 何时执行。Barrier 是 JobManager 通过 RPC 定时注入的，标志快照边界，协调全局状态一致性。Watermark 在多输入时取 min，Barrier 在多输入时对齐。一句话：Watermark 管"时间到了没"，Barrier 管"快照切在哪"。

### 考点 8：两阶段提交与端到端 Exactly-Once

> **面试官**：「Flink 怎么做到端到端 exactly-once？两阶段提交是怎么工作的？」

端到端 exactly-once 取决于整个数据管道的最弱一环，分为三层：内部 Checkpoint 保证、Source 支持重放、Sink 支持事务或幂等。Flink 内部通过 Checkpoint + Barrier 对齐实现状态一致性；Source 端如 Kafka 将 offset 保存到 State，故障后按 offset 重放；Sink 端通过 2PC 与外部事务系统配合：收到 Barrier 时预提交数据（写入但不 commit），Checkpoint 完成后收到 JobManager 确认再正式提交。Kafka 的 Consumer 下游必须设置 `read_committed` 隔离级别，否则可能读到未提交数据。生产环境常见坑是 Kafka 事务超时（默认 60s）小于 Flink Checkpoint 间隔，导致事务被 abort 数据丢失。若下游支持幂等，也可选择 at-least-once + 下游去重，性能更好。

### 考点 9：Checkpoint 为什么越大恢复越慢？增量 Checkpoint 恢复为什么可能比全量慢？

> **面试官**：「Checkpoint 大小和延迟有什么关系？增量 Checkpoint 恢复为什么可能比全量慢？」

Checkpoint 大小直接影响三个维度：快照写入时间（越大写入 HDFS 越慢）、网络带宽占用（并发 Checkpoint 时可能挤占业务带宽）、恢复时间（需要下载并反序列化更多状态数据）。

增量 Checkpoint 恢复快的原因：恢复时需要回溯从上次全量 Checkpoint 以来的所有增量文件链。如果增量链很长（比如几十个增量 Checkpoint 后做一次全量），恢复时需要下载几十个文件并在本地重建 RocksDB，可能比下载一个全量文件更慢。因此 Flink 内部会定期做全量对齐（或触发全量 Checkpoint），以控制增量链长度。

### 考点 10：为什么 Flink 1.11+ 引入 Unaligned Checkpoint？解决了什么问题？

> **面试官**：「Unaligned Checkpoint 解决了什么问题？和对齐 Checkpoint 的本质区别是什么？」

对齐 Checkpoint 在反压场景下，下游处理不过来导致 Barrier 无法对齐，对齐时间无限拉长，最终 Checkpoint 超时。Unaligned Checkpoint 通过不对齐、直接把缓冲数据也存入快照的方式，让 Checkpoint 不受反压影响，对齐时间几乎为零。本质区别在于：对齐 Checkpoint 的算子状态只包含 barrier 之前的数据结果；Unaligned Checkpoint 额外把 barrier 之后还在缓冲区的数据也快照进去，恢复时重新注入重新计算。用更大的快照体积换取更稳定的 Checkpoint 成功率。

**Unaligned Checkpoint 的副作用**：

- **快照体积显著增大**：除了算子状态本身，还需要额外保存所有输入缓冲区（input buffer）和输出缓冲区（output buffer）中的在途数据。反压越严重、缓冲区堆积越多，快照越大。极端情况下快照体积可能膨胀数倍。
- **恢复时间更长**：恢复时除了重建算子状态，还需要把快照中的缓冲数据重新注入各算子的输入队列，然后重新计算。缓冲数据越多，恢复后的"追赶"时间越长。
- **不支持 Savepoint**：Unaligned Checkpoint 只能用于故障恢复，不能用于 Savepoint（手动触发的全量快照），因为 Savepoint 的格式要求与 Unaligned 的缓冲数据快照不兼容。升级作业、调整并行度仍然必须用对齐模式的 Savepoint。
- **磁盘 I/O 压力增大**：更大的快照意味着写入 HDFS/S3 的数据量更大，对存储系统的吞吐和网络带宽有更高要求。如果 HDFS 本身就是瓶颈，可能导致快照写入变慢。
- **与部分功能不兼容**：Flink 早期版本中（1.11~1.14），Unaligned Checkpoint 与增量 Checkpoint、某些 Source/Sink 连接器存在兼容性问题。Flink 1.15+ 已修复大部分兼容性问题，但使用前仍需确认连接器版本。

**生产实践建议**：优先使用对齐 Checkpoint（默认行为），仅在 Checkpoint 频繁因反压超时时才启用 Unaligned Checkpoint。也可以配置 `execution.checkpointing.aligned-checkpoint-timeout`（如 30 秒），让 Flink 在对齐超时后自动降级为 Unaligned，兼顾两者优势。

### 考点 11：Broadcast State 的 Checkpoint 有什么特点？

> **面试官**：「Broadcast State 改变并行度时怎么恢复？」

Broadcast State 属于 Operator State，所有并行子任务持有**完全相同的副本**。Checkpoint 时每个 Sub-Task 都会保存完整副本；恢复时（无论并行度是否变化），每个 Sub-Task 都会获得完整副本。因此 Broadcast State 不涉及 Key Group 重分配，也不受并行度变化影响。典型场景是动态配置（如规则引擎），通过 Broadcast Stream 将规则广播到所有 Sub-Task，每个 Sub-Task 本地保存一份规则状态，避免每个 key 都存一份规则。

### 考点 12：场景题——每天从 00:00 开始，每小时累计统计 PV/UV

> **面试官**：「有一张 Kafka topic `user_visit_log`，字段有 user_id、user_type、visit_time。要求每天从 00:00 开始，每隔 1 小时输出一次截至当前小时的累计访问指标（按 user_type 分组，累计 PV 和 UV）。怎么设计 Flink SQL 或 DataStream 实现？」

**核心思路**：这不是普通的滚动窗口（Tumbling Window），而是**累积窗口（Cumulative Window）**——窗口起点固定（每天 00:00），终点逐步扩展，每次输出从起点到当前时刻的累计结果。Flink SQL 1.13+ 提供了 `CUMULATE` 窗口函数直接支持：

```sql
SELECT
  user_type,
  CUMULATE_START(visit_time, INTERVAL '1' HOUR, INTERVAL '1' DAY) AS window_start,
  CUMULATE_END(visit_time, INTERVAL '1' HOUR, INTERVAL '1' DAY) AS window_end,
  COUNT(*) AS pv,
  COUNT(DISTINCT user_id) AS uv
FROM user_visit_log
GROUP BY
  user_type,
  CUMULATE(visit_time, INTERVAL '1' HOUR, INTERVAL '1' DAY);
-- 语法：CUMULATE(timeCol, step, maxSize)
--   step    = INTERVAL '1' HOUR  → 每隔 1 小时输出一次
--   maxSize = INTERVAL '1' DAY   → 总跨度为 1 天（00:00 起始）
```

如果用 DataStream API，需要自己维护状态：用 `ProcessFunction` 或 `KeyedProcessFunction`，按 `user_type` 分组，状态中保存累计的 PV 计数和 UV 的 `MapState<user_id, Boolean>`（或 `ValueState<Set<String>>`，但大状态用 `MapState` 更好）。每来一条数据就更新状态，然后用 `Timer`（每天每小时触发）输出当前累计值。注意：状态必须设置 TTL（比如 25 小时），防止无限增长。

### 考点 13：场景题——实时 TopN（每小时热门商品 Top10）

> **面试官**：「实时统计每个小时销量最高的 Top10 商品，数据流中有商品 ID 和订单时间，怎么实现？」

**核心思路**：`TumblingWindow` + `aggregate` 先聚合出每个商品每小时的销量，然后用 `KeyedProcessFunction` 维护一个大小为 N 的优先队列（小顶堆）做 TopN。关键在于：**先按窗口聚合，再在每个窗口内做 TopN 排序**，避免把所有数据都存到内存里。

Flink SQL 实现更简洁：

```sql
SELECT *
FROM (
  SELECT
    product_id,
    window_start,
    window_end,
    sales_cnt,
    ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY sales_cnt DESC) AS rn
  FROM (
    SELECT
      product_id,
      TUMBLE_START(order_time, INTERVAL '1' HOUR) AS window_start,
      TUMBLE_END(order_time, INTERVAL '1' HOUR) AS window_end,
      COUNT(*) AS sales_cnt
    FROM orders
    GROUP BY product_id, TUMBLE(order_time, INTERVAL '1' HOUR)
  )
) WHERE rn <= 10;
```

**踩坑点**：如果商品数量极大，`COUNT(DISTINCT)` 或 `ROW_NUMBER()` 的状态可能很大。如果只需要近似 TopN，可以用 `AggState` 或 `MapState<product_id, count>` 配合 TTL，避免状态无限增长。

### 考点 14：场景题——双流 Join（订单流 join 商品信息流）

> **面试官**：「有两条流：订单流（实时）和商品信息流（更新较少）。怎么把订单和商品信息 Join 起来？有哪些方案？」

**四种方案对比**：

| 方案 | 适用场景 | 原理 | 缺点 |
|------|---------|------|------|
| **Window Join** | 两条流都有明确时间窗口，且在窗口内能匹配 | 划分时间窗口（Tumbling/Sliding/Session），窗口内做笛卡尔积匹配 | 窗口外的数据无法匹配，容易丢数据；大窗口状态大 |
| **Interval Join** | 订单流在商品流前后一段时间内有匹配 | `KEY BY` 后，`between` 指定时间范围（如订单前后 5 分钟），状态保留范围内的数据 | 需要 key 相同，时间范围不能太大，否则状态爆炸 |
| **Temporal Table Join** | 商品流是维度表（更新少），订单流是事实流 | 商品流作为 Temporal Table（类似版本表），订单流按处理时间 lookup | 只支持处理时间，不支持事件时间；商品流更新频率不能太高 |
| **Async I/O + 外部存储** | 商品信息在外部（Redis/MySQL/HBase） | 订单流用 Async I/O 异步查询外部维度表 | 有外部依赖延迟，需要处理查询失败和超时 |

**推荐策略**：如果商品信息更新频率低且全量可控，可以先用 Broadcast Stream 将商品信息广播到所有 Task 做 `Broadcast Join`；如果商品信息量大，用 `Async I/O` 查 Redis/HBase；如果两条流都是高频流，用 `Interval Join` 控制时间窗口。

### 考点 15：场景题——数据倾斜怎么处理？

> **面试官**：「Flink 作业中某个 Task 的并行度处理量远大于其他 Task，导致整体延迟，怎么排查和解决？」

**排查方法**：

- 看 Flink Web UI 的 `Backpressure` 和 `Records Received/Sent`，确认哪个 Sub-Task 接收或发送量异常。
- 看 Checkpoint 的 `Subtask-level Checkpoint Duration`，如果某个 Sub-Task 耗时远大于其他，说明该 Sub-Task 的状态量远大于其他，大概率是数据倾斜。

**解决方案**：

- **两阶段聚合**：先局部预聚合（如加随机前缀 `keyBy(randomPrefix + key)`），再全局聚合。Flink SQL 会自动做 `Local-Global` 聚合优化。
- **拆分热点 key**：如果某个 key 的数据量占 80%，可以把这个 key 的数据拆成多个子 key（比如 `key_0`、`key_1`...`key_N`），分散到多个 Sub-Task，最后再合并。
- **自定义分区器**：实现 `Partitioner` 接口，按数据分布自定义分区逻辑，避免默认的 `hashCode % parallelism` 导致倾斜。
- **状态层面**：如果倾斜是由 Keyed State 过大导致，考虑增加 `maxParallelism` 或重新设计 key（如从用户 ID 改为"用户 ID + 设备 ID"）。

### 考点 16：场景题——反压（Backpressure）怎么排查和处理？

> **面试官**：「Flink 作业出现反压，数据消费变慢，怎么排查和处理？」

#### 一、现象：怎么发现反压？

| 观察方式 | 具体指标 | 含义 |
|---------|---------|------|
| **Flink Web UI** | `Backpressure` 标签页显示 `OK / LOW / HIGH` | 从 Source 往下游逐个看，第一个出现 `HIGH` 的算子就是瓶颈所在 |
| **Metrics** | `outPoolUsage` 持续 > 90% | 该算子的输出缓冲区满了，说明**下游消费不过来** |
| **Metrics** | `inPoolUsage` 持续 > 90% | 该算子的输入缓冲区满了，说明**自己处理不过来** |
| **Metrics** | `numRecordsOutPerSecond` 突降 | 算子吞吐量骤降，下游开始堆积 |
| **Checkpoint** | 某个 Sub-Task 的 Checkpoint Duration 远大于其他 | 该 Sub-Task 状态过大或处理过慢 |
| **日志** | TaskManager 频繁 Full GC | 堆内存不足 |

**定位瓶颈算子的方法**：从 Source 开始往下游看，第一个 `outPoolUsage` 正常但 `inPoolUsage` 高的算子就是瓶颈——它处理不过来，导致上游的 `outPoolUsage` 也被撑满，压力一路向上传导到 Source。

#### 二、原因 → 处理方案

**原因 1：算子计算逻辑太重**

现象：某个算子的 CPU 使用率高、`numRecordsOutPerSecond` 远低于 `numRecordsInPerSecond`。

处理方案：

- 增加该算子的并行度，分摊计算压力。
- 优化算子逻辑：避免在 `processElement` 中做复杂计算（如正则匹配、JSON 反序列化可以提前做），减少每条数据的处理耗时。
- 如果是窗口聚合算子，考虑用 `AggregateFunction`（增量聚合）替代 `ProcessWindowFunction`（全量聚合），避免窗口触发时一次性处理大量数据。

**原因 2：State 操作成为瓶颈（大 State / RocksDB 慢查询）**

现象：`rocksdb_compaction_times` 突增、`rocksdb_memtable_flush_duration` > 5s、磁盘 I/O 使用率高。

处理方案：

- 检查 State 是否设置了 TTL，清理过期数据，控制 State 大小。
- 磁盘从 HDD 换成 SSD，消除 RocksDB 的 I/O 瓶颈。
- 增大 RocksDB 的 Block Cache（`state.backend.rocksdb.block.cache-size`），减少磁盘读取次数。
- 检查是否存在数据倾斜（某个 key 的 State 远大于其他），参考考点 15 的倾斜处理方案。

**原因 3：GC 停顿（JVM 堆内存不足）**

现象：TaskManager 日志频繁出现 Full GC，每次停顿几秒甚至几十秒，期间算子完全停止处理数据。

处理方案：

- 增加 TaskManager 的堆内存（`taskmanager.memory.task.heap.size`）。
- 如果使用 FsStateBackend（State 在堆内），切换到 RocksDBStateBackend（State 在堆外磁盘）。
- 检查用户代码是否有内存泄漏（如缓存了大量对象未释放）。

**原因 4：Sink 写入外部系统慢**

现象：Sink 算子的 `inPoolUsage` 高，但 CPU 不高；外部系统（MySQL/ES/Kafka）的写入延迟增大。

处理方案：

- 开启批量写入：将逐条写入改为攒批写入（如 JDBC Sink 的 `batch.size`、ES Sink 的 `bulk.flush.max.actions`）。
- 增加连接池大小或 Sink 并行度。
- 改用异步写入（`AsyncSinkBase` 或 `Async I/O`），让写入不阻塞主线程。
- 检查外部系统本身的瓶颈（如 MySQL 的锁等待、ES 的 refresh interval、Kafka 的分区数不足）。

**原因 5：网络传输瓶颈**

现象：跨 TaskManager 的数据传输量大，网络带宽打满。

处理方案：

- 在 `keyBy` 之前先做 `map` / `filter` 过滤不需要的字段，减少每条数据的体积。
- 避免传输大字段（如把完整日志传到下游，实际只需要其中几个字段）。
- 调整 TaskManager 的网络缓冲区参数（`taskmanager.network.memory.fraction`）。

**原因 6：反压导致 Checkpoint 超时（次生问题）**

现象：Checkpoint 频繁失败，日志报 `Checkpoint expired before completing`。反压导致 Barrier 在缓冲区中排队，迟迟无法对齐。

处理方案：

- 启用 Unaligned Checkpoint（`execution.checkpointing.unaligned: true`），让 Barrier 不需要排队，直接跳到队首。
- 或者配置 `execution.checkpointing.aligned-checkpoint-timeout`（如 30s），对齐超时后自动降级为 Unaligned。
- 增大 `checkpoint.timeout`，给更多时间完成 Checkpoint（治标不治本，需同时解决反压根因）。

#### 三、排查流程总结

```
发现反压（Web UI / Metrics）
    ↓
定位瓶颈算子（第一个 inPoolUsage 高的算子）
    ↓
判断原因类型
    ├─ CPU 高 → 算子逻辑太重 → 优化逻辑 / 加并行度
    ├─ 磁盘 I/O 高 → RocksDB 瓶颈 → SSD / 加 Cache / TTL
    ├─ GC 频繁 → 堆内存不足 → 加内存 / 换 RocksDB
    ├─ Sink 延迟高 → 外部系统慢 → 批量写 / 异步写
    ├─ 网络带宽高 → 传输量大 → 过滤字段 / 减少体积
    └─ Checkpoint 超时 → Barrier 排队 → Unaligned Checkpoint
```

### 考点 17：场景题——实时去重（用户行为去重）

> **面试官**：「实时统计每个用户的首次购买行为，同一个用户多次购买只算一次，怎么实现？」

**核心思路**：本质上就是**按 key 去重**。Flink 提供两种去重机制：

**方案一：状态去重（First-Row）**

完整的 `KeyedProcessFunction` 实现，包含 State 初始化、TTL 配置和去重逻辑：

```java
// 1. 按 user_id 分组，交给去重算子处理
orderStream
    .keyBy(Order::getUserId)
    .process(new FirstRowDeduplicator())
    .addSink(new KafkaSink<>());

// 2. 去重算子：只输出每个用户的首次购买行为
public class FirstRowDeduplicator extends KeyedProcessFunction<String, Order, Order> {

    // 用 ValueState<Boolean> 记录该 key（user_id）是否已经出现过
    // 因为已经按 user_id 做了 keyBy，所以每个 key 只需要一个 Boolean 值
    private transient ValueState<Boolean> seenState;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 配置 State TTL：7 天后自动清理，防止状态无限增长
        StateTtlConfig ttlConfig = StateTtlConfig
            .newBuilder(Time.days(7))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)  // 写入时刷新过期时间
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .cleanupFullSnapshot()  // 全量快照时清理过期状态
            .build();

        ValueStateDescriptor<Boolean> descriptor =
            new ValueStateDescriptor<>("seen", Boolean.class);
        descriptor.enableTimeToLive(ttlConfig);  // 启用 TTL
        seenState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(Order order, Context ctx, Collector<Order> out) throws Exception {
        // 如果 seenState 为 null，说明该用户是首次出现
        if (seenState.value() == null) {
            seenState.update(true);     // 标记为已处理
            out.collect(order);          // 输出首次购买行为
        }
        // 非首次购买，直接丢弃，不输出
    }
}
```

**为什么用 `ValueState<Boolean>` 而不是 `MapState<String, Boolean>`？** 因为已经按 `user_id` 做了 `keyBy`，Flink 会自动按 key 隔离状态，每个 key 只需要一个简单的 Boolean 值标记"是否已出现"。`MapState` 适合在一个 key 下存多个子项的场景（如"一个商户下的多个订单"）。

**Flink SQL 写法更简洁**（SQL 层面的 First-Row 去重）：

```sql
-- Flink SQL 原生去重：按 user_id 分组，只保留每个用户的第一条记录
SELECT user_id, product_id, order_time
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY order_time ASC) AS rn
  FROM orders
) WHERE rn = 1;
```

优点：精确，不需要外部依赖。缺点：状态无限增长（所有用户 ID 都要存）。**必须设置 TTL**，比如"只保留 7 天内的用户状态"，超期的用户可以重新算一次（业务上通常可接受）。

**方案二：Bloom Filter 去重**

用 Bloom Filter 近似判断用户是否已出现。优点：内存占用极小（百万级用户只需几 MB）。缺点：有误判率（可能把新用户误判为已存在，漏掉首次行为）。适合允许少量误差的场景（如广告曝光去重）。

**方案三：HyperLogLog 去重**

如果只需要统计 UV 数量而不需要精确知道每个用户是否首次，可以用 HyperLogLog 近似计数。Flink SQL 的 `COUNT(DISTINCT)` 底层会自动优化。

### 考点 18：场景题——延迟数据怎么处理？

> **面试官**：「Watermark 已经推进到 12:00 了，但突然来了一条 11:55 的数据，怎么处理？」

**Flink 的延迟数据处理机制**：

- **Watermark 的作用**：Watermark 标记了"小于该时间戳的数据应该都已到达"，窗口基于 Watermark 触发。如果 Watermark 已到 12:00，则 11:00~12:00 的窗口已经计算并输出，11:55 的数据属于**延迟数据**。
- **Allowed Lateness**：窗口可以配置 `allowedLateness`，允许在窗口触发后的一段时间内继续接收延迟数据。延迟数据到达时，窗口会重新触发计算（输出更新结果）。适合窗口结果需要修正的场景。

```java
window
    .allowedLateness(Time.minutes(5))  // 窗口触发后，允许 5 分钟内延迟数据到达
    .sideOutputLateData(lateTag);       // 超过 allowedLateness 的延迟数据输出到侧输出流
```

- **Side Output**：超过 `allowedLateness` 的延迟数据会被输出到侧输出流（Side Output），可以单独处理（如写入日志、或进入离线补数流程）。
- **Watermark 策略**：如果业务对延迟数据容忍度低，可以用 `WatermarkStrategy.forBoundedOutOfOrderness` 设置更长的乱序容忍时间，但会增大窗口延迟（Watermark 推进更慢）。需要在"延迟容忍度"和"结果实时性"之间权衡。

#### Watermark 延迟 + allowedLateness 的叠加关系

这两个"延迟"是**叠加关系**，作用在不同阶段。假设 Watermark 乱序容忍 5 分钟、`allowedLateness` 5 分钟，则总共允许数据延迟 10 分钟，但前 5 分钟是"静默等待"（不额外输出），后 5 分钟是"补算更新"（会输出修正结果）。

**第一层：Watermark 乱序容忍（推迟窗口触发）**

```java
WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMinutes(5))
```

Watermark = 当前看到的最大事件时间 - 5 分钟。假设当前最大事件时间是 13:10，Watermark 只推进到 13:05。此时 12:00~13:00 的窗口还不会触发，13:05 之前到达的"迟到"数据仍然能正常进入窗口参与计算。这一层的效果是**推迟窗口触发时机**，让窗口"多等一会儿"，迟到数据静默进入，不会产生额外输出。

**第二层：allowedLateness（窗口触发后的补救）**

窗口触发并输出结果后，窗口状态**不会立即清除**，而是再保留 5 分钟。在这 5 分钟内到达的迟到数据，仍然会被加入窗口并**重新触发一次计算**（输出更新后的结果）。

**用一个具体例子串起来**（`12:00~13:00` 的滚动窗口）：

```
时间线（Watermark 时间）：
12:00 ──────────── 13:00 ──── 13:05 ──── 13:10
  │                  │          │          │
  │   窗口收集数据    │          │          │
  │                  │ Watermark │ allowed  │ 窗口状态
  │                  │ 到达13:00 │ Lateness │ 彻底清除
  │                  │ 窗口触发  │ 到期     │
  │                  │ 输出结果  │          │
  │                  │          │          │
  ├── 正常数据 ──────┤          │          │
  │  （静默进入窗口） ├─ 迟到但  ─┤          │
  │                  │  仍能补算 │          │
  │                  │ （重新输出）│          │
  │                  │          ├─ 超期 ───┤→ 进入 Side Output
```

- **13:05 之前到达**的 12:xx 数据：被 Watermark 容忍，正常进入窗口，窗口还没触发，不算迟到，不产生额外输出。
- **13:05~13:10 之间到达**的 12:xx 数据：窗口已触发，但 `allowedLateness` 还没过期，数据加入窗口并**重新触发计算**，输出修正结果。
- **13:10 之后到达**的 12:xx 数据：窗口状态已清除，数据被丢弃或进入 Side Output。

**容易踩的坑：allowedLateness 导致下游数据重复**

`allowedLateness` 期间的迟到数据会让窗口**重复触发**，下游 Sink 会收到**多条结果**（初始结果 + 一次或多次修正结果）。如果下游是追加写入（如 Kafka、日志文件），同一个窗口会出现多条记录；如果下游是 MySQL，必须用 `UPSERT`（按窗口主键 `INSERT ... ON DUPLICATE KEY UPDATE`）而不是 `INSERT`，否则会出现重复数据。

```java
// 完整的延迟数据处理示例
OutputTag<Event> lateTag = new OutputTag<Event>("late-data") {};

SingleOutputStreamOperator<Result> result = stream
    .keyBy(Event::getUserId)
    .window(TumblingEventTimeWindows.of(Time.hours(1)))
    .allowedLateness(Time.minutes(5))
    .sideOutputLateData(lateTag)
    .aggregate(new MyAggregateFunction());

// 主流：窗口正常结果 + allowedLateness 期间的修正结果
result.addSink(new UpsertMySQLSink());  // 必须用 UPSERT，不能 INSERT

// 侧输出流：超过 allowedLateness 的严重迟到数据
DataStream<Event> lateStream = result.getSideOutput(lateTag);
lateStream.addSink(new LateDataLogSink());  // 写日志或进入离线补数
```

---

[← 6.4 Spark](./04-Spark.md) | [返回本章目录](./README.md) | [6.6 Doris →](./06-Doris.md)
