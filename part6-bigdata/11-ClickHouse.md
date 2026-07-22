# 6.11 ClickHouse

> **一句话定位**：ClickHouse 是俄罗斯 Yandex 开源的列式 OLAP 数据库，以**单表大规模聚合查询的极致性能**著称——同等硬件下，聚合查询速度通常是 MySQL 的 100-1000 倍。

---

## 一、ClickHouse 是什么

### 1.1 定位与适用场景

| 适合 | 不适合 |
|------|--------|
| 大宽表聚合分析（数十亿行） | 高并发点查（QPS > 1000） |
| 实时看板、Ad-hoc 查询 | 事务型业务（OLTP） |
| 日志分析、用户行为分析 | 频繁 UPDATE/DELETE |
| 时序数据分析 | 复杂多表 JOIN |

### 1.2 核心设计哲学

- **列式存储** — 只读取查询涉及的列，IO 大幅减少
- **向量化执行** — 批量处理数据（SIMD 指令），CPU 缓存友好
- **数据压缩** — 同列数据类型相同，压缩率极高（通常 5-10x）
- **无共享架构** — 每个节点独立计算，水平扩展
- **近似计算** — 提供 HyperLogLog、分位数等近似函数，牺牲精度换速度

---

## 二、核心架构

### 2.1 单机架构

```
┌─────────────────────────────────────────┐
│              ClickHouse Server           │
├─────────────────────────────────────────┤
│  SQL Parser → Query Planner → Pipeline  │
│         (向量化执行引擎)                  │
├─────────────────────────────────────────┤
│           MergeTree 引擎族               │
│  ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐      │
│  │Part │ │Part │ │Part │ │Part │ ...   │
│  └─────┘ └─────┘ └─────┘ └─────┘      │
│  (每个 Part 是一个有序的列式数据块)       │
├─────────────────────────────────────────┤
│           磁盘存储（列文件）              │
│  column1.bin │ column1.mrk │ primary.idx │
└─────────────────────────────────────────┘
```

### 2.2 分布式架构

```
┌─────────────────────────────────────────────────┐
│                 Distributed Table                 │
│          (逻辑表，路由查询到各 Shard)             │
├────────────┬────────────┬────────────────────────┤
│  Shard 1   │  Shard 2   │  Shard 3              │
│ ┌────────┐ │ ┌────────┐ │ ┌────────┐           │
│ │Replica1│ │ │Replica1│ │ │Replica1│           │
│ │Replica2│ │ │Replica2│ │ │Replica2│           │
│ └────────┘ │ └────────┘ │ └────────┘           │
├────────────┴────────────┴────────────────────────┤
│              ZooKeeper（协调副本同步）             │
└─────────────────────────────────────────────────┘
```

- **Shard**：数据水平分片，每个 Shard 存一部分数据
- **Replica**：同一 Shard 的多个副本，通过 ZooKeeper 协调同步
- **Distributed Table**：逻辑表，查询时自动路由到各 Shard 并汇总结果

---

## 三、MergeTree 引擎族

MergeTree 是 ClickHouse 最核心的表引擎，所有生产级表几乎都基于它。

### 3.1 MergeTree 基础

```sql
CREATE TABLE events (
    event_date Date,
    user_id UInt64,
    event_type String,
    amount Decimal(18,2)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)    -- 按月分区
ORDER BY (user_id, event_date)        -- 排序键（也是稀疏索引）
SETTINGS index_granularity = 8192;    -- 索引粒度
```

**核心机制**：

| 概念 | 说明 |
|------|------|
| **Part** | 一次写入生成一个 Part（有序的列式数据块），后台异步合并 |
| **Partition** | 数据按分区键物理隔离，DROP PARTITION 秒级删除 |
| **Primary Key（稀疏索引）** | 每 8192 行记录一个索引条目，定位数据块范围 |
| **Merge** | 后台线程定期合并小 Part 为大 Part（类似 LSM Compaction） |

### 3.2 MergeTree 家族

| 引擎 | 特点 | 适用场景 |
|------|------|---------|
| **MergeTree** | 基础引擎，支持排序和分区 | 通用分析 |
| **ReplacingMergeTree** | 合并时按排序键去重（保留最新版本） | 需要去重的场景 |
| **SummingMergeTree** | 合并时按排序键聚合数值列 | 预聚合/汇总表 |
| **AggregatingMergeTree** | 合并时执行聚合函数（更灵活） | 物化视图的底层引擎 |
| **CollapsingMergeTree** | 通过 +1/-1 标记实现逻辑删除/更新 | CDC 场景 |
| **VersionedCollapsingMergeTree** | 带版本号的 Collapsing，解决乱序问题 | 乱序 CDC |

### 3.3 数据写入流程

```
INSERT 数据
  → 按分区键分组
  → 每个分区生成一个新 Part（内存排序 → 写磁盘）
  → 后台 Merge 线程异步合并同分区的小 Part
```

**关键点**：
- 写入是**追加式**的，不会修改已有 Part
- 合并是**异步**的，写入和查询互不阻塞
- 查询时如果有未合并的 Part，会在读取时做合并（对 Replacing/Summing 等引擎）

---

## 四、为什么 ClickHouse 查询快

### 4.1 列式存储

```
行式存储（MySQL）：
  [user_id=1, name="张三", age=25, city="北京"]
  [user_id=2, name="李四", age=30, city="上海"]
  → SELECT AVG(age) 需要读取整行

列式存储（ClickHouse）：
  user_id.bin: [1, 2, 3, 4, ...]
  name.bin:    ["张三", "李四", ...]
  age.bin:     [25, 30, 28, 35, ...]
  city.bin:    ["北京", "上海", ...]
  → SELECT AVG(age) 只读 age.bin，IO 减少 80%+
```

### 4.2 向量化执行

传统数据库逐行处理（Volcano 模型）；ClickHouse 按列批量处理（向量化）：

```
传统 Volcano 模型:
  for each row:
    调用 next() 虚函数 → 从子算子拉取一行
    evaluate expression → 计算表达式
    output → 输出一行
  → 每行都有一次虚函数调用开销

向量化模型:
  for each batch (8192 rows):
    load column values into contiguous memory
    → SIMD batch compute (一条指令处理 4-16 个值)
    → output batch
  → 每批只有一次函数调用
```

**SIMD（Single Instruction, Multiple Data）指令详解**：

CPU 提供的"一条指令同时计算多个数据"的硬件能力：

```
普通指令：
  ADD a1, b1 → c1    ← 1 条指令处理 1 对数据
  ADD a2, b2 → c2
  ADD a3, b3 → c3
  ADD a4, b4 → c4
  → 4 条指令，4 个时钟周期

SIMD 指令（AVX2，256位寄存器）：
  VADD [a1,a2,a3,a4,a5,a6,a7,a8], [b1,...,b8] → [c1,...,c8]
  → 1 条指令，1 个时钟周期，同时完成 8 个 int32 加法
```

CPU 中的 SIMD 寄存器宽度：

| 指令集 | 寄存器宽度 | 一次处理 int32 数 | 一次处理 double 数 |
|--------|-----------|-----------------|-------------------|
| SSE4.2 | 128 位 | 4 个 | 2 个 |
| AVX2 | 256 位 | 8 个 | 4 个 |
| AVX-512 | 512 位 | 16 个 | 8 个 |

**实际应用示例**：

```
场景：SUM(amount)，amount 列有 100 万个 int32

不用 SIMD：逐个累加 → 100 万次加法指令
用 AVX2：每次加载 8 个值，一条指令求和 → 12.5 万次指令，快 ~8 倍

场景：WHERE status = 'paid' 过滤
不用 SIMD：逐行比较 → 100 万次比较 + 分支跳转
用 SIMD：批量比较生成位图（无分支）→ 12.5 万次指令，且无分支预测失败
```

**虚函数调用的问题**：

传统 Volcano 模型中，每个算子（Scan/Filter/Aggregate）通过虚函数 `next()` 逐行拉取数据。虚函数的代价：

```
普通函数调用：编译器知道目标地址 → CPU 可以预取指令 → 流水线不中断
虚函数调用：运行时查 vtable 才知道目标地址 → CPU 无法预取 → 流水线停顿

每行调用一次虚函数的代价：
  ① 查 vtable 指针（一次内存访问）
  ② 查函数地址（又一次内存访问）
  ③ 跳转到目标函数（流水线清空，~10-20 个时钟周期浪费）
  ④ 无法内联优化（编译器不知道实际调用哪个函数）

100 万行 × 每行 1 次虚函数调用 = 100 万次流水线停顿
向量化后：100 万行 ÷ 8192 = 122 次函数调用 → 开销忽略不计
```

**向量化 + SIMD 的协作关系**：

| 层级 | 做什么 | 谁提供 |
|------|--------|--------|
| **向量化执行**（软件层） | 把数据按批组织成连续数组 | 数据库引擎（ClickHouse/Doris） |
| **SIMD 指令**（硬件层） | 对连续数组一条指令处理多个元素 | CPU 硬件（Intel/AMD） |

向量化是前提——只有数据按批排成连续数组后，SIMD 才能发挥作用。两者配合 = 软件把数据排好队，硬件一次处理一排。

> **ClickHouse vs Doris**：两者都采用向量化 + SIMD 架构，都是 C++ 实现。ClickHouse 的向量化更彻底（从诞生就是向量化设计），Doris 在 2.0 版本后也全面转向向量化引擎。两者在纯计算性能上接近，差异主要在分布式架构和易用性上。详见 [6.6 Doris 向量化执行](./06-Doris.md#41-列式存储--向量化执行)。

### 4.3 数据压缩

同列数据类型相同、值域相近，压缩效果极好：

| 压缩算法 | 压缩率 | 速度 | 适用 |
|---------|--------|------|------|
| LZ4 | 中等（3-5x） | ⭐ 最快 | 默认，平衡压缩率和速度 |
| ZSTD | 高（5-10x） | 中等 | 冷数据、存储敏感 |
| Delta | 极高（时序数据） | 快 | 时间戳、递增 ID |
| DoubleDelta | 极高 | 快 | 时序数据的时间列 |
| Gorilla | 极高（浮点数） | 快 | 监控指标 |

### 4.4 稀疏索引 + Data Skipping

```
Primary Index（稀疏）：每 8192 行一个条目
  → 定位到可能包含目标数据的 granule 范围
  → 跳过不相关的 granule

Skip Index（二级索引）：
  - minmax: 每个 granule 的 min/max
  - set: 每个 granule 的去重值集合
  - bloom_filter: 布隆过滤器
  - ngrambf_v1: N-gram 布隆过滤器（模糊匹配）
```

---

## 五、数据导入方式

| 方式 | 延迟 | 适用场景 |
|------|------|---------|
| **INSERT INTO** | 实时 | 小批量写入（建议攒批 > 1000 行） |
| **Kafka Engine** | 秒级 | 从 Kafka 实时消费 |
| **Materialized View** | 实时 | 写入时自动触发聚合计算 |
| **HDFS/S3 Engine** | 分钟级 | 批量导入外部文件 |
| **clickhouse-copier** | 分钟级 | 集群间数据迁移 |

**写入最佳实践**：
- 每次 INSERT 至少 1000-10000 行（避免产生过多小 Part）
- 每秒不超过 1 次 INSERT（让后台 Merge 跟上）
- 大批量导入用 `INSERT INTO ... SELECT` 或文件导入

---

## 六、物化视图

ClickHouse 的物化视图是**写入时触发**的增量计算，不是查询时实时计算：

```sql
-- 源表
CREATE TABLE events (
    dt DateTime, user_id UInt64, event String, amount Decimal(18,2)
) ENGINE = MergeTree() ORDER BY (dt, user_id);

-- 物化视图：实时聚合每小时 GMV
CREATE MATERIALIZED VIEW hourly_gmv
ENGINE = SummingMergeTree() ORDER BY (hour)
AS SELECT
    toStartOfHour(dt) AS hour,
    sum(amount) AS total_amount,
    count() AS order_count
FROM events
WHERE event = 'purchase'
GROUP BY hour;
```

**特点**：
- 数据写入源表时，自动增量计算并写入物化视图
- 查询物化视图 = 查询预计算结果，极快
- 适合固定维度的聚合场景（看板、报表）

---

## 七、ClickHouse vs Doris 深度对比

| 维度 | ClickHouse | Doris |
|------|-----------|-------|
| **架构** | 无共享，每节点独立 | MPP，FE+BE 分离 |
| **单表聚合** | ⭐ 极致性能 | 优秀 |
| **多表 JOIN** | 较弱（大表 JOIN 需优化） | ⭐ 更强（MPP Shuffle Join） |
| **并发能力** | 较弱（单查询占满资源） | ⭐ 更强（资源隔离） |
| **SQL 兼容性** | 自有方言 | ⭐ MySQL 协议兼容 |
| **运维复杂度** | 高（ZooKeeper/手动分片） | ⭐ 低（自动均衡/弹性扩缩） |
| **实时写入** | 需要攒批，不支持高频小写入 | ⭐ 支持（Stream Load） |
| **UPDATE/DELETE** | 异步合并，非实时生效 | ⭐ 支持 Unique Key 实时更新 |
| **生态集成** | 丰富（Kafka/HDFS/S3 Engine） | 丰富（多种 Catalog 外表） |
| **向量化** | ⭐ 全面向量化 | 向量化（较新版本） |
| **压缩** | ⭐ 多种编码，压缩率极高 | 好 |
| **社区** | 全球活跃，ClickHouse Inc. | 国内活跃，百度/SelectDB |

### 选型决策

```
选 ClickHouse 当：
  ✓ 单表超大规模聚合（百亿级）
  ✓ 团队有 ClickHouse 运维经验
  ✓ 对压缩率和存储成本敏感
  ✓ 时序/日志分析场景

选 Doris 当：
  ✓ 需要多表 JOIN
  ✓ 需要 MySQL 协议兼容（业务迁移成本低）
  ✓ 需要简单运维（自动均衡）
  ✓ 需要较高并发（数百 QPS）
  ✓ 需要实时 UPDATE
```

---

## 八、生产实践要点

### 8.1 表设计

- **选择合适的排序键**：把高频过滤列放前面（如 tenant_id, dt, user_id）
- **分区不要太细**：按天/按月分区，避免按小时（Part 过多）
- **用低基数类型**：LowCardinality(String) 替代 String，性能提升 2-5x
- **避免 Nullable**：Nullable 列有额外存储开销，用默认值替代

### 8.2 查询优化

```sql
-- ❌ 全表扫描
SELECT count() FROM events WHERE event_type = 'click';

-- ✅ 利用排序键（假设 ORDER BY (event_type, dt)）
SELECT count() FROM events WHERE event_type = 'click' AND dt >= '2024-01-01';

-- ✅ 用 PREWHERE 替代 WHERE（先过滤再读取其他列）
SELECT user_id, amount FROM events PREWHERE event_type = 'click' WHERE amount > 100;
```

### 8.3 常见坑

| 问题 | 原因 | 解决方案 |
|------|------|---------|
| 写入报错 "Too many parts" | 高频小批量写入 | 攒批写入，增大 `max_parts_in_total` |
| 查询 OOM | 大 JOIN 或 GROUP BY 基数过高 | 限制 `max_memory_usage`，优化查询 |
| 副本不同步 | ZooKeeper 压力大或网络抖动 | 监控 ZK、增加 ZK 节点 |
| Merge 跟不上 | 写入速度 > Merge 速度 | 增加 `background_pool_size` |
| 查询慢 | 未命中排序键 | 检查 WHERE 条件是否匹配 ORDER BY 前缀 |

---

## 九、面试深度剖析

### Q1: ClickHouse 为什么快？

**答**：四个层面——① 列式存储减少 IO（只读需要的列）；② 向量化执行利用 SIMD 指令批量计算；③ 高压缩率减少磁盘读取量；④ 稀疏索引 + Data Skipping 跳过无关数据。本质是针对 OLAP 场景（少列、大量行、聚合）做了极致优化。

### Q2: MergeTree 的 Merge 过程是怎样的？

**答**：后台线程选择同分区的多个小 Part，按排序键归并排序合并为一个大 Part。对于 ReplacingMergeTree 会去重，SummingMergeTree 会聚合。Merge 是异步的，不阻塞写入和查询。查询时如果有未合并的 Part，会在读取时做逻辑合并（FINAL 关键字）。

### Q3: ClickHouse 怎么处理 UPDATE/DELETE？

**答**：ClickHouse 不支持传统的行级 UPDATE/DELETE。提供两种方式：① `ALTER TABLE ... UPDATE/DELETE`（Mutation）——异步重写 Part，不适合高频操作；② CollapsingMergeTree——通过 +1/-1 标记实现逻辑删除，Merge 时物理删除。生产中通常用 ReplacingMergeTree + 版本号实现「最新值」语义。

### Q4: 分布式表的查询流程是怎样的？

**答**：查询发到任意节点 → 该节点作为协调者 → 将查询分发到所有 Shard → 各 Shard 本地执行（过滤+聚合）→ 协调者汇总各 Shard 结果 → 返回最终结果。如果涉及 JOIN，小表会被广播到所有 Shard（Broadcast Join），大表 JOIN 性能较差。

### Q5: ClickHouse 的高可用怎么做？

**答**：通过 ReplicatedMergeTree + ZooKeeper 实现副本同步。写入任一副本，ZooKeeper 协调其他副本拉取数据。读取时可以从任意副本读。ZooKeeper 是单点风险，生产中需要 3-5 节点的 ZK 集群。新版本（ClickHouse Keeper）用自研的 Raft 实现替代 ZooKeeper。

---

## 十、与本书其他章节的关联

| 关联章节 | 关系 |
|---------|------|
| [6.6 Doris](./06-Doris.md) | 同为 OLAP 引擎，互为竞品，选型对比 |
| [6.10 语义层](./10-语义层与指标平台.md) | ClickHouse 是语义层查询下推的目标引擎之一 |
| [3.12 消息队列 Kafka](../part3-java-deep/12-消息队列.md) | Kafka Engine 实现实时数据导入 |
| [A1 核心数据结构](../part3-java-deep/A1-核心数据结构原理.md) | 稀疏索引、布隆过滤器、跳表等数据结构 |
| [6.9 湖仓一体](./09-湖仓一体.md) | ClickHouse 可作为湖上加速层（查询 Iceberg 外表） |

---

[← 6.10 语义层与指标平台](./10-语义层与指标平台.md) | [返回本章目录](./README.md) | [6.12 Presto 查询引擎 →](./12-Presto查询引擎.md)
