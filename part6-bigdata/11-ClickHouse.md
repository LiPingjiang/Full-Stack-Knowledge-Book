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

### 2.3 数据分布与节点职责

**一张表的数据都在一个节点上吗？一个节点只存一个表吗？**

都不是。一张表可以分布在多个节点（通过 Shard 分片），一个节点可以存多张表：

```
Node 1: [orders 的 Shard 1] + [users 的 Shard 1] + [events 的 Shard 1]
Node 2: [orders 的 Shard 2] + [users 的 Shard 2] + [events 的 Shard 2]
Node 3: [orders 的 Shard 3] + [users 的 Shard 3] + [events 的 Shard 3]
```

**ClickHouse 分片 vs Doris 分片**：

| | ClickHouse | Doris |
|--|--|--|
| 分片方式 | 手动配置 Distributed 表 + 本地表 | `DISTRIBUTED BY HASH(col) BUCKETS N` 自动分配 |
| 均衡 | 手动管理，无自动均衡 | FE 自动调度 Tablet，自动均衡 |
| 扩容 | 需要手动迁移数据 | 自动 Rebalance |

### 2.4 Shared-Nothing 架构与查询模式

ClickHouse 是 **Shared-Nothing（无共享）架构**——每个节点独立拥有存储和计算，节点之间不共享磁盘。

**Scatter-Gather 查询模式**：

这是 ClickHouse 单表聚合的核心执行模式，也是它快的关键原因：

```
Scatter-Gather（分散-收集）：

  客户端 → 协调节点（任意 Node 兼任）
              │
    ┌─────────┼─────────┐         ← Scatter（分散）：把查询发给所有 Shard
    ↓         ↓         ↓
  Node 1    Node 2    Node 3
  本地扫描   本地扫描   本地扫描     ← 每个节点只读本地磁盘，独立计算
  本地聚合   本地聚合   本地聚合     ← 在本地完成大部分计算
    │         │         │
    └─────────┼─────────┘         ← Gather（收集）：汇总各节点的部分结果
              ↓
         协调节点合并
              ↓
           最终结果

特点：
  ✅ 中间没有 Shuffle（节点之间不交换原始数据）
  ✅ 每个节点只传回聚合后的小结果集（如 100 个城市的 SUM）
  ✅ 网络传输量极小
```

**对比 Doris 的多阶段执行**：

```
Doris 单表聚合（也是 Scatter-Gather，跟 ClickHouse 一样）：
  FE 分发 → 各 BE 本地扫描+聚合 → 汇总到某个 BE → 返回
  → 单表聚合时，Doris 和 ClickHouse 的执行模式几乎相同

Doris 多表 JOIN（需要 Shuffle）：
  FE 分发 → 各 BE 扫描各自的表
    → 按 JOIN Key 重新分布数据（Shuffle Exchange）← 这一步 ClickHouse 做不好
    → 各 BE 在本地做 JOIN
    → 汇总返回
```

**所以 Doris 单表查询时也是只扫描本地数据吗？**

**是的。** 单表聚合场景下，Doris 的 BE 也是各自扫描本地 Tablet、本地聚合、最后汇总——跟 ClickHouse 的 Scatter-Gather 模式一样。**两者在单表聚合上的执行模式没有本质区别。**

ClickHouse 单表更快的真正原因不是执行模式不同，而是：

| 原因 | 说明 |
|------|------|
| **单查询吃满资源** | ClickHouse 一条查询占满整个节点的 CPU/IO；Doris 为了高并发会做资源隔离和限制 |
| **更极致的压缩** | 更丰富的编码（Delta、DoubleDelta、Gorilla、T64），同样数据压缩后更小，IO 更少 |
| **更激进的 SIMD 优化** | 热路径手写 SIMD 代码，不完全依赖编译器自动向量化 |
| **无协调开销** | 没有独立 FE 进程的 RPC 开销（但这个差距很小，不是主因） |

### 2.5 为什么 ClickHouse 不支持行级 UPDATE/DELETE

**根本原因：MergeTree 的 Part 文件是不可变的（Immutable）。**

```
MergeTree 写入方式：
  INSERT 100 行 → 生成 Part_1（不可变的文件目录）
  INSERT 200 行 → 生成 Part_2（不可变的文件目录）
  后台 Merge  → Part_1 + Part_2 合并为 Part_3（新文件，旧文件删除）

Part 内部结构：
  Part_1/
    ├── primary.idx        ← 稀疏索引（不可变）
    ├── city.bin           ← city 列的压缩数据块（不可变）
    ├── city.mrk2          ← 标记文件，记录数据块偏移（不可变）
    ├── amount.bin         ← amount 列的压缩数据块（不可变）
    └── ...
```

**为什么不可变 = 不能行级更新**：

```
要 UPDATE 一行：
  1. 找到目标行在哪个 Part 的哪个压缩数据块
  2. 数据块是压缩的（如 64KB 压缩成 8KB）
  3. 改一行必须：解压整个块 → 修改 → 重新压缩 → 重写
  4. 但其他列的 .mrk 偏移量指向这个块，改了大小全部失效
  5. 实际上 = 重写整个 Part（可能几百 MB）

  → 改一行的代价 ≈ 重写整个 Part = 不可接受
```

**对比 MySQL InnoDB**：

```
InnoDB：数据页（16KB）是可变的
  → 找到目标行所在的页 → 页内原地修改 → 写 redo log → 完成
  → 改一行只影响一个 16KB 的页

MergeTree：Part（几百 MB）是不可变的
  → 改一行要重写整个 Part
  → 所以 ClickHouse 的 UPDATE 是异步 Mutation（后台重写），不是实时生效
```

**ClickHouse 的替代方案**：

| 方案 | 做法 | 适用场景 |
|------|------|---------|
| **Mutation** | `ALTER TABLE ... UPDATE/DELETE` → 后台异步重写 Part | 低频修正（分钟级生效） |
| **ReplacingMergeTree** | 写入新版本行，Merge 时保留最新版本 | 需要"最新值"语义 |
| **CollapsingMergeTree** | 写入 +1/-1 标记行，Merge 时抵消 | CDC 场景 |

**本质取舍**：

```
ClickHouse：数据不可变 → 写入极快（纯追加）、压缩率极高、无锁 → 但不能实时改
Doris/MySQL：数据可变 → 支持实时 UPDATE → 但有锁、有 WAL、有页分裂开销
```

> **面试记忆点**：ClickHouse 不支持行级 UPDATE 不是"没实现"，而是存储引擎的设计选择——不可变文件让写入和压缩都更高效，代价是不能原地修改。这是 OLAP（分析优先）vs OLTP（事务优先）的根本哲学差异。

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

### 3.2 MergeTree vs LSM-Tree 对比

MergeTree 和 [LSM-Tree](../part3-java-deep/A1-核心数据结构原理.md#十一lsm-tree-与-sst-文件写优化存储引擎的通用原理) 确实非常相似——都是"追加写入 + 后台合并"的设计。但有关键差异：

**相同点**：

```
两者的核心思路完全一致：
  1. 写入时不原地修改，而是追加生成新的不可变文件
  2. 后台异步合并（Compaction/Merge）多个小文件为大文件
  3. 合并时可以做额外操作（去重、聚合、清理过期数据）
  4. 读取时可能需要合并多个文件的结果
```

**关键差异**：

| 维度 | LSM-Tree（RocksDB/HBase） | MergeTree（ClickHouse） |
|------|--------------------------|------------------------|
| **数据组织** | 行式 Key-Value（按 Key 有序） | 列式存储（按排序键有序，但每列独立文件） |
| **内存缓冲** | MemTable（写入先到内存，满了再刷盘） | 无 MemTable（直接写磁盘 Part） |
| **WAL** | 有 Write-Ahead Log（保证不丢数据） | 无 WAL（依赖批量写入保证） |
| **分层** | 严格分层（Level 0 → Level 1 → ... → Level N） | 无严格层级（只有同分区的 Part 合并） |
| **合并策略** | Leveled / Tiered / FIFO 等多种策略 | 简单的同分区 Part 合并（按大小选择） |
| **点查能力** | 强（Bloom Filter + 有序查找） | 弱（面向批量扫描，不擅长单行查找） |
| **设计目标** | 通用 KV 存储（读写均衡） | OLAP 分析（写入快 + 批量扫描快） |
| **典型系统** | RocksDB、HBase、Cassandra、LevelDB | ClickHouse、Doris BE |

**为什么 MergeTree 没有 MemTable 和 WAL？**

```
LSM-Tree（RocksDB）：
  写入 → WAL（防丢）→ MemTable（内存排序）→ 满了刷盘为 SST 文件
  → 适合高频小写入（每秒百万次 PUT）

MergeTree（ClickHouse）：
  写入 → 直接在内存排序 → 立即写磁盘生成 Part
  → 不需要 WAL，因为 ClickHouse 要求客户端攒批写入（每次至少 1000+ 行）
  → 如果写入过程中崩溃，丢失的只是这一批数据，客户端重试即可
```

这也解释了为什么 ClickHouse 要求"每次 INSERT 至少 1000 行、每秒不超过 1 次 INSERT"——没有 MemTable 缓冲，每次 INSERT 都直接生成一个 Part 文件，太频繁就小文件爆炸。

**Doris BE 的存储引擎**也借鉴了 LSM-Tree 思想（Rowset/Segment 结构），但同样是列式存储。详见 [LSM-Tree 各系统对比](../part3-java-deep/A1-核心数据结构原理.md#116-各系统的-lsm-tree-实现对比)。

> **面试记忆点**：MergeTree 是 LSM-Tree 思想在列式 OLAP 场景的变体——保留了"追加写入 + 后台合并"的核心，去掉了 MemTable 和 WAL（因为 OLAP 场景是批量写入，不需要单条写入的持久性保证），把行式 KV 换成了列式存储（为了分析查询的扫描性能）。

### 3.3 MergeTree 家族

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
  - bloom_filter: [布隆过滤器](../part3-java-deep/A1-核心数据结构原理.md#三布隆过滤器bloom-filter用-1-的误判换-99-的内存节省)
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

**为什么 INSERT INTO 是"实时"，Kafka Engine 反而是"秒级"？**

这里的延迟指**数据可见延迟**：

```
INSERT INTO（你主动写入）：
  客户端攒好一批 → INSERT → ClickHouse 写入 Part → 立即可查
  延迟 = 网络往返（毫秒级），你控制什么时候写

Kafka Engine（ClickHouse 定时去拉）：
  Kafka 有新消息 → ClickHouse 定时轮询 → 攒一批 → 写入 Part → 可查
  延迟 = 轮询间隔（kafka_poll_timeout_ms，默认几秒）
```

ClickHouse 的 Kafka Engine 不是逐条消费的，而是**定时批量拉取**。因为 MergeTree 不适合高频小写入（每次写入生成一个 Part 文件，太频繁会小文件爆炸），所以故意攒批再写。

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

**ClickHouse 的 Engine 概念**：在 ClickHouse 中，**每张表都必须指定一个 Engine**（表引擎），Engine 决定了数据怎么存储、怎么索引、怎么合并。物化视图的目标表也是一张独立的表，有自己的 Engine：

```sql
-- 源表用 MergeTree（基础引擎，按排序键存储）
CREATE TABLE events (...) ENGINE = MergeTree() ORDER BY (dt, user_id);

-- 物化视图的目标表用 SummingMergeTree（合并时自动求和）
CREATE MATERIALIZED VIEW hourly_gmv
ENGINE = SummingMergeTree() ORDER BY (hour)  ← 这是目标表的 Engine
AS SELECT ...;
```

常见 Engine 类型：

| Engine | 用途 | 特点 |
|--------|------|------|
| MergeTree | 通用分析表 | 基础引擎，支持排序、分区、索引 |
| SummingMergeTree | 预聚合汇总表 | Merge 时自动对数值列求和 |
| AggregatingMergeTree | 复杂聚合表 | Merge 时执行聚合函数（物化视图常用） |
| ReplacingMergeTree | 去重表 | Merge 时按排序键保留最新行 |
| Kafka | Kafka 消费 | 不存数据，只是 Kafka 的读取接口 |
| Memory | 临时表 | 数据存内存，重启丢失 |

> **对比 Doris**：Doris 没有 Engine 概念，所有表用同一套存储引擎（BE 的列式存储），通过**数据模型**（Duplicate/Aggregate/Unique）区分行为。ClickHouse 的 Engine 更灵活（每张表可以选不同的存储和合并策略），但也更复杂。

### 6.2 ClickHouse vs Doris 物化视图对比

| 维度 | ClickHouse | Doris |
|------|-----------|-------|
| **触发方式** | 写入源表时同步触发 | 导入数据时同步构建 |
| **导入方式差异** | INSERT INTO 语句 | Stream Load / Broker Load / Routine Load / INSERT |
| **存储** | 独立目标表（有自己的 Engine） | 基表的附属 Rollup（同一 Tablet 组内） |
| **聚合能力** | 任意 SQL（支持 JOIN、子查询） | 仅单表聚合（不支持 JOIN） |
| **查询路由** | 需手动查目标表名 | **自动路由**——优化器自动命中最优物化视图 |
| **历史数据** | 创建后只处理新数据，历史需手动回填 | 创建时自动构建历史数据 |
| **DELETE 同步** | 物化视图不自动同步删除 | 引擎保证基表和物化视图一致 |

**Doris 的"导入触发"是什么意思？**

Doris 的数据写入不只有 INSERT INTO，还有专用的导入方式（这些才是生产中的主流）：

```
Stream Load  — HTTP 推送，实时小批量（最常用的实时导入）
Broker Load  — 从 HDFS/S3 批量拉取
Routine Load — 持续消费 Kafka（类似 ClickHouse 的 Kafka Engine）
INSERT INTO  — SQL 写入（也支持，但生产中不是主流）
```

无论用哪种方式导入，Doris 都会同步更新物化视图。所以说"导入触发"比"INSERT 触发"更准确——它覆盖了所有导入路径。

**查询路由的体验差异**（最大的使用区别）：

```sql
-- ClickHouse：必须知道物化视图的目标表名
SELECT * FROM hourly_gmv;  ← 你要记住这个名字

-- Doris：直接查基表，优化器自动路由
SELECT hour, sum(amount) FROM events GROUP BY hour;
  ← 优化器发现物化视图能满足，自动命中，用户无感
```

**Doris 2.0+ 异步物化视图**：

Doris 2.0 引入了异步物化视图，弥补了同步物化视图不支持 JOIN 的限制：

```sql
-- 支持 JOIN、定时刷新
CREATE MATERIALIZED VIEW mv_async
BUILD DEFERRED REFRESH AUTO ON SCHEDULE EVERY 5 MINUTE
AS SELECT o.city, p.category, sum(o.amount) AS total
FROM orders o JOIN dim_product p ON o.prod_id = p.id
GROUP BY o.city, p.category;
```

| | Doris 同步物化视图 | Doris 异步物化视图（2.0+） | ClickHouse 物化视图 |
|--|--|--|--|
| JOIN | ❌ | ✅ | ✅ |
| 数据新鲜度 | 实时 | 有延迟（刷新间隔） | 实时 |
| 查询路由 | 自动 | 自动 | 手动 |
| 灵活度 | 低 | 高 | 高 |

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
