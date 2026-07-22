# 6.12 Presto 查询引擎（附 Trino 简介）

> **一句话定位**：Presto 是 Facebook 开源的分布式 SQL 查询引擎，专为**交互式分析**设计——它不存储数据，而是通过 Connector 机制直接查询 Hive、MySQL、Kafka、Iceberg 等异构数据源，秒级返回结果。

---

## 一、Presto 是什么

### 1.1 核心定位

```
Presto ≠ 数据库（不存数据）
Presto = 计算引擎（读别人的数据，用 SQL 查询）
```

| 特性 | 说明 |
|------|------|
| **联邦查询** | 一条 SQL 同时查 Hive + MySQL + Kafka，各数据源的数据拉到 Worker 内存中做 JOIN |
| **内存计算** | 中间数据不落盘，通过网络在 Worker 之间流式传输（Pipeline 执行）——注意仍然涉及跨机器网络传输，"内存"指的是不写磁盘，不是所有数据在一台机器上 |
| **交互式** | 秒级~分钟级响应，适合 Ad-hoc 分析 |
| **标准 SQL** | ANSI SQL 兼容，学习成本低 |
| **插件化** | Connector 架构，接入新数据源只需实现接口 |

### 1.2 Presto vs 其他引擎的定位

| 引擎 | 定位 | 数据规模 | 延迟 | 是否存数据 |
|------|------|---------|------|-----------|
| **Presto** | 交互式联邦查询 | 中大规模 | 秒~分钟 | ❌ 不存 |
| **Spark SQL** | 批处理 + ETL | 超大规模 | 分钟~小时 | ❌ 不存 |
| **Doris** | 实时 OLAP | 中大规模 | 毫秒~秒 | ✅ 存储 |
| **ClickHouse** | 单表极致聚合 | 大规模 | 毫秒~秒 | ✅ 存储 |
| **Hive** | 离线批处理 | 超大规模 | 分钟~小时 | ❌（数据在 HDFS） |

**Presto 的甜蜜点**：数据量在 GB~TB 级、需要秒级响应、需要跨数据源查询的交互式分析场景。

---

## 二、核心架构

### 2.1 整体架构

```
┌─────────────────────────────────────────────────────┐
│                    Client (JDBC/CLI/BI)               │
└────────────────────────┬────────────────────────────┘
                         │ SQL
┌────────────────────────▼────────────────────────────┐
│                   Coordinator                        │
│  ┌──────────┐ ┌──────────┐ ┌──────────────────┐    │
│  │ Parser   │→│ Planner  │→│ Scheduler        │    │
│  │ & Analyzer│ │(CBO优化) │ │(Stage→Task分发)  │    │
│  └──────────┘ └──────────┘ └──────────────────┘    │
└────────────────────────┬────────────────────────────┘
                         │ Task 分发
┌────────────────────────▼────────────────────────────┐
│                    Worker 集群                        │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐            │
│  │ Worker1 │  │ Worker2 │  │ Worker3 │  ...        │
│  │ Task    │  │ Task    │  │ Task    │             │
│  │ Pipeline│  │ Pipeline│  │ Pipeline│             │
│  └────┬────┘  └────┬────┘  └────┬────┘            │
└───────┼─────────────┼───────────┼──────────────────┘
        │             │           │  Connector 读取数据
┌───────▼─────────────▼───────────▼──────────────────┐
│              数据源（不存储在 Presto 中）             │
│  Hive/HDFS │ MySQL │ Kafka │ Iceberg │ S3 │ ES    │
└─────────────────────────────────────────────────────┘
```

### 2.2 核心组件

| 组件 | 职责 |
|------|------|
| **Coordinator** | 接收 SQL、解析优化、生成执行计划、调度 Task |
| **Worker** | 执行具体的计算 Task，处理数据 |
| **Connector** | 数据源适配器，负责元数据获取和数据读取 |
| **Catalog** | 数据源注册，每个 Catalog 对应一个 Connector 实例 |

### 2.3 查询执行流程

```
1. Client 提交 SQL
2. Coordinator 解析 SQL → 生成逻辑计划 → CBO 优化 → 生成物理计划
3. 物理计划切分为多个 Stage（类似 Spark 的 Stage）
4. 每个 Stage 包含多个 Task，分发到 Worker 执行
5. Task 之间通过内存 Shuffle（Exchange）传递数据
6. 最终结果流式返回 Client
```

**关键特点**：Pipeline 执行——数据在 Worker 之间通过网络流式传输，中间结果不落盘。注意：这**不是说所有数据在一台机器的内存里**，而是说数据在多台 Worker 之间通过 HTTP 流式传递时，不写磁盘中间文件。上游产出一批数据立即通过网络发给下游 Worker，不需要等整个 Stage 完成。

### 2.4 Pipeline 执行 vs Stage-based 执行（核心差异详解）

这是 Presto 和 Spark 最本质的架构区别。**不是"一步完成后做下一步"vs"一步完成后做下一步"的区别**，而是"上下游能不能同时执行"的区别：

```
Spark 的 Stage-based 执行（串行阻塞）：

  时间轴 →→→→→→→→→→→→→→→→→→→→→→→→→→→→→→→→→→→→→→→→→→→→→

  Stage 1: [===扫描全部数据===][===写Shuffle文件到磁盘===]
                                                            ↓ 等 Stage 1 全部完成
  Stage 2:                                                  [===从磁盘读Shuffle===][===聚合===][===写磁盘===]
                                                                                                            ↓ 等 Stage 2 全部完成
  Stage 3:                                                                                                  [===读===][===输出===]

  特点：
    ① Stage 之间有"屏障"——上一个 Stage 的所有 Task 全部完成，下一个才能开始
    ② 中间数据写磁盘（Shuffle Write）→ 下游从磁盘读（Shuffle Read）
    ③ 总延迟 = Stage 1 时间 + Stage 2 时间 + Stage 3 时间（串行累加）
```

```
Presto 的 Pipeline 执行（流水线并行）：

  时间轴 →→→→→→→→→→→→→→→→→→→→→→→→→→→→→→→→→→→→→→→→→→→→→

  扫描层:  [批1][批2][批3][批4][批5]...
              ↓    ↓    ↓    ↓
  聚合层:    [批1][批2][批3][批4]...     ← 扫描层产出批1后，聚合层立即开始处理
                ↓    ↓    ↓
  输出层:      [批1][批2][批3]...        ← 聚合层产出批1后，输出层立即开始

  特点：
    ① 没有"屏障"——上游产出第一批数据后，下游立即开始处理
    ② 中间数据通过网络（HTTP Exchange）流式传输，不写磁盘
    ③ 总延迟 ≈ 最慢那一层的时间（流水线并行，不是串行累加）
```

**用生活类比**：

```
Stage-based（Spark）= 工厂流水线停机换模式：
  第一道工序把所有零件都加工完 → 全部搬到仓库 → 第二道工序从仓库取出来继续加工
  → 中间有"搬运到仓库"和"等待全部完成"的时间

Pipeline（Presto）= 真正的流水线：
  第一道工序加工完一个零件 → 立即传给第二道工序 → 第二道工序立即开始
  → 没有中间仓库，没有等待，所有工序同时运转
```

**为什么 Spark 要用 Stage-based？**

```
Spark 的设计目标是大规模 ETL（TB~PB 级），需要容错：
  → Shuffle 数据写磁盘 = 如果下游 Task 失败，可以从磁盘重读，不需要重跑上游
  → Stage 屏障 = 保证上游数据完整后再开始下游，避免数据不一致

Presto 的设计目标是交互式查询（GB~TB 级），追求低延迟：
  → 不落盘 = 省去磁盘 I/O 时间
  → Pipeline = 上下游并行，减少总延迟
  → 代价：没有容错，查询失败只能从头重跑
```

**数据在 Worker 之间的网络传输（Exchange）**：

```
Presto 的 Pipeline 执行中，数据确实要跨机器传输：

  Worker 1（扫描 Split 1）──┐
  Worker 2（扫描 Split 2）──┼── HTTP 流式传输 ──→ Worker 5（JOIN/聚合）──→ Coordinator
  Worker 3（扫描 Split 3）──┤     （Exchange）                                ↓
  Worker 4（扫描 Split 4）──┘                                              返回结果

  "内存中流式处理"的准确含义：
    ✅ 中间数据不写磁盘（对比 Spark 写 Shuffle 文件）
    ✅ 数据通过网络在 Worker 之间流式传递
    ❌ 不是说所有数据在一台机器的内存里
    ❌ 不是说不需要网络传输
```

---

## 三、Connector 机制

### 3.1 为什么需要 Connector

Presto 是纯计算引擎，自己不存数据。它需要对接各种数据源（HDFS、MySQL、Kafka、S3...），所以抽象出 Connector 作为统一的"适配器"接口——每种数据源实现一个 Connector，Presto 的计算引擎只需要面对统一的数据格式（Page），不需要关心底层是什么存储。

```
Presto 的 Connector 架构：

  SELECT * FROM hive.db.orders o
  JOIN mysql.db.users u ON o.user_id = u.id

  Presto Coordinator
    ├── Hive Connector    → 知道怎么读 HDFS 上的 Parquet/ORC
    ├── MySQL Connector   → 知道怎么连 MySQL 执行查询
    ├── Iceberg Connector → 知道怎么读 Iceberg 表的 Manifest + Data Files
    ├── Kafka Connector   → 知道怎么消费 Kafka Topic
    └── ...

  计算引擎只看到统一的 Page（一批列式数据），不关心数据从哪来
```

### 3.2 Connector 接口

```java
// Connector 需要实现的核心接口
public interface Connector {
    ConnectorMetadata getMetadata();      // 元数据（表/列/分区信息）
    ConnectorSplitManager getSplitManager(); // 数据分片（Split）
    ConnectorPageSourceProvider getPageSourceProvider(); // 数据读取
}
```

每个 Connector 告诉 Presto 三件事：
1. **有什么数据**（Metadata）：有哪些表、每张表有哪些列
2. **怎么拆分**（SplitManager）：数据可以拆成哪些分片并行读取
3. **怎么读取**（PageSourceProvider）：读取一个分片的数据，返回列式 Page

### 3.3 主流 Connector

| Connector | 数据源 | 特点 |
|-----------|--------|------|
| **Hive** | HDFS/S3 上的 Hive 表 | 最常用，支持 ORC/Parquet/Iceberg |
| **Iceberg** | Iceberg 表 | 支持 Time Travel、Partition Evolution |
| **MySQL** | MySQL 数据库 | 谓词下推到 MySQL 执行 |
| **Kafka** | Kafka Topic | 实时查询 Kafka 中的数据 |
| **Elasticsearch** | ES 索引 | 全文检索 + 聚合 |
| **Redis** | Redis | 查询 Redis 中的数据 |
| **TPCH/TPCDS** | 基准测试 | 内置测试数据生成 |

### 3.5 联邦查询示例

```sql
-- 同一条 SQL 查询 Hive 和 MySQL 的数据
SELECT
    h.user_id,
    h.page_views,
    m.user_name,
    m.vip_level
FROM hive.analytics.page_view_daily h
JOIN mysql.user_db.users m ON h.user_id = m.id
WHERE h.dt = '2024-01-20'
  AND m.vip_level >= 3;
```

这条 SQL 中：
- `hive.analytics.page_view_daily` — 从 HDFS 读取 Hive 表
- `mysql.user_db.users` — 从 MySQL 读取用户表
- Presto 自动处理跨源 JOIN

**联邦查询的执行过程**：

```
Step 1: Coordinator 解析 SQL，识别出两个数据源

Step 2: 各 Worker 并行从不同数据源拉取数据
  Worker 1-4: 通过 Hive Connector 从 HDFS 并行读 page_view_daily（多个 Split）
  Worker 5:   通过 MySQL Connector 从 MySQL 读 users（谓词 vip_level>=3 下推到 MySQL）

Step 3: 数据拉到 Worker 内存后，在 Presto 内部做 JOIN
  → users 表较小 → 广播（Broadcast）到所有 JOIN Worker 的内存中
  → page_view_daily 数据流式传入 JOIN Worker
  → 在 Worker 内存中完成 Hash JOIN

Step 4: 结果返回 Coordinator → 返回客户端
```

**中间数据缓存在 Worker 内存中**——各数据源的数据拉到 Worker 内存后做 JOIN，不落盘。这也是 Presto 联邦查询的局限：如果两侧数据都很大（超过集群内存），会 OOM 失败。

**适合联邦查询的场景**：
- ✅ 大表 JOIN 小表（小表广播到内存）
- ✅ 大表经过过滤后数据量可控
- ❌ 两张超大表 JOIN（中间数据超过集群内存 → OOM）

---

## 四、查询优化

### 4.1 CBO（Cost-Based Optimizer）

Presto 的优化器基于统计信息做代价估算：

| 优化规则 | 说明 |
|---------|------|
| **谓词下推** | WHERE 条件推到 Connector 层，减少数据读取 |
| **投影下推** | 只读取 SQL 中涉及的列 |
| **分区裁剪** | 只扫描匹配的分区 |
| **JOIN 重排序** | 小表放 Build 侧，大表放 Probe 侧 |
| **JOIN 策略选择** | Broadcast Join vs Partitioned Join |
| **聚合下推** | 部分聚合在 Worker 本地先做（Partial Aggregation） |

### 4.2 JOIN 策略

| 策略 | 条件 | 原理 |
|------|------|------|
| **Broadcast Join** | 小表 < 阈值 | 小表广播到所有 Worker，大表不动 |
| **Partitioned Join** | 两表都大 | 按 JOIN Key Hash 分区，相同 Key 到同一 Worker |
| **Lookup Join** | 维表在外部系统 | 逐行查询外部系统（如 MySQL） |

### 4.3 内存管理

Presto 是纯内存计算，内存是最关键的资源：

```
总内存 = 系统预留 + 查询内存池
查询内存池 = 通用池（General Pool）+ 预留池（Reserved Pool）
```

- **内存不足时**：Kill 占用内存最多的查询（OOM Killer）
- **Spill to Disk**：部分操作（JOIN/ORDER BY/聚合）支持溢写磁盘，但性能大幅下降
- **资源组（Resource Group）**：限制不同用户/队列的内存和并发

---

## 五、与 Spark SQL 的对比

| 维度 | Presto | Spark SQL |
|------|--------|-----------|
| **执行模型** | Pipeline（流水线并行） | Stage-based（串行阻塞） |
| **中间数据** | 内存中通过网络流式传输，不落盘 | Shuffle 写磁盘，下游从磁盘读 |
| **上下游关系** | 上下游同时执行（流水线） | 上游全部完成后下游才开始（屏障） |
| **延迟** | 秒级 | 分钟级 |
| **容错** | 无（查询失败需重跑） | 有（RDD 血缘重算） |
| **数据规模** | GB~TB | TB~PB |
| **适用场景** | Ad-hoc 交互式查询 | ETL、大规模批处理 |
| **资源利用** | 常驻进程，启动快 | 按需申请，启动慢 |

**选型建议**：
- 需要秒级响应的交互式查询 → Presto
- 需要处理 PB 级数据的 ETL → Spark
- 需要容错（长时间运行的大查询）→ Spark
- 需要联邦查询多数据源 → Presto

---

## 六、生产实践

### 6.1 集群规划

| 规模 | Coordinator | Worker | 内存/节点 | 适用 |
|------|-------------|--------|-----------|------|
| 小型 | 1 | 3-5 | 64GB | 团队内部分析 |
| 中型 | 1-2 | 10-50 | 128GB | 部门级 BI |
| 大型 | 2+（HA） | 50-200+ | 256GB | 公司级数据平台 |

### 6.2 性能调优

```properties
# 关键配置
query.max-memory=50GB                    # 单查询最大内存
query.max-memory-per-node=10GB           # 单节点单查询最大内存
query.max-total-memory-per-node=20GB     # 单节点所有查询总内存
join-distribution-type=AUTOMATIC         # JOIN 策略自动选择
optimizer.join-reordering-strategy=AUTOMATIC  # JOIN 顺序自动优化
```

### 6.3 常见问题

| 问题 | 原因 | 解决 |
|------|------|------|
| 查询 OOM | 大 JOIN/聚合超内存 | 限制内存、开启 Spill、优化 SQL |
| 查询慢 | 数据倾斜 | 加盐打散、调整 JOIN 顺序 |
| Coordinator 瓶颈 | 大量并发查询 | 资源组限流、多 Coordinator |
| Connector 慢 | 数据源响应慢 | 谓词下推、缓存元数据 |

---

## 七、Trino 简介

### 7.1 Presto 与 Trino 的关系

```
2012: Facebook 内部开发 Presto
2013: Facebook 开源 Presto（PrestoDB）
2018: Presto 核心创始人离开 Facebook，创立 Starburst
2020: 因商标争议，社区版更名为 Trino
      → PrestoDB: Facebook 维护的版本（偏内部优化）
      → Trino: 社区驱动的版本（原 PrestoSQL，更活跃）
```

**简单理解**：Trino 就是 Presto 的社区版，API 和架构几乎相同，但发展方向有差异。

### 7.2 Trino vs PrestoDB 差异

| 维度 | Trino（原 PrestoSQL） | PrestoDB（Facebook） |
|------|----------------------|---------------------|
| **社区** | ⭐ 更活跃，开放治理 | Facebook 主导 |
| **发版节奏** | 每周发版 | 较慢 |
| **新特性** | 更多 Connector、更好的 Iceberg 支持 | 偏 Facebook 内部需求 |
| **容错** | 支持 Task 级重试（Project Tardigrade） | 有限 |
| **商业化** | Starburst（Trino 商业版） | Ahana（PrestoDB 商业版，已被收购） |
| **湖仓集成** | ⭐ Iceberg/Delta/Hudi 支持更好 | 支持但更新较慢 |

### 7.3 Trino 的新特性亮点

| 特性 | 说明 |
|------|------|
| **Fault-Tolerant Execution** | Task 失败可重试，不需要重跑整个查询 |
| **Dynamic Filtering** | JOIN 时动态生成过滤条件，减少 Probe 侧扫描 |
| **Table Function** | 自定义表函数，扩展数据源能力 |
| **Polymorphic Table Function** | 更灵活的 UDF 机制 |
| **Iceberg REST Catalog** | 原生支持 Iceberg REST Catalog 协议 |

### 7.4 选择建议

- **新项目** → 选 Trino（社区更活跃、湖仓支持更好）
- **已有 PrestoDB 集群** → 评估迁移成本，Trino 迁移通常平滑
- **Facebook 生态** → PrestoDB（与 Facebook 内部工具集成更好）

---

## 八、面试深度剖析

### Q1: Presto 为什么比 Hive/Spark 快？

**答**：① Pipeline 执行——数据在内存中流式处理，不像 Spark 需要 Stage 间 Shuffle 落盘；② 常驻进程——不需要像 Spark 那样每次申请资源、启动 JVM；③ 代码生成——运行时生成字节码，减少虚函数调用；④ 向量化处理——批量处理数据。代价是没有容错——查询失败需要完全重跑。

### Q2: Presto 的 Connector 机制是怎么工作的？

**答**：Connector 是 Presto 的数据源适配层，实现三个核心接口：① Metadata——提供表/列/分区的元数据；② SplitManager——将数据切分为 Split（并行读取单元）；③ PageSourceProvider——实际读取数据并返回 Page（列式内存格式）。Coordinator 通过 Metadata 做查询规划，通过 SplitManager 分配读取任务，Worker 通过 PageSource 读取数据。

### Q3: Presto 怎么做联邦查询的 JOIN？

**答**：跨数据源 JOIN 时，Presto 将两侧数据都拉到 Worker 内存中做 JOIN。小表用 Broadcast（广播到所有 Worker），大表用 Partitioned（按 Key Hash 分发）。关键优化是谓词下推——尽量在数据源侧过滤，减少拉到 Presto 的数据量。如果两侧数据都很大，性能会很差（内存不够会 OOM）。

### Q4: Presto 内存不够怎么办？

**答**：① 优化 SQL 减少数据量（加过滤条件、减少 SELECT 列）；② 开启 Spill to Disk（JOIN/聚合/排序可溢写磁盘，但性能下降）；③ 调整 JOIN 策略（确保小表在 Build 侧）；④ 资源组限流（限制单查询内存上限）；⑤ 如果数据量确实超过 Presto 能力，考虑用 Spark。

### Q5: Presto 和 Trino 该选哪个？

**答**：对于新项目，推荐 Trino——社区更活跃、发版更快、湖仓（Iceberg/Hudi）支持更好、有 Fault-Tolerant Execution 容错能力。Trino 和 PrestoDB 的 SQL 语法和 Connector 接口高度兼容，迁移成本低。

---

## 九、与本书其他章节的关联

| 关联章节 | 关系 |
|---------|------|
| [6.3 Hive](./03-Hive.md) | Presto 最常用的数据源就是 Hive 表 |
| [6.4 Spark](./04-Spark.md) | Presto 和 Spark SQL 是互补关系（交互式 vs 批处理） |
| [6.9 湖仓一体](./09-湖仓一体.md) | Presto/Trino 是湖上查询的主力引擎 |
| [6.10 语义层](./10-语义层与指标平台.md) | Presto 是语义层查询路由的目标引擎之一 |
| [6.11 ClickHouse](./11-ClickHouse.md) | ClickHouse 存数据做预计算，Presto 做灵活的 Ad-hoc 查询 |
| [3.12 Kafka](../part3-java-deep/12-消息队列.md) | Kafka Connector 可以用 SQL 查询 Kafka Topic |

---

[← 6.11 ClickHouse](./11-ClickHouse.md) | [返回本章目录](./README.md) | [6.13 任务调度 →](./13-任务调度.md)
