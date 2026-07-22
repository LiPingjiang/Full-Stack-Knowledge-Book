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
| **联邦查询** | 一条 SQL 同时查 Hive + MySQL + Kafka |
| **内存计算** | 数据在内存中流式处理，不落盘（Pipeline 执行） |
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

**关键特点**：Pipeline 执行——数据在内存中流式处理，上游产出一批数据立即传给下游，不需要等整个 Stage 完成。这是 Presto 比 Spark 快的核心原因之一（Spark 是 Stage 间全量 Shuffle 落盘）。

---

## 三、Connector 机制

### 3.1 Connector 架构

```java
// Connector 需要实现的核心接口
public interface Connector {
    ConnectorMetadata getMetadata();      // 元数据（表/列/分区信息）
    ConnectorSplitManager getSplitManager(); // 数据分片（Split）
    ConnectorPageSourceProvider getPageSourceProvider(); // 数据读取
}
```

### 3.2 主流 Connector

| Connector | 数据源 | 特点 |
|-----------|--------|------|
| **Hive** | HDFS/S3 上的 Hive 表 | 最常用，支持 ORC/Parquet/Iceberg |
| **Iceberg** | Iceberg 表 | 支持 Time Travel、Partition Evolution |
| **MySQL** | MySQL 数据库 | 谓词下推到 MySQL 执行 |
| **Kafka** | Kafka Topic | 实时查询 Kafka 中的数据 |
| **Elasticsearch** | ES 索引 | 全文检索 + 聚合 |
| **Redis** | Redis | 查询 Redis 中的数据 |
| **TPCH/TPCDS** | 基准测试 | 内置测试数据生成 |

### 3.3 联邦查询示例

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
| **执行模型** | Pipeline（流式） | Stage-based（批式） |
| **中间数据** | 内存中流转，不落盘 | Shuffle 落盘 |
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
