# 附录 A5：ElasticSearch——全文检索与实时分析引擎

> **一句话定位**：MySQL 擅长事务和精确查询，但一旦涉及**全文搜索**（"包含关键词的所有商品"）、**模糊匹配**（纠错、同义词）、**海量数据聚合分析**（实时统计、日志分析），MySQL 的 LIKE '%keyword%' 就力不从心了。ElasticSearch（简称 ES）就是为这些场景而生的分布式搜索与分析引擎。

---

## 一、核心概念——ES 和 MySQL 的类比

| MySQL 概念 | ES 概念 | 说明 |
|-----------|---------|------|
| Database | Index（索引） | 一类数据的集合 |
| Table | Type（7.x 已废弃） | ES 7.x 起一个 Index 只有一个 Type（`_doc`） |
| Row | Document（文档） | 一条数据，JSON 格式 |
| Column | Field（字段） | 文档中的一个属性 |
| Schema | Mapping | 字段名、类型、分词器等定义 |
| SQL 查询 | DSL 查询（JSON） | 用 JSON 描述查询条件 |
| Index（索引） | 倒排索引 | ES 的核心数据结构 |

> **关键认知差异**：MySQL 的"索引"是辅助查找的 B+ 树，ES 的"Index"是数据集合的名字，而 ES 底层用的是**倒排索引**——完全不同的东西。

---

## 二、倒排索引——ES 为什么快

### 2.1 正排索引 vs 倒排索引

```
正排索引（MySQL 的思路）：
文档 → 包含哪些词
  doc1: "Java 并发编程"
  doc2: "Java 集合框架"
  doc3: "Python 并发"

倒排索引（ES 的思路）：
词 → 出现在哪些文档
  "Java"   → [doc1, doc2]
  "并发"   → [doc1, doc3]
  "编程"   → [doc1]
  "集合"   → [doc2]
  "框架"   → [doc2]
  "Python" → [doc3]
```

当你搜索"Java 并发"时，ES 查倒排索引取"Java"的文档列表 `[doc1, doc2]` 和"并发"的文档列表 `[doc1, doc3]`，取交集得 `[doc1]`——两次哈希查找 + 一次交集运算，不需要遍历所有文档。

### 2.2 分词器（Analyzer）

倒排索引的质量取决于**分词**——把文本拆成什么样的词条（Term）。

```
Standard Analyzer:  "Hello World" → ["hello", "world"]（英文按空格分，转小写）
IK Analyzer:        "中华人民共和国" → ["中华人民共和国", "中华", "人民", "共和国"]（中文分词）
```

| 分词器 | 适用场景 |
|--------|---------|
| Standard | 英文默认，按空格分词 |
| IK（ik_max_word / ik_smart） | 中文分词（最细粒度 / 智能切分） |
| Keyword | 不分词，整个字段作为一个词条（适合 ID、状态码） |
| Whitespace | 按空格分词，不转小写 |

---

## 三、集群架构

### 3.1 核心概念

```
Cluster（集群）
  └── Node（节点）：一个 ES 实例
       └── Index（索引）：一类数据
            └── Shard（分片）：索引的物理分割
                 ├── Primary Shard（主分片）：数据的原始存储
                 └── Replica Shard（副本分片）：主分片的拷贝
```

**分片策略**：一个 Index 的数据被切分为多个 Shard，分散到不同 Node 上。查询时并行查各分片再合并结果。主分片数量在创建 Index 时确定，**不可更改**（ES 7.x 默认 1 个主分片）；副本数量可以动态调整。

### 3.2 写入流程

```
① 客户端请求发到任意节点（协调节点）
② 协调节点根据 routing（默认 hash(doc_id) % 主分片数）定位目标主分片
③ 主分片写入成功后，转发给副本分片
④ 副本确认后，返回客户端成功
```

### 3.3 近实时（Near Real-Time）原理

ES 写入后**不是立即可搜**，而是有约 1 秒的延迟（`refresh_interval`）。写入流程：数据先进 in-memory buffer → 每秒 refresh 到一个新的 segment（Lucene 可搜索的最小单元）→ 后台 merge 合并小 segment。这就是"近实时"的由来。

---

## 四、DSL 查询入门

### 4.1 查询分类

| 类型 | 说明 | 示例场景 |
|------|------|---------|
| **match** | 全文匹配（先分词再查倒排索引） | 搜索框输入关键词 |
| **term** | 精确匹配（不分词） | 按状态码、ID 过滤 |
| **range** | 范围查询 | 价格区间、日期范围 |
| **bool** | 组合查询（must/should/must_not/filter） | 多条件组合 |
| **aggs** | 聚合分析 | 统计、分组、Top N |

### 4.2 查询示例

```json
// 搜索标题包含"Java"且价格在 50-100 之间的书
{
  "query": {
    "bool": {
      "must": [
        { "match": { "title": "Java" } }
      ],
      "filter": [
        { "range": { "price": { "gte": 50, "lte": 100 } } }
      ]
    }
  },
  "sort": [{ "price": "asc" }],
  "from": 0,
  "size": 10
}
```

> **must vs filter**：`must` 参与相关性评分（`_score`），`filter` 只做过滤不评分且会被缓存。对于不需要评分的条件（如日期范围、状态过滤），用 `filter` 性能更好。

### 4.3 聚合（Aggregation）

```json
// 按品牌分组统计销量，并计算平均价格
{
  "size": 0,
  "aggs": {
    "brands": {
      "terms": { "field": "brand.keyword" },
      "aggs": {
        "avg_price": { "avg": { "field": "price" } },
        "total_sales": { "sum": { "field": "sales" } }
      }
    }
  }
}
```

---

## 五、ES 与 MySQL 的互补关系

ES 不是要替代 MySQL，而是和 MySQL **各司其职**：

| 维度 | MySQL | ElasticSearch |
|------|-------|---------------|
| 定位 | 事务型数据库（OLTP） | 搜索与分析引擎 |
| 强项 | 事务、关联查询、精确查询 | 全文搜索、模糊匹配、聚合分析 |
| 弱项 | 全文搜索、海量数据聚合 | 事务、关联查询、频繁更新 |
| 数据一致性 | ACID 强一致 | 近实时（~1秒延迟） |
| 典型场景 | 订单表、用户表 | 搜索框、日志分析、商品搜索 |

**常见架构**：MySQL 作为主数据源，通过 Canal/Binlog 同步到 ES，MySQL 负责写和事务性读，ES 负责搜索和分析。

---

## 六、面试深度剖析：ES 高频考点

### 考点 1：ES 为什么比 MySQL 全文搜索快？

> **面试官**：「MySQL 也有 FULLTEXT 索引，为什么还要用 ES？」

MySQL FULLTEXT 索引功能有限（中文分词差、不支持复杂查询、不支持分布式）。ES 基于 Lucene 的倒排索引，原生支持分词、相关性评分、分布式分片并行查询、丰富的查询 DSL，是专门为搜索场景设计的。

### 考点 2：深分页问题

> **面试官**：「ES 的 from + size 深分页有什么问题？」

和 MySQL 类似，`from: 10000, size: 10` 意味着每个分片要返回 10010 条给协调节点，协调节点收集所有分片的结果后全局排序取 Top 10010 再截取最后 10 条。分片越多、from 越大，内存和计算开销越大。

解决方案：`search_after`（游标分页，类似 MySQL 的游标方案）或 `scroll` API（快照遍历，适合导出场景）。

### 考点 3：如何保证 MySQL 和 ES 的数据一致性？

> **面试官**：「MySQL 写了数据，ES 怎么保证同步？」

**异步双写**（应用层同时写 MySQL 和 ES）→ 简单但容易不一致。**Canal 监听 Binlog**（推荐）→ MySQL 写入后 Canal 监听 binlog 变更推送到 ES，最终一致。**MQ 中转**→ 业务写 MySQL 后发消息，消费者同步到 ES。

---

## 七、NoSQL 家族速览

ES 属于 NoSQL 大家族的一员。除了 ES 和 [Redis](./08-缓存与Redis.md)（已在 3.8 详细讲过），还有一个常见的 NoSQL 值得了解：

### 7.1 MongoDB——文档型数据库

**核心理念**：用**JSON-like 文档**（BSON）替代关系表的行和列。一个文档就是一条记录，文档可以嵌套，不需要预定义 Schema。

```json
// MongoDB 中的一个"订单"文档——把订单和订单项嵌套在一起
{
  "_id": ObjectId("..."),
  "user": "张三",
  "total": 299.00,
  "items": [
    { "name": "Java 编程思想", "price": 99.00, "qty": 1 },
    { "name": "深入理解 JVM", "price": 200.00, "qty": 1 }
  ],
  "address": { "city": "北京", "district": "海淀" }
}
```

**vs MySQL**：MySQL 这个场景需要 orders 表 + order_items 表 + 外键关联，查询要 JOIN。MongoDB 把相关数据嵌套在一个文档里，一次读取拿到所有信息，读性能好。但代价是不适合频繁跨文档关联查询，也没有 MySQL 那样的 ACID 事务保证（MongoDB 4.0+ 支持多文档事务，但性能代价较大）。

**适用场景**：内容管理系统（文章/评论结构灵活）、用户画像（字段不固定）、物联网设备数据（海量写入）、游戏存档。

**不适用场景**：强事务需求（如金融交易）、复杂关联查询、需要严格 Schema 约束的业务数据。

> 本书不单独开设 MongoDB 章节，因为对于 Java 后端转全栈的路径来说，MySQL + Redis + ES 覆盖了绝大多数场景，MongoDB 属于"了解定位，按需深入"的级别。

---

[← 附录 A4 SQL 语言与数据处理](./A4-SQL语言与数据处理.md) | [返回本章目录](./README.md) | [附录 A6 代码规范与设计原则 →](./A6-代码规范与设计原则.md)
