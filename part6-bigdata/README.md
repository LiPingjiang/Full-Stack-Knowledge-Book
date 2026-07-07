# 第六章 · 大数据基础——当单机 MySQL 扛不住时

> **读者画像**：你是一个 Java 后端开发者，日常和 MySQL 打交道。但当数据量从百万级增长到亿级甚至十亿级时，单机数据库在存储、计算、实时性三个维度上都会触碰天花板。这一章帮你理解大数据技术栈的全景、核心组件的定位与原理，以及它们和你熟悉的后端技术之间的关系。

---

## 本章定位

大数据和前端、后端一样，是全栈体系下的一个**平行技术领域**，不是后端的附属品。对于 Java 后端转全栈的开发者来说，你不需要成为大数据专家，但需要理解：

- 什么场景下该从 MySQL 切换到大数据技术栈
- 数据仓库分层模型（ODS→DWD→DWS→ADS）的设计思路
- 批处理（Hive/Spark）和实时计算（Flink）各自的定位
- 和你已经熟悉的 [消息队列 Kafka](../part3-java-deep/12-消息队列.md)、[MySQL](../part3-java-deep/09-数据库MySQL.md)、[ElasticSearch](../part3-java-deep/A5-ElasticSearch.md) 之间的协作关系

```mermaid
graph LR
    subgraph "全栈技术领域"
        A["第二章<br/>前端核心"]
        B["第三章<br/>Java 后端深潜"]
        C["第六章<br/>大数据基础"]
    end
    A -.用户界面.-> D["完整产品"]
    B -.业务逻辑.-> D
    C -.数据分析.-> D
    
    style C fill:#e1f5ff
```

---

## 各节导读

**[6.1 大数据技术栈全景](./01-大数据技术栈全景.md)** —— 为什么需要大数据技术（MySQL 三大瓶颈）、核心技术栈全景图（采集→存储→计算→应用）、存储分类（SQL/NoSQL/搜索/分布式文件）、计算/查询/采集/管道组件速查、数据仓库分层模型（ODS/DWD/DWS/ADS）、Lambda 架构 vs Kappa 架构、大数据与后端的协作模式。

**[6.2 HDFS](./02-HDFS.md)** —— 分布式文件系统的基石。NameNode + DataNode 架构、数据分块（128MB）与 3 副本放置策略、读写流程、NameNode HA 与 Federation。*面试剖析覆盖：HDFS 适合/不适合什么、NameNode 单点问题、小文件问题、Block 大小选择。*

**[6.3 Hive](./03-Hive.md)** —— 用 SQL 查大数据。Hive 的本质（SQL 翻译器 + MetaStore）、内部表 vs 外部表、分区裁剪（最重要的优化手段）、分桶、文件格式（ORC/Parquet）、SQL 差异速查、数据倾斜解决方案。*面试剖析覆盖：Hive 和数据库的区别、内外部表区别、数据倾斜排查、ORC vs Parquet 选型。*

**[6.4 Spark](./04-Spark.md)** —— 内存计算引擎。RDD 核心抽象（不可变/分区/惰性/血缘容错）、Transformation vs Action、宽窄依赖与 Shuffle、Job→Stage→Task 划分、Spark SQL 与 Catalyst 优化器、内存模型与 YARN 资源管理。性能优化专题（数据倾斜八种修复方案、Shuffle 调优、Spark UI 分析方法）见 [Spark 性能优化](./04-Spark-性能优化.md)，作业日志体系与调试运维（日志类型、排查方法、常见故障）见 [Spark 调试运维](./04-Spark-调试运维.md)。*面试剖析覆盖：Spark vs MapReduce、RDD 容错机制、Shuffle 原理、数据倾斜处理、内存模型。*

**[6.5 Flink](./05-Flink.md)** —— 实时计算引擎。流处理三大挑战（乱序/延迟/窗口）、Event Time vs Processing Time、Watermark 机制、四种窗口类型、Checkpoint + Exactly-Once 语义、Flink SQL。*面试剖析覆盖：Flink vs Spark Streaming、Checkpoint vs Savepoint、Watermark 设置、反压机制。*

**[6.6 Doris](./06-Doris.md)** —— MPP 实时分析数据库。FE + BE 架构、三种数据模型（Duplicate/Aggregate/Unique）、列式存储 + 向量化执行、物化视图、数据导入方式、Doris vs ClickHouse vs StarRocks 对比。*面试剖析覆盖：Doris 定位、数据模型选型、查询为什么快、实时数据导入方案。*

**[6.7 数据仓库设计](./07-数据仓库设计.md)** —— 数仓怎么建模。两大建模流派（Inmon 范式 vs Kimball 维度建模）、事实表与维度表、星型/雪花/星座模型对比、宽表设计、缓慢变化维（SCD）与拉链表、数仓分层各层设计要点、主题域划分、数据质量六维度。*面试剖析覆盖：星型 vs 雪花模型选型、事实表粒度设计、拉链表原理与实现、分层意义、宽表优缺点。*

**[6.8 大模型数据工程](./08-大模型数据工程.md)** —— 预训练数据从采集到入模。文本数据处理全流程（Common Crawl → 文本提取 → 语言识别 → 质量过滤 → 去重 → 安全过滤 → 数据配比）、多模态数据处理（图文对齐与 CLIP 打分、视频帧提取与字幕对齐、音频 ASR 转录、OCR 与文档理解）、去重技术深入（MinHash + LSH、SimHash）、工程实现技术栈（Spark + Ray、Parquet/WebDataset 格式）、与传统 ETL 的对比。*面试剖析覆盖：预训练数据核心流程、去重重要性与方法、多模态核心挑战、数据质量 vs 数量、数据配比策略。*

---

## 阅读建议

- 如果你是纯后端开发者，先读 6.1 建立全景认知，再挑和你工作相关的组件深入
- 如果面试大数据相关岗位，6.2-6.8 的面试剖析部分是重点
- 如果你需要和数据团队协作，重点理解数仓分层（6.1）、数仓建模（6.7）和 Kafka 的桥梁作用
- 如果你对 AI / 大模型方向感兴趣，6.8 介绍了预训练数据工程的全流程，和 6.4 Spark 的优化经验直接相通；更深入的 AI 工程知识（RAG、Prompt Engineering、模型部署、微调、Agent）详见 [第七章 AI 工程基础](../part7-ai-engineering/README.md)
- SQL 基础和查询优化详见 [附录 A4 SQL 语言与查询优化](../part3-java-deep/A4-SQL语言与数据处理.md)
- Kafka 的详细介绍在 [3.12 消息队列](../part3-java-deep/12-消息队列.md)
