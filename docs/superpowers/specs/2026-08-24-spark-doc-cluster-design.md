# Spark 文档组重构设计

**日期：** 2026-08-24  
**范围：** `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark*.md`

## 目标

把 Spark 相关文档从“主文 + 两篇超重子文”的松散结构，重构为“主文 / 主线专题 / 深专题 / 现代能力补位”的文档组，解决以下问题：

1. `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark.md` 既是主文又混入运维、面试、版本细节，主线不够清晰。
2. `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-性能优化.md` 过度膨胀，混合了 SQL 优化、资源调优、Shuffle、错误排查、兼容性与数据倾斜。
3. `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-调试运维.md` 结构相对稳定，但缺少 Kubernetes / 云原生排障视角。
4. 现有 Spark 文档组缺少对 Spark 3.x / 3.5+ 主流能力的结构性承接，包括 DPP、REBALANCE、AQE 限制、Spark Connect、Kubernetes 运行增强、Decommission 等。

## 非目标

1. 本轮不扩展到 `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/05-Flink*.md` 以外的文档。
2. 本轮不做技术事实的逐条全面校勘，只在必要处补位现代主流实践。
3. 本轮不重写整个大数据章节的命名体系，只对 Spark 文档组做统一。

## 设计原则

### 1. 文件角色固定

- **主文**：建立 Spark 心智模型与目录导航。
- **主线专题**：给读者一条稳定的阅读/调优顺序。
- **深专题**：承载细节、参数、案例、机制、错误模式。
- **前沿专题**：承载版本能力线、云原生运行与新使用方式。

### 2. 主文变薄，专题变深

`/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark.md` 只保留：

- Spark 为什么快
- RDD / DAG / 调度核心模型
- Spark SQL 核心抽象
- 部署模式概览
- 性能、运维、版本、面试导航

不再让主文承载：

- 详细参数表
- 大段运维流程
- 全量面试问答
- 深入 SQL 优化与错误速查

### 3. 性能优化文改为“决策树”

`/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-性能优化.md` 只负责：

- 调优优先级
- 典型判断信号
- 调优顺序
- 高频误区
- 跳转到细分专题

不再承载全部深挖。

### 4. 现代 Spark 能力补位

在新的文档组中必须有明确承接位：

- Dynamic Partition Pruning（DPP）
- REBALANCE 与更现代的小文件控制方式
- AQE 自动合并分区的限制与边界
- Spark Connect
- Spark on Kubernetes
- Decommission / 节点下线与弹性资源回收

### 5. 专题文独立编号

专题文不再复用主文中的 `六、七、八` 编号，也不保留 `6.1 / 6.7` 这类 inherited 编号。

## 目标文件结构

```text
/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/
  04-Spark.md
  04-Spark-性能优化.md
  04-Spark-调试运维.md
  04-Spark-面试深度剖析.md
  04-Spark-SQL与算子优化详解.md
  04-Spark-资源内存与Spill调优.md
  04-Spark-Shuffle机制与调优参数.md
  04-Spark-数据倾斜治理.md
  04-Spark-错误排查与兼容性.md
  04-Spark-部署与云原生运行.md
  04-Spark-版本演进与新能力.md
```

## 各文件职责

### `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark.md`
Spark 主章节。建立认知框架并承担全组导航。

### `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-性能优化.md`
Spark 调优总纲。讲顺序、讲判断、讲优先级，不吞细节。

### `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-调试运维.md`
日志、Spark UI、YARN/K8s 排障入口。

### `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-SQL与算子优化详解.md`
SQL 写法、Join、窗口函数、DPP、REBALANCE、Explain 等内容。

### `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-资源内存与Spill调优.md`
内存模型、Dynamic Allocation、OOM、Spill、Container 限制。

### `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-Shuffle机制与调优参数.md`
Shuffle 机制、分区参数、AQE 自动合并边界。

### `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-数据倾斜治理.md`
倾斜诊断与治理方法。

### `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-错误排查与兼容性.md`
错误模式、兼容性、运行时异常分型与修法。

### `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-部署与云原生运行.md`
YARN / Kubernetes、PVC、本地盘、调度器、Decommission。

### `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-版本演进与新能力.md`
AQE、DPP、Spark Connect、Spark 4.x 前瞻。

### `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-面试深度剖析.md`
高频问答专题。

## 迁移原则

1. 先搭骨架，再迁内容。
2. 先处理两份枢纽文档：`04-Spark.md` 与 `04-Spark-性能优化.md`。
3. 深专题优先落 4 篇：SQL、资源内存、Shuffle、错误排查。
4. 迁移完成后，再补部署、版本、面试、倾斜专题。
5. 所有源文档在删减后都必须保留摘要和跳转链接，避免信息突然消失。

## 必须显式补写的内容

1. DPP 与 EXPLAIN / PartitionFilters 验证方法。
2. REBALANCE 与 COALESCE / REPARTITION / REPARTITION_BY_RANGE 的边界。
3. AQE 自动合并不是万能；`parallelismFirst` 与 `advisoryPartitionSizeInBytes` 的关系。
4. Spark Connect 的定位与意义。
5. Spark on Kubernetes 的运行和排障入口。
6. Decommission / 节点下线与弹性资源回收。

## 成功标准

1. `04-Spark.md` 可以在 10 分钟内读完骨架，不再被细节淹没。
2. `04-Spark-性能优化.md` 成为稳定的调优主线，而不是知识仓库。
3. 每个新专题都有明确职责，没有大块内容角色重叠。
4. Spark 文档组在结构清晰度上达到或超过现有 Flink 文档组。
5. 所有 Spark 文档之间的相对链接有效，且专题标题不再保留 inherited 编号。
