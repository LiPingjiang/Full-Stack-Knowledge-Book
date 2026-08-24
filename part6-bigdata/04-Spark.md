# 6.4 Spark——内存计算引擎

## 目录

- [一、为什么 Spark 比 MapReduce 快](#一为什么-spark-比-mapreduce-快)
- [二、核心抽象——RDD](#二核心抽象rdd)
- [三、执行架构](#三执行架构)
- [四、Spark SQL——最常用的模块](#四spark-sql最常用的模块)
- [五、部署模式与运行环境](#五部署模式与运行环境)
- [六、性能优化导航](#六性能优化导航)
- [七、调试运维导航](#七调试运维导航)
- [八、版本演进与新能力导航](#八版本演进与新能力导航)
- [九、面试深度剖析导航](#九面试深度剖析导航)

## 一、为什么 Spark 比 MapReduce 快

Spark 比 MapReduce 快，核心不在“换了一个框架名字”，而在它把多轮计算、内存复用、DAG 调度和更高层的 SQL 优化组合到了一起。

### 1.1 内存计算与 DAG 执行

MapReduce 的每一轮 Map / Reduce 都天然以磁盘为边界，而 Spark 会把一串可以流水线执行的算子组合成一个 DAG，在内存中尽量连续完成。只要中间结果不需要被物化到外部存储，Spark 就能显著减少磁盘 IO 和任务启动开销。

### 1.2 减少中间落盘与重复启动开销

Spark 的优势经常体现在“多步 ETL”和“多轮迭代”任务中：

- 中间结果尽量不落 HDFS
- 多个 transformation 可以合并进同一个 stage
- SQL 层还有 Catalyst / AQE 做计划优化
- 缓存与广播可以避免重复计算或重复拉取

### 1.3 Spark 快的边界在哪里

Spark 并不是“永远更快”。当任务具备以下特征时，Spark 的优势会明显下降：

- 数据规模远超可用内存，需要大量 spill
- 上游 SQL 写法导致扫描量过大
- Shuffle 极重，网络与磁盘成为主瓶颈
- 数据倾斜导致少数 task 拖慢全局

所以，Spark 快不是因为“全都在内存里”，而是因为它把“少落盘、少重复、少无效计算”做成了默认路径。

## 二、核心抽象——RDD

RDD 是 Spark 最基础的抽象。即使今天大家更多通过 DataFrame / SQL 使用 Spark，理解 RDD 仍然是理解 Spark 执行模型的关键。

### 2.1 RDD 是什么

RDD（Resilient Distributed Dataset）可以理解为：

- 一个分布式的数据集
- 按分区切分，能并行处理
- 记录自身是如何从上游计算而来的
- 某个分区丢失时可以通过 lineage 重算

它的价值不只是“存数据”，而是同时承载了“分区 + 依赖关系 + 容错信息”。

### 2.2 Transformation vs Action

Spark 的算子分两类：

- **Transformation**：描述如何从旧 RDD 派生出新 RDD，例如 `map`、`filter`、`flatMap`
- **Action**：真正触发执行，例如 `count`、`collect`、`saveAsTextFile`

这套设计让 Spark 可以先积累一整条计算链，再统一做 DAG 划分与优化。也正因为如此，很多“看起来只是写了一段链式代码”的逻辑，真正执行时可能被划分成多个 stage。

### 2.3 宽依赖 vs 窄依赖

理解宽依赖和窄依赖，是理解 stage 划分的入口：

- **窄依赖**：子分区只依赖少量上游分区，通常可以流水线执行
- **宽依赖**：子分区需要依赖很多上游分区，通常意味着要发生 Shuffle

简单说：**宽依赖决定 stage 边界，Shuffle 往往是性能拐点**。

### 2.4 广播变量与累加器

Spark 提供了两类最常见的共享变量：

- **广播变量（Broadcast）**：把较小的只读数据分发到各 Executor，避免每个 task 重复拉取
- **累加器（Accumulator）**：让多个 task 向 Driver 汇总某个数值型统计结果

两者解决的问题完全不同：广播变量用于“共享只读输入”，累加器用于“汇总执行过程中的指标”。

### 2.5 Lineage 与容错

RDD 的容错不是依赖副本，而是依赖 lineage。也就是说，Spark 记录“这个 RDD 是怎么从前面的 RDD 算出来的”。某个分区丢失时，它只需要按依赖关系重算相关分区，而不是重做整个作业。

如果 lineage 太长、重算代价太高，可以用 checkpoint 截断血缘。但 checkpoint 是一种成本更高的“落地换恢复能力”手段，不应该滥用。

## 三、执行架构

Spark 的执行架构可以从“谁负责决策、谁负责执行、谁负责分配资源”三个角度理解。

### 3.1 Driver、Executor 与 Cluster Manager

- **Driver**：解析代码、构建 DAG、划分 stage、调度 task
- **Executor**：真正执行 task、缓存数据、维护运行时状态
- **Cluster Manager**：负责资源分配，常见的是 Standalone、YARN、Kubernetes

可以把 Driver 理解成“大脑”，Executor 理解成“工人”，Cluster Manager 理解成“资源调度中心”。

### 3.2 Job、Stage、Task 的关系

常见关系是：

- 一个 action 触发一个或多个 **job**
- job 会被切分成多个 **stage**
- 每个 stage 再按分区拆成多个 **task**

影响性能的很多问题，本质上都可以往这三层上归因：

- job 太多：通常是 action 太碎
- stage 太重：通常是 Shuffle 或依赖关系复杂
- task 分布不均：通常是倾斜、分区数不合理、单 task 压力过大

### 3.3 DAGScheduler 与 TaskScheduler

Spark 的调度大致分两层：

- **DAGScheduler**：根据宽依赖划分 stage，确定执行边界
- **TaskScheduler**：把 task 分发到具体 Executor 上执行

所以 DAGScheduler 负责“切图”，TaskScheduler 负责“派工”。

### 3.4 本地化调度

Spark 会尽量把 task 调度到离数据更近的节点上，以减少网络传输。这就是所谓的本地化调度。数据本地性越好，通常网络开销越低；但本地性并不是唯一目标，等待本地性太久也会拖慢整体调度，因此 Spark 会在等待与执行之间做折中。

### 3.5 Spark 运行时的资源心智模型

理解 Spark 资源问题，不能只盯着一个参数，而要同时看：

- 分区数
- 并行度
- executor 个数
- executor.cores
- executor.memory
- memoryOverhead

这些量是联动的。比如“task 慢”并不一定是 CPU 不够，也可能是分区太少导致单 task 太重；“加内存”也不一定能解决问题，因为瓶颈可能在 Shuffle、倾斜或 SQL 写法本身。

更细的资源、内存与 Spill 机制，请看：

- [Spark 资源内存与 Spill 调优](./04-Spark-资源内存与Spill调优.md)
- [Spark Shuffle 机制与调优参数](./04-Spark-Shuffle机制与调优参数.md)

## 四、Spark SQL——最常用的模块

今天的大多数 Spark 任务，实际上是通过 DataFrame / Dataset / SQL 完成的，因此理解 Spark SQL 比单独理解 RDD 更贴近生产环境。

### 4.1 RDD、DataFrame、Dataset 的区别与选型

可以把三者理解成三层抽象：

- **RDD**：最底层，灵活，但优化机会最少
- **DataFrame**：无强类型 schema + Catalyst 优化 + Tungsten 执行优势
- **Dataset**：在 JVM 语言中提供更强的类型安全，但在复杂场景里也可能引入额外开销

工程上常见建议是：

- 能用 SQL / DataFrame，就优先不用裸 RDD
- 只有在需要非常底层的控制时，再考虑 RDD
- Dataset 更适合 JVM 场景下对类型约束要求高的代码

### 4.2 Catalyst 做了什么

Catalyst 是 Spark SQL 的优化器。它会在逻辑计划和物理计划之间做多轮规则优化，例如：

- 谓词下推
- 列裁剪
- 常量折叠
- Join 重排
- 物理算子选择

所以“SQL 写法是否友好”会直接影响 Spark 能不能帮你做足够多的自动优化。

### 4.3 Tungsten 与 Whole-Stage CodeGen

Tungsten 是 Spark SQL 性能提升的关键基础设施之一，它做的事情包括：

- 更紧凑的内存表示
- 更少的对象开销
- 更高效的二进制处理
- Whole-Stage CodeGen 带来的执行路径压缩

如果说 Catalyst 负责“想清楚怎么执行”，那 Tungsten 更像是“把执行这件事做快”。

### 4.4 Spark SQL 的执行流程

Spark SQL 的大致流程可以理解为：

1. 解析 SQL / DataFrame 表达式
2. 生成逻辑计划
3. 逻辑优化
4. 生成物理计划
5. 执行物理算子

真正的性能差距，经常就出现在 3~5 这几步：同一个业务需求，不同写法会导向完全不同的扫描量、Join 策略和 Shuffle 代价。

### 4.5 CBO、AQE 与运行时优化的关系

Spark SQL 里的优化可以粗略分三层：

- **静态规则优化**：Catalyst 的规则变换
- **基于统计信息的优化**：CBO
- **运行时自适应优化**：AQE

AQE 的价值在于：它不是只靠编译期估算，而是能在查询执行过程中根据真实统计信息重新优化后续计划。但 AQE 不是万能的：它只能在特定物化点介入，也管不了上游扫描写法错误、CTE 重复计算、业务分布不均等问题。

SQL 写法、Join 策略、DPP、REBALANCE、Explain 的细节，请看：

- [Spark SQL 与算子优化详解](./04-Spark-SQL与算子优化详解.md)

## 五、部署模式与运行环境

### 5.1 Local、Standalone、YARN、Kubernetes

常见部署模式可以分成四类：

- **Local**：本地开发和学习
- **Standalone**：Spark 自带的集群管理器
- **YARN**：传统大数据平台里最常见
- **Kubernetes**：云原生环境里越来越重要

### 5.2 Client 模式与 Cluster 模式

部署模式之外，还要区分提交模式：

- **Client 模式**：Driver 在提交端
- **Cluster 模式**：Driver 在集群端

这会直接影响：

- 日志在哪里看
- 网络依赖在哪里
- 提交端挂了会不会影响任务
- 排障时该先看哪一层

### 5.3 传统大数据平台与云原生环境的差异

YARN 时代关注的是离线资源池、日志聚合、NodeManager、本地盘与 HDFS；Kubernetes 时代则更强调：

- Pod 生命周期
- 本地盘 / PVC
- 弹性扩缩容
- 多租户隔离
- 调度器协同

今天再讲 Spark 部署，如果只讲“有四种模式”，已经不够用了。

### 5.4 相关阅读：部署与云原生运行专题

- [Spark 部署与云原生运行](./04-Spark-部署与云原生运行.md)

## 六、性能优化导航

### 6.1 Spark 调优应该遵循什么顺序

推荐把 Spark 调优看成一条固定顺序：

1. 先减少扫描量和中间数据量
2. 再优化 SQL 写法和算子行为
3. 再看分区与分布是否合理
4. 再看资源与内存配置
5. 最后才是细节参数调优

把顺序反过来，经常就会变成“参数很多，但主矛盾没动”。

### 6.2 相关阅读：性能优化、SQL 优化、Shuffle、资源内存、数据倾斜专题

- [Spark 性能优化](./04-Spark-性能优化.md)
- [Spark SQL 与算子优化详解](./04-Spark-SQL与算子优化详解.md)
- [Spark 资源内存与 Spill 调优](./04-Spark-资源内存与Spill调优.md)
- [Spark Shuffle 机制与调优参数](./04-Spark-Shuffle机制与调优参数.md)
- [Spark 数据倾斜治理](./04-Spark-数据倾斜治理.md)

## 七、调试运维导航

### 7.1 日志、Spark UI、资源管理器分别解决什么问题

Spark 排障通常不是看某一个地方就够了，而是三类入口配合：

- **日志**：告诉你异常栈、组件归属、启动过程
- **Spark UI**：告诉你 stage、task、SQL、Executor 的运行表现
- **资源管理器（YARN / Kubernetes）**：告诉你容器、Pod、资源、调度状态

### 7.2 相关阅读：调试运维、错误排查与兼容性专题

- [Spark 调试运维](./04-Spark-调试运维.md)
- [Spark 错误排查与兼容性](./04-Spark-错误排查与兼容性.md)

## 八、版本演进与新能力导航

### 8.1 Spark 3.x 带来了什么变化

Spark 3.x 最重要的变化不是单个 API，而是执行模型和 SQL 优化能力整体升级，例如：

- AQE
- DPP
- 更完整的 Join Hint
- 更成熟的云原生运行支持

### 8.2 Spark Connect 是什么

Spark Connect 是 Spark 3.4 引入的新 client-server 架构，让客户端不再和 Spark runtime 紧耦合，更适合：

- Notebook / IDE 场景
- 平台化接入
- 多语言客户端
- 远端交互式分析

### 8.3 Spark 4.x 有哪些值得关注的新方向

Spark 4.x 值得重点关注的是两类方向：

- 使用方式进一步服务化、平台化
- 更面向现代数据工程的编排与 SQL 能力增强

### 8.4 相关阅读：版本演进与新能力专题

- [Spark 版本演进与新能力](./04-Spark-版本演进与新能力.md)

## 九、面试深度剖析导航

### 9.1 高频问题列表

高频问题通常集中在：

- Spark 为什么快
- RDD 怎么容错
- Shuffle 为什么慢
- 数据倾斜怎么处理
- OOM 怎么分型
- AQE 能做什么、不能做什么
- Spark Connect 是什么
- YARN 和 Kubernetes 有什么差异

### 9.2 相关阅读：面试深度剖析专题

- [Spark 面试深度剖析](./04-Spark-面试深度剖析.md)
