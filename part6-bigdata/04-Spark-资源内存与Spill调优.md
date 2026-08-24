# 6.4 Spark 资源内存与 Spill 调优

> 适用场景：任务慢、OOM、Spill 过多、资源参数难以理解  
> 阅读前提：已了解 Executor / Task / Container 基本概念  
> 相关阅读：  
> - [Spark 性能优化](./04-Spark-性能优化.md)  
> - [Spark 错误排查与兼容性](./04-Spark-错误排查与兼容性.md)

## 一、先建立资源调优的心智模型

### 1.1 CPU、内存、并行度不是独立参数

Spark 的资源调优不是把 CPU、内存、分区数分别调一遍，而是要把它们当成一组联动关系来理解。单个参数“看起来合理”，并不代表整体运行就合理。

### 1.2 Executor 维度与 Task 维度的关系

Executor 是资源容器，task 是实际工作单元。你需要同时考虑：

- 一个 Executor 同时能跑多少 task
- 每个 task 平均吃多少内存
- task 是 CPU 型、内存型还是 IO 型

### 1.3 为什么“加机器”不一定解决问题

如果主矛盾是 SQL 写法、数据倾斜或小文件，那么简单加机器经常只会让总资源成本变高，而不会真正解决瓶颈。

## 二、动态资源分配

### 2.1 静态资源分配的痛点

静态配置常见问题是：

- 高峰时不够用
- 低峰时大量浪费
- 不同作业难以共存

### 2.2 Dynamic Allocation 的工作方式

Dynamic Allocation 会根据任务积压情况动态增减 Executor。它的价值在于资源弹性，而不是改变执行计划本身。

### 2.3 Shuffle Tracking 与 External Shuffle Service

这两类机制都服务于“Executor 回收后 Shuffle 数据还能不能安全被后续阶段读取”。简化理解：

- 较新的方式更强调 Spark 自己跟踪 Shuffle 文件生命周期
- 传统方式更依赖额外 Shuffle Service 承接

### 2.4 Dynamic Allocation 与 AQE 的边界

两者解决的问题不同：

- Dynamic Allocation：有多少工人
- AQE：工人怎么干活

它们可以一起开启，但不要混为一谈。

## 三、Spark 内存模型

### 3.1 执行内存与存储内存

Spark 内存里最重要的区分是：

- **执行内存**：排序、聚合、Join、Shuffle 等运行期使用
- **存储内存**：缓存、持久化数据使用

### 3.2 memory.fraction 的作用

`memory.fraction` 本质上影响的是 Spark 可用于统一内存管理的比例。它不是“越大越好”或“越小越安全”的简单问题，而要结合具体任务形态来看。

### 3.3 堆内、堆外与统一内存管理

理解堆内与堆外的意义，在于你要区分：

- JVM 本身可控的堆空间
- Netty / Python / 本地库等可能额外消耗的空间
- YARN / Kubernetes 实际看到的是总容器占用

### 3.4 Task 级内存竞争

即使单个 Executor 总内存够大，也可能因为同时跑的 task 太多，导致单 task 可用内存不足，进而出现大量 Spill 或直接 OOM。

## 四、YARN / 容器内存约束

### 4.1 executor.memory 与 memoryOverhead

- `executor.memory`：更接近 JVM 堆
- `memoryOverhead`：更接近堆外与附加开销

很多“明明堆没满却被 kill”的问题，都出在第二项。

### 4.2 为什么会被 YARN kill

YARN / 容器会看总物理内存占用，不只看 JVM 堆。所以进程被 kill 不一定是经典的 JVM OOM，也可能是容器总体超限。

### 4.3 Exit code 143 与 JVM OOM 的区别

一个非常重要的区分是：

- JVM OOM：应用自己抛 `OutOfMemoryError`
- exit code 143：更像是容器被外部强制终止

排障方向完全不同。

## 五、Spill 机制

### 5.1 Memory Spill 与 Disk Spill 是什么关系

很多时候 Spill 不是“先有一个，再有另一个”的简单线性关系，而是内存吃紧后的中间结果落盘策略。你更应该关心的是：为什么需要 Spill、Spill 是否过量、是否持续成为瓶颈。

### 5.2 Spill 是坏事吗

不是。适度 Spill 是 Spark 处理大数据量时的正常保护机制。真正有问题的是：

- Spill 量过大
- 大量 task 都在频繁 Spill
- Spill 导致 stage 耗时明显异常

### 5.3 Spill 多到什么程度需要处理

没有绝对阈值，但如果你看到：

- stage 时间大部分耗在 spill / sort / shuffle
- 同时伴随高 GC、低吞吐
- 单 task 输入并不大却频繁 Spill

那就需要回到分区、内存和 SQL 写法一起看。

### 5.4 相关参数如何理解

与其死记很多参数，不如先问：

- 我是在控制单 task 负载，还是在控制总体并发？
- 我是在推迟 Spill，还是在更早 Spill 以保护稳定性？
- 我是在解决根因，还是只是在缓和症状？

## 六、常见资源型问题

### executor.cores 与内存竞争

`executor.cores` 调高后，并发 task 数也会上去。如果单 task 本来就吃内存，这可能反而让 Executor 内部竞争更激烈。

### 动态分区写入 OOM

动态分区写出容易出现“前面都还好，写出阶段突然出问题”。这类问题通常要重点看：

- 写出前最后一个 stage 的分区与聚合方式
- 单 task 写出负载
- 下游分区字段分布

### collect / toPandas / 广播引发的 Driver 问题

Driver 内存问题最常见的几个触发点是：

- `collect()` 拉太多数据回 Driver
- `toPandas()` 在本地做巨量收集
- 广播对象本身太大

这类问题不是 Executor 调参能解决的。

### 大对象、长字符串、超长 JSON 的特殊影响

一些任务数据量不大却依然很痛苦，往往不是行数问题，而是对象形态问题：

- 单行很宽
- 字段很长
- JSON / 字符串对象特别大

这种场景下，Spill、序列化和 GC 压力都会被放大。

## 七、调优方法论

### 先调分区还是先调内存

一般先看分区和单 task 负载，再决定是否加内存。因为如果单 task 本来就太重，加内存只是拖延问题暴露。

### 先降单 task 压力还是先扩容

如果问题集中在个别大 task，优先考虑降单 task 压力；如果问题是全局并发明显不足，再考虑扩容。

### 哪些场景下参数调优收益很有限

以下场景里，参数调优收益通常有限：

- SQL 写法明显不合理
- 上游扫描量过大
- 倾斜很严重
- 输出分布失控

这时更应该回到逻辑层，而不是继续堆资源。
