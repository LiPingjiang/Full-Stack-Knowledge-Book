# Flink 配置参数速查

> **本文从 [05-Flink.md](./05-Flink.md) 独立拆分而来**，集中整理 Flink 生产环境常用的核心配置参数。

---

## 一、并行度相关参数

| 参数名（适用范围） | 默认值 | 含义 | 生产推荐值 |
|---|---|---|---|
| `parallelism.default`（通用） | `1` | 全局默认并行度，作业未显式设置并行度时的兜底值 | 不建议依赖全局默认值，应按算子/作业显式设置；集群级可设为 CPU 核数的整数倍 |
| `pipeline.max-parallelism`（通用，SQL 唯一入口） | 未设置（见下方默认值规则） | maxParallelism 的**全局配置项**，决定 Key Group 数量上限 | 按未来 3~5 年数据量预估，通常设为当前并行度的 2~4 倍，常见取值 128、256、512 |
| `table.exec.resource.default-parallelism`（SQL） | 未设置（跟随 `parallelism.default`） | Flink SQL 作业的默认并行度 | 按算子吞吐量和 Kafka 分区数评估设置 |

### maxParallelism 在 Flink SQL 中如何设置

这是实践中最容易踩坑的一点：**DataStream API 和 Flink SQL 设置 maxParallelism 的方式完全不同**。

**DataStream API**：可以在代码里直接调用 API 设置：

```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.getConfig().setMaxParallelism(512);
```

**Flink SQL**：**没有等价的 API**，SQL 作业里不存在类似 `env.setMaxParallelism()` 的调用入口。唯一的设置方式是通过配置项 `pipeline.max-parallelism`，有两种途径：

```sql
-- 方式一：在 SQL Client / SQL 作业开头用 SET 语句设置（作用于当前会话/作业）
SET 'pipeline.max-parallelism' = '512';

SELECT ...
```

```yaml
# 方式二：在 flink-conf.yaml 中全局配置（作用于该集群提交的所有作业）
pipeline.max-parallelism: 512
```

也可以在提交作业时通过命令行参数传入：

```bash
flink run -Dpipeline.max-parallelism=512 -c com.example.MyJob my-job.jar
```

**默认值规则**（未手动设置 `pipeline.max-parallelism` 时）：

- 当并行度 < 128 时，最大并行度默认 = 128
- 当并行度 ≥ 128 时，最大并行度 = `parallelism + parallelism / 2`，上限为 32768（2^15）

**修改 maxParallelism 后旧 Savepoint 无法恢复**：这是生产环境中最常见的事故之一。maxParallelism 决定了 Key Group 的数量（Key Group 数 = maxParallelism），Keyed State 的 Savepoint/Checkpoint 快照是按 Key Group 组织存储的。一旦修改了 `pipeline.max-parallelism`：

- Key Group 的数量发生变化，旧快照里的 Key Group 索引范围与新配置下的映射关系完全对不上；
- Flink 无法把旧快照中的状态数据重新映射到新的 Key Group 布局，恢复时会直接报错或导致状态丢失；
- 因此 maxParallelism **一旦上线就应视为不可变参数**，必须在作业首次上线前就规划好足够大的余量，中途只应调整 `parallelism.default`（当前并行度），不应调整 `pipeline.max-parallelism`。

如果确实需要修改 maxParallelism，只能放弃从旧 Savepoint 恢复状态（即状态重新计算/回放数据），或者提前通过 State Processor API 离线改写 Savepoint 中的 Key Group 布局后再恢复。

---

## 二、State Backend 配置

| 参数名（通用） | 默认值 | 含义 | 生产推荐值 |
|---|---|---|---|
| `state.backend` | `hashmap` | 状态后端类型：`hashmap`（纯内存，即 HashMapStateBackend）或 `rocksdb`（EmbeddedRocksDBStateBackend，本地磁盘 + 可增量） | 小状态（几百 MB 内）用 `hashmap` 追求吞吐；大状态（GB~TB 级）必须用 `rocksdb` |
| `state.backend.rocksdb.memory.managed` | `true` | RocksDB 是否使用 Flink 托管内存（纳入 TaskManager 统一内存管理，避免和 JVM 堆内存抢占物理内存导致 OOM） | 保持 `true`，配合 `taskmanager.memory.managed.fraction` 一起调优 |
| `state.backend.incremental` | `false` | 是否启用增量 Checkpoint（只上传自上次快照以来变化的 SST 文件，而非全量） | 大状态场景强烈建议 `true`；仅 RocksDB Backend 支持，HashMapStateBackend 不支持 |
| `state.checkpoints.dir` | 未设置 | Checkpoint 数据的默认存储路径（HDFS/S3 等），部分版本/场景也可用作业级 `execution.checkpointing.dir` 覆盖 | 生产必须显式配置，指向所有 JobManager/TaskManager 都可访问的分布式存储 |
| `state.savepoints.dir` | 未设置 | Savepoint 的默认存储目录，`flink savepoint` 命令未指定目标路径时使用 | 与 Checkpoint 目录分开存放，便于区分手动触发的版本快照和自动容错快照 |

> **说明**：不同 Flink 版本中，Checkpoint/Savepoint 目录既有全局配置项（`state.checkpoints.dir` / `state.savepoints.dir`），也有作业级配置项（`execution.checkpointing.dir` / `execution.checkpointing.savepoint-dir`）。生产环境建议全局配置一份兜底路径，同时按作业需要在代码或 SQL 中覆盖具体子目录。

---

## 三、RocksDB 调优参数

以下参数仅在 `state.backend: rocksdb` 时生效，通用于 DataStream API 和 Flink SQL 作业。

| 参数名 | 默认值 | 含义 | 生产推荐值 |
|---|---|---|---|
| `state.backend.rocksdb.block.cache-size` | `8 MB` | RocksDB Block Cache 大小，缓存热点数据块，减少磁盘读放大 | 根据可用内存调大，常见 64MB~256MB，配合托管内存统一调度 |
| `state.backend.rocksdb.writebuffer.size` | `64 MB` | 单个 MemTable（写缓冲区）的大小，MemTable 写满后刷盘生成 SST 文件 | 写多读少场景可适当调大（如 128MB），减少刷盘频率 |
| `state.backend.rocksdb.writebuffer.count` | `2` | 每个 State 允许同时存在的 MemTable 数量（含正在刷盘的） | 一般保持默认；写入极高峰场景可调至 3~4，但会增加内存占用 |
| `state.backend.rocksdb.thread.num` | `2` | RocksDB 后台线程数（用于 Compaction 和 Flush） | 大状态、Compaction 压力大时调高至 4；需权衡 CPU 资源 |
| `state.backend.rocksdb.bloom-filter.per-key-bits` | `10`（10 bits/key，约 1% 误判率） | 每个 Key 用于构建 [Bloom Filter](../part3-java-deep/A1-核心数据结构原理.md#三布隆过滤器bloom-filter用-1-的误判换-99-的内存节省) 的 bit 数，用于快速判断 key 是否可能存在于某个 SST 文件，降低读放大 | 保持默认即可满足大部分场景，读密集型可适当调高（如 16bit）以降低误判率 |
| `state.backend.rocksdb.predefined-options` | `DEFAULT` | RocksDB 预定义调优模板，如 `SPINNING_DISK_OPTIMIZED`、`SPINNING_DISK_OPTIMIZED_HIGH_MEM`、`FLASH_SSD_OPTIMIZED` | 机械盘用 `SPINNING_DISK_OPTIMIZED_HIGH_MEM`；SSD 用 `FLASH_SSD_OPTIMIZED` |
| `state.backend.rocksdb.localdir`（也写作 `state.backend.rocksdb.local-dir`） | 系统临时目录 | RocksDB 本地文件存放目录，支持配置多个目录（逗号分隔）实现多磁盘均摊 I/O | 生产必须配置到独立数据盘（优先 SSD），多盘场景配置多个路径分摊 I/O，避免多个 Sub-Task 共用一块盘导致 I/O 打满 |

> **经验数据**：实测显示三个并行度分别使用三块独立磁盘时，单盘 I/O 利用率约 45%；而两个并行度共用一块磁盘时，该盘 I/O 利用率飙升至 91.6%，吞吐量腰斩。因此 `state.backend.rocksdb.localdir` 配置多磁盘路径，是大 State 作业性价比最高的调优手段之一。

---

## 四、Checkpoint 配置

以下配置均属于 `execution.checkpointing.*` 系列（Flink 1.15+ 推荐命名空间），通用于 DataStream API 和 Flink SQL。

| 参数名 | 默认值 | 含义 | 生产推荐值 |
|---|---|---|---|
| `execution.checkpointing.interval` | 未设置（不开启 Checkpoint） | 两次 Checkpoint 的基础触发间隔 | 通常 1~10 分钟；太小拖累吞吐，太大增加故障恢复时的数据重放量 |
| `execution.checkpointing.timeout` | `10 min` | 单次 Checkpoint 的超时时间，超时视为本次失败 | 通常设为 interval 的 2~3 倍，避免大状态场景下正常快照被误判超时 |
| `execution.checkpointing.min-pause` | `0` | 两次 Checkpoint 之间的最小间隔（即使上一次很快完成，也要等这个时间再触发下一次） | 防止 Checkpoint 过于密集挤占处理资源，建议设为 interval 的 50%~100% |
| `execution.checkpointing.mode` | `EXACTLY_ONCE` | Checkpoint 一致性语义：`EXACTLY_ONCE`（精确一次，需要 Barrier 对齐或 Unaligned）或 `AT_LEAST_ONCE`（至少一次，性能更高但可能重复处理） | 绝大多数场景保持 `EXACTLY_ONCE`；仅极端追求低延迟且能容忍重复时才用 `AT_LEAST_ONCE` |
| `execution.checkpointing.unaligned.enabled` | `false` | 是否启用 Unaligned Checkpoint（Barrier 到达即打快照，不等待对齐，快照包含缓冲区数据） | 反压严重、Barrier 对齐经常超时导致 Checkpoint 失败时开启；要求 `EXACTLY_ONCE` 且并发 Checkpoint 数为 1 |
| `execution.checkpointing.aligned-checkpoint-timeout` | `0`（即不自动降级） | 对齐 Checkpoint 的超时阈值，超过该时长自动从 Aligned 降级为 Unaligned Checkpoint | 常设为几十秒（如 `30 s`），作为兼顾正常场景效率和反压场景稳定性的折中方案 |

---

## 五、内存配置

以下参数均属于 TaskManager 层面，通用于 DataStream API 和 Flink SQL 作业（两者共用同一套 TaskManager 进程）。

| 参数名 | 默认值 | 含义 | 生产推荐值 |
|---|---|---|---|
| `taskmanager.memory.task.heap.size` | 由 `taskmanager.memory.process.size` 按比例推导 | 用户代码和 Flink 框架运行时可用的 JVM 堆内存，存放用户对象、算子逻辑等 | 按作业实际内存占用压测调整，避免设置过小触发频繁 GC 或 OOM |
| `taskmanager.memory.managed.fraction` | `0.4` | 托管内存占 Flink 总内存的比例，RocksDB State Backend、批处理排序/哈希等都使用这部分内存 | 使用 RocksDB 时保持较高比例（默认 0.4 通常够用，State 极大时可调高） |
| `taskmanager.memory.network.fraction` | `0.1` | 网络缓冲内存占 Flink 总内存的比例，用于算子间数据 Shuffle 传输的缓冲区 | 高并行度、多 Shuffle 阶段的作业可适当调高，配合 `min`/`max` 一起控制 |
| `taskmanager.memory.process.size` / `taskmanager.memory.flink.size` | 未设置（二选一必配） | TaskManager 进程总内存（`process.size`，含 JVM 元空间/开销）或 Flink 框架管理的内存（`flink.size`，不含 JVM 额外开销），二者只需配置一个，Flink 会自动推导另一个及各分量 | 生产环境按容器/物理机规格显式配置，通常直接设置 `taskmanager.memory.process.size`（如 `8g`、`16g`），让 Flink 自动拆分各内存区域 |

---

## 六、网络与反压配置

| 参数名（通用） | 默认值 | 含义 | 生产推荐值 |
|---|---|---|---|
| `taskmanager.network.memory.fraction` | `0.1` | 网络内存占 JVM 总内存（或托管内存池）的比例，用于分配网络缓冲区（Buffer），直接影响反压传导的缓冲能力 | 高吞吐、多并行度 Shuffle 场景可适当调高，避免缓冲区不足导致反压过早传导到 Source |
| `taskmanager.network.memory.min` | `64 MB` | 网络内存的下限，无论按比例算出多小，都不会低于这个值 | 保持默认或按最小可用内存下限适当上调，避免小内存机器网络缓冲不足 |
| `taskmanager.network.memory.max` | `1 GB` | 网络内存的上限，避免网络缓冲占用过多内存挤占 Task 堆内存和托管内存 | 大规模集群、高并行度作业可适当调高（如 2GB），需和总内存预算一起权衡 |

> **反压排查提示**：网络缓冲区（`taskmanager.network.memory.*`）耗尽是反压最直接的表现之一。当下游处理慢导致缓冲区堆满时，上游算子会被阻塞，最终反压传导至 Source。适当调大网络内存能提升瞬时抗抖动能力，但无法解决根本的处理能力瓶颈，需配合定位慢算子、增加并行度等手段一起使用。

---

## 七、Flink SQL 特有配置

以下参数仅对 Flink SQL / Table API 生效，DataStream API 无对应配置项。

| 参数名（SQL） | 默认值 | 含义 | 生产推荐值 |
|---|---|---|---|
| `table.exec.mini-batch.enabled` | `false` | 是否开启 MiniBatch（微批处理），将一段时间窗口内的多条数据攒批后一次性处理，减少 State 访问和输出更新次数 | 高 QPS 的聚合/去重场景强烈建议开启，配合下面两个参数一起配置 |
| `table.exec.mini-batch.allow-latency` | `0 ms`（未生效） | MiniBatch 攒批的最大等待时间，超过该时长强制触发一次批处理 | 根据业务对延迟的容忍度设置，常见 1s~5s，延迟敏感场景可缩短 |
| `table.exec.mini-batch.size` | `-1`（未生效） | MiniBatch 攒批的最大条数，达到该条数即使未到延迟阈值也会触发处理 | 根据吞吐量设置，常见 1000~10000，与 `allow-latency` 谁先达到谁触发 |
| `table.optimizer.agg-phase-strategy` | `AUTO` | 聚合执行策略：`AUTO`（优化器自动选择）、`TWO_PHASE`（强制 Local-Global 两阶段聚合，先局部预聚合再全局聚合，缓解数据倾斜）、`ONE_PHASE`（强制单阶段聚合） | 存在数据倾斜或 GROUP BY 基数较小的高 QPS 场景建议设为 `TWO_PHASE` |
| `table.optimizer.distinct-agg.split.enabled` | `false` | 是否对 `COUNT DISTINCT` 等去重聚合做打散（Split），将单一热点 Key 的去重计算拆分到多个并行任务，缓解 distinct key 倾斜 | 存在 distinct 聚合热点倾斜（如全局 UV 统计）时建议开启，需配合 `table.optimizer.distinct-agg.split.bucket-num` 调整分桶数 |
| `table.exec.state.ttl` | `0 ms`（不过期，State 永久保留） | Flink SQL 内部状态（如 Join、聚合、去重的中间结果）的存活时间，超时后自动清理，防止 State 无限增长 | 生产环境几乎必须设置，按业务允许的最大乱序/回溯时间设定，如维表 Join、开窗聚合可设为几小时到几天 |
| `table.exec.resource.default-parallelism` | 跟随 `parallelism.default` | Flink SQL 算子的默认并行度（未被单独指定时使用） | 按 Source 分区数、下游吞吐能力评估，避免和全局 DataStream 作业混用导致并行度不一致 |

---

[← 返回 Flink 主文档](./05-Flink.md)
