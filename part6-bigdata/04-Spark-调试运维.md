# 6.4 Spark 调试运维

> 本文是 [Spark 主文档](./04-Spark.md) 的专题子文档，涵盖作业日志体系与调试运维：日志来源、文件类型、查看方法、常见问题排查路径、参数设置语法。

---

## 七、作业日志体系与调试运维

前面几节讲了怎么优化 Spark 任务，这一节讲怎么查看作业日志和排查问题——这是开发和运维 Spark 作业的基本功。很多人写了 Spark 代码却不知道去哪里看日志，出了问题无从下手。理解日志体系的本质是理解 Spark 作业的组件构成——每个组件都会产生日志，不同组件的日志在不同地方查看。

### 7.1 日志从哪来——各组件的日志产出

回顾前面的架构知识：一个 Spark on YARN 作业涉及多个组件，每个组件都会产生日志：

```
从 Spark 作业角度看（Driver + Executor）：
  Driver    → 输出 DAG 依赖关系、Job 提交、Task 调度和 Task 执行状态相关 log
              → 执行过程中的报错（OOM、异常栈）通常都在这里
  Executor  → 输出执行 Task 的相关 log
              → Task 级别的异常（如 UDF 报错、数据格式错误）

从 YARN 角度看（提交 Client + AM + Container）：
  Client    → 输出作业提交过程和作业状态（ACCEPTED、RUNNING、FINISHED 等）log
              → yarn-client 模式下 Driver log 也在 Client 端
  AM        → 输出 Container 的申请和启动 log
              → yarn-cluster 模式下 Driver log 和 AM log 在同一个 Container 中
```

> **看日志的优先级**：大多数情况 Driver 的日志足以帮助了解作业运行情况和排查问题；少数问题需要看 Executor 的日志（如 Task 级别的数据异常）；个别特定问题（如资源申请失败、Executor 启动环境问题）需要看 AM 或 Client 的日志。**Driver 日志是最重要的。**

### 7.2 日志文件分类——stdout / stderr / syslog

无论是 Driver 还是 Executor，YARN 聚合后的日志通常包含三个流：

| 日志流 | 内容 | 来源 |
|--------|------|------|
| **stdout** | 用户代码中 `System.out.println()` 的输出 | 用户代码 |
| **stderr** | 用户代码中 `System.err.println()` 的输出、`printStackTrace()` | 用户代码 |
| **syslog** | Driver/AM/Executor 框架本身输出的 log，包含 Spark 内部调度信息、异常栈 | Spark 框架 + 用户的 log 配置（如 log4j） |

如果你在代码中使用了日志框架（如 SLF4J + Log4j），输出的 log 也会出现在 syslog 中。所以排查问题时主要看 syslog——它既有框架的调度信息，也有你通过 logger 输出的业务日志。stdout 和 stderr 一般只用于调试时的临时打印。

### 7.3 查看正在运行作业的日志

作业提交后，Client 进程会在 AM 启动后输出 Application 的 tracking URL：

```
# Client 端日志中会输出类似信息：
# tracking URL: http://xxx:8088/proxy/application_xxx_xxx/
# Application ID: application_xxx_xxx
```

也可以从 YARN 的 ResourceManager（RM）页面查询。RM 页面地址因集群和机房而异，通常格式为 `http://<rm-host>:8088/cluster`。在哪个机房提交的任务，就去对应机房 RM 页面查看。如果是多机房联邦集群，可以通过联邦入口页面跳转到具体机房。

在 RM 页面通过 Application ID（格式为 `application_xxx_xxx`）或作业名搜索作业，搜索到后点击 Application ID 链接可以看到作业详情：

```
作业详情页面包含：
  - Application 基本信息（状态、队列、用户、提交时间）
  - ApplicationMaster 链接 → 点击打开 Spark UI
  - Logs 链接 → 点击查看 AM 日志
  - Tracking URL → 作业的实时跟踪页面
```

> **yarn-client 模式的特殊情况**：Driver 的日志在本地提交作业的 Client 端输出，不会出现在 YARN 的日志聚合中。如果你的作业是 yarn-client 模式，Driver 日志只在你提交作业的终端能看到——如果终端关闭或机器重启，日志就丢了。所以生产环境推荐使用 yarn-cluster 模式。

点击日志链接后可以看到三个日志流（stdout、stderr、syslog）。对于 yarn-cluster 模式，这里看到的就是 Driver（也是 AM）的日志。

**Spark UI 的打开方式**：在作业详情页面点击 ApplicationMaster 链接即可打开 Spark UI。Spark UI 的详细分析方法见 [性能优化专题 — Spark UI 分析方法](./04-Spark-性能优化.md#64-spark-ui-分析方法etl-优化的第一步)。

**AM 重试日志**：yarn-cluster 模式的作业在执行失败后会自动重试（这是 YARN 的设定，重试次数可通过 `spark.yarn.maxAppAttempts` 配置）。每次重试都会产生独立的日志，可以在作业详情页面看到所有 Attempt 的日志链接。排查问题时**推荐先看第一次执行的日志**——第一次失败的原因往往就是根因，后续重试大概率因相同原因失败。

### 7.4 查看已结束作业的日志

RM 页面的信息来自 RM 进程的内存，存储量有限，通常只保留最近几个小时的作业信息。已经执行完成的作业需要通过 History Server 或日志聚合服务查看。

**方式一：Spark History Server / JobSearch 服务**

各公司大数据平台通常提供作业日志搜索服务（如美团内部的 JobSearch），通过 Application ID 或作业名搜索已完成的作业。点击 Application ID 链接可以跳转到 Spark History Server，在 Spark UI 中查看执行计划和日志。

在 Spark UI 的 Executors 标签页中，可以看到 Driver 和每个 Executor 的日志链接：

```
Spark UI → Executors 页面：
  Driver     → stdout / stderr 链接
  Executor 0 → stdout / stderr 链接
  Executor 1 → stdout / stderr 链接
  ...
```

这里默认只显示 stdout 和 stderr 的链接。要查看 syslog，可以在日志链接的 URL 中去掉 `.stdout` 或 `.stderr` 后缀，直接访问日志根路径即可看到所有日志流的列表。

> **yarn-client 模式的注意**：已结束的 yarn-client 作业，Driver 日志是在本地输出的，YARN 日志聚合中看不到。如果本地没有保存，Driver 日志就永久丢失了。

**方式二：yarn logs 命令**

如果有 Hadoop 客户端环境，可以通过命令行直接拉取作业日志：

```bash
# 拉取指定作业的全部日志（stdout + stderr + syslog）
yarn logs -applicationId application_xxx_xxx -appOwner hadoop-data

# 只查看某个 Executor 的日志
yarn logs -applicationId application_xxx_xxx -containerId container_xxx_xxx_xxx -nodeAddress xxx:8041
```

> **大日志下载建议**：有时日志文件较大（几个 GB），直接在浏览器打开链接可能导致卡顿。建议复制链接地址，在终端用 `wget` 命令下载到本地后再用 `less` 或 `grep` 查看。

### 7.5 从调度系统查看日志

当作业在调度系统（如 Cantor、DolphinScheduler、Airflow）中配置为例行执行后，可以通过调度系统的执行日志跳转到 YARN 日志：

```
调度系统查看日志的步骤：
  ① 在调度系统的作业搜索页面找到对应作业的执行实例
  ② 点击查看执行详情
  ③ 从"MR/Spark 执行日志"等入口跳转到 YARN JobSearch 页面
  ④ 继续通过 Spark UI 或 yarn logs 查看具体日志
```

这种方式的好处是可以从调度链路的角度看到作业的上下游依赖关系和执行时间线，便于排查"为什么作业比平时慢"这类问题。

### 7.6 日志不存在时的排查方法

实际排查中经常遇到"日志链接打不开"的情况，分两种场景：

**场景一：日志超过保留期被自动清理**

```
报错信息：
  Sorry! Application's log Not Found in either NodeManager or HDFS.
  Application's log has already been deleted.

原因：
  YARN 日志聚合后的日志有保留期（通常 5-7 天，具体取决于集群配置）
  超过保留期后日志被自动清理，无法再查看

应对：
  ① 尽快在日志保留期内查看和下载
  ② 重要作业的日志建议在作业完成后立即下载归档
  ③ 如果必须查看已过期的日志，只能联系平台运维确认是否有备份
```

**场景二：AM Container 启动失败，日志链接无法跳转**

```
作业诊断信息：
  Application application_xxx failed 1 times due to AM Container for
  appattempt_xxx exited with  exitCode: 1
  For more detailed output, check application tracking page ...

原因：
  Spark 作业提交后，在初始化 SparkContext 之前，执行业务自身代码异常退出
  由于 Spark 初始化未完成，Spark 无法为 HistoryServer 建立日志跳转链接

常见原因（按频率排序）：
  ① 主入口类不正确或 Maven 打包不正确 → 主入口类加载失败（ClassNotFoundException）
  ② 提交参数不对 → 程序直接退出（如 --class 写错、jar 包路径不对）
  ③ 作业代码有 bug → 初始化 SparkContext 之前发生异常退出

排查方法：
  ① 通过 YARN API 查看 AM 日志（不依赖 Spark 初始化完成）：
     https://<yarn-api-server>/tool/containerLog/application_xxx
  ② 或通过 yarn logs 命令拉取日志：
     yarn logs -applicationId application_xxx_xxx -appOwner hadoop-data
  ③ 查看日志中的异常栈信息，定位具体原因
```

> **关键区分**：AM Container 启动失败 ≠ Executor 启动失败。前者是 Driver/AM 自身的 JVM 退出（SparkContext 还没建好），后者是 Executor 进程启动失败但 Driver 还活着。前者的日志需要通过 YARN API 或命令获取，后者的日志在 Driver 的 syslog 中可以看到（Driver 会记录 Executor 注册失败的信息）。

### 7.7 Spark UI 显示正常但作业状态为失败

有时 Spark UI 显示所有 Stage 都成功了，但作业最终状态却是 FAILED。这类问题通常由两个原因引起：

**原因一：Driver 崩溃**

Driver 进程本身崩溃（OOM、JVM 错误），导致 Application 被 YARN 标记为失败。此时 Spark UI 可能还显示着之前的执行状态（因为 History Server 加载的是最后的快照），但 Driver 已经不在了。

```
排查方法：
  ① 查看 Driver 的 stderr 日志 → 搜索 OutOfMemoryError 或 JVM crash 信息
  ② 查看 Driver 的 syslog → 搜索 "Final app status: FAILED"
     → 如果有这条日志，说明 Driver 主动报告了失败状态
  ③ 如果没有 "Final app status: FAILED" 但 Driver 日志中断
     → 说明 Driver 是被强杀的（如 OOM Killer、机器重启）

常见修复：
  ① Driver OOM → SET spark.driver.memory=4G（或更大）
  ② Driver JVM 错误 → 通过 spark.driver.extraJavaOptions 设置 JVM 参数
     如 SET spark.driver.extraJavaOptions="-XX:MaxMetaspaceSize=512m";
```

**原因二：日志丢失**

作业确实失败了，但由于日志聚合延迟或日志丢失，Spark UI 显示的信息不完整，导致看起来"一切正常"。

```
排查方法：
  ① 通过作业诊断平台（如 Themis）查看作业诊断信息
  ② 手动拉取 Driver 端的 syslog 日志，搜索异常信息
  ③ 如果日志完全丢失，联系平台运维协助排查
```

### 7.8 参数设置语法与注意事项

在 Spark SQL 中通过 `SET` 语句调整参数时，有几个语法细节需要注意，否则可能导致参数不生效：

```sql
-- ✓ 正确写法：等号两边无空格，SET 用小写
set spark.executor.memory=3g;
set spark.sql.shuffle.partitions=4000;
set spark.yarn.executor.memoryOverhead=2048M;

-- ✗ 错误写法：等号两边有空格 → 可能导致参数不生效
set spark.executor.memory = 3g;

-- ✗ 错误写法：SET 用大写（部分版本可能不兼容）
SET spark.executor.memory=3g;

-- ✗ 错误写法：值带了多余的单位或空格
set spark.executor.memory=3 G;
set spark.executor.memory=3g ;
```

> **底层原理**：Spark 的参数解析使用模式匹配从用户 SQL 中提取 `set` 语句，然后拼接到 spark-submit 命令中。非标准写法可能触发解析 bug，导致参数被忽略。建议严格遵循 `set key=value;` 的格式，等号两边不留空格。

**参数设置的生效范围**：

```
在 Spark SQL / ETL 中设置参数的生效范围：
  ① set 语句在 SQL 文件中的位置不影响生效——所有 set 语句在作业提交前统一解析
  ② set 语句只对当前作业生效，不影响其他作业
  ③ 同一个参数多次 set，最后一个生效（后覆盖前）
  ④ 通过 spark-submit 命令行参数设置的优先级高于 SQL 中的 set 语句
```

### 7.9 日志排查实战——从报错到定位的完整路径

将上面的知识串联起来，排查一个 Spark 作业问题的完整路径：

```
Step 1：确认作业状态
  → 在 RM 页面或调度系统查看作业是 RUNNING、FAILED 还是 FINISHED
  → 记录 Application ID（application_xxx_xxx）

Step 2：定位报错位置
  → 如果作业 FAILED → 先看 Driver 的 syslog（yarn-cluster 模式看 AM 日志）
  → 搜索关键字：Exception、Error、OOM、killed、timeout
  → 如果 Driver 日志显示是某个 Task 失败 → 再看对应 Executor 的日志

Step 3：根据报错类型选择排查方向
  → OOM（Java heap space）→ 见 [性能优化专题 — OOM/Spill 排查路径](./04-Spark-性能优化.md#oom--spill-排查路径)
  → YARN kill（exitCode 143）→ 堆外内存超限，调大 memoryOverhead
  → 数据倾斜（部分 Task 极慢）→ 见 [性能优化专题 — 数据倾斜八种修复方案](./04-Spark-性能优化.md#65-数据倾斜八种修复方案)
  → Shuffle 文件丢失 → 见主文档 [考点 10：FileNotFoundException 区分](./04-Spark.md#考点-10spark-作业报-filenotfoundexception-怎么区分原因)
  → 上游数据文件丢失（FileNotFoundException on HDFS）→ 检查调度依赖和上游重导
  → 权限错误（AccessControlException）→ 申请表或目录的读写权限
  → 动态分区严格模式 → 开启 hive.exec.dynamic.partition 参数
  → AM 启动失败 → 见 7.6 场景二
  → 广播超时 → 见广播超时排查（含隐蔽场景）
  → Spark UI 正常但作业失败 → 见 7.7 节（Driver 崩溃或日志丢失）

Step 4：验证修复
  → 修改代码或参数后重新提交作业
  → 对比优化前后的 Spark UI 指标（耗时、Spill、Shuffle 数据量）
  → 确认问题解决后记录到团队 Wiki，避免重复踩坑
```

> **经验法则**：排查问题的第一步永远是"找到正确的日志"，而不是"猜测原因"。Driver 的 syslog 是最重要的信息来源——90% 的问题都能从 Driver 日志的异常栈中找到线索。如果 Driver 日志看不出问题，再看 Executor 日志。如果日志链接打不开，参考 7.6 节的排查方法。

---

[← 返回 Spark 主文档](./04-Spark.md)

