# 6.4 Spark 调试运维

> 适用场景：任务挂了、秒退、UI 正常但结果失败、日志找不到  
> 阅读前提：已了解 Spark 基本执行模型  
> 相关阅读：  
> - [Spark 错误排查与兼容性](./04-Spark-错误排查与兼容性.md)  
> - [Spark 部署与云原生运行](./04-Spark-部署与云原生运行.md)

## 一、先建立排障顺序

### 1.1 先区分提交失败、运行失败、结果异常

Spark 排障第一步不是看栈，而是先分型：

- **提交失败**：任务刚提交就退出，通常连完整 UI 都起不来
- **运行失败**：任务跑了一段时间后失败，往往能看到 stage / task 痕迹
- **结果异常**：任务跑成功了，但数据量、分布或结果不对

不同类型的问题，入口完全不一样。

### 1.2 先找日志，再看 UI，再下结论

日志告诉你异常栈和组件归属，UI 告诉你执行路径与性能症状。两者结合才有解释力。只看 UI 容易误判，只看日志又容易看不到全局运行结构。

### 1.3 先定位组件，再定位参数

先弄清楚问题在哪一层：

- Driver
- Executor
- AM / Pod
- 资源管理器
- 数据源 / 外部系统

否则很容易对着错误的地方一直调参数。

## 二、日志从哪里来

### 2.1 Driver、Executor、AM、Client 日志

Spark on YARN 或 Kubernetes 时，日志来源至少要分清：

- **Driver 日志**：提交、SparkContext 初始化、全局异常
- **Executor 日志**：task 执行、数据读写、OOM、FetchFailed
- **AM 日志**：YARN 下 ApplicationMaster 的启动与资源申请过程
- **Client 日志**：提交端输出，尤其是 client 模式下很关键

### 2.2 stdout、stderr、syslog 的分工

- `stdout`：程序显式 `println` / `print`
- `stderr`：程序错误输出
- `syslog`：框架日志、log4j / slf4j 日志，通常最有排障价值

大多数生产问题，优先看 `syslog`。

### 2.3 YARN 聚合日志与本地日志

YARN 下要区分：

- 正在运行作业的在线日志入口
- 已结束作业的聚合日志
- client 模式下提交机本地日志

如果是 Kubernetes，则要转向 Driver / Executor Pod 的日志入口。

## 三、如何查看运行中与已结束作业日志

### 3.1 运行中作业

运行中的作业通常从资源管理器页面进入：

- YARN：通过 tracking URL、Application ID
- K8s：通过 Driver Pod / Executor Pod 状态与日志

### 3.2 已结束作业

常见方式包括：

- YARN 聚合日志
- Spark History Server
- 调度平台保留的作业日志

### 3.3 调度系统中的日志入口

很多公司环境下，实际排障第一入口并不是 Spark 原生日志，而是调度平台任务实例页。需要先看：

- 作业实例是否真的提交成功
- 上下游依赖是否满足
- 是否被调度系统提前终止或重试

## 四、Spark UI 的排障价值

### 4.1 SQL / Stage / Executor 页面怎么看

- **SQL 页面**：看查询计划节点耗时与整体结构
- **Stage 页面**：看 task 分布、输入输出量、Shuffle 情况
- **Executor 页面**：看资源使用、失败 Executor、黑名单节点等

### 4.2 Spark UI 正常但任务失败说明什么

如果 UI 正常出现，通常说明：

- Driver 至少已经成功初始化
- SparkContext 已经创建
- 作业至少进入了执行期

这时排障重点就从“提交失败”转向“执行期错误”。

### 4.3 UI 与日志如何互相验证

推荐方式是：

1. 在 UI 里找到异常 stage / task
2. 拿到对应 Executor / Pod / Container 信息
3. 回到日志里定位具体异常栈

这样能把“表现症状”和“技术根因”连起来。

## 五、常见运维问题

### 5.1 AM 启动失败

AM 启动失败经常意味着：

- 主类配置错了
- 提交参数有误
- 提交前置逻辑在 SparkContext 初始化前就异常退出

这类问题的特征是：任务秒退、Spark UI 很可能起不来。

### 5.2 Driver 提交后秒退

提交后秒退通常优先看：

- client 模式下提交端日志
- cluster 模式下 Driver / AM 日志
- 依赖包是否缺失
- 主入口类、参数、环境变量是否正确

### 5.3 Executor 丢失与重试

Executor 丢失不一定就是 Spark 逻辑 bug，也可能是：

- 节点资源紧张
- 容器被 kill
- 本地盘或网络异常
- Kubernetes 下 Pod 被驱逐 / 重建

这类问题要先分辨是“业务代码把 Executor 打挂了”，还是“运行环境把 Executor 回收了”。

### 5.4 日志缺失与聚合失败

如果日志都拿不到，优先排查：

- 日志聚合配置是否开启
- 本地目录是否被过早清理
- 容器 / Pod 生命周期是否太短
- 资源管理器端是否保留足够长的历史

## 六、运行环境差异

### YARN 环境下如何排障

YARN 排障的关键词是：

- Application ID
- AM Container
- NodeManager
- 聚合日志
- Container exit code

### Kubernetes 环境下如何排障

Kubernetes 下则要更多看：

- Driver Pod 是否成功启动
- Executor Pod 是否被调度成功
- Pod 事件、驱逐、重启原因
- 本地存储 / PVC 是否配置合理

### 为什么同样的报错在不同运行环境里看法不同

例如同样是进程退出：

- 在 YARN 下你更关注 Container 被 kill 的原因
- 在 Kubernetes 下你更关注 Pod 生命周期、调度事件、节点驱逐

运行环境不同，排障入口和解释框架也应该跟着变。

## 七、从报错到定位的完整路径

### 先识别报错归属

先回答两个问题：

- 谁报的错？Driver、Executor、AM、外部系统？
- 错误发生在提交期、执行期、写出期还是结果校验期？

### 再识别问题类型

再进一步分类为：

- 资源问题
- Shuffle / 网络问题
- 数据源 / 兼容性问题
- 业务写法或 SQL 逻辑问题

### 最后跳转到错误专题做深挖

- [Spark 错误排查与兼容性](./04-Spark-错误排查与兼容性.md)
