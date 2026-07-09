# 大厂 Notebook 产品全景调研（临时草稿）

> 本文件为调研草稿，尚未纳入知识体系正式章节。

---

## 一、腾讯四条 Notebook 产品线（内部口径 + 公开验证）

### 1. TEG 数据中台部 — TBDS Notebook / 数平 Notebook（内部大数据底座）

- 归属：TEG 技术工程事业群，平台技术部大数据中心
- 定位：集团内部自用大数据基建，不对外，全公司数据分析师/算法/数仓唯一标准工作台
- 内核：重度魔改 JupyterHub
- 强项：原生打通腾讯内部元数据 MetaLake，Hive/StarRocks/Flink/Spark 零拷贝直读内部数仓，自动血缘解析、权限继承、行级脱敏、数据水印、全链路审计
- 运行模式：支持"交互式 Kernel + 一键把 cell 转换成定时调度任务"，单元格直接变成 DataOps 调度节点，自动生成 DAG

**公开验证：**
- TBDS（Tencent Big Data Suite）确实是腾讯大数据处理套件，主打"统一湖仓"底座
- 公开文章提到 TBDS 支持"存储-调度-治理"三层一体架构
- 内部"数平 Notebook"命名属于内部口径，公开资料较少直接提及
- MetaLake 零拷贝直读、行级脱敏等属于内部数据安全体系，公开不披露

### 2. CSIG 大数据产品部 — WeData Notebook（对外商业化）

- 归属：CSIG 云，大数据产品中心（徐晓敏那条线）
- 定位：对外政企客户 DataOps + MLOps 一体化开发平台，TBDS 对外包装版
- 和内部 TBDS 90% 同源，阉割内部私有元数据协议，对外兼容 AWS Glue
- 额外加 NL2SQL、大模型自动生成 SQL/Python、数据异常自愈

**公开验证（高度吻合）：**
- 2024 年 8 月 QQ 大数据团队官方发布《腾讯云 WeData Notebook 探索：从大数据迈向数据科学》
- 明确说明：WeData Notebook 基于腾讯云 Cloud Studio 的 Jupyter Notebook 构建
- 功能亮点：AI 原生能力（预装 Scikit-learn/Pandas、对接 TI 平台、内置 AI 代码助手）、无缝对接 EMR/DLC 大数据引擎、Serverless 弹性资源
- 2025 年 2 月官方发布 WeData AI 助手，对接 DeepSeek 满血版
- WeData 在 2023 年中国数据治理平台市场份额第二，增长率 67.1% 市场第一（IDC）

### 3. CSIG 混元 AI 平台部 — TI-ONE / HAI Notebook（AI 训练专用）

- 归属：CSIG 混元 AI 中台，机器学习平台团队（原 TEG AI Lab 划过去）
- 两条子产品：
  - TI-ONE Notebook：传统机器学习建模 Notebook
  - HAI Notebook（HyperAI）：专门大模型微调、预训练、LoRA、推理开发
- 不侧重大数据，侧重异构算力调度：GPU/TPU/分布式训练集群统一 Kernel 调度
- 原生对接 Angel 框架、混元训练框架、Checkpoint 托管、模型版本管理、实验追踪
- 支持分布式 Kernel，一个单元格自动切多机多卡，原生支持 DeepSpeed、Megatron-LM

**公开验证：**
- TI-ONE 官方明确提供开发机（交互式）和任务式建模两种方式
- 开发机模式支持 Jupyter Notebook 和 VSCode 两种在线编码 IDE
- 已全面支持 LLM 大模型的增训和有监督精调（SFT）
- 2025 年 TI-ONE 已上架 DeepSeek 系列模型
- "HAI Notebook（HyperAI）"命名属于内部口径，公开侧统一叫 TI-ONE

### 4. PCG 云原生研发部 — Cloud Studio Notebook（代码原生，集团统一前端标准）

- 归属：PCG 研发效能部（原 CODING 团队，现在全部并入 CSIG 效率工作台线）
- 对外产品：Cloud Studio，内部叫 DevStudio
- 前端完全自研，抛弃 JupyterLab 前端，自研组件化编辑器内核（基于 Monaco，VSCode 内核）
- 多模态单元格（代码/Markdown/PDF/表格/图表/Agent 指令块），可嵌套知识库、RAG 上下文、长上下文会话记忆
- 所有元宝、WorkBuddy、ima 全部复用这套 Cell 执行引擎，只是换上层 UI
- 战略最高优先级：全集团统一 Notebook 前端引擎，废弃老 TEG Jupyter 前端

**公开验证：**
- Cloud Studio 是腾讯云官方在线 IDE，基于 Monaco Editor（VSCode 内核）二次开发
- 2024 年 WeData Notebook 官方文章明确"集成了基于腾讯云 Cloud Studio 的 Jupyter Notebook"
- Cloud Studio 正被多个产品复用（WeData、TI-ONE 等），统一前端引擎趋势成立
- "马化腾定调全集团统一前端标准"属于内部战略信息，公开侧无法验证

### 附加说明：容易混淆的"伪 Notebook"

- ima、WorkBuddy、元宝：上层应用层，不是 Notebook 引擎，只是把 Cloud Studio Notebook 内核封装成了知识库 Agent 界面（对标 NotebookLM）
- 腾讯文档代码块：只是轻量 Kernel 调用，没有完整 Notebook 运行时

---

## 二、腾讯 Notebook 技术架构（四层模型）

### 标准分层架构（所有大厂通用四层模型）

#### 1. 通信协议层（标准层，全部兼容 Jupyter）
- 严格实现 Jupyter 消息协议、ZeroMQ 通信、ipynb 文件格式、Kernel 接口规范
- ipynb 文件可直接互通导入导出，内核完全通用 Python/R/Spark Kernel
- → 这一层 100% 属于 Jupyter 生态

#### 2. 服务网关层（大厂自研分水岭，90% 工作量）
JupyterHub 只做多用户登录，腾讯全部重写：
- **KernelProxy 网关服务**（Go 写的自研网关，TEG 核心代码）
  - 多租户 Kernel 路由、弹性伸缩、跨集群代理
  - 解决原生 JupyterHub 单机瓶颈（原生上万用户直接崩，腾讯支撑十万级租户）
- **统一元数据注入服务 MetaInjector**
  - 单元格执行前自动注入表权限、数据血缘、存储上下文
  - Cell 里面直接写 hive 表名不用写完整路径，自动鉴权
- **Cell2DAG 编译器**
  - 静态解析每个单元格代码 AST，自动识别表依赖
  - 把交互式单元格自动编译成调度 DAG，一键上线定时任务
  - DataOps 核心，开源没有任何实现

#### 3. 前端渲染层
- 老架构：基于原生 JupyterLab 前端二次修改（已废弃，停止迭代）
- 新统一标准 CloudStudio：彻底废掉 JupyterLab 前端，自研 Monaco 组件化 Cell 引擎

#### 4. 上层生态粘合层
- 和自家湖仓、调度平台、大模型、权限体系打通
- 内置混元代码模型、AST 代码解析、自动血缘生成、NL2Cell、上下文 RAG 注入

### 一句话总结

协议层 100% 兼容 Jupyter（同源体系），调度层、前端层、生态层全部自研重构，不是套壳 JupyterHub，是"Jupyter 内核 + 自研企业运行时"。与字节 DolphinScheduler Notebook、阿里 PAI Notebook 架构完全一模一样，国内云厂商全部这套范式。

---

## 三、腾讯 Notebook 未来 3 年战略方向

### 阶段 1（已完成）
从原生 Jupyter → 自研统一运行时，统一集团底层网关

### 阶段 2（2026–2027 核心战略，最高优先级）
全面 AI 原生重构，从"代码笔记本"升级为「Computational Knowledge Notebook（计算知识库笔记本）」，对标 Google NotebookLM + Databricks Lakehouse IQ

三条核心路线：

1. **彻底统一前端标准**：全集团废弃 JupyterLab 前端，所有内部/外部 Notebook 统一到 CloudStudio 的 Monaco Cell 引擎，所有工作台 WorkBuddy/元宝/ima 共用一套 Cell 运行内核
2. **湖仓一体深度融合**：Notebook 直接作为湖仓交互入口，单元格原生支持结构化 SQL、非结构化 PDF 解析、向量检索、多模态数据就地计算
3. **Agent 化单元格**：支持自定义 Agent Cell，一个单元格就是一个智能体，自动拆分任务、调用上下游数据资产、递归执行多轮推理

### 长期终局形态
不再区分文档、笔记、代码、数据平台、AI 工作台，全部统一为同构的 Cell 式计算笔记本生态。

---

## 四、阿里 Notebook 产品线

### 1. MaxCompute Notebook（大数据计算）
- 定位：云原生大数据计算服务的交互式模块，全托管
- 内核：基于 Jupyter 协议，支持 SQL、PyODPS、Python
- 特色：与 MaxCompute 计算引擎深度集成，MaxFrame 分布式 Python 计算框架
- 对标腾讯：TBDS Notebook + WeData 大数据计算部分

### 2. PAI-DSW（Data Science Workshop，AI 训练）
- 定位：一站式 AI 开发 IDE，算法科学家的 Notebook
- 内核：JupyterLab + WebIDE + Terminal 三种环境
- 特色：内置 PyTorch/TensorFlow 镜像，支持 CPU/GPU 异构计算，可挂载 OSS/NAS/CPFS
- 对标腾讯：直接对标 TI-ONE Notebook，但 PAI-DSW 更偏数据科学，TI-ONE 更强调大模型训练

### 3. DataWorks Notebook（数据开发治理）
- 定位：数据开发治理平台中的 Notebook 组件
- 特色：与 MaxCompute 深度集成，Data+AI 一体化 Pipeline
- 对标腾讯：对标 WeData Notebook，DataWorks 更成熟，WeData 增长更快

### 4. 阿里架构特点
- 同样遵循"四层模型"：Jupyter 协议层 + 自研网关/调度层 + 前端层 + 生态粘合层
- 底层调度由阿里云自研飞天系统支撑
- DataWorks 的调度引擎可以把 Notebook 单元格直接转成工作流节点（类似腾讯 Cell2DAG）

---

## 五、字节 Notebook 产品线

### 1. DataLeap Notebook（一站式数据中台）
- 定位：火山引擎 DataLeap 的交互式开发环境
- 内核：基于 JupyterHub + JupyterLab + Enterprise Gateway 等开源项目深度修改
- 基础组件：TCE（字节云引擎）、YARN、MySQL、TLB、TOS（对象存储）
- 核心目标：支持大规模用户、稳定、易扩展的 Notebook 服务

### 2. 架构特点
- 与腾讯类似，"Jupyter 内核 + 自研调度层"模式
- 资源调度基于 YARN 和 TOS
- 字节没有公开披露类似 Cloud Studio 的独立在线 IDE 产品
- AI 训练平台与 DataLeap 的 Notebook 可能是两套体系

---

## 六、其他大厂 Notebook 产品简览

### 百度：双轨制（BML + 飞桨）
- BML Notebook：全功能 AI 开发平台，支持 Notebook 建模、可视化建模、作业建模，内置 ERNIE 和 NLP 算子
- 飞桨 AI Studio：面向开发者/学生的 AI 学习社区，基于 Jupyter 提供免费 GPU 算力
- 特点：BML 面向企业，AI Studio 面向生态

### 华为：ModelArts + 开发者空间
- ModelArts Notebook：基于 JupyterLab 的 AI 开发环境，昇腾 GPU 加速，支持端-边-云模型部署
- 开发者空间 Notebook：2025 年上线，融合交互式编程、云端资源管理与自动化工作流
- 特点：与昇腾芯片深度绑定，硬件-软件一体化

### 滴滴：数据梦工厂（内部）
- 数据梦工厂：一站式数据开发生产平台，提供实时/离线数仓构建
- 使用 Zeppelin 等开源 Notebook 工具进行数据分析和可视化
- 更偏数据工程，AI 训练 Notebook 不是核心方向

### 美团/快手
- 美团：大数据平台包含"交互式开发工具"，公开资料极少
- 快手：大数据平台服务化实践，统一数据服务平台（Octo），Notebook 作为独立产品的公开信息有限

---

## 七、大厂 Notebook 横向对比（一页面试版）

| 维度 | 腾讯 | 阿里 | 字节 | 百度 | 华为 |
|---|---|---|---|---|---|
| 大数据 Notebook | TBDS / WeData Notebook | MaxCompute / DataWorks | DataLeap Notebook | — | — |
| AI 训练 Notebook | TI-ONE Notebook | PAI-DSW | 火山引擎机器学习平台 | BML Notebook | ModelArts Notebook |
| 统一前端/IDE | Cloud Studio（Monaco） | 各产品独立前端 | DataLeap 自研 | AI Studio（Jupyter） | 开发者空间 Notebook |
| 协议层 | Jupyter 协议 | Jupyter 协议 | Jupyter 协议 | Jupyter 协议 | Jupyter 协议 |
| 自研调度层 | KernelProxy（Go） | 飞天系统 | YARN + TOS | PaddlePaddle 框架 | 昇腾 CANN |
| 前端策略 | 统一 Monaco 内核（集团标准） | JupyterLab 二次开发 | JupyterLab 深度改造 | JupyterLab | JupyterLab |
| AI 原生能力 | 混元 + DeepSeek 双引擎 | 通义千问 + PAI 集成 | 豆包大模型 | 文心一言 + ERNIE | 盘古大模型 |
| DataOps 特色 | Cell2DAG | DataWorks 工作流 | DataLeap 调度 | — | — |
| 生态打通 | 微信/QQ/企业微信/腾讯文档 | 钉钉/阿里云/淘宝 | 飞书/抖音/火山引擎 | 百度智能云/飞桨社区 | 鸿蒙/鲲鹏/昇腾 |

---

## 八、关键结论与行业趋势

### 1. "Jupyter 协议 + 自研运行时"已成行业共识
所有大厂在通信协议层都严格兼容 Jupyter Message Protocol + ZeroMQ + ipynb 格式。真正的护城河在调度层和生态层，这些都不会开源。

### 2. 前端层正在分化：两条路线
- 路线 A（腾讯）：彻底抛弃 JupyterLab 前端，自研 Monaco 组件化 Cell 引擎，支持多模态单元格，向 AI 原生 Notebook 演进
- 路线 B（阿里/字节/华为）：基于 JupyterLab 深度改造，保留原有生态，叠加自研插件
- 腾讯的前端激进程度最高，与 Google Colab、Databricks 的演进方向更接近

### 3. "Notebook = 湖仓交互入口"是共同终局
- 腾讯：WeData Notebook 直接绑定 EMR/DLC，Cell2DAG 一键转调度
- 阿里：MaxCompute Notebook + MaxFrame 实现 Data+AI 一体化
- 字节：DataLeap Notebook 与数据中台全链路打通

### 4. Agent 化单元格是下一个竞争焦点
- 腾讯 ima 2.0 的"任务模式"已具备 Agent 自主规划能力
- 阿里 PAI Designer 已有 Notebook 组件与工作流联动
- 字节 DataLeap 尚未公开披露 Agent 化单元格能力
- 国际上 Databricks AI/BI Genie、GitHub Copilot Workspace 也在探索类似方向

### 5. 组织调整反映战略优先级
腾讯 2025 年 2 月 AI 产品线集中调整（ima/QQ 浏览器/搜狗输入法从 PCG 迁入 CSIG），统一前端引擎需要跨事业群协同，组织集中化是为了解决"产品线分散、底座重复建设"。

---

## 九、内部信息与公开验证差异表

| 内部信息 | 公开验证结果 | 备注 |
|---|---|---|
| 四条完全独立 Notebook 产品线 | 基本验证 | WeData 和 TBDS 界限在公开侧较模糊 |
| TEG 数平 Notebook 不对外 | 无法验证 | 公开侧 TBDS 是商业化产品 |
| Cloud Studio 是集团统一前端标准 | 趋势验证，细节无法验证 | 确实被 WeData 复用 |
| KernelProxy 开源到 tars 生态 | 无法验证 | 公开资料未找到 |
| HAI Notebook 命名 | 未验证 | 公开侧统一叫 TI-ONE |
| Agent Cell 是最大创新点 | 方向验证，落地程度未知 | ima 2.0 任务模式已展示能力 |
