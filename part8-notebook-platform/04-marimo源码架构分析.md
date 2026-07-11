# 8.4 marimo 源码架构与响应式执行模型

## 五、marimo 源码架构与响应式 Notebook 分析

> 源码：marimo 官方仓库（https://github.com/marimo-team/marimo）

### 1. marimo 是什么

marimo 是一个响应式 Python Notebook，核心特征是：运行一个 Cell 后自动运行所有依赖它的 Cell（或标记为 stale），保证代码和输出始终一致。存储格式是纯 Python `.py` 文件（不是 JSON），可直接作为脚本执行、部署为 Web App、在浏览器中通过 WASM 运行。

### 2. 核心架构

marimo 的架构分为三层：

**AST 分析层（`marimo/_ast/`）：** `ScopedVisitor` 继承 Python 标准库的 `ast.NodeVisitor`，对每个 Cell 的代码做静态分析，提取定义的变量（`VariableData`，包含 kind——function/class/import/variable）和引用的变量。`ImportData` 记录导入的模块信息。`codegen.py` 负责将 Cell 编译为带 `@app.cell` 装饰器的 Python 函数，实现纯 Python 文件存储。

**数据流图层（`marimo/_runtime/dataflow/`）：** 这是 marimo 的核心创新。`DirectedGraph` 类（继承 `GraphTopology`）组合了三个组件——`MutableGraphTopology`（拓扑结构，维护节点和边）、`DefinitionRegistry`（变量定义注册表，追踪每个变量被哪个 Cell 定义）、`CycleTracker`（循环依赖检测）。`Edge` 类型定义为 `(CellId_t, CellId_t)` 元组，表示 Cell 间的依赖关系。

`DirectedGraph` 的核心方法：`register_cell()` 在添加 Cell 时先注册变量定义、再计算边、最后检测循环（原子操作，通过 `threading.Lock` 保证线程安全）；`delete_cell()` 移除 Cell 并返回其子节点；`set_stale()` 通过 `transitive_closure` 传递性标记所有下游 Cell 为过期；`descendants()` 和 `ancestors()` 查询拓扑关系。

**服务层（`marimo/_server/`）：** 基于 Starlette/ASGI（不是 Tornado），包含 AI 集成（`_server/ai/`，支持 GitHub Copilot 和自定义 LLM API）、实时协作 RTC（`_server/rtc/`）、导出（`_server/export/`，支持 HTML/PDF/脚本）、工作空间管理（`_server/workspace/`）。API 端点覆盖 ai、assets、cache、config、datasources、document、editing、execution、export、file_explorer、files、health、home、login、lsp、packages、secrets、sql、terminal、ws 等。

**前端层（`frontend/src/`）：** React + CodeMirror 6。有完整的插件系统（`frontend/src/plugins/`），分为 core 插件（注册 React 组件、RPC 通信）和 impl 插件（Slider、Dropdown、DataTable、Vega、Plotly、Matplotlib 等 UI 组件）。设计系统（`DESIGN.md`）定义了完整的颜色、排版（PT Sans/Lora/Fira Mono）、间距、圆角规范。

### 3. 响应式执行的工作原理

当用户修改并运行 Cell A 时，marimo 执行以下流程：

第一步，`ScopedVisitor` 对 Cell A 的代码做 AST 分析，提取它定义的变量（defs）和引用的变量（refs）。

第二步，`DirectedGraph.register_cell()` 更新拓扑结构——通过 `DefinitionRegistry` 查找 Cell A 定义的变量被哪些 Cell 引用，建立边关系。

第三步，通过 `transitive_closure()` 计算 Cell A 的所有下游 Cell（递归遍历 children），根据运行模式决定是自动执行（reactive 模式）还是标记为 stale（lazy 模式）。

第四步，按拓扑排序依次执行需要运行的 Cell，每个 Cell 的执行结果通过 WebSocket 推送到前端。

删除 Cell 时，`DirectedGraph.delete_cell()` 移除节点和边，同时通过 `DefinitionRegistry.unregister_definitions()` 清除该 Cell 定义的变量，消除隐藏状态。

### 4. 存储格式设计

marimo 的 `.py` 文件格式是通过 `codegen.py` 生成的。每个 Cell 被编译为一个带 `@app.cell` 装饰器的函数，整个 Notebook 是一个标准的 Python 模块。`to_decorator()` 函数处理 Cell 配置（如 `disabled=True`），`format_tuple_elements()` 处理多行格式化。这种设计使得 Notebook 天然 Git 友好（diff 可读）、可直接 `python notebook.py` 执行、可用 pytest 测试、可跨 Notebook import 函数。

---

## 六、marimo vs Jupyter Notebook v7 深度对比（面试重点）

### 1. 执行模型：响应式 DAG vs 手动顺序执行

这是两者最根本的差异。Jupyter 的执行模型是线性的、手动的——用户依次运行每个 Cell，Kernel 维护全局命名空间，Cell 执行顺序完全由用户决定。这导致"隐藏状态"问题：Cell 3 可以引用 Cell 5 定义的变量（只要已执行过），代码和输出之间没有一致性保证。

marimo 通过 AST 分析 + DirectedGraph 构建响应式数据流。修改 Cell A 后，所有依赖它的下游 Cell 自动重新执行或被标记为 stale。删除 Cell 时变量自动清除。这从根本上消除了"忘了重新运行某个 Cell"的错误。

面试要点：理解 DAG 的拓扑排序如何保证执行顺序的正确性、`transitive_closure` 如何计算传递性依赖、线程安全的 `threading.Lock` 如何避免并发问题。

### 2. 存储格式：纯 Python vs JSON

Jupyter 使用 `.ipynb`（JSON），包含代码、输出、元数据。缺点是 Git diff 难以阅读、输出污染仓库、不能直接执行。marimo 使用 `.py` 文件，每个 Cell 是 `@app.cell` 装饰的函数，天然 Git 友好、可直接执行、可 pytest 测试、可跨 Notebook import。

### 3. 架构对比

| 维度 | Jupyter Notebook v7 | marimo |
|---|---|---|
| 后端框架 | Tornado / Jupyter Server | Starlette / ASGI |
| 前端框架 | TypeScript + Lumino Widget | React + CodeMirror 6 |
| 执行模型 | 手动顺序执行，隐藏状态 | 响应式 DAG，无隐藏状态 |
| 存储格式 | .ipynb（JSON） | .py（纯 Python） |
| 通信协议 | Jupyter Message Protocol（ZMQ 5 通道） | 自定义 WebSocket 协议 |
| 构建系统 | Rspack + Module Federation | React 标准构建 |
| 插件体系 | JupyterFrontEndPlugin + Token 依赖注入 | React 组件 + RPC 插件系统 |
| 部署能力 | 仅 Notebook（需 nbconvert/Voilà 辅助） | 原生支持 Script/App/Slides/WASM |
| AI 集成 | 依赖第三方扩展（jupyter-ai） | 内置 AI 编辑器 + Agent CLI 协作 |
| SQL 支持 | 需安装额外 Kernel（xeus-sql） | 内置 SQL 引擎，SQL Cell 引用 Python 变量 |
| 可复现性 | 差（手动顺序，隐藏状态） | 好（DAG 确定性执行，无隐藏状态） |
| 多租户 | 需 JupyterHub / Enterprise Gateway | 内置多用户模式 |

### 4. 核心差异总结

Jupyter Notebook 是一个交互式计算环境，强调灵活性和生态——庞大的插件体系和多年积累的工具链是它的护城河。marimo 是一个响应式编程环境，强调一致性、可复现性和可部署性——AST 分析 + DAG 执行模型是核心创新，纯 Python 存储是工程优势。

从源码架构角度，Jupyter v7 本质上是 JupyterLab 的简化版，继承了 Jupyter 生态的优缺点；marimo 则是从零开始的全新设计，`DirectedGraph` 是它最精巧的部分。
