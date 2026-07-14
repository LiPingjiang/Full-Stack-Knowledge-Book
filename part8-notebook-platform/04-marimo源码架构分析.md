# 8.4 marimo 源码架构与响应式执行模型

## 五、marimo 源码架构与响应式 Notebook 分析

> 源码：marimo 官方仓库（https://github.com/marimo-team/marimo）

### 1. marimo 是什么

marimo 是一个响应式 Python Notebook，核心特征是：运行一个 Cell 后自动运行所有依赖它的 Cell（或标记为 stale），保证代码和输出始终一致。存储格式是纯 Python `.py` 文件（不是 JSON），可直接作为脚本执行、部署为 Web App、在浏览器中通过 WASM 运行。

### 2. 核心架构

marimo 的架构分为三层：

**AST 分析层（`marimo/_ast/`）：** `ScopedVisitor` 继承 Python 标准库的 `ast.NodeVisitor`，对每个 Cell 的代码做静态分析，提取定义的变量（`VariableData`，包含 kind——function/class/import/variable）和引用的变量。`ImportData` 记录导入的模块信息。`codegen.py` 负责将 Cell 编译为带 `@app.cell` 装饰器的 Python 函数，实现纯 Python 文件存储。

AST 分析的本质是**从代码中抽取实体和关系**。实体是指每个 Cell 定义了哪些变量（defs）、引用了哪些变量（refs）、删除了哪些变量（deleted_refs），每个变量还附带元数据——是普通变量、函数、类还是 import，有没有类型标注，是不是私有变量。关系是指这些实体之间的依赖——Cell A 定义了 `x`，Cell B 引用了 `x`，就产生一条 A→B 的依赖边。实体是节点属性，关系是图的边，两者合在一起构成 DAG。AST 分析把代码文本转换成结构化的依赖关系数据，让执行引擎可以据此做调度决策（跑哪些 Cell、按什么顺序跑、每个 Cell 需要什么输入），而不是盲目地"用户点什么就跑什么"。Jupyter 没有这一层，所以它不知道 Cell 之间谁依赖谁，用户必须自己保证执行顺序。

**数据流图层（`marimo/_runtime/dataflow/`）：** 这是 marimo 的核心创新。`DirectedGraph` 类（继承 `GraphTopology`）组合了三个组件——`MutableGraphTopology`（拓扑结构，维护节点和边）、`DefinitionRegistry`（变量定义注册表，追踪每个变量被哪个 Cell 定义）、`CycleTracker`（循环依赖检测）。`Edge` 类型定义为 `(CellId_t, CellId_t)` 元组，表示 Cell 间的依赖关系。

`DirectedGraph` 的核心方法：`register_cell()` 在添加 Cell 时先注册变量定义、再计算边、最后检测循环（原子操作，通过 `threading.Lock` 保证线程安全）；`delete_cell()` 移除 Cell 并返回其子节点；`set_stale()` 通过 `transitive_closure` 传递性标记所有下游 Cell 为过期；`descendants()` 和 `ancestors()` 查询拓扑关系。

**服务层（`marimo/_server/`）：** 基于 Starlette/ASGI（不是 Tornado），包含 AI 集成（`_server/ai/`，支持 GitHub Copilot 和自定义 LLM API）、实时协作 RTC（`_server/rtc/`）、导出（`_server/export/`，支持 HTML/PDF/脚本）、工作空间管理（`_server/workspace/`）。API 端点覆盖 ai、assets、cache、config、datasources、document、editing、execution、export、file_explorer、files、health、home、login、lsp、packages、secrets、sql、terminal、ws 等。

**前端层（`frontend/src/`）：** React + CodeMirror 6。有完整的插件系统（`frontend/src/plugins/`），分为 core 插件（注册 React 组件、RPC 通信）和 impl 插件（Slider、Dropdown、DataTable、Vega、Plotly、Matplotlib 等 UI 组件）。设计系统（`DESIGN.md`）定义了完整的颜色、排版（PT Sans/Lora/Fira Mono）、间距、圆角规范。

### 3. AST 分析在 Cell 执行全流程中的角色

AST 分析不参与代码的实际执行，但它决定了整个响应式调度的所有关键决策。从源码看，AST 分析贯穿 Cell 生命周期的四个阶段：

**编译期——把源码变成可执行对象。** `compile_cell`（`_ast/compiler.py:256`）是每个 Cell 进入执行流程的第一站。它先用 `ast.parse` 把源码解析成 AST 树，然后做两件事：一是让 `ScopedVisitor` 遍历 AST 提取 defs/refs/deleted_refs，二是把 AST 编译成 Python 字节码——`body`（exec 模式，Cell 主体）和 `last_expr`（eval 模式，最后一个表达式，用于渲染输出）。最终产出一个 `CellImpl` 对象，同时携带字节码（用于执行）和元数据（defs/refs，用于依赖分析）。如果源码有语法错误，在这一步就被拦住，Cell 不会进入图。

**注册期——构建依赖边。** `_maybe_register_cell`（`runtime.py:845`）把 `CellImpl` 注册到 `DirectedGraph` 中。`graph.register_cell` 调用 `edges.compute_edges_for_cell`，用 Cell 的 defs 和 refs 在图中连线。这一步完全依赖 AST 分析的产出——没有 defs 和 refs，就无法计算 parent/child 边，图就是空的。

**调度期——计算要跑哪些 Cell，以及按什么顺序跑。** `Runner._collect_cells_to_run`（`cell_runner.py:185`）先用 `transitive_closure` 从被修改的 Cell 出发，沿着 children 边向下搜索，找到所有下游受影响的 Cell；然后用 `topological_sort`（`dataflow/__init__.py:96`）对这些 Cell 做拓扑排序，确保被依赖的 Cell 先执行、依赖它的 Cell 后执行。在 autorun 模式下，所有后代 Cell 都纳入执行范围；在 lazy 模式下，只标记为 stale 不立即执行。

**执行期——变量注入和临时变量清理。** `CellImpl.run`（`cell.py:549`）执行时，Runner 根据 Cell 的 refs 列表决定需要哪些变量，从全局 `globals` 中取出这些变量的当前值传入。执行完成后，Cell 的 `temporaries`（以下划线开头的私有变量，由 AST 分析识别）会被清理掉，但 `closed_over_temporaries`（被闭包引用的私有变量）会保留，防止函数调用时找不到引用。

用一句话概括整个链路：用户改了 Cell → `compile_cell` 用 AST 解析出 defs/refs 并编译字节码 → `register_cell` 用 defs/refs 在图中连线 → `transitive_closure` 沿边找到所有受影响的下游 Cell → `topological_sort` 按边排序确定执行顺序 → Runner 按顺序执行，执行时用 refs 注入变量、用 temporaries 清理状态。

### 4. 响应式执行的工作原理

当用户修改并运行 Cell A 时，marimo 执行以下流程：

第一步，`ScopedVisitor` 对 Cell A 的代码做 AST 分析，提取它定义的变量（defs）和引用的变量（refs）。

第二步，`DirectedGraph.register_cell()` 更新拓扑结构——通过 `DefinitionRegistry` 查找 Cell A 定义的变量被哪些 Cell 引用，建立边关系。

第三步，通过 `transitive_closure()` 计算 Cell A 的所有下游 Cell（递归遍历 children），根据运行模式决定是自动执行（reactive 模式）还是标记为 stale（lazy 模式）。

第四步，按拓扑排序依次执行需要运行的 Cell，每个 Cell 的执行结果通过 WebSocket 推送到前端。

删除 Cell 时，`DirectedGraph.delete_cell()` 移除节点和边，同时通过 `DefinitionRegistry.unregister_definitions()` 清除该 Cell 定义的变量，消除隐藏状态。

### 5. 存储格式设计

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

### 5. 进程架构与多用户模型

marimo 的进程架构与 Jupyter 完全不同。Jupyter 的架构是三层：JupyterHub（网关）→ Notebook Server（每用户一个进程）→ Kernel（每用户一个进程）。marimo 只有一个 Server 进程，内部通过 Session 管理多用户隔离。

marimo 的层次关系是：**Server（1 个进程）→ SessionManager（1 个，进程内对象）→ Session（N 个，每用户一个）→ KernelManager（1 per Session）→ Kernel（1 per Session，子进程或线程）**。

Server 进程基于 Starlette/ASGI，用 uvicorn 运行，所有用户共享。不同用户的请求通过 HTTP 头 `Marimo-Session-Id` 区分（`_server/api/deps.py:136`），Server 将请求路由到对应的 Session。`SessionManager`（`_server/session_manager.py`）维护 `sessions` 字典（SessionId → Session），负责创建、查找、关闭 Session。每个 `SessionImpl`（`_session/session.py`）内部持有一个 `KernelManager`，由 `KernelManager` 负责启动和管理 Kernel。

Kernel 的运行方式取决于模式。edit 模式下用 `multiprocessing.Process`（独立进程，支持 SIGINT 中断），run 模式下用 `threading.Thread`（线程，省内存但不能中断）。在多 App run 模式下还有 `AppHostPool`（`_session/app_host/pool.py`），每个 Notebook 文件运行在独立的 AppHost 进程中，避免不同 Notebook 之间的 `sys.modules` 和 Python 全局状态冲突；多个用户访问同一个 Notebook 时共享同一个 AppHost，但各自有独立的 Session 和 Kernel 线程。

与 JupyterHub 对比：JupyterHub 为每个用户启动一个独立的 Notebook Server 进程（通过 Spawner），进程级隔离天然解决了用户间串扰，但每个用户约 100-200MB 内存开销。marimo 用单进程内的 Session 隔离，通过 `PathValidator` 做文件权限校验、Session 管理用户→Kernel 映射，资源开销小得多，但需要在架构设计阶段就考虑多租户隔离逻辑。marimo 不存在 JupyterHub 的 Spawner 概念——不需要为每个用户启动一个完整的服务器进程，Session 是进程内对象而非操作系统进程。

### 6. 安全与沙箱机制

**沙箱模式——两种隔离级别。**

marimo 通过 `SandboxMode` 枚举（`_cli/sandbox.py`）定义两种沙箱模式。`SINGLE` 模式用于单文件场景，整个 marimo 进程被 `uv run --isolated --no-project` 包裹，在一个临时虚拟环境中运行，退出即销毁。`MULTI` 模式用于多文件/目录场景，每个 Notebook 的 Kernel 作为独立子进程启动，运行在自己独立的 venv 中——`build_sandbox_venv` 创建临时目录，用 `uv venv` 建虚拟环境，用 `uv pip install` 安装 Notebook 声明的依赖，Kernel 子进程通过 ZeroMQ IPC 通道与主进程通信。

`uv run --isolated --no-project` 是 uv（Astral 团队用 Rust 写的 Python 包管理器）提供的依赖隔离运行模式。`--isolated` 表示每次创建全新的临时虚拟环境，不缓存不复用；`--no-project` 表示忽略当前目录的 `pyproject.toml`，只使用指定的依赖文件。uv 内部用的就是 Python 标准 venv 机制（虚拟环境目录结构相同），但加了自动化层——创建环境、安装依赖、执行命令、清理环境一步完成，且 Rust 实现比 `python -m venv` + `pip install` 快一到两个数量级。需要注意的是，这只是 Python 依赖层面的隔离，不是操作系统级别的沙箱——用户代码仍可执行 `os.system` 等系统调用。

此外还有 Docker 容器模式（`_cli/run_docker.py`），`marimo edit --docker` 启动 Docker 容器，在容器内再跑 `--sandbox`，实现双重隔离。marimo 在线服务 molab 还使用 gVisor 沙箱（`kernel_exit.py` 注释提及），但这是托管服务层面的能力，不在开源代码中。

**防止代码逃逸——进程隔离 + 路径校验，但无 OS 级强制隔离。**

沙箱模式下 Kernel 是独立子进程（`subprocess.Popen`），有自己的 venv 和环境变量，这是基本的进程级隔离。文件系统层面，`PathValidator`（`_server/files/path_validator.py`）做目录包含校验——检查用户请求的文件路径是否在允许的工作目录内，防止路径穿越攻击（`../` 逃逸），还处理了符号链接安全问题和 Windows 短路径名问题。

但 marimo 没有操作系统级别的强制隔离——没有 seccomp、没有 cgroup 限制、没有 namespace 隔离。用户代码在 Kernel 进程中可以执行任意系统命令，可以读写进程权限范围内的任意文件。真正的操作系统级隔离需要依赖 Docker 模式或外部部署环境。

**权限控制——Token 认证，无 RBAC。**

`TokenManager`（`_server/token_manager.py`）管理两种 token。edit 模式使用 `AuthToken.random()` 生成随机 token，用户需要在 URL 中携带才能访问。run 模式使用 `AuthToken.from_code(source_code)`，基于源码哈希生成确定性 token，使得多个实例可以共享同一个 token。还有 `SkewProtectionToken` 防止版本不一致问题。marimo 没有用户体系——没有用户注册、登录、角色（RBAC）、权限分级。认证是"有 token 就能访问"的共享密钥模式，不是基于用户身份的访问控制。所有拿到 token 的人都有完全相同的权限。

**多用户处理——每用户一个 Session，编辑模式每用户一个 Kernel 进程。**

`SessionManager`（`_server/session_manager.py`）管理所有会话。`SessionImpl`（`_session/session.py`）的注释明确写了"Each session has its own Python kernel"。在 edit 模式下，每个 Session 创建一个 `multiprocessing.Process` 运行 Kernel，所以是每用户一个 Kernel 进程。在 run 模式下，使用 `threading.Thread` 运行 Kernel（线程而非进程，省内存），也是每用户一个 Kernel 线程。

还有一个更高级的隔离机制——`AppHostPool`（`_session/app_host/pool.py`），在多 App run 模式下启用，每个 Notebook 文件运行在独立的 AppHost 进程中，避免不同 Notebook 之间的 `sys.modules` 和 Python 全局状态冲突。多个用户访问同一个 Notebook 时共享同一个 AppHost，但各自有独立的 Session 和 Kernel 线程。

总结 marimo 的安全层次：venv 隔离（依赖不串）→ 进程隔离（Kernel 是子进程）→ Token 认证（防止未授权访问）→ 路径校验（防止文件系统穿越）→ Docker 可选（操作系统级隔离）。缺少的是用户级权限控制（RBAC）、资源配额（CPU/内存限制）和网络隔离。这些缺失恰好是企业级 Notebook 平台（如 JupyterHub + Enterprise Gateway）需要补充的部分。
