# 8.3 Jupyter Notebook v7 源码架构深度分析

> 源码路径：`/Users/lipingjiang/Codes/notebook-projects/jupyter-notebook`（Jupyter Notebook v7 官方仓库）

### 1. 整体架构定位

Notebook v7 最核心的变化是：它不再是独立实现的前端，而是基于 JupyterLab 组件 + Jupyter Server 的上层封装。本质是用 JupyterLab 的积木，搭出一个经典 Notebook 的单文档界面。

### 2. 六大核心模块

**模块 1 — Python 服务端（`notebook/`）：** 核心是 `app.py` 中的 `JupyterNotebookApp`，继承自 `LabServerApp`。注册路由处理器（`/tree`、`/notebooks`、`/edit`、`/consoles`、`/terminals`），生成 `page_config`（注入 baseUrl/token/hub 信息），检测 JupyterHub 环境。这一层是薄壳，真正的服务端逻辑在 `jupyter_server` 和 `jupyterlab_server` 依赖库中。

**模块 2 — 前端应用核心（`packages/application/`）：** 包含三个关键文件。`app.ts` 中的 `NotebookApp` 继承自 `JupyterFrontEnd`，初始化文档注册表、注册 MIME 渲染插件、通过 `registerPluginMods` 加载所有插件。`shell.ts` 中的 `NotebookShell` 是最重要的文件——定义 6 个区域（top/menu/main/left/right/down），main 区域只允许一个 Widget（单文档约束），布局是嵌套 SplitPanel。`panelhandler.ts` 管理按 rank 排序的 Widget 列表，`SidePanelHandler` 实现侧边栏展开/折叠/切换。

**模块 3 — 应用扩展（`packages/application-extension/`）：** 18 个插件，包括 opener（路由系统）、tree（Tree 路由解析）、shell（注册 NotebookShell）、rendermime（MIME 渲染器）、topVisibility、sidePanelVisibility、zen（禅模式）、tabTitle、title、dirty、settingsConnector、splash 等。

**模块 4 — Notebook 专用扩展（`packages/notebook-extension/`）：** 12 个插件处理 Notebook 文档交互——checkpoints（检查点轮询）、kernelLogo、kernelStatus（Kernel 状态显示）、scrollOutput（自动滚动长输出）、trusted（信任指示器）、fullWidthNotebook、closeTab 等。

**模块 5 — 其他扩展包：** 文件树页面（`packages/tree/`）、Console scratchpad（`packages/console-extension/`）、终端集成（`packages/terminal-extension/`）、界面切换器（`packages/lab-extension/`）等。

**模块 6 — 构建系统（`app/`）：** 使用 Rspack + Module Federation。`rspack.config.js` 从 `package.json` 的 `jupyterlab` 字段读取扩展依赖，`createShared()` 构建 Module Federation 的 shared 配置（所有 `@jupyterlab/*`、`@lumino/*` 包都是 singleton 共享）。`index.template.js` 是前端启动入口，按页面分组加载不同插件。

### 3. 三个最重要的部分

**第一重要：`NotebookShell`** — 整个项目灵魂。定义"什么是 Notebook 界面"：单文档主区域 + 可折叠侧边栏 + 顶栏 + 菜单栏 + 底部面板。理解了 NotebookShell 就理解了 Notebook v7 和 JupyterLab 的本质区别。

**第二重要：插件系统** — 18+12 个插件通过 `JupyterFrontEndPlugin` 接口注册，依赖注入通过 Token（`INotebookShell`、`IRouter`、`IDocumentManager` 等）实现。这套系统直接来自 JupyterLab。

**第三重要：Module Federation 构建体系** — 允许第三方扩展以联邦模块方式动态加载，不需要重新构建主应用。这是 Jupyter 生态能繁荣的根基。

### 4. 六大难点与应对方案

**难点 1 — Shell 布局系统的复杂度。** Lumino 的 `SplitPanel`、`Panel`、`TabPanel`、`StackedPanel` 嵌套组合，嵌套层级深。侧边栏的展开/折叠涉及 `_isHiddenByUser`、`_currentWidget`、`_lastCurrentWidget` 三个状态协调。应对方案：封装 `SidePanelHandler` 统一管理状态，通过 `restoreLayout` 支持自定义布局恢复。

**难点 2 — 路由系统。** 基于模式匹配 + Signal 信号触发，不是标准前端路由框架。`ITreeResolver` 的 paths 是 Promise，在路由解析完成后 resolve，其他插件依赖这个 Promise。应对方案：理解 Signal + Promise 的异步协调模式，注意竞态条件。

**难点 3 — Module Federation 的 shared 配置。** `createShared()` 处理三种来源的共享配置，需要处理 `import: false`、`singleton: true`、`file:` 协议版本号转换。应对方案：严格管理 `package.json` 的 `jupyterlab.sharedPackages` 配置，版本对齐。

**难点 4 — 多页面架构限制。** 每个页面（tree/notebooks/edit）有独立 HTML 模板，加载不同插件集，页面间不共享状态，切换页面是整页刷新。应对方案：理解这是 Notebook v7 相比 JupyterLab 的根本限制，大厂通过自研前端解决。

**难点 5 — 与 JupyterLab 的依赖协调。** 几乎所有核心能力来自 `@jupyterlab/*` 包，JupyterLab 的 breaking change 直接冲击 Notebook v7。应对方案：`SettingConnector` 注入式覆盖、精确版本锁定、持续跟踪 JupyterLab 发版。

**难点 6 — 经典 Notebook 行为兼容。** 自动滚动长输出、Close and Halt 行为、检查点显示等。应对方案：边缘 case 需精确匹配用户习惯，充分测试。

### 5. 源码级架构图

```
Jupyter Notebook v7 架构全景

┌─────────────────────────────────────────────────────────────┐
│                    Python 服务端（薄壳）                      │
│  notebook/app.py — JupyterNotebookApp                       │
│  ├── 路由处理器（Tree/Notebook/Console/Terminal/File）       │
│  ├── page_config 生成（注入 baseUrl/token/hub 信息）         │
│  └── HTML 模板渲染（6 个模板）                               │
│  依赖：jupyter_server（Kernel/文件/会话）+ jupyterlab_server │
└─────────────────────────────────────────────────────────────┘
                              │
                    HTTP + WebSocket
                              │
┌─────────────────────────────────────────────────────────────┐
│                    前端构建层（Rspack + MF）                  │
│  app/rspack.config.js — Module Federation 共享配置           │
│  app/index.template.js — 按页面分组加载插件 → app.start()   │
└─────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────┐
│                    前端应用核心                               │
│  packages/application/                                      │
│  ├── app.ts — NotebookApp（继承 JupyterFrontEnd）            │
│  ├── shell.ts — NotebookShell（6 区域单文档布局）             │
│  └── panelhandler.ts — PanelHandler + SidePanelHandler      │
└─────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────┐
│                    插件层（38+ 个插件）                       │
│  application-extension/（18 个应用插件）                     │
│  notebook-extension/（12 个 Notebook 插件）                  │
│  tree-extension / console-extension / terminal-extension /  │
│  lab-extension / help-extension / docmanager-extension /    │
└─────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────┐
│                    JupyterLab 依赖层（不在本仓库）             │
│  @jupyterlab/application — JupyterFrontEnd 基类             │
│  @jupyterlab/notebook — NotebookPanel / Cell / CodeCell     │
│  @jupyterlab/services — Kernel 通信 / 会话管理               │
│  @jupyterlab/docmanager — 文档管理 / 上下文                  │
│  @jupyterlab/rendermime — MIME 渲染体系                     │
│  @lumino/widgets — Widget 树 / SplitPanel / TabPanel        │
│  @lumino/signaling — 信号系统                               │
└─────────────────────────────────────────────────────────────┘
```

### 6. JupyterLab vs Jupyter Notebook — 什么关系？

两者的关系可以用一句话概括：**JupyterLab 是 Jupyter Notebook 的下一代替代品，Notebook v7 是 JupyterLab 组件的"单文档模式"封装。**

**界面模型不同。** JupyterLab 是单页应用（SPA），类似 VSCode 的多 Tab + 分屏布局——可以同时打开多个 Notebook、终端、Console、文件编辑器，左右分屏拖拽排列。Notebook v7 是多页面应用——每个页面（tree/notebooks/edit）是独立的 HTML，页面切换是整页刷新，主区域只能显示一个文档。底层实现上，JupyterLab 使用 `LabShell`（基于 Lumino 的 `DockPanel`，支持多 Tab 和分屏），Notebook v7 使用 `NotebookShell`（限制 main 区域只允许一个 Widget，强制单文档模式）。

**功能丰富度不同。** JupyterLab 有完整的 IDE 体验——文件浏览器、目录树、终端、属性面板、命令面板（Cmd+Shift+P）、拖拽布局、主题切换、扩展管理器。Notebook v7 是精简版——保留了经典 Notebook 的简洁界面，只有顶栏 + 菜单 + 主区域 + 可折叠侧边栏。

**插件加载不同。** JupyterLab 在一个 SPA 中加载所有插件，插件间可以共享状态。Notebook v7 按页面分组加载插件——tree 页面加载 tree 相关插件，notebooks 页面加载 notebook 相关插件，页面间不共享状态。

**演进历史。** 经典 Jupyter Notebook（v6 及之前）是独立的前端实现，基于 jQuery + RequireJS。2018 年 JupyterLab 1.0 发布，是全新设计的 TypeScript 前端。2023 年 Notebook v7 发布，它不再独立维护前端，而是直接复用 JupyterLab 的组件（`@jupyterlab/*` 包），通过 `NotebookShell` 限制为单文档模式。所以 Notebook v7 的代码量很小（约 4000 行 TS），核心能力全部来自 JupyterLab。

**大厂选择。** 阿里、字节、华为选择基于 JupyterLab 深度改造（保留多 Tab + 分屏，叠加自研插件）。腾讯 Cloud Studio 选择完全自研前端（Monaco Editor），抛弃 JupyterLab 整套体系。选择 JupyterLab 的优势是生态复用，选择自研的优势是灵活度。
