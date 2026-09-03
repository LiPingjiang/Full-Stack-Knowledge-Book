# Split `7.12-点富科技案例文档` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part7-ai-engineering/12-案例-点富科技大模型数据评测岗.md` 从一个失去边界的巨型案例文档，拆分成主文 + 6 个专题子文档，保留原有信息价值，同时显著提升可读性、可维护性与时效管理能力。

**Architecture:** 保留一个“岗位案例主文”作为用户入口，只承载岗位背景、JD 拆解、阅读地图和跳转关系；将“尽调/市场与产品/监管合规/数据与评测/面试准备/0→1 规划”等主题外溢内容拆成独立专题，统一通过目录、导航和交叉引用连接。拆分优先保持原文内容不重写、不新增观点，先做结构重组，再做局部压缩。

**Tech Stack:** Markdown, repo-local cross-links, manual content refactor, heading normalization

**Spec:** `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part7-ai-engineering/12-案例-点富科技大模型数据评测岗.md`

## Global Constraints

- 仅做文档结构重组，不新增未经验证的新事实。
- 先拆分边界，再压缩内容；不要在同一轮同时大幅重写论证。
- 主文必须可独立阅读，并能在 10 分钟内让读者知道“这是什么岗位、该不该读哪一部分”。
- 子文必须主题单一；一个子文只回答一个稳定问题。
- 所有新文件名保持与现有中文命名风格一致。
- 所有交叉引用使用仓库内相对路径。
- 不改动与本计划无关的章节。

---

### Task 1: 先抽出“岗位案例主文”骨架

**Files:**
- Modify: `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part7-ai-engineering/12-案例-点富科技大模型数据评测岗.md`
- Create: `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part7-ai-engineering/12-案例-点富科技大模型数据评测岗-主文.md`

**Interfaces:**
- Consumes: 原文的开场导语、这一节讲什么、先看结论、岗位拆解部分的目录结构
- Produces: 一个新的“主文入口”，后续所有子文都从这里被链接

- [ ] **Step 1: 写出主文应保留的结构清单**

```md
保留这五块：
1. 开场定位（为什么这个案例特殊）
2. 阅读导航（这篇文档拆成了哪些专题）
3. 岗位/JD 拆解与能力映射（保留最核心部分）
4. 面试使用说明（不同读者怎么读）
5. 专题链接导航
```

- [ ] **Step 2: 从原文中裁出主文最小骨架**

```md
目标长度：800-1400 行以内
必须删除出主文的内容类型：
- 公司尽调细节
- 市场真实性长论证
- 监管法规逐条分析
- 竞品全景长表述
- 0→1 数据体系详细路线图
- 论文深读与评测工具箱
```

- [ ] **Step 3: 在新主文中补一个稳定目录**

```md
## 目录
- 一、为什么这个案例特殊
- 二、岗位拆解与能力映射
- 三、怎么使用这组专题
- 四、专题导航
```

- [ ] **Step 4: 让原文件退化为跳转页或保留为兼容入口**

```md
如果要平滑迁移：
- 原文件保留原路径
- 内容改为：简短摘要 + 链接到 `12-案例-点富科技大模型数据评测岗-主文.md`
- 避免外部链接全部失效
```

- [ ] **Step 5: 验证主文是否仍可独立阅读**

Run: `rg -n '^#{1,4} ' '/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part7-ai-engineering/12-案例-点富科技大模型数据评测岗-主文.md'`
Expected: 目录层级稳定，一级主题不超过 4-5 个

- [ ] **Step 6: Commit**

```bash
git add '/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part7-ai-engineering/12-案例-点富科技大模型数据评测岗.md' '/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part7-ai-engineering/12-案例-点富科技大模型数据评测岗-主文.md'
git commit -m "docs: split dianfu case into main entry doc"
```

### Task 2: 抽出“公司尽调与招聘方核验”专题

**Files:**
- Create: `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part7-ai-engineering/12-点富案例-公司尽调与招聘方核验.md`
- Modify: `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part7-ai-engineering/12-案例-点富科技大模型数据评测岗-主文.md`

**Interfaces:**
- Consumes: 原文“一、点富尽调”全部内容
- Produces: 单独的公司研究专题，供主文跳转引用

- [ ] **Step 1: 建立该专题的稳定问题定义**

```md
这个专题只回答三件事：
1. 公司公开口径哪些可证实
2. 哪些地方有注水/存疑
3. 这些核验结果如何转化为面试探针
```

- [ ] **Step 2: 把原文尽调相关内容整体迁移**

```md
迁移范围：
- “点富尽调：宣传册的七处注水与一条监管记录”
- 相关核验结论、团队规模、方法论沉淀
不迁移：
- 岗位拆解
- 数据体系规划
- 市场/产品终局判断
```

- [ ] **Step 3: 在专题顶部补“信息时效声明”**

```md
> 本文强依赖公司公开信息、媒体报道和监管口径，时效性强。阅读时请优先关注核验方法和结论边界，而不是把具体数字视为长期稳定事实。
```

- [ ] **Step 4: 在主文中留下摘要而非长论证**

```md
主文只保留：
- 一段 5-8 行摘要
- 一个“为什么值得先看尽调”的说明
- 一个跳转链接
```

- [ ] **Step 5: 检查这个专题是否没有混入技术/市场终局判断**

Run: `rg -n '市场|技术路线终局|0→1|竞品全景' '/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part7-ai-engineering/12-点富案例-公司尽调与招聘方核验.md'`
Expected: 即使出现，也应只作为过渡，不应成为主结构

- [ ] **Step 6: Commit**

```bash
git add '/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part7-ai-engineering/12-点富案例-公司尽调与招聘方核验.md' '/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part7-ai-engineering/12-案例-点富科技大模型数据评测岗-主文.md'
git commit -m "docs: extract dianfu diligence section"
```

### Task 3: 抽出“面试准备与反问清单”专题

**Files:**
- Create: `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part7-ai-engineering/12-点富案例-面试准备与反问清单.md`
- Modify: `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part7-ai-engineering/12-案例-点富科技大模型数据评测岗-主文.md`

**Interfaces:**
- Consumes: 原文“五、面试准备”“六、反问清单”
- Produces: 候选人最直接可用的面试专题

- [ ] **Step 1: 定义这个专题只服务“候选人临场使用”**

```md
只保留：
- 面试策略
- 技术底牌
- 预测题
- 反问问题
- 上场前清单
删除或迁出：
- 大段行业论证
- 市场判断
- 合规长分析
```

- [ ] **Step 2: 合并“面试准备”与“反问清单”为单专题**

```md
推荐结构：
## 一、面试策略
## 二、技术底牌
## 三、预测题与答题要点
## 四、反问清单
## 五、现场使用建议
```

- [ ] **Step 3: 给专题加“阅读路径”提示**

```md
- 时间少：先看技术底牌 + 反问清单
- 一轮技术面前：看预测题 A-D 组
- 负责人面：补看负责人视角问题
```

- [ ] **Step 4: 主文中只保留面试导流入口**

```md
在主文目录中写：
- 若你的目标是“准备面试”，直接进入本专题
```

- [ ] **Step 5: 验证专题长度和目标读者是否单一**

Run: `wc -l '/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part7-ai-engineering/12-点富案例-面试准备与反问清单.md'`
Expected: 拆出后比原文显著更聚焦，主题不漂移到市场/合规/产品战略

- [ ] **Step 6: Commit**

```bash
git add '/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part7-ai-engineering/12-点富案例-面试准备与反问清单.md' '/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part7-ai-engineering/12-案例-点富科技大模型数据评测岗-主文.md'
git commit -m "docs: extract dianfu interview prep guide"
```

### Task 4: 抽出“数据与评测方法”专题

**Files:**
- Create: `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part7-ai-engineering/12-点富案例-数据与评测方法.md`
- Modify: `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part7-ai-engineering/12-案例-点富科技大模型数据评测岗-主文.md`

**Interfaces:**
- Consumes: 原文“三、专家数字分身技术全景”中与数据/评测相关部分，原文“四、领域知识”以及后段论文/工具箱相关内容
- Produces: 单独的“数据岗专业深挖”专题

- [ ] **Step 1: 把主题边界限定为“数据构建 + 评测体系 + Bad Case + 工具箱”**

```md
保留：
- 数据集谱系
- 数据构建配方
- Persona / Character / MedBench 评测
- Bad Case 归因
- 方法论工具箱
不保留：
- 公司尽调
- 产品定位
- 市场真假
- 监管边界
```

- [ ] **Step 2: 统一标题风格，避免“领域知识 / 论文深读 / 工具箱”三套体裁混排**

```md
推荐结构：
## 一、为什么这是数据岗核心竞争力
## 二、数据构建
## 三、评测体系
## 四、Bad Case 归因
## 五、论文与方法深读
## 六、实战工具箱
```

- [ ] **Step 3: 在主文中只保留“这个岗位真正稀缺的是评测和数据策略能力”这一结论**

```md
主文只需要 1 段解释：
这个岗位最值得深读的不是通用大模型知识，而是数据与评测方法
```

- [ ] **Step 4: 检查专题内部是否还有明显的公司特定信息依赖**

Run: `rg -n '点富|灵均|杨凤池|点点专家' '/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part7-ai-engineering/12-点富案例-数据与评测方法.md'`
Expected: 允许有少量场景化例子，但正文应已能脱离该公司独立阅读

- [ ] **Step 5: Commit**

```bash
git add '/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part7-ai-engineering/12-点富案例-数据与评测方法.md' '/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part7-ai-engineering/12-案例-点富科技大模型数据评测岗-主文.md'
git commit -m "docs: extract dianfu data and evaluation methods"
```

### Task 5: 抽出“监管、市场、产品定位”专题

**Files:**
- Create: `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part7-ai-engineering/12-点富案例-监管市场与产品定位.md`
- Modify: `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part7-ai-engineering/12-案例-点富科技大模型数据评测岗-主文.md`

**Interfaces:**
- Consumes: 原文九到十五章式内容（产品视角、监管边界、实证核查、竞品全景、市场真实性、突破点分析）
- Produces: 一个公司/赛道判断专题，而非岗位主文的一部分

- [ ] **Step 1: 先明确这个专题只回答“这门生意是否成立、产品该怎么定位”**

```md
主问题：
1. 监管边界在哪
2. 市场是否真实
3. 当前产品定位的问题是什么
4. 可行替代路径是什么
```

- [ ] **Step 2: 将原文相关部分整体迁移，并按“监管→市场→产品→竞品→结论”重排**

```md
推荐结构：
## 一、监管边界
## 二、市场真实性
## 三、产品定位问题
## 四、竞品与替代路径
## 五、综合判断
```

- [ ] **Step 3: 降低过强结论语气，增强“证据—判断”顺序**

```md
把这类句式优先改写：
- “终局判断” → “当前阶段判断”
- “市场是虚的” → “市场规模与融资叙事存在明显偏差”
- “唯一的重要例外” → “当前观察到的关键例外”
```

- [ ] **Step 4: 在主文中只保留简短提醒**

```md
如果读者只是求职准备，可先跳过本专题；
如果读者需要判断公司和赛道值不值得投入，再读本专题。
```

- [ ] **Step 5: 验证专题是否还承担面试题/方法论工具箱职责**

Run: `rg -n '预测题|反问|工具箱|0→1规划' '/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part7-ai-engineering/12-点富案例-监管市场与产品定位.md'`
Expected: 不应再承担这些职责

- [ ] **Step 6: Commit**

```bash
git add '/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part7-ai-engineering/12-点富案例-监管市场与产品定位.md' '/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part7-ai-engineering/12-案例-点富科技大模型数据评测岗-主文.md'
git commit -m "docs: extract dianfu market and compliance analysis"
```

### Task 6: 抽出“数据体系 0→1 规划”专题

**Files:**
- Create: `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part7-ai-engineering/12-点富案例-数据体系0到1规划.md`
- Modify: `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part7-ai-engineering/12-案例-点富科技大模型数据评测岗-主文.md`

**Interfaces:**
- Consumes: 原文“七、数据体系 0→1 规划”全部内容
- Produces: 一个可迁移到其他创业公司场景的规划专题

- [ ] **Step 1: 把它从“这个公司案例的一个章节”变成“创业公司数据 0→1 模板”**

```md
保留：
- 约束识别
- 体量测算
- 架构蓝图
- 三期路线图
- ROI/团队协作/风险
改造：
- 将公司特定表述提炼为可复用模板
```

- [ ] **Step 2: 统一这个专题的工程文档风格**

```md
推荐结构：
## 一、约束条件
## 二、体量测算
## 三、架构蓝图
## 四、分期路线图
## 五、效果评估与 ROI
## 六、团队与协作
## 七、风险清单
```

- [ ] **Step 3: 在主文中只留“如果面试官问你入职后怎么搭数据体系，去看这个专题”**

```md
主文不再承载完整路线图细节
```

- [ ] **Step 4: 验证这个专题对非点富场景是否仍有价值**

Run: `rg -n '点富|点点专家|灵均' '/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part7-ai-engineering/12-点富案例-数据体系0到1规划.md'`
Expected: 允许少量背景，但大部分内容应可迁移

- [ ] **Step 5: Commit**

```bash
git add '/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part7-ai-engineering/12-点富案例-数据体系0到1规划.md' '/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part7-ai-engineering/12-案例-点富科技大模型数据评测岗-主文.md'
git commit -m "docs: extract dianfu data platform planning guide"
```

### Task 7: 做全链路导航收口

**Files:**
- Modify: `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part7-ai-engineering/README.md`
- Modify: `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part7-ai-engineering/12-案例-点富科技大模型数据评测岗-主文.md`
- Modify: 新创建的所有 `12-点富案例-*.md`

**Interfaces:**
- Consumes: 前 6 个任务产生的主文与专题文件
- Produces: 一套稳定的目录、前后跳转、读者路径

- [ ] **Step 1: 在 `part7-ai-engineering/README.md` 中更新条目**

```md
将原先单一的 7.12 条目改为：
- 7.12 点富案例主文
- 7.12.x 子专题列表（可作为子弹列表而非一级章节）
```

- [ ] **Step 2: 在每个专题顶部加入统一导航块**

```md
> 相关文档：
> - 主文：...
> - 面试专题：...
> - 数据与评测专题：...
> - 市场与合规专题：...
> - 0→1 规划专题：...
```

- [ ] **Step 3: 在主文尾部加入“按需求阅读”导航**

```md
- 只准备面试 → 看《面试准备与反问清单》
- 想判断公司值不值得去 → 看《公司尽调与招聘方核验》+《监管市场与产品定位》
- 想展示方法论深度 → 看《数据与评测方法》+《数据体系0到1规划》
```

- [ ] **Step 4: 逐个检查相对链接是否有效**

Run: `rg -n '\]\(\./12-' '/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part7-ai-engineering/'`
Expected: 新专题互链完整，无明显死链拼写错误

- [ ] **Step 5: 做一次人工结构复核**

```md
检查清单：
- 主文是否仍然像“案例入口”而不是“大而全总集”
- 每个子文是否只回答一个稳定问题
- 是否仍有大段重复内容横跨多个子文
- 是否仍有强烈的身份漂移（候选人/决策者/产品负责人混在一起）
```

- [ ] **Step 6: Commit**

```bash
git add '/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part7-ai-engineering/README.md' '/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part7-ai-engineering/'12-*.md
git commit -m "docs: add navigation for split dianfu case docs"
```
