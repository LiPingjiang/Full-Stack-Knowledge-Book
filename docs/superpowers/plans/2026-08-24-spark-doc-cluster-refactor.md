# Spark Doc Cluster Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebuild the Spark document cluster into a clear main chapter, concise guide docs, and focused topic docs with modern Spark 3.x / 4.x capability coverage.

**Architecture:** Keep `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark.md` as the single entry chapter, trim `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-性能优化.md` into a decision-tree guide, preserve `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-调试运维.md` as the diagnostics entry, and move deep material into new topic docs. Add explicit coverage for DPP, REBALANCE, AQE limits, Spark Connect, Kubernetes, and Decommission where each topic naturally belongs.

**Tech Stack:** Markdown, ripgrep, Python 3 verification scripts, shell utilities

**Spec:** `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/docs/superpowers/specs/2026-08-24-spark-doc-cluster-design.md`

## Global Constraints

- Touch only the Spark document cluster under `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark*.md` plus the approved spec/plan files under `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/docs/superpowers/`.
- Keep the main chapter readable as a chapter: concepts + architecture + navigation, not parameter dumps or long troubleshooting catalogs.
- Keep `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-性能优化.md` as a concise optimization mainline with “详见专题” links.
- Remove inherited numbering such as `## 六、...` and `### 6.1 ...` from standalone Spark topic docs.
- Preserve discoverability: when migrating content out of a source doc, replace it with a short conclusion and a relative Markdown link to the new topic doc.
- Add explicit coverage for DPP, REBALANCE, AQE limitations, Spark Connect, Kubernetes operation, and Decommission.
- Do not widen scope to Flink or non-Spark big-data chapters in this plan.

---

## File Structure

### Modify
- `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark.md` — main Spark chapter; keep framework and add navigation.
- `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-性能优化.md` — concise optimization guide; reduce detail and link outward.
- `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-调试运维.md` — diagnostics and log entry; add K8s angle and tighten role.

### Create
- `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-面试深度剖析.md`
- `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-SQL与算子优化详解.md`
- `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-资源内存与Spill调优.md`
- `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-Shuffle机制与调优参数.md`
- `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-数据倾斜治理.md`
- `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-错误排查与兼容性.md`
- `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-部署与云原生运行.md`
- `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-版本演进与新能力.md`

### Verification helpers
- Use shell commands only; no permanent helper script file is required.

### Task 1: Rewrite the main Spark chapter as the entry doc

**Files:**
- Modify: `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark.md`

**Interfaces:**
- Consumes: existing chapter content from `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark.md`
- Produces: a main chapter with stable sections for architecture, SQL, deployment overview, and navigation links to all Spark topic docs

- [ ] **Step 1: Capture the current chapter outline and source ranges**

Run:
```bash
python3 - <<'PY'
from pathlib import Path
p = Path('/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark.md')
for i, line in enumerate(p.read_text().splitlines(), 1):
    if line.startswith(('## ', '### ')):
        print(f'{i}: {line}')
PY
```
Expected: A stable list of current `##` / `###` headings to map into the new structure.

- [ ] **Step 2: Replace the top-level structure with the approved entry-doc skeleton**

Edit `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark.md` so it contains these `##` sections in order:
```md
## 目录
## 一、为什么 Spark 比 MapReduce 快
## 二、核心抽象——RDD
## 三、执行架构
## 四、Spark SQL——最常用的模块
## 五、部署模式与运行环境
## 六、性能优化导航
## 七、调试运维导航
## 八、版本演进与新能力导航
## 九、面试深度剖析导航
```
Expected: the main chapter is clearly chapter-like rather than topic-encyclopedic.

- [ ] **Step 3: Keep only framework-level content in the main chapter**

Move or delete detail-heavy blocks from the main chapter, keeping only short concept summaries for:
- serialization comparison
- deep executor memory formulas
- spark-submit command examples
- long DataFrame/Dataset code comparison blocks
- full interview Q&A answers

Expected: the chapter reads as a guided overview, not a dump of deep detail.

- [ ] **Step 4: Add relative links to every child topic doc**

Insert links such as:
```md
详细内容请查看 → [Spark 性能优化专题](./04-Spark-性能优化.md)
详细内容请查看 → [Spark 调试运维专题](./04-Spark-调试运维.md)
详细内容请查看 → [Spark SQL 与算子优化详解](./04-Spark-SQL与算子优化详解.md)
```
Expected: every navigation section points outward to the right topic document.

- [ ] **Step 5: Verify the main chapter is slim and link-complete**

Run:
```bash
wc -l /Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark.md
rg -n '^## ' /Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark.md
rg -n '\]\(\./04-Spark.*\.md\)' /Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark.md
```
Expected: the chapter line count is materially reduced from 1891 lines, has the target `##` headings, and contains relative links to the Spark topic docs.

- [ ] **Step 6: Commit**

```bash
git add /Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark.md
git commit -m "docs: reshape spark main chapter as entry doc"
```

### Task 2: Turn the optimization doc into a decision-tree guide

**Files:**
- Modify: `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-性能优化.md`

**Interfaces:**
- Consumes: current optimization doc plus links to the new topic docs created in later tasks
- Produces: a concise optimization mainline doc with stable sections for order-of-operations, evidence-first tuning, and links to deep dives

- [ ] **Step 1: Snapshot the current optimization headings and large content blocks**

Run:
```bash
python3 - <<'PY'
from pathlib import Path
p = Path('/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-性能优化.md')
for i, line in enumerate(p.read_text().splitlines(), 1):
    if line.startswith(('## ', '### ', '#### ')):
        print(f'{i}: {line}')
PY
```
Expected: a heading map showing which blocks will be retained vs migrated.

- [ ] **Step 2: Replace inherited numbering with the approved standalone structure**

Edit the file to use these top-level `##` sections:
```md
## 一、先建立正确的调优顺序
## 二、减少数据量
## 三、优化计算逻辑
## 四、调整计算资源
## 五、Spark UI：优化从哪里开始看
## 六、数据倾斜治理总览
## 七、小文件与输出控制
## 八、优化效果评估
```
Expected: no `## 六、性能优化要点` or `### 6.1` style inherited numbering remains.

- [ ] **Step 3: Keep only summary-level content and move the rest out**

Reduce each section to:
- what to inspect first
- why this matters
- the tuning priority
- one or two minimal examples
- a “详见专题” link

Specifically remove deep detail for:
- full join strategy catalog
- full window function syntax teaching
- memory.fraction and container formulas
- full Shuffle parameter encyclopedia
- full runtime error quick-reference
Expected: the doc becomes a guide, not a warehouse.

- [ ] **Step 4: Add the new modern Spark guidance**

Ensure the guide explicitly contains short sections for:
- DPP in the “减少数据量” section
- AQE can / cannot solve in “调整计算资源”
- AQE auto-coalescing is not universal in “小文件与输出控制”
- REBALANCE as part of output-file strategy
Expected: modern Spark 3.x guidance appears in the mainline doc.

- [ ] **Step 5: Verify numbering cleanup and outward links**

Run:
```bash
rg -n '^(##|###) [一二三四五六七八九十]|^(##|###) [0-9]' /Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-性能优化.md
rg -n '6\.[0-9]' /Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-性能优化.md || true
rg -n '\]\(\./04-Spark.*\.md\)' /Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-性能优化.md
```
Expected: the file uses the standalone sectioning, no inherited `6.x` numbering survives, and links point to deep topic docs.

- [ ] **Step 6: Commit**

```bash
git add /Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-性能优化.md
git commit -m "docs: turn spark optimization doc into mainline guide"
```

### Task 3: Create the SQL and operator deep-dive doc

**Files:**
- Create: `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-SQL与算子优化详解.md`

**Interfaces:**
- Consumes: migrated content from the old optimization doc and main Spark chapter
- Produces: the authoritative destination for join strategy, DPP, windows, CTE, EXPLAIN, REBALANCE, and operator-level optimization

- [ ] **Step 1: Create the file with the approved skeleton**

Create `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-SQL与算子优化详解.md` with these `##` headings:
```md
## 一、先看懂 Spark SQL 的优化链路
## 二、JOIN 优化详解
## 三、分区裁剪与过滤下推
## 四、窗口函数与聚合优化
## 五、CTE、子查询与重复计算
## 六、算子级优化
## 七、小文件与输出分布控制
```
Expected: the new doc has a stable destination structure before any content is copied.

- [ ] **Step 2: Migrate the mapped legacy blocks into the matching sections**

Copy and adapt content from:
- `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-性能优化.md:13-69`
- `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-性能优化.md:109-921`
- `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-性能优化.md:1539-1776`
- `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark.md:1380-1610`
Expected: each copied block lands under a single-topic heading rather than remaining in the mainline doc.

- [ ] **Step 3: Add new sections for DPP, EXPLAIN verification, and REBALANCE**

Add fresh content for:
- DPP principles and applicability
- how to verify DPP using EXPLAIN / PartitionFilters
- `coalesce` / `repartition` / `repartitionByRange` / `REBALANCE`
Expected: the doc covers the modern SQL tuning gaps identified in the design.

- [ ] **Step 4: Verify that the new doc is topic-focused**

Run:
```bash
rg -n '^## ' /Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-SQL与算子优化详解.md
rg -n 'DPP|动态分区裁剪|REBALANCE|EXPLAIN|PartitionFilters' /Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-SQL与算子优化详解.md
```
Expected: the doc contains the target sections and the new capability coverage.

- [ ] **Step 5: Commit**

```bash
git add /Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-SQL与算子优化详解.md
git commit -m "docs: add spark sql and operator optimization deep dive"
```

### Task 4: Create the resource/memory and Shuffle deep-dive docs

**Files:**
- Create: `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-资源内存与Spill调优.md`
- Create: `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-Shuffle机制与调优参数.md`

**Interfaces:**
- Consumes: memory, spill, dynamic allocation, partitioning, and shuffle detail currently embedded in the optimization doc and main chapter
- Produces: one doc for resource/memory reasoning and one doc for shuffle mechanics, AQE coalescing limits, and partition tuning

- [ ] **Step 1: Create the two files with their target section skeletons**

Create both files with these top-level headings:
```md
# 6.4 Spark 资源内存与 Spill 调优
## 一、先建立资源调优的心智模型
## 二、动态资源分配
## 三、Spark 内存模型
## 四、YARN / 容器内存约束
## 五、Spill 机制
## 六、常见资源型问题
## 七、调优方法论
```

```md
# 6.4 Spark Shuffle 机制与调优参数
## 一、为什么 Shuffle 是 Spark 最贵的操作
## 二、Shuffle 底层机制
## 三、Shuffle 分区数怎么理解
## 四、AQE 与 Shuffle
## 五、Shuffle 调优参数
## 六、常见 Shuffle 问题
```
Expected: both deep-dive docs exist with stable scopes.

- [ ] **Step 2: Migrate memory, spill, dynamic allocation, and container content**

Copy and adapt content from:
- `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark.md:1057-1324`
- `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-性能优化.md:924-1380`
- `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-性能优化.md:1777-1897`
Expected: the resource doc becomes the single home for memory, OOM, spill, and dynamic allocation detail.

- [ ] **Step 3: Migrate shuffle partitions, parameters, and mechanics**

Copy and adapt content from:
- `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-性能优化.md:1381-1538`
- `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-性能优化.md:2472-2601`
Expected: the Shuffle doc becomes the single home for partition-count reasoning and low-level shuffle tuning.

- [ ] **Step 4: Add the AQE coalescing limitation section**

In `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-Shuffle机制与调优参数.md`, add explicit prose for:
- AQE auto-coalescing is heuristic, not globally optimal
- `spark.sql.adaptive.coalescePartitions.parallelismFirst`
- `spark.sql.adaptive.advisoryPartitionSizeInBytes` is advisory, not a hard target
- why busy clusters may prefer stricter manual control
Expected: the doc contains the new AQE-limit guidance requested by the user.

- [ ] **Step 5: Verify doc boundaries and new concepts**

Run:
```bash
rg -n 'Dynamic Allocation|memory.fraction|Spill|YARN kill|collect|toPandas' /Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-资源内存与Spill调优.md
rg -n 'AQE|parallelismFirst|advisoryPartitionSizeInBytes|shuffle.partitions|default.parallelism' /Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-Shuffle机制与调优参数.md
```
Expected: the resource doc owns resource topics, and the Shuffle doc owns AQE/partition/shuffle topics.

- [ ] **Step 6: Commit**

```bash
git add /Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-资源内存与Spill调优.md /Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-Shuffle机制与调优参数.md
git commit -m "docs: split spark resource and shuffle deep dives"
```

### Task 5: Create the skew, error, deployment, version, and interview docs

**Files:**
- Create: `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-数据倾斜治理.md`
- Create: `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-错误排查与兼容性.md`
- Create: `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-部署与云原生运行.md`
- Create: `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-版本演进与新能力.md`
- Create: `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-面试深度剖析.md`

**Interfaces:**
- Consumes: the mapped legacy content and the new cross-links from the main chapter and guide docs
- Produces: dedicated homes for skew handling, error taxonomy, cloud-native operation, version capabilities, and interview questions

- [ ] **Step 1: Create all five files with the approved skeletons**

Create files with the `##` section structures from the approved design for:
- 数据倾斜治理
- 错误排查与兼容性
- 部署与云原生运行
- 版本演进与新能力
- 面试深度剖析
Expected: every remaining responsibility has a document home before content is moved.

- [ ] **Step 2: Migrate skew, error, and interview legacy blocks**

Copy and adapt content from:
- `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-性能优化.md:2228-2461`
- `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-性能优化.md:1898-2679`
- `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark.md:1825-1891`
Expected: skew handling, error catalogs, and interview Q&A are removed from their old mixed contexts.

- [ ] **Step 3: Migrate deployment and version content and add the missing modern topics**

Copy and adapt content from:
- `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark.md:469-578`
- `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark.md:1031-1056`
- `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark.md:1666-1811`
Then add fresh sections for:
- Spark Connect
- Kubernetes operation basics
- PVC / local shuffle storage idea
- Decommission
- Spark 4.x forward-looking notes
Expected: modern Spark capability coverage lands in dedicated docs rather than as loose notes.

- [ ] **Step 4: Verify that each new doc has a single clear role**

Run:
```bash
for f in \
  /Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-数据倾斜治理.md \
  /Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-错误排查与兼容性.md \
  /Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-部署与云原生运行.md \
  /Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-版本演进与新能力.md \
  /Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-面试深度剖析.md
 do
   echo "== $f =="
   rg -n '^## ' "$f"
 done
```
Expected: all five docs exist with the intended top-level scopes.

- [ ] **Step 5: Commit**

```bash
git add /Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-数据倾斜治理.md /Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-错误排查与兼容性.md /Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-部署与云原生运行.md /Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-版本演进与新能力.md /Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-面试深度剖析.md
git commit -m "docs: add remaining spark topic docs"
```

### Task 6: Tighten diagnostics doc and run cluster-wide verification

**Files:**
- Modify: `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-调试运维.md`
- Modify: `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark.md`
- Modify: `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-性能优化.md`
- Modify: all new Spark topic docs as needed for cross-link fixes

**Interfaces:**
- Consumes: the completed Spark doc cluster from Tasks 1-5
- Produces: a coherent doc set with valid links, clean boundaries, and no inherited-numbering leaks

- [ ] **Step 1: Update diagnostics doc to match the new role**

Ensure `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-调试运维.md` keeps diagnostics entry content only, adds a Kubernetes subsection, and points deeper error analysis to `/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark-错误排查与兼容性.md`.
Expected: the diagnostics doc no longer tries to be both a log manual and a full error encyclopedia.

- [ ] **Step 2: Validate all local relative links between Spark docs**

Run:
```bash
python3 - <<'PY'
from pathlib import Path
import re
root = Path('/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata')
files = sorted(root.glob('04-Spark*.md'))
pat = re.compile(r'\]\((\./[^)#]+\.md)\)')
missing = []
for f in files:
    text = f.read_text()
    for rel in pat.findall(text):
        target = (f.parent / rel[2:]).resolve()
        if not target.exists():
            missing.append((str(f), rel))
if missing:
    for item in missing:
        print('MISSING', item[0], item[1])
    raise SystemExit(1)
print('All Spark relative links resolve.')
PY
```
Expected: `All Spark relative links resolve.`

- [ ] **Step 3: Validate that standalone docs no longer use inherited numbering**

Run:
```bash
python3 - <<'PY'
from pathlib import Path
import re
root = Path('/Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata')
for f in sorted(root.glob('04-Spark*.md')):
    if f.name == '04-Spark.md':
        continue
    text = f.read_text()
    bad = re.findall(r'^(##|###)\s+(六、|七、|八、|6\.|7\.|8\.)', text, re.M)
    if bad:
        print('BAD NUMBERING', f)
        raise SystemExit(1)
print('No inherited numbering found in standalone Spark docs.')
PY
```
Expected: `No inherited numbering found in standalone Spark docs.`

- [ ] **Step 4: Validate scope coverage across the cluster**

Run:
```bash
rg -n 'DPP|动态分区裁剪' /Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark*.md
rg -n 'REBALANCE' /Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark*.md
rg -n 'Spark Connect' /Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark*.md
rg -n 'Kubernetes|K8s' /Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark*.md
rg -n 'Decommission|decommission' /Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark*.md
```
Expected: each required modern topic appears somewhere appropriate in the Spark doc cluster.

- [ ] **Step 5: Commit**

```bash
git add /Users/lipingjiang/Codes/Full-Stack-Knowledge-Book/part6-bigdata/04-Spark*.md
git commit -m "docs: finalize spark document cluster refactor"
```
