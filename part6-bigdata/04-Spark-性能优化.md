# 6.4 Spark 性能优化

> 本文是 [Spark 主文档](./04-Spark.md) 的专题子文档，深入展开性能优化的各个方向：减少数据量、优化计算逻辑、调整计算资源、Spark UI 分析、数据倾斜修复、Shuffle 底层机制与调优。

---

## 六、性能优化要点

Spark ETL 任务优化可以从三个维度系统思考：**减少数据量**（让引擎处理更少的数据）、**优化计算逻辑**（让同样的数据处理得更快）、**调整计算资源**（给引擎更合适的硬件配置）。下面按这三个维度展开。

### 6.1 减少数据量

#### 输入数据量——分区裁剪与列裁剪

分区裁剪（Partition Pruning）是大数据查询的第一道优化——让 Spark 只读需要的分区目录，跳过其他分区。列裁剪是第二道——只读 SELECT 中出现的列（ORC/Parquet 列式存储下，少读一列就少一列的 IO）。

但分区裁剪在实际开发中很容易失效，以下是六种常见的失效场景：

```
分区裁剪失效六大场景：

① 函数包裹分区字段：WHERE to_date(dt) = '2024-06-29'
   → 修复：WHERE dt = '2024-06-29'（直接字面量比较）

② 隐式类型转换：WHERE dt = 20240629（数字 vs 字符串）
   → 修复：WHERE dt = '20240629'（加引号保持类型一致）

③ 分区条件写在 JOIN ON 而非 WHERE：
   → 修复：将分区条件移到 WHERE 子句

④ OR 条件导致裁剪失效：
   → 修复：拆分为 UNION ALL，各子查询带分区条件

⑤ 视图穿透失效：查询视图时，底层物理表的分区字段未透传
   → 修复：直接查物理表，显式加分区过滤条件

⑥ 谓词下推失效：过滤条件在外层子查询，内层未下推
   → 修复：将过滤条件移入子查询内部
```

验证分区裁剪是否生效：在 Spark UI 的 SQL 详情页查看 `PartitionFilters` 字段——有值且非空则生效，空列表或缺失则失效。也可以用 `EXPLAIN EXTENDED <SQL>` 查看物理执行计划中 `Filter` 算子是否在 `FileScan` 节点内部。

#### 谓词下推在外连接中的行为——Catalyst 不能自动优化的盲区

后端开发者习惯了"把过滤条件写在 WHERE 里就行，优化器会处理"，但外连接下这个直觉是错的。LEFT JOIN 场景下，过滤条件写在不同位置，Catalyst 的行为完全不同：

| 条件位置 | LEFT JOIN 中左表条件 | LEFT JOIN 中右表条件 |
|---------|---------------------|---------------------|
| 写在 **JOIN ON** 中 | 不下推（ON 中只影响匹配，不影响左表输出） | **下推** ✓ |
| 写在 **WHERE** 中 | **下推** ✓ | **不下推** ✗（会把 LEFT 变 INNER） |

```sql
-- ✗ 错误写法：右表过滤条件在 WHERE 中，Catalyst 不会下推
-- Spark 会先 LEFT JOIN 全量数据，再过滤 → 无效数据参与了 Shuffle
SELECT a.*, b.value
FROM big_table a
LEFT JOIN small_table b ON a.id = b.id
WHERE b.dt = '2024-06-29';

-- ✓ 正确写法：右表过滤条件移到子查询内部，读数据时就过滤
SELECT a.*, b.value
FROM big_table a
LEFT JOIN (
    SELECT * FROM small_table WHERE dt = '2024-06-29'
) b ON a.id = b.id;
```

> **经验法则**：INNER JOIN 可以放心把条件写在 WHERE 里，Catalyst 能处理。但 LEFT/RIGHT JOIN 一定要手动检查：右表（或 LEFT JOIN 的非保留表）的过滤条件，要写在子查询内部或 JOIN ON 中，别指望 Catalyst 替你下推。

#### 中间数据量——数据膨胀问题

Spark 任务中一个隐蔽的性能杀手是**中间数据膨胀**——原始数据量不大，但经过某些操作后数据量暴增几十甚至上百倍。

两个最常见的膨胀源：

**count(distinct) 膨胀**：当 SQL 中有多个 `count(distinct col)` 时，Spark 会通过 Expand 算子把每行数据复制 N 份（N = distinct 字段数），然后对每份数据分别做去重。3 个 count(distinct) 意味着数据量膨胀 3 倍，6 个就是 6 倍。

```sql
-- 膨胀方案（3 个 count distinct → 数据量膨胀 3 倍）
SELECT dt,
       count(distinct customer_id),
       count(distinct sku_id),
       count(distinct concat(customer_id, '-', sku_id))
FROM orders GROUP BY dt;

-- 优化方案一：size + collect_set 避免膨胀
SELECT dt,
       size(collect_set(customer_id)),
       size(collect_set(sku_id)),
       size(collect_set(concat(customer_id, '-', sku_id)))
FROM orders GROUP BY dt;
-- 注意：collect_set 会把所有不同值收集到内存，数据量极大时可能 OOM

-- 优化方案二：拆分为多个小查询 UNION ALL
SELECT dt, count(distinct customer_id), count(distinct sku_id), null
FROM orders GROUP BY dt
UNION ALL
SELECT dt, null, null, count(distinct concat(customer_id, '-', sku_id))
FROM orders GROUP BY dt;
-- 虽然多扫一次表，但每次膨胀倍数更小，单 Task 压力大幅降低
```

**grouping sets 膨胀**：`GROUP BY GROUPING SETS ((a), (a,b), (a,b,c))` 等价于对数据做 3 次不同粒度的聚合再 UNION，数据量随组合数线性膨胀。

优化手段：在 grouping sets 之前先做一次预聚合，只保留需要的维度和指标字段，输入最少的数据进入膨胀阶段。

### 6.2 优化计算逻辑

#### Spark JOIN 策略全景——五种物理 JOIN

Spark SQL 的 Catalyst 优化器在生成物理执行计划时，会根据数据大小、JOIN 类型、是否有 Hint 等因素，从五种物理 JOIN 策略中选择一种。理解这五种策略的触发条件和适用场景，是优化 JOIN 性能的基础。

| 策略 | 是否 Shuffle | 是否排序 | 适用场景 | 触发条件 |
|------|------------|---------|---------|---------|
| **BroadcastHashJoin** | 否（小表广播） | 否 | 大表 JOIN 小表 | 小表 < `autoBroadcastJoinThreshold`（默认 10MB）或使用 `/*+ BROADCAST(t) */` |
| **SortMergeJoin** | 是 | 是 | 大表 JOIN 大表（等值 JOIN） | 两表都超过广播阈值，JOIN key 可排序（默认策略） |
| **ShuffleHashJoin** | 是 | 否 | 大表 JOIN 中等表（等值 JOIN） | 需设置 `spark.sql.join.preferSortMergeJoin=false`，或使用 `/*+ SHUFFLE_HASH(t) */` |
| **CartesianJoin** | 是 | 否 | 笛卡尔积（无 JOIN 条件） | CROSS JOIN 或 INNER JOIN 无 ON 条件 |
| **BroadcastNestedLoopJoin** | 否（小表广播） | 否 | 非等值 JOIN（如 `a.price > b.min_price`） | 小表可广播 + 非等值条件 |

```
Spark 选择 JOIN 策略的决策树（简化版）：

  有 JOIN Hint？
  ├── /*+ BROADCAST(t) */ → BroadcastHashJoin
  ├── /*+ SHUFFLE_HASH(t) */ → ShuffleHashJoin
  ├── /*+ MERGE(t) */ → SortMergeJoin
  └── 无 Hint → 自动判断 ↓

  是等值 JOIN？
  ├── 是 → 小表 < 广播阈值？
  │         ├── 是 → BroadcastHashJoin
  │         └── 否 → SortMergeJoin（默认）
  └── 否 → 小表可广播？
            ├── 是 → BroadcastNestedLoopJoin
            └── 否 → CartesianJoin（慎用，性能极差）
```

**BroadcastHashJoin** 是性能最优的策略——小表被序列化后广播到每个 Executor 内存中，构建 Hash 表，大表流式探测。完全避免 Shuffle，适合维度表 JOIN 事实表的星型模型场景。

```sql
-- 使用 MAPJOIN hint 强制广播（MAPJOIN / BROADCAST / BROADCASTJOIN 三个 hint 等价）
SELECT /*+ BROADCAST(dim) */
    fact.order_id, fact.amount, dim.category_name
FROM fact_order fact
JOIN dim_product dim ON fact.prod_key = dim.prod_key;
```

广播表大小的判断标准：Spark UI 执行计划中各 Scan 节点的实际大小（注意 ORC/Parquet 解压后会膨胀 2-3 倍），小于 60MB 可直接广播，60-200MB 需要评估 Driver 内存是否充足，超过 200MB 不建议广播。

也可以通过 AQE 动态广播：`SET spark.sql.adaptive.autoBroadcastJoinThreshold=100MB`，让 Spark 在运行时根据实际数据量自动决定是否广播。

**SortMergeJoin 和 ShuffleHashJoin 的深入对比**

这两种策略的第一步都是 Shuffle——按 JOIN key 重新分区，让相同 key 的数据落到同一个分区。区别在于 Shuffle 之后怎么在分区内做匹配。

**SortMergeJoin 为什么要排序？**

注意：Shuffle 和排序是**两个独立的步骤**，解决的是不同的问题，不要混淆——

- **Shuffle 解决的是"数据应该去哪个节点"**：通过哈希函数 `hash(JOIN key) % 分区数` 决定每条数据发往哪个下游分区。这和分库分表的路由规则（`user_id % 8` 决定去哪个库）一模一样，不需要排序。
- **排序解决的是"到达同一个分区后怎么高效匹配"**：Shuffle 完成后，一个下游分区里同时存在 A 表和 B 表中 JOIN key 相同的数据，但它们是无序混杂的。如果不排序，只能嵌套循环 O(N×M) 暴力匹配；排序之后，就能用双指针归并 O(N+M) 线性扫完。排序的代价是 O(N log N)，但换来的归并效率远高于暴力匹配。

所以排序不是为了决定"数据去哪个节点"（那是 Shuffle 的哈希函数干的事），而是数据**已经到达正确节点后**，为了让归并匹配能高效进行。

**关键细节：一个分区里有很多不同的 key。** Shuffle 只保证"相同 key 去同一个分区"，但一个分区里会包含**若干个不同的 key**——比如 `hash(3) % 4 = 3`、`hash(7) % 4 = 3`、`hash(15) % 4 = 3`，key=3、7、15 全部落到分区 3。Shuffle 后一个分区内的数据是这样的：

```
Shuffle 后分区 3 收到的原始数据（乱序，包含多个不同 key）：

A 侧到达顺序: id = [7, 3, 15, 3, 7]    ← 乱序，key 有 3、7、15 三种
B 侧到达顺序: id = [15, 7, 3, 7]       ← 乱序，key 有 3、7、15 三种

如果不排序，要找出匹配的行只能暴力嵌套循环：
  A 的每一行 × B 的每一行逐个比较 → O(N×M) = O(5×4) = 20 次比较

排序后：
A 侧: id = [3, 3, 7, 7, 15]            ← 有序，相同 key 聚在一起
B 侧: id = [3, 7, 7, 15]               ← 有序，相同 key 聚在一起

现在双指针就能"对齐扫描"了：
  两个指针同步前进，相同 key 的区间互相匹配 → O(N+M) = O(5+4) = 9 次比较
```

排序的核心作用就是**让两侧数据按 key 对齐**，这样双指针才能从小到大同步推进，遇到相同 key 就输出匹配，遇到不同就移动较小的那个指针。

```
SortMergeJoin 的完整执行过程（以 A JOIN B ON A.id = B.id 为例）：

① Shuffle（决定数据去哪个节点）：
   用 hash(id) % 分区数 计算每条数据的目标分区
   A 表和 B 表中 id 相同的行会被发往同一个下游分区
   但同一个分区也会收到 hash 值相同的其他 key（哈希碰撞 + 取模）
   注意：这一步只是"搬运"，不排序，数据到达时是乱序的

② Sort（在下游节点上排序）：
   每个下游分区收到数据后，在本地对 A 侧和 B 侧各自按 id 排序
   排序后，相同 key 的行聚集在一起，不同 key 按大小排列
   是的，排序发生在下游节点上，是纯本地操作，不涉及网络传输

③ Merge（双指针归并扫描）：
   两个指针分别指向 A 侧和 B 侧的开头，同步前进：
   - A 的 key < B 的 key → A 指针右移（A 当前行在 B 侧没有匹配）
   - A 的 key > B 的 key → B 指针右移（B 当前行在 A 侧没有匹配）
   - A 的 key = B 的 key → 匹配！输出结果，处理完相同 key 的区间后继续
   线性扫完，时间复杂度 O(N+M)

具体示例（分区 3 排序后的数据）：
A 侧: id = [3, 3, 7, 7, 15]     指针 pA →
B 侧: id = [3, 7, 7, 15]        指针 pB →

pA=3, pB=3 → 匹配！输出 (A的3, B的3)
            A 有两个 3，B 有一个 3 → 输出 2 行
pA=7, pB=7 → 匹配！A 有两个 7，B 有两个 7 → 输出 2×2=4 行
pA=15, pB=15 → 匹配！输出 1 行
扫描完毕，共 7 行输出，总比较次数 ≈ N+M = 9 次

关键优势：
  内存中只需要两个指针，不需要把任何一侧的数据全部加载到内存
  → 两张表都是 TB 级也能跑，只要磁盘够大
  → 排序阶段如果内存不够，Spark 会自动 Spill 到磁盘（外部排序）
```

**ShuffleHashJoin 为什么可以不排序？**

因为它用的是**哈希表**做匹配——把较小那侧的数据构建成一个 HashMap（key = JOIN key，value = 行数据），然后逐行扫描较大那侧，用 JOIN key 去 HashMap 里 O(1) 查找。不需要排序，因为哈希表本身就支持快速查找。

```
ShuffleHashJoin 的执行过程：

① Shuffle：A 和 B 各自按 id 重新分区（和 SortMergeJoin 相同）
② Build：对较小的一侧（假设 B）构建 HashMap
   HashMap = {2: row_B2, 3: row_B3, 5: row_B5, 6: row_B6, 8: row_B8}
③ Probe：逐行扫描较大的一侧（A），用 id 去 HashMap 查找
   A.id=1 → HashMap.get(1) → null → 不匹配
   A.id=3 → HashMap.get(3) → row_B3 → 匹配！输出
   A.id=5 → HashMap.get(5) → row_B5 → 匹配！输出
   ...

关键限制：Build 阶段需要把 B 侧整个分区的数据放进内存构建 HashMap
→ 如果 B 侧某个分区太大放不进内存 → OOM
→ 这就是为什么 ShuffleHashJoin 要求"小表的每个分区能放进内存"
```

**HashMap 是一口气全放进去吗？内存不够怎么办？**

注意——HashMap 里放的是**每个分区的数据**，不是整张表。Shuffle 之后数据被分散到 N 个分区，每个分区只持有较小侧的一部分：

```
小表 10GB，Shuffle 后 200 个分区
→ 每个分区的较小侧数据 ≈ 10GB / 200 = 50MB
→ 50MB 构建 HashMap 完全没问题

但如果数据倾斜：
→ 某个分区分到 80% 的数据 = 8GB
→ 这个分区的 HashMap 构建可能 OOM
```

内存不够时 Spark 会 Spill 到磁盘（Grace Hash Join）——把 HashMap 拆成多块，先处理一部分，再处理下一部分。但这会让性能急剧下降——HashMap 的 O(1) 查找优势大打折扣，变成多次磁盘 I/O。所以 ShuffleHashJoin 的适用条件是：**每个分区的较小侧数据必须能舒适地放进内存**。

> **为什么 Spark 默认优先选 SortMergeJoin 而不是 ShuffleHashJoin？** SortMergeJoin 更"安全"——它只用两个指针做归并扫描，内存 O(1)，多大的表都能跑，排序内存不够时 Spill 到磁盘也只是降速不会 OOM。ShuffleHashJoin 虽然省了排序开销，但有 OOM 风险。Spark 宁可慢一点也不要 OOM，所以默认选 SortMergeJoin。只有你明确知道小侧能放进内存时，才用 `/*+ SHUFFLE_HASH(t) */` 手动指定。

**不只是"排不排序"——完整的差异对比**

| 维度 | SortMergeJoin | ShuffleHashJoin |
|------|---------------|-----------------|
| 分区内匹配算法 | 排序 + 双指针归并 | 构建 HashMap + 探测 |
| 时间复杂度 | O(N log N + M log M)（排序占大头） | O(N + M)（HashMap 构建 + 探测） |
| 内存要求 | 极低（只需两个指针） | 需要把小表分区放进内存 |
| 大数据安全性 | 非常安全，两侧都可以 Spill 到磁盘 | 有 OOM 风险（分区 > 内存时） |
| 排序开销 | 有，且通常是性能瓶颈 | 无 |
| 适合场景 | 两张大表（默认策略，最稳定） | 一大一中（中表分区能放进内存） |
| Spark 默认 | 是（`preferSortMergeJoin=true`） | 否（需手动开启或 Hint） |

**两者有实现上的依赖关系吗？**

没有依赖关系，它们是两种独立的 JOIN 实现，共享的只是 Shuffle 这一步（按 JOIN key 重新分区）。Shuffle 是 Spark 的通用基础设施，和具体的 JOIN 策略无关。你可以这样理解：Shuffle 是"把数据送到正确的分区"（相当于快递分拣），SortMergeJoin 和 ShuffleHashJoin 是"在分区内怎么找匹配"（相当于收到快递后怎么查件），两者在"查件"的方法上完全独立。

实际上，这两种算法在数据库领域早于 Spark 存在——SortMergeJoin 来自传统关系型数据库的归并连接（Oracle/PostgreSQL 都有），HashJoin 来自哈希连接（MySQL 8.0+ 才支持）。Spark 只是把它们搬到了分布式场景下，前面加了一步 Shuffle。

> **什么时候考虑从 SortMergeJoin 切换到 ShuffleHashJoin？** 当你在 Spark UI 中发现 SortMergeJoin Stage 的排序耗时占比很高（比如排序花了 5 分钟，归并只花了 30 秒），而且 JOIN 的一侧表不算太大（Shuffle 后每个分区 < 可用内存），就可以尝试用 `/*+ SHUFFLE_HASH(small_side) */` 切换，跳过排序直接走 HashMap 匹配。

```sql
-- 用 Hint 指定 ShuffleHashJoin
SELECT /*+ SHUFFLE_HASH(b) */
    a.*, b.value
FROM big_table a JOIN medium_table b ON a.id = b.id;
```

**为什么 Hint 需要手动指定 small_side？Shuffle 完不就知道谁大谁小了？**

因为执行计划是在 Shuffle **之前**确定的，不是之后。Spark 的执行流程是先解析 SQL → Catalyst 优化（选 JOIN 策略）→ 生成物理计划 → 执行（Shuffle + JOIN）。到了 Shuffle 的时候，JOIN 策略早就定好了，来不及改：

```
时间线：
SQL 解析 → Catalyst 优化器选 JOIN 策略 → 生成物理计划 → 开始执行
                    ↑                                    ↑
              这里就要决定用哪种 JOIN          到这里才知道实际数据量
              但此时只有表统计信息（可能不准）    但策略已经定了，改不了

Catalyst 的决策依据：
  表的统计信息（ANALYZE 命令收集的行数、大小）
  如果统计信息不准（没 ANALYZE、或过滤后数据量变化大）
  → Catalyst 可能误判哪侧小，选错策略
  → 你用 hint 强制指定，覆盖 Catalyst 的判断
```

Spark 3.x 的 AQE 部分解决了这个问题——可以在 Shuffle 后根据真实数据量动态切换 JOIN 策略（比如从 SortMergeJoin 切到 BroadcastHashJoin）。但 ShuffleHashJoin 的选择仍然依赖编译期判断，AQE 不会自动切到 ShuffleHashJoin，所以需要你手动指定 Hint。

**CartesianJoin 怎么实现？为什么危险？**

CartesianJoin 就是嵌套循环——左侧的每一行和右侧的每一行配对，产生 M × N 行结果，没有任何优化空间：

```
表 A：3 行          表 B：4 行
  a1                 b1
  a2                 b2
  a3                 b3
                     b4

CartesianJoin 结果：3 × 4 = 12 行
  (a1,b1) (a1,b2) (a1,b3) (a1,b4)
  (a2,b1) (a2,b2) (a2,b3) (a2,b4)
  (a3,b1) (a3,b2) (a3,b3) (a3,b4)

两张各 100 万行的表 → 1 万亿行结果 → 几乎必然 OOM 或跑数小时
```

危险在于结果集是乘法关系。它只在 `CROSS JOIN`（笛卡尔积）或 `INNER JOIN` 没有 `ON` 条件时触发——正常业务几乎不应出现，通常是 SQL 写漏了 JOIN 条件的 bug。

**BroadcastNestedLoopJoin** 广播小表后对每行大表遍历小表匹配，适合非等值条件（如范围 JOIN `a.start <= b.ts AND b.ts < a.end`）且小表可广播的场景。

> **经验法则**：90% 的 ETL 场景只需关注两种策略——小表走 BroadcastHashJoin，大表走 SortMergeJoin。如果发现 SortMergeJoin 的排序阶段成为瓶颈（Spark UI 中 Stage 的排序耗时占比高），且其中一侧表不算太大，可以尝试切换为 ShuffleHashJoin。非等值 JOIN 尽量控制其中一侧为小表，走 BroadcastNestedLoopJoin 避免笛卡尔积。

#### CTE / WITH 子查询的陷阱——不会被复用

CTE（Common Table Expression，通用表表达式）就是 SQL 中的 `WITH name AS (SELECT ...)` 语法。后端开发者的直觉是 CTE 像变量一样"算一次，到处引用"，但在 Spark 中 **CTE 每次引用都会重新计算**。如果同一个 CTE 被引用 3 次，底层数据会被扫描 3 次。

```sql
-- 问题：cte_result 被引用 2 次 = 底层表扫描 2 次
WITH cte_result AS (
    SELECT user_id, SUM(amount) as total FROM orders WHERE dt = '2024-06-29' GROUP BY user_id
)
SELECT * FROM cte_result WHERE total > 1000
UNION ALL
SELECT * FROM cte_result WHERE total <= 100;

-- 修复方案一：数据量小，CACHE 到内存
CACHE TABLE cte_result AS
SELECT user_id, SUM(amount) as total FROM orders WHERE dt = '2024-06-29' GROUP BY user_id;

-- 修复方案二：数据量大，落临时表
CREATE TABLE tmp_cte_result AS
SELECT user_id, SUM(amount) as total FROM orders WHERE dt = '2024-06-29' GROUP BY user_id;
-- 后续步骤直接读 tmp_cte_result
```

#### 窗口函数——语法、分类与面试常考函数

窗口函数（Window Function）是 SQL 中"既要聚合又要保留明细"的利器。和 GROUP BY 不同，窗口函数**不会合并行**——每行都保留，同时附带一个在"窗口"内计算出的值。Spark SQL 从 **1.4 版本**开始完整支持 SQL 标准的窗口函数语法，后续版本持续增强（如 Spark 2.0 引入 Dataset API 的窗口支持，Spark 3.x 在 AQE 下优化窗口执行）。

**完整语法**

```sql
函数名(参数) OVER (
    [PARTITION BY 分区字段, ...]     -- 按哪个维度划分窗口（不合并行，不丢失行）
    [ORDER BY 排序字段 [ASC|DESC]]   -- 分区内按什么排序
    [窗口框架子句]                    -- 计算范围：哪些行参与计算
)
```

三个子句都是可选的：省略 PARTITION BY 则整张表为一个分区；省略 ORDER BY 则分区内不排序（聚合窗口用）；省略窗口框架则根据是否有 ORDER BY 使用不同的默认值（下文详述）。

**三类窗口函数**

| 类别 | 函数 | 用途 | 是否需要 ORDER BY |
|------|------|------|-----------------|
| **排名函数** | `ROW_NUMBER()` | 分区内行号，1,2,3,4（不跳号） | 必须 |
| | `RANK()` | 排名（并列跳号），1,1,3,4 | 必须 |
| | `DENSE_RANK()` | 排名（并列不跳号），1,1,2,3 | 必须 |
| | `NTILE(n)` | 分区内分成 n 等份，返回桶号 | 必须 |
| **聚合函数** | `SUM()` / `COUNT()` / `AVG()` / `MAX()` / `MIN()` | 在窗口内做聚合 | 可选（有无 ORDER BY 语义不同） |
| **偏移函数** | `LAG(col, n, default)` | 往前数第 n 行的值（LAG = 滞后，看过去） | 必须 |
| | `LEAD(col, n, default)` | 往后数第 n 行的值（LEAD = 领先，看未来） | 必须 |
| | `FIRST_VALUE(col)` | 窗口内第一行的值 | 通常需要 |
| | `LAST_VALUE(col)` | 窗口内最后一行的值 | 通常需要 |

```sql
-- 面试高频场景 1：每个部门薪资排名 TOP 3
SELECT * FROM (
    SELECT name, dept_id, salary,
           ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC) as rn
    FROM employees
) t WHERE rn <= 3;
-- PARTITION BY dept_id → 按 dept_id 划分窗口（每个部门一个独立窗口）
-- ORDER BY salary DESC → 在每个窗口内按薪资降序排列
-- ROW_NUMBER() → 在每个窗口内从 1 开始编号，编号不跨窗口
-- 外层 WHERE rn <= 3 → 选出每个部门薪资前 3 名
--
-- 结果示例：
-- name     dept_id  salary  rn
-- 张三     1        15000   1     ← 部门 1 薪资最高
-- 李四     1        12000   2
-- 王五     1        10000   3
-- 赵六     1         8000   4     ← rn=4，被 WHERE 过滤掉
-- 孙七     2        20000   1     ← 部门 2 重新从 1 开始编号
-- 周八     2        18000   2
--
-- PARTITION BY vs WHERE vs GROUP BY 的区别：
-- WHERE dept_id = 1   → 过滤行，只留部门 1，其他部门数据丢掉
-- GROUP BY dept_id    → 聚合行，每个部门合并成一行，逐行信息丢失
-- PARTITION BY dept_id → 划窗口，所有行保留，每行多一个 rn 列
--
-- 原始数据（5 行）：
-- name   dept_id  salary
-- 张三   1        15000
-- 李四   1        12000
-- 王五   1        10000
-- 孙七   2        20000
-- 周八   2        18000
--
-- WHERE dept_id = 1 的结果（过滤，剩 3 行）：
-- 张三   1        15000
-- 李四   1        12000
-- 王五   1        10000
--
-- GROUP BY dept_id 的结果（聚合，剩 2 行，逐行信息丢失）：
-- dept_id  SUM(salary)
-- 1        37000
-- 2        38000
--
-- PARTITION BY dept_id + ROW_NUMBER() 的结果（划窗口，5 行全在，多了 rn 列）：
-- name   dept_id  salary  rn
-- 张三   1        15000   1
-- 李四   1        12000   2
-- 王五   1        10000   3
-- 孙七   2        20000   1     ← 编号重新开始
-- 周八   2        18000   2
--
-- 类比：WHERE 是"选人"——只留部门 1，其他人赶走。
--       GROUP BY 是"汇报"——每个部门派代表报总数，人员信息没了。
--       PARTITION BY 是"分组排队"——所有人都在，按部门分成两队，每队内部各自排名。
--
-- ROW_NUMBER 的内部实现（非每行独立计算，也非先算再 JOIN）：
-- ① Shuffle：按 PARTITION BY 的 dept_id 重新分区（相同部门去同一 Executor）
-- ② Sort：分区内按 (dept_id, salary DESC) 排序
-- ③ WindowExec：流式遍历排序后的行，维护一个计数器
--    - 每读一行，计数器 +1，作为 rn 附加到该行
--    - 检测到 dept_id 变了（新部门），计数器重置为 1
--    - 全程一次顺序扫描，没有 JOIN，没有回头查找
-- 源码依据：WindowExec.doExecute() 用 mapPartitions 逐行迭代，
-- ROW_NUMBER 对应 UnboundedPrecedingWindowFunctionFrame，
-- 框架范围为 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW

-- 面试高频场景 2：同比/环比——取上一期的值做对比
-- LAG 和 LEAD 都是纯英文单词，不是缩写：
--   LAG  = "滞后"（lag behind），看过去，取前面的行
--   LEAD = "领先"（lead the way），看未来，取后面的行
--   时间线：  ← LAG（往后看）    当前行    LEAD（往前看） →
--
-- LAG(col, n, default) 三个参数：
--   col     → 要取的列
--   n       → 偏移量（LAG(gmv, 1) = 上一条，LAG(gmv, 7) = 上上上...上 7 条）
--   default → 超出窗口边界时的默认返回值，只能填常量字面量，不能填列引用
--             LAG(salary, 1, 0)     → 第一行没有上一行，返回 0
--             LAG(name, 1, '无')     → 第一行返回字符串 '无'
--             LAG(salary, 1, NULL)   → 第一行返回 NULL
--             LAG(salary, 1)         → 不填 default，默认就是 NULL

SELECT dt, gmv,
       LAG(gmv, 1) OVER (ORDER BY dt) as prev_day_gmv,           -- 昨天的 GMV
       LAG(gmv, 7) OVER (ORDER BY dt) as prev_week_gmv,          -- 上周同一天的 GMV
       gmv - LAG(gmv, 1) OVER (ORDER BY dt) as day_over_day      -- 日环比增量
FROM daily_gmv;

-- 面试高频场景 3：RANK vs DENSE_RANK vs ROW_NUMBER 的区别
-- 薪资：8000, 8000, 7000, 6000
-- ROW_NUMBER: 1, 2, 3, 4（强制不重复，常用于去重取一条）
-- RANK:       1, 1, 3, 4（并列后跳号，常用于竞赛排名）
-- DENSE_RANK: 1, 1, 2, 3（并列后不跳号，常用于"排名第几"的业务语义）
```

**窗口框架（Frame）——ROWS BETWEEN 详解**

窗口框架决定"当前行往前往后看多远"。这是理解窗口函数行为的关键，也是面试常考的细节。

```
窗口框架语法：
  ROWS BETWEEN <起点> AND <终点>

五种边界关键词：
  UNBOUNDED PRECEDING  = 分区的第一行（从最开头开始）
  n PRECEDING          = 当前行往前 n 行
  CURRENT ROW          = 当前行
  n FOLLOWING          = 当前行往后 n 行
  UNBOUNDED FOLLOWING  = 分区的最后一行（到最末尾）

常见组合（每种搭配一个实际应用场景）：

① ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW → 从头累计到当前行
   场景：用户累计消费金额
   SELECT user_id, order_date, amount,
          SUM(amount) OVER (PARTITION BY user_id ORDER BY order_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumulative_spent
   FROM orders;
   -- 每个用户按下单时间排序，逐笔累加消费金额
   -- user_id=1 的结果：100 → 100+200=300 → 300+50=350 → ...

② ROWS BETWEEN 6 PRECEDING AND CURRENT ROW → 最近 7 行（含当前行）
   场景：7 日移动平均（股票、DAU、GMV）
   SELECT dt, dau,
          AVG(dau) OVER (ORDER BY dt
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as avg_dau_7d
   FROM daily_active_users;
   -- 每天的 DAU 和它前面 6 天的 DAU 取平均，平滑波动看趋势
   -- dt=01-07 的结果：(dau_01+dau_02+...+dau_07) / 7

③ ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING → 前一行 + 当前行 + 后一行
   场景：3 天滑动窗口检测异常波动
   SELECT dt, gmv,
          gmv - AVG(gmv) OVER (ORDER BY dt
            ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as deviation
   FROM daily_gmv;
   -- 当天 GMV 与前后各 1 天的平均 GMV 的差值，差值过大则可能是异常
   -- 如果当天 GMV 突然暴跌/暴涨，deviation 会明显偏离 0

④ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING → 整个分区（全量）
   场景：每个员工薪资占部门总薪资的比例
   SELECT name, dept_id, salary,
          salary / SUM(salary) OVER (PARTITION BY dept_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as pct_of_dept
   FROM employees;
   -- 分母是整个部门的总薪资，分子是当前员工薪资
   -- 张三 3000 / 10000 = 30%，李四 5000 / 10000 = 50%
   -- 注意：这里如果不写 ROWS BETWEEN，且没有 ORDER BY，默认就是这个全量框架
```

**ROWS vs RANGE 的区别——Frame 的两种模式**

窗口框架（Frame）确实有两种模式：`ROWS BETWEEN` 和 `RANGE BETWEEN`。两者都是定义"当前行的窗口包含哪些行"，但计算边界的方式不同：

```
ROWS：按物理行数计算边界（第几行到第几行），与值无关
RANGE：按 ORDER BY 字段的值范围计算边界，与行数无关

具体例子（按 salary 排序的数据）：
name   salary
王五   2000
张三   3000
李四   3000     ← 与张三 salary 相同
赵六   5000

当前行 = 张三(salary=3000) 时：

ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
  → 看物理位置：王五(1行前) + 张三(当前) + 李四(1行后) = 3 行
  → 参与计算的行：王五, 张三, 李四

RANGE BETWEEN 1000 PRECEDING AND 1000 FOLLOWING
  → 看 salary 值范围：salary 在 [3000-1000, 3000+1000] = [2000, 4000] 之间
  → 参与计算的行：王五(2000)✓ 张三(3000)✓ 李四(3000)✓ 赵六(5000)✗
  → 注意李四也包含进来了，因为它的 salary=3000 在 [2000,4000] 范围内
  → 这里李四不是按"第几行"选的，而是按"值在不在范围内"选的
```

| 维度 | ROWS | RANGE |
|------|------|-------|
| **边界依据** | 物理行数（第几行） | ORDER BY 字段的值范围 |
| **窗口行数** | 固定（就是 n 行） | 不固定（取决于有多少行的值落在范围内） |
| **是否依赖 ORDER BY** | 不依赖（按位置即可） | **必须依赖**（需要 ORDER BY 字段的值来计算范围） |
| **行为可预测性** | 高（行数确定） | 低（行数随数据分布变化） |
| **典型用途** | 移动平均、累计求和、Top-N | 按值范围聚合（如价格 ±10% 内的订单） |
| **实际开发频率** | 常用（90% 场景） | 少用（特殊场景） |

> **实际开发中 ROWS 用得更多**，因为行为可预测——你写 `ROWS BETWEEN 6 PRECEDING AND CURRENT ROW` 就一定是 7 行参与计算。RANGE 的行数不确定，容易出性能问题（如果某个值有大量重复行，窗口会暴增）。另外，前面说的"有 ORDER BY 时默认框架是 `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`"——这也是为什么默认行为对相同值会"一起算"而非"逐行算"，如果你的排序字段有重复值，ROWS 和 RANGE 的默认结果会不同。如果不确定，**显式写 ROWS BETWEEN 最安全**。

**默认框架规则**（不写 ROWS BETWEEN 时 Spark 自动使用的框架）：

```
有 ORDER BY 时：默认 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  → 相当于"从分区开头累计到当前行"，SUM 变成累计求和

无 ORDER BY 时：默认 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  → 相当于"整个分区"，SUM 变成分区内总和
```

这就是为什么同样是 `SUM() OVER(PARTITION BY ...)`，加不加 ORDER BY 结果完全不同的原因。具体例子：

```
原始数据（部门 1 的 3 个员工）：
name   dept_id  salary
张三   1        3000
李四   1        5000
王五   1        2000

① SUM(salary) OVER (PARTITION BY dept_id)  — 不加 ORDER BY
   → 默认框架：ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
   → 整个分区参与计算，每行的结果都是同一个值（部门总和）

   name   dept_id  salary  sum
   张三   1        3000    10000   ← 3000+5000+2000
   李四   1        5000    10000   ← 3000+5000+2000（和上面一样）
   王五   1        2000    10000   ← 3000+5000+2000（和上面一样）

② SUM(salary) OVER (PARTITION BY dept_id ORDER BY salary)  — 加 ORDER BY
   → 默认框架：RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
   → 从分区第一行累计到当前行，每行的结果不同（累计求和）

   name   dept_id  salary  running_sum
   王五   1        2000    2000        ← 按 salary 排序后第一行，累计 = 2000
   张三   1        3000    5000        ← 累计 = 2000 + 3000
   李四   1        5000    10000       ← 累计 = 2000 + 3000 + 5000

   注意：加了 ORDER BY 后数据会按 salary 排序，且每行的 SUM 值不同（逐行累加）
```

> **一句话记忆**：不加 ORDER BY，聚合函数对整个分区算一遍（每行结果相同）；加了 ORDER BY，聚合函数变成从第一行累计到当前行（每行结果不同）。如果你要的是"部门总薪资"而不是"累计薪资"，就不要加 ORDER BY。

#### 窗口函数优化

理解了窗口函数的语法和框架之后，再来看优化手段。窗口函数是 Spill 和倾斜的高发区——因为同一个 PARTITION BY key 下的所有行必须集中到同一个 Task 处理。以下逐项说明优化手段，每个都附带 SQL 对比。

**① 去掉不必要的 ORDER BY**

纯聚合窗口（如"算每个部门的总薪资"）不需要排序，去掉 ORDER BY 可省去分区内 O(n log n) 的排序开销。

```sql
-- ✗ 不必要的 ORDER BY：只是求部门总薪资，不需要排序
SELECT name, dept_id, salary,
       SUM(salary) OVER (PARTITION BY dept_id ORDER BY salary) as dept_total
FROM employees;
-- 注意：加了 ORDER BY 后 SUM 变成"累计求和"（默认 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW）

-- ✓ 去掉 ORDER BY：SUM 变成整个分区的总和，且不需要排序
SELECT name, dept_id, salary,
       SUM(salary) OVER (PARTITION BY dept_id) as dept_total
FROM employees;
```

**② 聚合 + JOIN 替代窗口函数**

对无 ORDER BY 的分组聚合窗口，先 GROUP BY 压缩数据量，再 Broadcast JOIN 回原表，整体比窗口函数更轻量——因为窗口函数要求整个分区的数据进入同一个 Task，而 GROUP BY + JOIN 可以让聚合阶段的数据量大幅缩小。

```sql
-- ✗ 窗口函数方案：每个员工行都带着整个部门的聚合结果
-- 如果某个 dept_id 有 500 万行，这 500 万行全在同一个 Task 里
SELECT name, dept_id, salary,
       SUM(salary) OVER (PARTITION BY dept_id) as dept_total,
       COUNT(*) OVER (PARTITION BY dept_id) as dept_count
FROM employees;

-- ✓ 聚合+JOIN 方案：先 GROUP BY 压缩到每个部门一行，再广播回去
SELECT e.name, e.dept_id, e.salary, d.dept_total, d.dept_count
FROM employees e
JOIN /*+ BROADCAST(d) */ (
    SELECT dept_id, SUM(salary) as dept_total, COUNT(*) as dept_count
    FROM employees
    GROUP BY dept_id
) d ON e.dept_id = d.dept_id;
-- 部门维度表通常很小（几百行），广播后零 Shuffle
```

**③ 检查 PARTITION BY key 热点**

某个 key 对应百万行时，这百万行全部集中到一个 Task 处理——其他 Task 可能几千行就跑完了，但这个 Task 卡住整个 Stage。

```sql
-- 诊断：查看 PARTITION BY key 的数据分布
SELECT dept_id, COUNT(*) as row_count
FROM employees
GROUP BY dept_id
ORDER BY row_count DESC
LIMIT 20;
-- 如果 TOP 1 的 row_count 远超中位数（比如 100 倍以上），就存在热点
```

**④ 显式指定 ROWS BETWEEN 范围**

不指定范围时，有 ORDER BY 的聚合窗口默认使用 `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`（从分区开头累计到当前行），内存消耗随数据量线性增长——处理到分区最后一行时，需要持有从第一行到当前行的所有中间状态。如果业务只需要"最近 N 行"的滑动计算，显式指定 ROWS BETWEEN 可以把内存占用从 O(n) 降到 O(N)。

```sql
-- ✗ 默认范围：从分区第一行累计到当前行（UNBOUNDED PRECEDING → CURRENT ROW）
-- 随着行数增加，每一行要累加的范围越来越大
SELECT user_id, order_date, amount,
       SUM(amount) OVER (PARTITION BY user_id ORDER BY order_date) as running_total
FROM orders;

-- ✓ 显式指定：只看当前行和它前面 6 行（共 7 行），滑动窗口
-- ROWS BETWEEN 6 PRECEDING AND CURRENT ROW 含义：
--   6 PRECEDING = 往前数 6 行（物理行，不是 6 天）
--   CURRENT ROW = 当前行
--   合计 7 行参与计算，内存占用恒定
SELECT user_id, order_date, amount,
       SUM(amount) OVER (
           PARTITION BY user_id ORDER BY order_date
           ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
       ) as rolling_7row_total
FROM orders;
-- 注意：ROWS BETWEEN 按物理行数而非日期值计算。如果数据不是每天一行
-- （有缺失日期），"6 PRECEDING"不等于"最近 7 天"。
-- 需要严格按日期范围的场景，用 RANGE BETWEEN INTERVAL 7 DAYS PRECEDING AND CURRENT ROW
-- （Spark 3.0+ 支持 INTERVAL 语法）
```

**⑤ 多窗口规格拆分为独立 CTE**

不同的 OVER 规格（不同的 PARTITION BY + ORDER BY 组合）各自触发独立的 Shuffle。拆成独立 CTE 后，每次 Shuffle 携带的列更少，数据量更小。

**为什么拆分能减少 Shuffle 数据量？——链式执行 vs 独立执行**

关键在于理解单 SELECT 中多个窗口是**串行链式执行**的：第一个窗口的输出是第二个窗口的输入，第二个窗口不得不拖着第一个窗口的结果一起 Shuffle。

```
假设 orders 表有 5 列：order_id, user_id, category, order_date, amount

═══════════════════════════════════════════════════════════════════
单 SELECT 版本（两个窗口串行链式执行）
═══════════════════════════════════════════════════════════════════

输入：orders 表的全部 5 列
  order_id | user_id | category | order_date | amount
  001      | U1      | C1       | 2024-01-01 | 100
  002      | U1      | C2       | 2024-01-02 | 200
  003      | U2      | C1       | 2024-01-03 | 150

① Shuffle by user_id（携带全部 5 列）
   输入：order_id, user_id, category, order_date, amount（5 列）
   输出：order_id, user_id, category, order_date, amount（5 列，按 user_id 重新分区）

② WindowExec #1：按 user_id 分区，amount DESC 排序，算 RANK
   输入：上面 Shuffle 后的 5 列
   输出：order_id, user_id, category, order_date, amount, user_rank（6 列）
                                                    ↑ 新增的列

③ Shuffle by category（必须携带 6 列！）
   输入：order_id, user_id, category, order_date, amount, user_rank（6 列）
                                                         ↑ 被迫带着
   因为 WindowExec #2 的输入是 WindowExec #1 的输出
   → 第一个窗口的结果 user_rank 被"裹挟"进了第二次 Shuffle
   → 如果有 10 个窗口，最后一次 Shuffle 会拖着前面所有窗口的结果列

④ WindowExec #2：按 category 分区，amount DESC 排序，算 RANK
   输入：上面 6 列
   输出：order_id, user_id, category, order_date, amount, user_rank, category_rank（7 列）

⑤ 输出 7 列结果

═══════════════════════════════════════════════════════════════════
CTE 拆分版本（两个窗口各自独立执行，最后 JOIN）
═══════════════════════════════════════════════════════════════════

分支 A：user_ranked CTE
  ① 读 orders → 只取 3 列：user_id, order_id, amount
  ② Shuffle by user_id（只携带 3 列）
  ③ WindowExec：算 RANK
     输入：user_id, order_id, amount（3 列）
     输出：user_id, order_id, amount, user_rank（4 列）

分支 B：category_ranked CTE
  ① 读 orders → 只取 3 列：category, order_id, amount
  ② Shuffle by category（只携带 3 列）
  ③ WindowExec：算 RANK
     输入：category, order_id, amount（3 列）
     输出：category, order_id, amount, category_rank（4 列）

合并：JOIN 两个结果
  ④ Shuffle by order_id（JOIN user_ranked，携带 order_id, user_rank = 2 列）
  ⑤ Shuffle by order_id（JOIN category_ranked，携带 order_id, category_rank = 2 列）
  ⑥ 输出：order_id, user_id, ..., user_rank, category_rank

═══════════════════════════════════════════════════════════════════
对比
═══════════════════════════════════════════════════════════════════

                    单 SELECT 版本              CTE 拆分版本
窗口 Shuffle 次数    2 次                        2 次（一样）
窗口 Shuffle 列数    5 列 → 6 列（越滚越大）     3 列 → 3 列（各自独立）
JOIN Shuffle         无                          2 次（额外开销）
读表次数             1 次                        2 次（多读一次）
```

> **trade-off**：CTE 拆分不是无条件优化——它用"多读一次表 + 多两次 JOIN Shuffle"换"每次窗口 Shuffle 携带的列更少"。如果 orders 是宽表（几十列），列裁剪的收益远大于 JOIN 的开销，拆分划算。如果 orders 本身列就不多，或者数据量小，单 SELECT 反而更快。**本质是数据量 vs 次数的 trade-off**。

```sql
-- ✗ 单 SELECT：两个窗口串行链式，第二次 Shuffle 携带第一次的结果
SELECT user_id, order_date, amount, category,
       RANK() OVER (PARTITION BY user_id ORDER BY amount DESC) as user_rank,
       RANK() OVER (PARTITION BY category ORDER BY amount DESC) as category_rank
FROM orders;
-- 两次窗口 Shuffle，且第二次拖着 user_rank 一起 Shuffle

-- ✓ CTE 拆分：两个窗口各自独立，每次 Shuffle 只携带需要的列
WITH user_ranked AS (
    SELECT user_id, order_id,
           RANK() OVER (PARTITION BY user_id ORDER BY amount DESC) as user_rank
    FROM orders
),
category_ranked AS (
    SELECT order_id,
           RANK() OVER (PARTITION BY category ORDER BY amount DESC) as category_rank
    FROM orders
)
SELECT o.user_id, o.order_date, o.amount, u.user_rank, c.category_rank
FROM orders o
JOIN user_ranked u ON o.order_id = u.order_id
JOIN category_ranked c ON o.order_id = c.order_id;
-- 两次窗口 Shuffle（列少）+ 两次 JOIN Shuffle，适合宽表场景
```

#### reduceByKey vs groupByKey——从源码看本质区别

后端开发者可能觉得 reduceByKey 和 groupByKey 是两个独立的 API，但看 Spark 源码会发现：**所有 ByKey 聚合操作都收敛到同一个底层方法 `combineByKeyWithClassTag`**，它们的区别只是传入的三个函数参数不同。这三个函数控制了聚合的两个阶段：

```
三个函数参数的含义：

createCombiner(v)          → 分区内遇到第一个 key 时，怎么初始化聚合容器
mergeValue(buf, v)         → 分区内遇到后续相同 key 时，怎么把值合并进容器（Map 端 combine）
mergeCombiners(buf1, buf2) → 跨分区合并两个聚合容器（Reduce 端）

执行流程：
  分区 1 内：  v1 → createCombiner(v1) → buf1 → mergeValue(buf1, v2) → buf1'
  分区 2 内：  v3 → createCombiner(v3) → buf2 → mergeValue(buf2, v4) → buf2'
  Shuffle + Reduce：  mergeCombiners(buf1', buf2') → 最终结果
```

五个 API 按灵活度从低到高排列：

| API | createCombiner | mergeValue（分区内） | mergeCombiners（跨分区） | 灵活度 |
|-----|---------------|-------------------|----------------------|--------|
| **groupByKey** | 固定：创建列表 | 固定：追加到列表 | 固定：合并列表 | 最低（零自定义） |
| **foldByKey** | 用零值 + func 初始化 | 自定义 func | **同 func** | 低 |
| **reduceByKey** | 固定：值本身是初始值 | 自定义 func | **同 func** | 低 |
| **aggregateByKey** | 用 zeroValue + seqOp 初始化 | 自定义 seqOp | 自定义 combOp（**可与 seqOp 不同**） | 高 |
| **combineByKey** | 自定义 | 自定义 | 自定义（**三个全自定义**） | 最高 |

```scala
// Spark 源码：PairRDDFunctions.scala（简化版）

// ① groupByKey：三个函数全固定，零自定义
def groupByKey(): RDD[(K, Iterable[V])] =
  combineByKeyWithClassTag[CompactBuffer[V]](
    (v: V) => CompactBuffer(v),      // createCombiner：创建列表（固定）
    (buf, v) => buf += v,            // mergeValue：往列表追加元素（固定）
    (buf1, buf2) => buf1 ++= buf2    // mergeCombiners：合并两个列表（固定）
  )
  // 你无法改变聚合方式——它永远是把值收集成列表
  // Map 端不做任何聚合，Shuffle 传全量数据

// ② foldByKey：自定义一个 func，分区内和跨分区用同一个 func
def foldByKey(zeroValue: V)(func: (V, V) => V): RDD[(K, V)] =
  combineByKeyWithClassTag[V](
    (v: V) => func(zeroValue, v),    // createCombiner：用零值 + func 初始化
    func,                             // mergeValue：分区内用 func 聚合
    func                              // mergeCombiners：跨分区也用 func（和分区内相同）
  )
  // 比 groupByKey 灵活：可以自定义聚合逻辑（如 _ + _）
  // 但分区内和跨分区必须是同一种操作

// ③ reduceByKey：自定义一个 func，分区内和跨分区用同一个 func
def reduceByKey(func: (V, V) => V): RDD[(K, V)] =
  combineByKeyWithClassTag[V](
    (v: V) => v,                      // createCombiner：值本身就是初始值（不需要零值）
    func,                             // mergeValue：分区内用 func 聚合
    func                              // mergeCombiners：跨分区也用 func（和分区内相同）
  )
  // 和 foldByKey 的区别：不需要零值，直接用第一个值做初始值
  // 灵活度和 foldByKey 一样，只是少了零值参数

// ④ aggregateByKey：分区内和跨分区可以用不同的 func
def aggregateByKey[U](zeroValue: U)(seqOp: (U, V) => U, combOp: (U, U) => U): RDD[(K, U)] =
  combineByKeyWithClassTag[U](
    (v: V) => seqOp(zeroValue, v),    // createCombiner：用零值 + seqOp 初始化
    seqOp,                             // mergeValue：分区内用 seqOp 聚合
    combOp                             // mergeCombiners：跨分区用 combOp（可以和 seqOp 不同！）
  )
  // 比 reduceByKey 灵活：分区内和跨分区可以用不同的聚合逻辑
  // 典型场景：求平均值——分区内用 seqOp 做 (sum, count)，跨分区用 combOp 合并后除

// ⑤ combineByKey：三个函数全自定义
def combineByKey[C](
    createCombiner: V => C,            // 自定义初始值逻辑
    mergeValue: (C, V) => C,           // 自定义分区内聚合
    mergeCombiners: (C, C) => C        // 自定义跨分区聚合
  ): RDD[(K, C)]
  // 最灵活：三个函数各自独立定义
  // aggregateByKey 其实就是 combineByKey 的 createCombiner = seqOp(zeroValue, v) 的特例
```

**关键区别在于 `mergeValue`（Map 端 combine 时做什么）**：

```
reduceByKey 的 mergeValue = func（比如 _ + _）
  → Map 端每个分区内，相同 key 的值直接聚合成一个数
  → Shuffle 时只传聚合后的结果，数据量大幅减少

groupByKey 的 mergeValue = list.append(value)
  → Map 端每个分区内，相同 key 的值只是追加到列表里
  → Shuffle 时列表中的每个元素都要传，数据量没有减少
```

```
场景：统计每个单词出现次数，数据分布在 3 个分区

reduceByKey(_ + _)：
  分区 1：(hello,1)(hello,1)(world,1) → combine → (hello,2)(world,1)
  分区 2：(hello,1)(spark,1)         → combine → (hello,1)(spark,1)
  分区 3：(hello,1)(world,1)         → combine → (hello,1)(world,1)
  Shuffle 传输：6 条记录（已聚合）
  Reduce 端：(hello, 2+1+1=4)(world, 1+1=2)(spark, 1)

groupByKey() + mapValues(_.sum)：
  分区 1：(hello,1)(hello,1)(world,1) → 追加 → (hello,[1,1])(world,[1])
  分区 2：(hello,1)(spark,1)         → 追加 → (hello,[1])(spark,[1])
  分区 3：(hello,1)(world,1)         → 追加 → (hello,[1])(world,[1])
  Shuffle 传输：8 条原始值（未聚合，全量过网络）
  Reduce 端：(hello, [1,1,1,1].sum=4)(world, [1,1].sum=2)(spark, [1].sum=1)
```

> **经验法则**：灵活度从低到高：groupByKey < foldByKey ≈ reduceByKey < aggregateByKey < combineByKey。但**灵活度越高不代表越好**——选你能用的最低灵活度。能用 `reduceByKey` 就别用 `aggregateByKey`，能用 `aggregateByKey` 就别用 `combineByKey`。唯一不要用的是 `groupByKey`——它灵活度最低且性能最差（Map 端不做任何聚合）。只有在确实需要保留原始值列表（如 `groupByKey().flatMap(...)`）时才用 groupByKey。

### 6.3 调整计算资源

#### 静态资源分配的痛点

spark-submit 时指定的资源是**静态固定**的——`--num-executors 10 --executor-memory 8G` 意味着整个 Application 生命周期内始终占用 10 个 Executor，不管实际需不需要。这带来两个问题：

```
问题 1：资源浪费
  Stage 0 需要 10 个 Executor 并行计算 → 充分利用
  Stage 1 只有 3 个分区 → 7 个 Executor 空闲摸鱼，但资源不释放
  → 其他任务想用这些资源也用不了（被占着）

问题 2：资源不足
  提交时指定了 10 个 Executor，但某个 Stage 数据量暴增需要 20 个
  → 没有更多的 Executor 可分配，只能慢慢排队跑
  → 而且不能在运行中追加资源（除非重新提交任务）
```

#### 动态资源分配（Dynamic Resource Allocation）

Spark 提供了 Dynamic Resource Allocation（动态资源分配），根据运行时负载自动增减 Executor 数量。默认关闭，需要手动开启：

```bash
# 开启动态资源分配
spark-submit \
  --conf spark.dynamicAllocation.enabled=true \           # 总开关
  --conf spark.dynamicAllocation.minExecutors=2 \         # 最少保留几个 Executor
  --conf spark.dynamicAllocation.maxExecutors=50 \        # 最多扩展到几个 Executor
  --conf spark.dynamicAllocation.initialExecutors=10 \    # 初始 Executor 数
  --conf spark.dynamicAllocation.executorIdleTimeout=60s \  # Executor 空闲超过 60s 就回收
  --conf spark.dynamicAllocation.schedulerBacklogTimeout=5s \  # Task 排队超过 5s 就申请新 Executor
  ...
```

**什么时候加资源？什么时候减资源？**

```
加资源的触发条件：
  Driver 调度器发现有 Task 处于 pending（排队等待）状态
  且等待时间超过 schedulerBacklogTimeout（默认 60s）
  → 向集群管理器申请新 Executor
  → 新增数量呈指数级增长（1 → 2 → 4 → 8），快速响应

减资源的触发条件：
  某个 Executor 空闲时间超过 executorIdleTimeout（默认 60s）
  → 该 Executor 被 kill 释放
  → 资源还给集群管理器供其他任务使用
```

**为什么需要 External Shuffle Service？**

动态分配回收 Executor 时面临一个问题：被回收的 Executor 上可能还有 Shuffle 数据，下游 Task 正等着拉这些数据。如果直接 kill Executor，Shuffle 文件丢失，下游 Task 就会失败。

```
没有 External Shuffle Service：
  Executor 2 被回收 → Executor 2 上的 Shuffle 文件被删除
  → 下游 Task 去 Executor 2 拉数据 → 找不到 → 任务失败

有 External Shuffle Service：
  Executor 2 被回收 → Shuffle 文件由 External Shuffle Service 保管
  → 下游 Task 去 External Shuffle Service 拉数据 → 正常获取
  → External Shuffle Service 是每个节点上的独立守护进程，不随 Executor 生死
```

开启方式（二选一）：

```bash
# 方式 1：Shuffle Tracking（Spark 3.0+，无需额外服务）
--conf spark.dynamicAllocation.enabled=true
--conf spark.dynamicAllocation.shuffleTracking.enabled=true
# Spark 自己跟踪 Shuffle 文件的生命周期，不需要额外部署服务
# 简单但回收延迟略高（要等所有依赖的 Shuffle 都被消费完才回收）

# 方式 2：External Shuffle Service（传统方式，需要在每个节点部署）
--conf spark.dynamicAllocation.enabled=true
--conf spark.shuffle.service.enabled=true
# 需要在每个 NodeManager 上部署 Spark 的 External Shuffle Service
# 配置稍复杂但回收更及时
```

> **Dynamic Allocation vs AQE——不要混淆**：这是两个完全不同的东西。Dynamic Allocation 调整的是** Executor 数量**（资源层面），根据 Task 排队情况增减工人。AQE（Adaptive Query Execution）调整的是**执行计划**（逻辑层面），根据真实数据量合并分区、切换 JOIN 策略、处理倾斜。两者可以同时开启、互不冲突——Dynamic Allocation 管"有多少工人"，AQE 管"工人怎么干活"。

| 维度 | Dynamic Allocation | AQE |
|------|-------------------|-----|
| **调整对象** | Executor 数量（资源） | 执行计划（逻辑） |
| **触发依据** | Task 排队等待 / Executor 空闲 | Shuffle 后真实数据量 |
| **作用层面** | 资源管理 | 查询优化 |
| **适用场景** | 长时间运行的 Application（多 Job） | 单个 SQL 查询的执行过程 |
| **版本** | Spark 1.2+ | Spark 3.0+ |
| **默认状态** | 关闭 | Spark 3.2+ 默认开启 |

#### 内存模型与 memory.fraction

Spark Executor 的 JVM 堆内存管理经历了两代演进：

**第一代：静态内存管理（StaticMemoryManager，Spark 1.5 及以前）**

静态管理将堆内存按固定比例分配给 Storage 和 Execution，两者**互不借用**：

```
静态内存管理（已废弃）：

  Heap × 20%    → Other/Reserved（系统预留）
  Heap × 60%    → Storage Memory（缓存 RDD）   ← spark.storage.memoryFraction = 0.6
  Heap × 20%    → Execution Memory（Shuffle/JOIN） ← spark.shuffle.memoryFraction = 0.2

  问题：Storage 和 Execution 之间有硬边界，不能跨界借用
  → 某些任务 Shuffle 重但不用缓存 → Execution 不够用，Storage 空着浪费
  → 某些任务疯狂 cache 但不 Shuffle → Storage 不够用，Execution 空着浪费
```

![静态内存管理模型（已废弃）——Storage/Execution 硬隔离，不可借用](assets/spark-memory-static.png)

> **注意**：上图是旧版静态内存管理的结构。其中 `spark.storage.memoryFraction=0.6`、`spark.shuffle.memoryFraction=0.2` 等参数是 Spark 1.5 及以前静态管理的参数，Spark 1.6+ 已废弃。统一内存管理后，用 `spark.memory.fraction` 和 `spark.memory.storageFraction` 替代，但底层逻辑更灵活。

**第二代：统一内存管理（UnifiedMemoryManager，Spark 1.6+，默认）**

统一管理把 Storage 和 Execution 放进同一个池子，可以动态借用——这就是下面要讲的四层模型。Spark 1.6+ 默认使用统一管理，静态管理的代码在 Spark 3.0 已被移除。

> **为什么统一管理更好？** 因为实际任务中 Storage 和 Execution 的需求是动态变化的——ETL 任务几乎不用缓存（Storage 空闲），而迭代式机器学习任务疯狂缓存（Execution 空闲）。统一管理让两者共享空间，谁需要谁用，避免"一半撑死一半饿死"。代价是 Storage 的缓存可能被 Execution 驱逐（但缓存可以从磁盘重建，而计算中间数据不能丢，所以这个优先级设计是合理的）。

Spark Executor 的 JVM 堆内存从下到上分为四个层次（Unified Memory Management，Spark 1.6+）：

```
┌─────────────────────────────────────────────────────────┐
│                  Executor JVM Heap                      │
│  ┌──────────────────────────────────────────────────┐   │
│  │  Unified Memory（统一内存）                        │   │
│  │  = (Heap - 300MB) × spark.memory.fraction        │   │
│  │  ┌────────────────────┐  ┌────────────────────┐  │   │
│  │  │  Storage Memory    │  │  Execution Memory  │  │   │
│  │  │  用于缓存 RDD      │  │  用于 Shuffle/     │  │   │
│  │  │  /广播变量/数据    │  │  JOIN/聚合中间数据 │  │   │
│  │  │  = Unified × 50% │  │  = Unified × 50%   │  │   │
│  │  │  默认 storageFraction=0.5                    │  │   │
│  │  │                    │  │                    │  │   │
│  │  │  ┌────────────┐    │  │    ┌────────────┐  │  │   │
│  │  │  │ 动态借用   │◄───┼──┼───►│ 动态借用   │  │  │   │
│  │  │  └────────────┘    │  │    └────────────┘  │  │   │
│  │  └────────────────────┘  └────────────────────┘  │   │
│  └──────────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────┐   │
│  │  User Memory（用户内存）                           │   │
│  │  = (Heap - 300MB) × (1 - memory.fraction)        │   │
│  │  用于用户代码、UDF、数据结构、RDD 依赖关系等       │   │
│  └──────────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────┐   │
│  │  Reserved Memory（系统预留）                       │   │
│  │  默认 300MB，用于存储 Spark 内部对象（不可配置）    │   │
│  └──────────────────────────────────────────────────┘   │
├─────────────────────────────────────────────────────────┤
│  堆外内存（spark.executor.memoryOverhead）              │
│  （JNI、网络缓冲区、Python UDF 等）                      │
└─────────────────────────────────────────────────────────┘
```

**关键参数——与内存模型的逐层对应关系**：

```
模型结构（从上到下）与参数的对应：

┌─────────────────────────────────────────────────────────┐
│  Executor JVM Heap（spark.executor.memory）              │
│                                                          │
│  ┌──────────────────────────────────────────────────┐   │
│  │  Unified Memory                                    │   │
│  │  ┌────────────────────┐  ┌────────────────────┐  │   │
│  │  │  Storage Memory    │  │  Execution Memory  │  │   │
│  │  │  ← storageFraction │  │  ← 1-storageFraction│  │   │
│  │  └────────────────────┘  └────────────────────┘  │   │
│  │           ↑                                        │   │
│  │    spark.memory.fraction 控制这一整块的大小         │   │
│  └──────────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────┐   │
│  │  User Memory  ← 1 - spark.memory.fraction         │   │
│  └──────────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────┐   │
│  │  Reserved Memory（固定 300MB，不可配置）            │   │
│  └──────────────────────────────────────────────────┘   │
├─────────────────────────────────────────────────────────┤
│  堆外内存（spark.executor.memoryOverhead + offHeap.size）│
└─────────────────────────────────────────────────────────┘
```

| 参数 | 默认值 | 对应模型哪一层 | 公式 | 调大/调小的影响 |
|------|--------|--------------|------|----------------|
| `spark.executor.memory` | — | 整个 JVM 堆 | 即 `-Xmx` | 调大增加总内存，但受 YARN Container 上限制约 |
| `spark.memory.fraction` | 0.6 (Spark 2.0+) | Unified Memory 占可用堆的比例 | `(Heap - 300MB) × fraction` | 调大→ Execution/Storage 增多，User Memory 减少（UDF 可能 OOM） |
| `spark.memory.storageFraction` | 0.5 | Unified Memory 中 Storage 的**初始**保留比例 | `Unified × storageFraction` | 调大→ Storage 初始保留更多，Execution 可借用空间减少 |
| `spark.executor.memoryOverhead` | memory × 0.1 | 堆外内存（JNI/网络/Python UDF） | 独立于堆 | 调大避免堆外 OOM 被 YARN kill，但不参与统一管理 |
| `spark.memory.offHeap.enabled` | false | 是否启用堆外统一内存 | — | 启用后堆外也有 Storage/Execution 分区，与堆内互不影响 |
| `spark.memory.offHeap.size` | 0 | 堆外统一内存总量 | 需启用 offHeap | 堆外 Execution 内存不受 GC 影响，适合大 Shuffle 场景 |

> **参数与模型的对应关系速记**：`executor.memory` 决定整个蛋糕大小 → 减去 300MB Reserved → 乘以 `memory.fraction` 得到 Unified Memory（Execution + Storage 共享池）→ 再乘以 `storageFraction` 得到 Storage 的初始保留量 → 剩余给 Execution。User Memory 是 `1 - memory.fraction` 那部分，Spark 不管，由用户代码自由使用。

> **公司平台默认值问题**：很多公司大数据平台将 `memory.fraction` 默认值设为 **0.3**（最初为兼容 Hive UDF），这意味着 8G 的 Executor 只有 2.4G 用于 Shuffle/JOIN/聚合——严重偏低。如果你的任务不使用自定义 UDF，建议调整到 **0.6**，执行内存翻倍，Spill 大幅减少。

**Storage 与 Execution 的动态借用机制**：

Storage 和 Execution 共享统一内存池，但有一个**优先级差异**：

```
动态借用规则：

① Storage 可以借用 Execution 的空闲内存
   → 当缓存数据量临时增加时，Storage 可以占用 Execution 未使用的部分
   → Execution 后续需要时可以强制回收（Storage 数据会被驱逐到磁盘）

② Execution 可以借用 Storage 的空闲内存
   → 当 Shuffle/JOIN 需要更多内存时，Execution 可以占用 Storage 未使用的部分
   → Storage 后续需要时可以尝试回收，但 Execution 不会主动归还
   → 如果 Storage 被占用的部分有缓存数据，Execution 需要时可以强制驱逐

③ 两者都不足时 → 触发 Spill 到磁盘
```

![统一内存管理的动态借用机制——三种场景](assets/spark-memory-dynamic-borrow.png)

> **图说明**：从左到右三阶段。
> ① 双方空间都被占满后，新增内容双方都需要存储到硬盘（Spill to disk）。
> ② Storage 不足时借用 Execution 空闲内存，后续 Execution 可强制驱逐（Eviction）。
> ③ Execution 占用 Storage 内存不可被淘汰（No Eviction），Storage 只能等待 Execution 释放。

> **关键区别**：Execution 内存有**优先权**。当 Execution 需要内存时，可以强制回收 Storage 借用的部分（驱逐缓存数据到磁盘）。反之，Storage 不能强制回收 Execution 的内存——只能等待 Execution 主动释放。这是合理的：计算中的中间数据不能丢，但缓存的数据可以从磁盘重新读取。

**完整公式（以 8GB Executor 为例）**：

```
Heap = 8GB = 8192MB
  Reserved = 300MB（固定）
  Usable = 8192 - 300 = 7892MB
    Unified Memory = 7892 × 0.6 = 4735MB
      Storage = 4735 × 0.5 = 2367MB
      Execution = 4735 × 0.5 = 2367MB
    User Memory = 7892 × 0.4 = 3157MB
  
  堆外内存 = memoryOverhead（默认 8GB × 0.1 = 819MB，最小 384MB）
```

> **Task 内存分配与调度机制**：`spark.executor.cores = N` 时，每个 Task 能申请到的内存为 `1/2N ~ 1/N`。例如 N=4、Execution Memory 为 4735MB → 每个 Task 初始可申请 4735/8 ≈ 592MB，最大可扩展到 4735/4 ≈ 1184MB。

**Task 内存的挂起与唤醒**：

Executor 中每个 Task 以线程方式运行，共享 Execution Memory。Spark 用 `memoryForTask`（HashMap）跟踪每个 Task 的内存使用量。当多个 Task 竞争内存时：

```
Task 内存调度规则（n = 当前活跃 Task 数）：

① 每个 Task 的内存下限 = Execution Memory × 1/2n
   → 当剩余内存 < 1/2n 时，新 Task 被挂起（等待内存释放）

② 每个 Task 的内存上限 = Execution Memory × 1/n
   → Task 可以在 [1/2n, 1/n] 区间内申请内存

③ Task 挂起后，当其他 Task 完成释放内存，剩余内存 ≥ 1/2n 时被唤醒

④ 如果 Task 申请内存失败（无空闲且无法驱逐 Storage）→ 抛出 OutOfMemoryError

举例（Execution Memory = 4000MB，4 个 Task）：
  每个 Task 区间 = [500MB, 1000MB]
  Task 1 启动 → 占用 500MB（下限）→ 可扩展到 1000MB
  Task 2 启动 → 占用 500MB
  Task 3 启动 → 占用 500MB
  Task 4 启动 → 占用 500MB（此时 4 × 500 = 2000MB 已用，剩余 2000MB）
  Task 1 需要更多内存 → 扩展到 1000MB（从剩余 2000MB 中拿 500MB）
  Task 2 需要更多内存 → 扩展到 1000MB
  此时剩余 0MB，Task 3 和 4 无法扩展 → 如果强制申请 → OutOfMemoryError 或 Spill
```

> **为什么是 1/2n ~ 1/n 而不是均分 1/n？** 因为 Spark 允许 Task 动态扩展内存——先到的 Task 可以用到 1/n（上限），后到的 Task 保底有 1/2n（下限）。这比严格均分更灵活：短 Task 快速完成释放内存，长 Task 可以借用其份额。代价是先到的 Task 可能"占便宜"用更多内存，后到的 Task 内存偏少——这就是为什么同一个 Stage 中有时个别 Task OOM 而其他 Task 正常。

**堆外内存作为 Task 的稳定缓冲区**：

```
没有启用堆外统一内存时（默认）：
  Task 内存范围 = [Execution/2n, Execution/n]
  → 完全依赖堆内 Execution Memory，Task 间竞争激烈
  → 某个 Task 用多了，其他 Task 可能不够用

启用堆外统一内存后（spark.memory.offHeap.enabled=true）：
  堆内 Execution = (Heap - 300MB) × fraction × (1 - storageFraction)
  堆外 Execution = spark.memory.offHeap.size × (1 - storageFraction)
  Task 可用总量 = 堆内 + 堆外

  → 堆外内存不受 GC 影响，不会被 GC 停顿打断
  → 堆外内存作为"共享缓冲区"，降低 Task 因内存不足被挂起/失败的概率
  → 但堆外内存使用的是操作系统的物理内存，需要确保机器有足够剩余
```

![堆外内存的 Storage/Execution 动态借用——与堆内统一管理一致](assets/spark-memory-offheap.png)

> **图说明**：堆外内存同样划分为 Storage（50%）和 Execution（50%），虚线表示动态占用机制。启用 `spark.memory.offHeap.enabled=true` 后，堆外也参与统一内存管理，与堆内互不影响。

**Unroll 内存——缓存展开的预分配机制**：

当 Spark 把 RDD 分区缓存到内存（Storage Memory）时，数据需要从序列化的紧凑格式"展开"（Unroll）成 Java 对象。展开过程需要临时内存：

```
Unroll 过程：
  ① 先遍历整个分区的数据，计算展开后需要多少内存
  ② 向 MemoryManager 预约这块内存（currentUnrollMemory）
  ③ 如果内存足够 → 展开数据，存入 Storage Memory，释放 Unroll 预留
  ④ 如果内存不够 → 放弃内存缓存，改为磁盘缓存（或不缓存）

注意：Unroll 内存是"预约制"——先申请再写入，防止写到一半 OOM
     预约期间这部分内存被标记为占用，但实际还没存数据
```

> **为什么 Unroll 要预分配？** 因为 RDD 分区的数据可能是序列化的字节数组，展开成 Java 对象后体积会膨胀 2-5 倍（对象头、引用、对齐填充等开销）。如果不预分配直接展开，可能展开到一半内存不够，前面的工作全白费。预分配机制确保"要么全部缓存成功，要么直接放弃不开始"，避免半成品浪费内存。

**真实案例**：某推荐团队的 LTV 指标计算任务，executor.memory = 8g 但 memory.fraction = 0.3，导致执行内存仅 2.4G。核心 Stage 处理 3.4TB 数据时 Memory Spill 高达 1751GB、Disk Spill 78GB，单个 Stage 耗时 25 分钟。将 memory.fraction 从 0.3 调到 0.6 后（执行内存从 2.4G 变为 4.8G），Stage 耗时从 25 分钟降至约 12 分钟，**零风险、不增加资源申请量**——这是性价比最高的优化手段。

#### YARN Container 内存约束——调参的天花板

上面讲了 Executor 内部的内存分配，但调参时还有一个外部约束：**YARN Container 的最大内存限制**。Spark on YARN 模式下，Executor 运行在 YARN 的 Container 中，Container 的物理内存（堆 + 堆外）不能超过 YARN 的限制：

```
约束公式：
  spark.executor.memory + spark.yarn.executor.memoryOverhead <= YARN Container 最大内存限制

各参数含义：
  spark.executor.memory           → Executor JVM 的堆内存（-Xmx）
  spark.yarn.executor.memoryOverhead → Executor 的堆外内存
    （包括 JVM overhead、Shuffle 排序、Netty 网络缓冲区、Python UDF 等）
    默认值 = executor.memory × 0.1，最小 384MB

举例：
  spark.executor.memory = 8g
  spark.yarn.executor.memoryOverhead = 2g（手动设置）
  → Container 总内存 = 8g + 2g = 10g
  → 如果 YARN 限制单 Container 最大 16g，则 10g < 16g，合法

  如果调到 spark.executor.memory = 14g + memoryOverhead = 4g = 18g > 16g
  → YARN 会拒绝启动 Container，或运行中超限被 kill
```

> **Driver 的约束类似**：yarn-cluster 模式下，`spark.driver.memory + spark.yarn.driver.memoryOverhead` 也不能超过 YARN Container 最大内存限制。yarn-client 模式下 Driver 不在 Container 中，不受此限制。

这个约束意味着调大 `executor.memory` 时必须同步检查 `memoryOverhead` 是否足够，且两者之和不超过 Container 上限。常见错误是只调大 `executor.memory` 而忽略了 `memoryOverhead`，导致堆外内存（Shuffle、Netty 等）不足被 YARN kill。

#### 为什么执行内存不放在堆外？

上面内存模型图中，Shuffle/JOIN/聚合这些最吃内存的操作放在堆内执行，堆外只放 JNI、网络缓冲区等。一个自然的疑问是：**为什么不把执行内存放到容量更大的堆外？或者堆内放不下时先溢到堆外，再溢到磁盘？**

答案分三层。

**第一层：堆内和堆外是两套不同的内存管理范式，不能简单 fallback**

```
堆内（On-Heap）：
  数据是 Java 对象 → 有对象头（12-16字节）、引用、GC 管理
  分配：bump pointer（指针碰撞，极快，约 10 纳秒）
  回收：GC 自动扫描回收，不需要手动 free
  适合频繁创建、短生命周期的对象

堆外（Off-Heap）：
  数据是二进制 byte buffer → 无对象头，手动管理
  分配：Unsafe.allocateMemory()（本质是 malloc，约 100 纳秒，慢 10 倍）
  回收：必须手动调用 free，GC 不管
  适合大块、长生命周期的数据

如果"堆满了搬到堆外"：
  需要把 Java 对象序列化成二进制 → 等于做了一次 Spill 的序列化
  搬回来又要反序列化 → 又一次开销
  → 这跟 Spill 到磁盘本质上是一回事，只是目的地从磁盘换成了内存
  → 而磁盘虽然慢，但容量无限，且 Spill 时顺便做了序列化+压缩
```

所以 Spark 的设计不是"堆 → 堆外 → 磁盘"三级溢出，而是**"堆内计算，放不下就 Spill 到磁盘"**。

**第二层：Tungsten 已经在堆内做了"伪堆外"优化**

Tungsten 的核心思路不是把数据搬到堆外，而是**在堆内用 byte 数组模拟堆外的二进制格式**：

```
Tungsten 的 UnsafeRow（堆内模式）：
  数据存在 byte[] 中（在堆内，GC 能看到）
  但通过 Unsafe 类直接按 offset 读写，绕过 Java 对象模型
  → 没有对象头开销（省 12-16 字节/对象）
  → GC 只看到一个 byte 数组，不需要扫描内部字段
  → 排序时只交换 8 字节指针（row pointer），不移动数据

  相当于"在堆内享受了堆外的数据布局优势"
  同时保留了 GC 管理内存的生命周期安全性
```

对于大多数场景，Tungsten 堆内模式已经够好——既避免了 Java 对象模型的开销，又不用手动管理内存。

**第三层：堆外模式什么时候才值得开**

```
spark.memory.offHeap.enabled=true
spark.memory.offHeap.size=1g

开启后的效果：
  执行内存和存储内存各拆成 on-heap + off-heap 两部分
  Tungsten 的内存分配优先从 off-heap 池申请
  → 堆内对象更少 → GC 扫描更快 → Full GC 频率降低

适合的场景：
  ① Executor 堆很大（>32G），GC 扫描耗时明显
  ② 任务有大量 Shuffle/JOIN 中间数据，GC 压力大
  ③ 数据量稳定，能准确预估 off-heap size

不适合的场景：
  ① Executor 堆较小（<8G），GC 本身不是瓶颈
  ② off-heap size 设得太大 → 堆内可用内存减少 → 反而 OOM
  ③ off-heap size 设得太小 → 频繁 Spill，不如不开
```

**总结：Spark 的内存设计哲学**

```
Spark 选择的架构：堆内（Tungsten 二进制格式）→ Spill 到磁盘
                    ↑                          ↑
                 快速计算                   无限容量
                 GC 安全                   序列化+压缩

而不是：          堆内 → 堆外 → 磁盘
                      ↑        ↑
                   序列化    再序列化
                   手动管理   两套代码路径

堆外是"可选的 GC 减压阀"，不是"堆内的溢出缓冲区"。
Tungsten 在堆内已经用二进制格式解决了对象开销问题，
堆外的增量收益（绕过 GC）只在堆很大时才显著。
```

#### Memory Spill 和 Disk Spill 是什么关系？

Spark UI 上同时显示 Memory Spill 和 Disk Spill 两个指标，很多人以为 Spark 会"选择"用 memory 还是 disk 来溢写——其实不是。**它们是同一次溢写动作的两个统计视角**：

```
当执行内存不够时，Spark 会把内存中的数据溢写到磁盘。这一次溢写产生两个数字：

Memory Spill = 数据在内存中的大小（反序列化、未压缩的 Java 对象）
Disk Spill   = 同一份数据序列化 + 压缩后写到磁盘的大小

例如上面的案例：
  Memory Spill = 1751 GB（内存中的 Java 对象大小）
  Disk Spill   = 78 GB（序列化 + 压缩后的磁盘大小）
  压缩比 ≈ 22:1（这是正常的，Java 对象头 + 指针 + 对齐填充占了大量空间）
```

溢写的触发机制是 Spark 的**统一内存管理器（Unified Memory Manager）**：执行内存和存储内存共享同一个内存池（由 `memory.fraction` 控制总量）。当执行内存不足时，Spark 先尝试从存储内存中"借"空间（驱逐缓存的 RDD 数据）；如果借不到了，就触发 Spill——把内存中排序好的数据序列化压缩后写到本地磁盘的临时文件中，释放内存继续处理后续数据。

```
Spill 对性能的影响链：
  内存不足 → 触发 Spill → 序列化 + 压缩（CPU 开销）→ 写磁盘（IO 开销）
  → 后续还要从磁盘读回来（再一次 IO 开销）→ 反序列化（CPU 开销）
  → 同一份数据被处理了两遍，性能可能降低 2-5 倍
```

> **看 Spark UI 的经验法则**：如果 Memory Spill > 0，说明执行内存不够用了。优化优先级：先调 memory.fraction（零成本），再调 shuffle.partitions（减少单 Task 数据量），最后才调大 executor.memory（增加资源）。Disk Spill 的大小不重要——它只是 Memory Spill 压缩后的结果，你需要关注的是 Memory Spill 本身。

#### Shuffle 分区数

```
spark.sql.shuffle.partitions = 200（默认值，通常偏小）

估算公式：合理分区数 ≈ Shuffle Read 总数据量 / 目标分区大小（128-256MB）
例如：Shuffle Read 100GB → 100GB / 200MB = 500 个分区
```

分区数过小 → 单 Task 数据量大 → OOM 或 Spill；分区数过大 → Task 太多、调度开销大、可能产生大量小文件。

推荐开启 AQE（Adaptive Query Execution）自动合并分区：`SET spark.sql.adaptive.enabled=true`，Spark 会在运行时根据实际数据量自动合并过小的分区。

#### 小文件问题

小文件是 HDFS 和 Spark 的共同敌人——读取端每个小文件产生一个 Task（10GB 数据如果是 10 万个小文件就产生 10 万个 Task），写入端输出过多小文件会拖慢下游任务。

```
读取端小文件（Task 膨胀）：
  调大 spark.sql.files.maxPartitionBytes（默认 128MB → 256MB）
  让 Spark 合并多个小文件到一个 Task

写入端小文件（输出文件过多）：
  开启 AQE 自动合并：spark.sql.adaptive.coalescePartitions.enabled=true
  或在写出前加 DISTRIBUTE BY 控制输出文件数

动态分区插入的小文件（最容易踩坑）：
  INSERT OVERWRITE TABLE A PARTITION (aa) SELECT * FROM B;
  → 最差情况：每个 Task 都包含多个分区的数据
  → 文件数 = Task 数 × 分区数（如 200 Task × 50 分区 = 10000 个小文件）

  解决方案：用 DISTRIBUTE BY 把同一分区的数据路由到同一个 Task
  INSERT OVERWRITE TABLE A PARTITION (aa)
  SELECT * FROM B DISTRIBUTE BY aa;
  → 同一分区的数据被 hash 到同一个 Task，只产生 N 个文件（N = 分区数）

  如果分区键本身倾斜（某个分区数据量远大于其他）：
  → 拆成两部分分别处理
  -- 非倾斜分区：正常 DISTRIBUTE BY 分区字段
  INSERT OVERWRITE TABLE A PARTITION (aa)
  SELECT * FROM B WHERE aa != '大key' DISTRIBUTE BY aa;

  -- 倾斜分区：DISTRIBUTE BY 随机数打散
  INSERT OVERWRITE TABLE A PARTITION (aa)
  SELECT * FROM B WHERE aa = '大key' DISTRIBUTE BY cast(rand() * 5 as int);

  Spark 3.2+ 补充：AQE 的 Rebalance 操作可以自动平衡分区
  SET spark.sql.adaptive.coalescePartitions.enabled=true;
  → AQE 会在写入前自动合并过小分区，配合 DISTRIBUTE BY 效果更好
```

> **Hive 四种排序关键字在 Spark SQL 中的支持情况**：Spark SQL 兼容 Hive 的 ORDER BY、SORT BY、DISTRIBUTE BY、CLUSTER BY 语法，语义一致。关键区别：Spark SQL 中 ORDER BY 不再强制单 Reducer——当查询有 LIMIT 时，Spark 会用 Top-K 算法（TakeOrdered）避免全量排序；没有 LIMIT 时仍是全局排序但可以多分区并行收集后合并。DISTRIBUTE BY 在 Spark 中最常用于控制输出文件数和解决动态分区写入 OOM，而非排序场景。详见 [Hive — 四种排序关键字](./03-Hive.md#26-四种排序关键字order-by--sort-by--distribute-by--cluster-by)。

#### spark.default.parallelism vs spark.sql.shuffle.partitions——两个最容易混淆的参数

```
spark.default.parallelism：
  → 控制 RDD API 的默认分区数（join、reduceByKey、parallelize 等）
  → 不影响 Spark SQL / DataFrame
  → 推荐值：总 CPU core 数的 2-3 倍

spark.sql.shuffle.partitions：
  → 控制 Spark SQL / DataFrame 的 Shuffle 后分区数
  → 不影响 RDD API
  → 默认 200，推荐根据数据量调整（每分区 128MB-256MB）

关键区别：
  RDD 操作（reduceByKey 等）→ 用 spark.default.parallelism
  Spark SQL / DataFrame 操作（JOIN、GROUP BY 等）→ 用 spark.sql.shuffle.partitions
  两者互不干扰，各管各的

Spark SQL 的并行度陷阱：
  Spark SQL 读取 Hive 表时，并行度由 HDFS Block 数决定（不受 parallelism 控制）
  如果 Hive 表只有 20 个 Block → Spark SQL 的 Stage 只有 20 个 Task
  即使你设置了 spark.default.parallelism=1000 也没用
  → 如果这个 Stage 有复杂计算，20 个 Task 可能极慢

  解决：用 repartition 打散
  val df = spark.sql("SELECT * FROM big_hive_table")
  val repartitioned = df.repartition(200)  // 强制重新分区为 200
  → 后续操作就有 200 个 Task 了
  → 代价：多一次 Shuffle
```

#### 算子级优化

**mapPartitions vs map——减少函数调用开销**

```
map：对每条记录调用一次函数
  partition 有 10000 条 → 函数被调用 10000 次
  → 每次调用有函数调用开销 + 资源初始化开销（如创建连接）

mapPartitions：对每个分区调用一次函数，传入整个分区的迭代器
  partition 有 10000 条 → 函数只调用 1 次，接收 Iterator[10000 条]
  → 减少函数调用开销
  → 资源初始化只做一次（如数据库连接、外部服务客户端）

适用场景：
  ① 需要初始化外部资源（数据库连接、HTTP 客户端）→ mapPartitions 初始化一次复用
  ② 分区数据量适中（不会 OOM）

风险：
  分区数据量过大 → 一次加载整个分区到内存 → OOM
  如果分区有 100 万条数据，mapPartitions 一次性接收 100 万条 → 可能 OOM
  → map 是流式处理（一条处理完 GC 回收），mapPartitions 是批量加载
```

> **Spark 3.x 注意**：对于 DataFrame API，Tungsten 的全阶段代码生成已经自动融合了多个算子，map 和 mapPartitions 的性能差距比 RDD 时代小很多。但在需要初始化外部资源（DB 连接、HTTP 客户端）的场景，mapPartitions 仍有不可替代的优势。

**foreachPartition 优化写数据库**

```
foreach（逐条写）：
  100 万条数据 → 100 万次函数调用
  → 每条数据获取一个数据库连接 + 发送一条 SQL
  → 性能极差（连接创建/销毁是最大瓶颈）

foreachPartition（按分区批量写）：
  100 万条数据 / 200 分区 = 每分区 5000 条
  → 每个分区获取一个数据库连接 + 批量 INSERT
  → 连接数 = 分区数（200 个），SQL 执行次数大幅减少

  典型写法：
  df.foreachPartition { partition: Iterator[Row] =>
    val conn = DriverManager.getConnection(url)  // 每分区一个连接
    val pstmt = conn.prepareStatement("INSERT INTO t VALUES (?, ?)")
    partition.grouped(1000).foreach { batch =>
      batch.foreach { row =>
        pstmt.setString(1, row.getString(0))
        pstmt.setInt(2, row.getInt(1))
        pstmt.addBatch()                          // 攒批
      }
      pstmt.executeBatch()                        // 每 1000 条执行一次
    }
    conn.close()
  }
```

> **Spark 3.x 推荐做法**：如果写 MySQL 等关系型数据库，优先用 `df.write.jdbc()` —— Spark 内部已做批量写入优化，比手动 foreachPartition 更简洁可靠。只有需要自定义写入逻辑（如 UPSERT、多表联动、复杂映射）时才用 foreachPartition。注意：JDBC 连接对象不可序列化，不能在 Driver 端创建后传给 Executor，必须在 foreachPartition 内部创建。

**filter 后用 coalesce 减少分区**

```
filter 过滤掉 80% 数据后：
  原本 200 个分区，每分区 10000 条
  filter 后每分区只剩 2000 条，但分区数还是 200
  → 200 个 Task 各处理 2000 条，Task 调度开销 > 计算开销
  → 而且各分区数据量不均匀（filter 条件对不同分区影响不同）→ 轻度数据倾斜

用 coalesce 压缩分区：
  rdd.filter(...).coalesce(40)   // 200 分区 → 40 分区
  → 40 个 Task 各处理 10000 条，调度开销减少 5 倍
  → coalesce 默认不触发 Shuffle（narrow dependency），开销极小

coalesce vs repartition：
  coalesce(40)    → 默认 shuffle=false，不触发 Shuffle，只是合并相邻分区
  repartition(40) → 内部调用 coalesce(40, shuffle=true)，触发 Shuffle，数据更均匀
  → 减少 partition 数用 coalesce（不 Shuffle，快）
  → 增加 partition 数或需要均匀分布用 repartition（Shuffle，慢但均匀）
```

#### cache vs persist——RDD 缓存的正确姿势

**cache() 和 persist() 的关系**：

```scala
// cache() 内部就是调用了 persist()，使用默认存储级别 MEMORY_ONLY
def cache(): this.type = persist()

// persist() 可以指定存储级别
def persist(storageLevel: StorageLevel): this.type
def persist(newLevel: StorageLevel, allowOverride: Boolean): this.type
```

| 方法 | 存储级别 | 说明 |
|------|---------|------|
| `cache()` | MEMORY_ONLY | 只存内存，放不下就丢弃（不落盘） |
| `persist()` | 可指定 | 默认和 cache() 一样，但可以选其他级别 |

**Spark 的 12 种存储级别**：

| 存储级别 | 含义 | 适用场景 |
|---------|------|---------|
| MEMORY_ONLY | 只存内存，放不下丢弃 | 数据量不大、频繁读取（默认） |
| MEMORY_ONLY_2 | 同上，但复制 2 份到不同节点 | 需要容错 |
| MEMORY_AND_DISK | 先存内存，放不下写磁盘 | 数据量较大、可以接受部分磁盘读取 |
| MEMORY_AND_DISK_2 | 同上，复制 2 份 | 大数据 + 容错 |
| DISK_ONLY | 只存磁盘 | 数据量极大、内存不够 |
| DISK_ONLY_2 | 只存磁盘，复制 2 份 | 大数据 + 容错 |
| MEMORY_ONLY_SER | 序列化后存内存（省空间） | 内存紧张、能接受序列化开销 |
| MEMORY_AND_DISK_SER | 序列化存内存，放不下写磁盘 | 内存紧张 + 大数据 |
| OFF_HEAP | 堆外内存 | 避免GC影响（需配置 spark.memory.offHeap.enabled） |

> **选哪个级别？** 大多数场景用默认的 MEMORY_ONLY 就够了。如果 RDD 数据量大于 Executor 内存，用 MEMORY_AND_DISK。如果 GC 压力大（缓存数据导致频繁 Full GC），考虑 OFF_HEAP。_SER 后缀的级别在内存紧张时可以省 50%+ 空间，但序列化/反序列化有 CPU 开销，Spark 2.x 后 Tungsten 已经自动做了高效序列化，手动用 _SER 的场景变少了。

**SQL 级别：CACHE TABLE**

```sql
-- Spark SQL 的缓存语法（底层也是 persist）
CACHE TABLE hot_data AS SELECT * FROM big_table WHERE dt='2024-01-01';

-- 释放缓存
UNCACHE TABLE hot_data;
```

CACHE TABLE 默认使用 MEMORY_AND_DISK 存储级别（和 RDD 的 MEMORY_ONLY 不同），因为 DataFrame/Dataset 的数据量通常较大。

**缓存的注意事项**：

```
① 缓存是惰性的——cache() / persist() 只是标记，真正触发缓存是在第一次 Action 执行时
   rdd.cache()       ← 此时不会缓存
   rdd.count()       ← 第一次 Action，触发计算并缓存
   rdd.collect()     ← 第二次 Action，直接从缓存读取，不重新计算

② 缓存不会自动释放——用完必须手动 unpersist()
   rdd.unpersist()   ← 释放缓存，否则会占用内存直到 Application 结束

③ 缓存丢失后会自动重建——如果内存不够被 evict（MEMORY_ONLY 场景），
   下次使用时会重新计算，不会报错但会有性能损失

④ Checkpoint vs Cache：
   Cache：存内存/磁盘，生命周期随 Application，Driver 崩溃就没了
   Checkpoint：存 HDFS，生命周期持久，RDD 血缘会被截断（不再依赖上游）
   → Checkpoint 适合超长血缘链（防止重建代价过大）和需要持久化的场景
```

#### OOM / Spill 排查路径

```
Executor OOM / Spill 排查优先级：
  ① 先查 memory.fraction 是否偏低（0.3 → 0.6，零成本）
  ② 再查 Shuffle 分区数是否偏小（调大 shuffle.partitions）
  ③ 检查是否有数据倾斜（单 Task 数据量远超中位数）
  ④ 检查 executor.cores 是否过大（每个 core 一个 Task 共享内存）
  ⑤ 最后才考虑调大 executor.memory（增加资源申请量）

Driver OOM 排查：
  ① collect() 把大结果集拉到 Driver → 改用 write 写出
  ② Broadcast 表过大 → 检查 autoBroadcastJoinThreshold
  ③ 动态分区数量过多 → 限制 maxDynamicPartitions

YARN kill（exitCode 143）：
  ≠ JVM OOM。YARN kill 是容器物理内存（堆 + 堆外）超出申请量
  → 调大 spark.executor.memoryOverhead，不是调 executor.memory
```

#### executor.cores 与内存的竞争关系

上面 OOM 排查路径中提到"检查 executor.cores 是否过大"，这里展开解释原因。

```
executor.cores 决定同一 Executor 内同时运行的 Task 数量：
  executor.cores = 4 → 4 个 Task 共享同一个 Executor 的 JVM 堆内存
  executor.memory = 8G → 每个 Task 平均可用 8G / 4 = 2G

如果 executor.cores = 1 → 1 个 Task 独占 8G，几乎不可能 OOM
如果 executor.cores = 8 → 8 个 Task 共享 8G，每个只有 1G，容易 OOM

这就是为什么"调大 cores 提高并行度"有时反而引发 OOM——
cores 越多，单 Task 可用内存越少，大表 JOIN 或聚合时更容易触发 Spill 甚至 OOM。

推荐配置思路：
  内存密集型（JOIN、聚合、排序）→ executor.cores 2-3，每核内存充裕
  CPU 密集型（UDF、正则、JSON 解析）→ executor.cores 4-5，CPU 利用率高
  不要盲目调大 cores，cores × 每 Task 数据量 = 实际内存压力
```

#### 动态分区写入 OOM——容易被忽视的 OOM 诱因

除了常规的 JOIN/聚合导致 OOM，还有一种隐蔽的 OOM 来源：**动态分区写入（INSERT OVERWRITE TABLE ... PARTITION (...)）**时，如果分区数过多，单个 Task 可能同时持有大量分区的写入缓冲区，导致内存溢出。

```
动态分区写入的内存陷阱：

  INSERT OVERWRITE TABLE target_table PARTITION (dt, city)
  SELECT * FROM source_table;

  如果 source_table 包含 500 个 (dt, city) 组合
  → 最差情况：一个 Task 的数据覆盖全部 500 个分区
  → 该 Task 需要同时维护 500 个写入缓冲区
  → 每个缓冲区累积数据到一定大小才 flush 到 HDFS
  → 500 个缓冲区的内存总和可能远超单 Task 可用内存 → OOM

  更危险的情况：非分区字段被误写入分区字段
  → 如把 user_id 当成分区字段 → 产生数万个分区 → 几乎必 OOM
```

解决方案：

```sql
-- 方案 1：用 DISTRIBUTE BY 将同一分区的数据路由到同一个 Task
-- 这样每个 Task 只处理少数几个分区，写入缓冲区数量可控
INSERT OVERWRITE TABLE target_table PARTITION (dt, city)
SELECT * FROM source_table DISTRIBUTE BY dt, city;

-- 方案 2：如果分区键本身倾斜（某个分区数据量远大于其他）
-- 拆成倾斜部分和非倾斜部分分别处理
-- 倾斜部分用 DISTRIBUTE BY 随机数打散，非倾斜部分正常 DISTRIBUTE BY
INSERT OVERWRITE TABLE target_table PARTITION (dt, city)
SELECT * FROM source_table WHERE dt != 'hot_date' DISTRIBUTE BY dt, city
UNION ALL
INSERT OVERWRITE TABLE target_table PARTITION (dt, city)
SELECT * FROM source_table WHERE dt = 'hot_date' DISTRIBUTE BY cast(rand() * 10 as int);
```

> **排查方法**：如果 Spark UI 显示 OOM 发生在写入阶段（Stage 的 Output 是写入操作而非计算），且该 Stage 的分区数远大于正常值，大概率是动态分区写入导致的。检查 SQL 中的 PARTITION 子句和 SELECT 的字段是否匹配。

#### 控制 Spill 阈值的参数

当内存不足以容纳所有数据时，Spark 会将数据 Spill 到磁盘。有两个参数可以控制 Spill 的触发时机，在特定场景下调低它们可以缓解 OOM：

```bash
# ① 控制 SortMergeJoin 时内存中缓存的行数
#    超过该阈值时，join 中的行会被 spill 到磁盘
#    默认值很大（Long.MaxValue），即不主动限制
#    如果 JOIN 的 key 有大量重复值（一对多 JOIN），可以适当调低
spark.sql.sortMergeJoinExec.buffer.spill.threshold

# ② 控制 Shuffle 时内存中缓存的元素数量
#    超过该阈值时强制 spill 到磁盘
#    默认值 = 1024 × 1024 × 1024（约 10 亿）
#    如果数据中有非常大的字段（如超长 JSON），可以适当降低该值
#    让 Spark 更早地 spill，避免内存中累积过多大对象
spark.shuffle.spill.numElementsForceSpillThreshold
```

> **使用建议**：这两个参数是"最后手段"——优先通过调大 `memory.fraction`、调大 `shuffle.partitions`、减少 `executor.cores` 来解决内存问题。只有在上述方法无效且确定是特定场景（大字段、大量重复 key）时，才考虑调低 spill 阈值。调得太低会导致频繁 Spill，性能急剧下降。

#### 常见运行时错误速查

除了 OOM 和 Spill，Spark 任务还有几个高频运行时错误，下面按错误类型逐一说明。

**广播超时（Broadcast Join Timeout）**

```
报错信息：org.apache.spark.SparkException: Could not execute broadcast in 300 secs.
         You can increase the timeout for broadcasts via spark.sql.broadcastTimeout
         or disable broadcast join by setting spark.sql.autoBroadcastJoinThreshold to -1

原因：
  Broadcast Join 需要把小表全量数据从 Driver 广播到所有 Executor
  如果"小表"其实不小（如 autoBroadcastJoinThreshold 设得过大，或手动用了 broadcast hint）
  → 广播数据量大 → 传输时间超过 spark.sql.broadcastTimeout（默认 300 秒）→ 超时报错

解决：
  ① 增大超时时间：SET spark.sql.broadcastTimeout=600（治标）
  ② 真正的问题是小表不够小——检查 autoBroadcastJoinThreshold 是否设得过大
  ③ 如果表确实大，禁用广播：SET spark.sql.autoBroadcastJoinThreshold=-1
     → 让 Spark 走 SortMergeJoin（安全但慢）
  ④ broadcast hint 打在大表上 → 移除 hint，让 Catalyst 自行选择 JOIN 策略
```

> **经验法则**：广播超时通常是"症状"而非"病因"。真正的问题是把不该广播的大表广播了。增大 timeout 只是给错误更多时间发生，正确做法是减小广播数据量或禁用广播。

**广播超时的隐蔽场景**

除了上述"小表不够小"的常见原因，还有几种隐蔽的广播超时场景：

```
场景 1：被广播的表有很多小文件
  表本身数据量不大（如 5MB），但被拆成了 10000 个小文件
  → Driver 需要串行从 HDFS 逐个 scan 这些小文件
  → 即使数据量小，文件 scan 的耗时也远超预期
  → 超过 spark.sql.broadcastTimeout（默认 300s）后超时
  解决：在被广播表的生产逻辑中合并小文件
       或设置 spark.hadoopRDD.targetBytesInPartition 合并读取

场景 2：Semi Join 被解析为 Broadcast Join
  SQL: SELECT * FROM a WHERE a.id IN (SELECT id FROM b)
  → Spark 把 IN 子查询解析成 Broadcast Join，b 表被广播
  → 如果 b 表数据量较大，广播超时
  解决：改写 SQL 避免对大表做 semi join
       或 SET spark.sql.autoBroadcastJoinThreshold=-1 禁用自动广播

场景 3：Driver RPC 超时（WARN Client: Fail to get RpcResponse: Timeout）
  Driver 在广播表的同时还要处理 Executor 心跳、Task 调度等 RPC 请求
  如果广播的大表占用了大量 Driver 内存和 CPU
  → Driver 压力过大，无法及时响应 Executor 的 RPC → 超时
  → 严重时 Driver 被 AM 判定超时，整个 Application 失败：
    "ApplicationMaster for attempt appattempt_xxx timed out. Failing the application."
  解决：检查是否有大表被意外广播（通过 Spark UI 的 SQL DAG 查看被广播的表）
       禁用自动广播：SET spark.sql.autoBroadcastJoinThreshold=-1
       或调大 Driver 内存：SET spark.driver.memory=4G
```

> **排查广播问题的方法**：打开 Spark UI → SQL 页面 → 查看执行计划 DAG → 找到 BroadcastExchange 节点 → 查看被广播的数据量大小。如果被广播的数据量异常大，说明是误触发了广播。常见误触发原因：`autoBroadcastJoinThreshold` 设得过大、手动加了 `/*+ BROADCAST(t) */` hint、或 Semi Join 被自动解析为广播。

**Executor 心跳超时（Heartbeat Timeout）**

```
报错信息：Executor heartbeat timed out after xxxms
         → 随后 Driver 标记该 Executor 为 Lost，任务重试或失败

原因（按频率排序）：
  ① Full GC 停顿（最常见）
     → Executor JVM 长时间 Full GC，STW 期间所有线程暂停，心跳线程无法发送
     → 查 Executor 日志搜索 "Full GC" 或 "real=" 确认
  ② Executor 内存不足被 OOM Killer 杀死
     → 进程直接消失，心跳自然断
  ③ 网络问题
     → Executor 和 Driver 之间网络不通或延迟过高
  ④ Executor 负载过重
     → 大量 Task 同时运行，CPU 被占满，心跳线程得不到调度

关键参数（三者必须递增）：
  spark.executor.heartbeatInterval  = 10s   （心跳发送间隔，默认 10s）
  spark.storage.blockManagerSlaveTimeoutMs = 120s  （BlockManager 超时）
  spark.network.timeout             = 120s  （网络超时，必须 > heartbeatInterval）

注意：增大 heartbeatInterval 不能解决根本问题——
  如果是 Full GC 导致的，增大间隔只是延迟报错，GC 问题依然存在。
  正确做法：排查 GC 原因（内存不足、数据倾斜），而不是调大超时时间。
```

**Shuffle 文件丢失（FileNotFoundException / Shuffle file lost）**

```
报错信息：java.io.FileNotFoundException: /path/to/shuffle/block (No such file or directory)
         或：Lost executor X on host: Container killed by YARN

原因：
  Spark 的 Shuffle 中间文件存在 Executor 的本地临时目录中
  ① Executor 被 YARN kill（内存超限）→ NodeManager 清理 Container 临时目录
     → 其他 Executor 来读 Shuffle 文件时发现文件不存在
  ② Executor 崩溃（OOM、JVM crash）
     → 同上，临时目录可能被清理
  ③ 磁盘故障
     → 本地磁盘损坏导致 Shuffle 文件丢失

解决：
  ① 根本解法：开启 External Shuffle Service（ESS）
     → Shuffle 文件由独立服务管理，Executor 崩溃不影响 Shuffle 文件读取
     → 动态资源分配也依赖 ESS
  ② 治标：增大 Executor 内存，避免被 YARN kill
  ③ 调大 spark.shuffle.io.maxRetries（默认 3）和 retryWait（默认 5s）
     → 给 Spark 更多重试机会
  ④ 如果频繁出现，检查磁盘健康状态和磁盘空间
```

**Metaspace OOM（Spark 3.x / Java 8+）**

```
报错信息：java.lang.OutOfMemoryError: Metaspace

原因：
  Metaspace 存储类的元数据（类名、方法、常量池等）
  Spark 任务中大量使用反射、CGLIB 动态代理、UDF 编译等会动态生成类
  → 加载的类数量超出 Metaspace 上限

注意（Spark 3.x 关键变化）：
  Java 8 移除了永久代（PermGen），替换为元空间（Metaspace）
  → MaxPermSize 已废弃（Java 7 及之前用），Spark 3.x 必须 Java 8+
  → 正确参数：-XX:MaxMetaspaceSize=512m
  → 通过 spark.executor.extraJavaOptions 设置：
     spark.executor.extraJavaOptions="-XX:MaxMetaspaceSize=512m"

  Metaspace 与 PermGen 的关键区别：
    PermGen：在 JVM 堆内，有固定上限，容易 OOM
    Metaspace：在本地内存（Native Memory），默认无上限，使用物理内存
    → 不设 MaxMetaspaceSize 可能吃光物理内存导致进程被 OOM Killer 杀死

什么时候需要调：
  ① 任务有大量 UDF / 自定义函数 → 类加载多
  ② 报 OutOfMemoryError: Metaspace
  ③ 一般设 256m-512m 足够，除非有极端的动态类生成场景
```

**上游数据文件丢失（FileNotFoundException on HDFS）**

与上面 Shuffle 文件丢失不同，这类错误是读取 HDFS 上的 Hive 表数据时，文件在任务执行期间被删除：

```
报错信息：java.io.FileNotFoundException: File does not exist:
         /user/hive/warehouse/db.table/partition_date=2024-06-29/part-00009

         at org.apache.hadoop.hdfs.server.namenode.INodeFile.valueOf(INodeFile.java:71)
         at org.apache.hadoop.hdfs.server.namenode.FSNamesystem.getBlockLocations(...)

原因：
  Spark 在生成执行计划时该文件存在，但任务开始执行时文件被删除
  → 通常是因为上游 ETL 在此期间执行了重导（INSERT OVERWRITE）
  → 或本作业的依赖配置不合理——上游和本作业同时执行

排查：
  ① 检查报错文件路径对应的表和分区
  ② 在调度系统中查看该表的上游作业是否有重导记录
  ③ 检查依赖配置：本作业是否正确依赖了上游作业

解决：
  ① 修正调度依赖关系，确保上游作业完成后本作业才启动
  ② 如果是上游重导导致，协调上游重导的时间窗口避开本作业
  ③ 如果是并发写入同一张表，改为串行执行
```

> **区分两种 FileNotFoundException**：如果丢失的文件路径在 `/user/hive/warehouse/` 下，说明是上游数据文件被删除（本节内容）；如果丢失的文件路径在 Executor 本地临时目录（如 `/data/yarn/nm-local-dir/`）下，说明是 Shuffle 中间文件丢失（见上文 Shuffle 文件丢失部分）。

**权限错误（AccessControlException）**

```
报错信息：java.util.concurrent.ExecutionException:
         org.apache.hadoop.security.AccessControlException: Permission denied

原因：
  ① 查询的 Hive 表没有读权限
  ② 写入的目标 HDFS 目录没有写权限
  ③ Kerberos 认证过期（安全集群）

解决：
  ① 联系表 owner 或 DBA 申请读写权限
  ② 检查提交作业的用户身份是否正确
  ③ Kerberos 过期需要重新 kinit 或配置 keytab 自动续期
```

**动态分区严格模式错误**

```
报错信息：Dynamic partition is disabled
     或：Dynamic partition strict mode requires at least one static partition column

原因：
  Hive 兼容模式下，动态分区写入需要显式开启相关参数
  严格模式要求至少指定一个静态分区列（防止误操作产生过多分区）

解决：
  SET hive.exec.dynamic.partition=true;           -- 开启动态分区
  SET hive.exec.dynamic.partition.mode=nonstrict;  -- 关闭严格模式（允许全动态分区）
  -- 注意：关闭严格模式要谨慎，确保不会误产生海量分区
```

<details>
<summary><b>展开：常见 OOM / 错误类型速查表</b></summary>

| 错误类型 | 报错关键词 | 发生位置 | 根因 | 修复方向 |
|---------|-----------|---------|------|---------|
| Executor 堆内 OOM | `OutOfMemoryError: Java heap space` | Executor JVM | 执行内存不足（JOIN/聚合数据过大） | 调 memory.fraction、增大 shuffle.partitions、减少 executor.cores |
| YARN kill（物理内存超限） | `Container killed by YARN` / `exitCode 143` | YARN NodeManager | 堆 + 堆外总物理内存超限 | 调大 `executor.memoryOverhead` |
| Metaspace OOM | `OutOfMemoryError: Metaspace` | Executor JVM | 类元数据过多（UDF/反射/CGLIB） | 设置 `-XX:MaxMetaspaceSize=512m` |
| Driver OOM | `OutOfMemoryError` in Driver | Driver JVM | collect() 拉大数据 / 广播大表 / 动态分区过多 | 改用 write 写出、检查 broadcast threshold |
| 动态分区写入 OOM | `OutOfMemoryError` at write stage | Executor JVM | 单 Task 持有过多分区写入缓冲区 | DISTRIBUTE BY 分区字段、检查分区字段是否正确 |
| StackOverflow | `StackOverflowError` | Executor / Driver | 递归过深（UDF 或 Spark 内部） | `-XX:ThreadStackSize=1m`（通过 extraJavaOptions） |
| 上游数据文件丢失 | `FileNotFoundException` (HDFS path) | Executor Task | 上游 ETL 重导覆盖分区、调度依赖不合理 | 修正调度依赖、协调重导时间窗口 |
| 权限错误 | `AccessControlException: Permission denied` | Executor / Driver | 表或 HDFS 目录无读写权限 | 申请权限、检查用户身份 |
| 动态分区严格模式 | `Dynamic partition strict mode` | Driver | 未开启动态分区参数 | `SET hive.exec.dynamic.partition=true; SET hive.exec.dynamic.partition.mode=nonstrict;` |

</details>

### 6.4 Spark UI 分析方法——ETL 优化的第一步

在优化 Spark 任务之前，先用 Spark UI 定位瓶颈，避免"凭感觉调参"。

```
Spark UI 分析清单：

① Jobs 页面：看哪些 Job 耗时最长
② Stages 页面：按耗时排序，找 TOP 5 最慢的 Stage
③ Stage 详情页：
   - Task 数量：是否合理（太多 = 小文件，太少 = 并行度不足）
   - Shuffle Read/Write：数据量是否异常大
   - Memory Spill / Disk Spill：> 0 说明内存不足
   - Task 耗时分布：最慢 Task / 中位数 > 5x = 数据倾斜
   - GC Time：占比 > 10% 说明内存压力大
④ SQL 页面：查看执行计划，确认 PartitionFilters 是否生效、
   JOIN 策略是否合理（BroadcastHashJoin vs SortMergeJoin）
```

<details>
<summary><b>展开：ETL 任务优化实战案例——从 Spark UI 到参数调优</b></summary>

**场景**：某配送团队的快送范围计算任务（核心 ETL 链路），Spark 3 引擎，137 行 SQL，涉及多表 JOIN 和聚合计算，运行时长持续增长触发超时告警。

**第一步：时间线分析**

通过调度系统和 YARN 日志，还原任务的完整时间线：

| 环节 | 耗时 | 占比 |
|------|------|------|
| 上游依赖等待 | — | — |
| YARN 队列等待 | 52 min | 46% |
| Spark 实际运行 | 58 min | 51% |
| 提交/收尾 | 3 min | 3% |

发现 YARN 等待占了 46%——这是资源层面的问题（队列在早高峰资源紧张），不是 Spark 本身的问题。

**第二步：Spark UI 定位瓶颈 Stage**

| Stage | 耗时 | Input | Memory Spill | Disk Spill |
|-------|------|-------|-------------|------------|
| Stage 8 | 25 min（占 43%） | 3.4 TB | 1751 GB | 78 GB |
| 其他 Stage | 合计 33 min | — | 少量 | — |

Stage 8 是明确的性能瓶颈，Memory Spill 高达 1751GB 说明内存严重不足。

**第三步：参数分析**

```
当前参数：
  spark.executor.memory = 8g
  spark.memory.fraction = 0.3  ← 问题根因
  spark.sql.files.maxPartitionBytes = 32MB  ← 偏小
  spark.executor.cores = 2

内存计算：8G × 0.3 = 2.4G 执行内存
Stage 8 每个 Task 平均输入 344MB，但执行内存只有 2.4G
→ 大量中间数据无法放入内存 → 触发 Spill → 磁盘 IO 成为瓶颈
```

**第四步：优化方案**

| 优先级 | 类型 | 内容 | 预期效果 |
|--------|------|------|---------|
| P0 | 纯参数 | memory.fraction 0.3 → 0.6 | Stage 8 预计 25min → 12min，零风险 |
| P1 | 纯参数 | maxPartitionBytes 32MB → 128MB | Task 数从 10123 降至 2500，减少调度开销 |
| P2 | 调度 | 错峰提交或申请扩容 | 消除 52min 队列等待 |

**结果**：任务运行时长降低约 40%，CU 消耗降低约 25%，优化从分析到验证完成仅 30 分钟。

**关键教训**：`spark.memory.fraction` 是最被忽视但性价比最高的参数——调整它不增加资源申请量，只是改变了内存的分配比例。很多团队在 OOM 时第一反应是调大 executor.memory（增加资源），其实先检查 memory.fraction 是否合理，往往就能解决问题。

</details>

### 6.5 数据倾斜——八种修复方案

数据倾斜是 Spark 任务中最常见的性能杀手。诊断标准：Stage 内 Task 倾斜分析中，最慢 Task / 中位数 > 5 倍，且耗时差值 > 10 分钟或输入量差值 > 1GB。

修复前的**必做前置步骤**：先判断是 JOIN 倾斜还是聚合倾斜——两类倾斜的修复方向完全不同，混用无效。对 JOIN Key 和 GROUP BY Key 分别做分布分析，找出热点 Key。

**诊断流程四步法：**

**Step 1：通过 Spark UI 定位倾斜 Stage**

打开 Spark UI → Stages 页面，观察每个 Stage 的 Task 耗时分布（Duration）和数据量分布（Shuffle Read Size / Input Size）。倾斜的典型表现：

- 数据倾斜：大部分 Task 在 30s 内完成，但 1~2 个 Task 耗时 10min+，Shuffle Read Size 是中位数的 10 倍以上
- 数据量过大导致 OOM：Task 直接 Failed，错误信息包含 `java.lang.OutOfMemoryError` 或 `Container killed by YARN for exceeding memory limits`

**Step 2：从 Stage 编号推算对应代码**

Spark 以 Shuffle 算子为边界划分 Stage。代码中每遇到一个宽依赖算子（`join`、`groupByKey`、`reduceByKey`、`repartition`、`distinct` 等），就会产生新的 Stage。

```
-- 假设 SQL 如下：
SELECT a.id, b.name, count(*)      -- Stage 2: 最终聚合
FROM table_a a
JOIN table_b b ON a.key = b.key    -- Stage 1: Shuffle JOIN（第一个 shuffle 边界）
GROUP BY a.id, b.name              -- Stage 2: Shuffle 聚合（第二个 shuffle 边界）

-- 如果 Spark UI 显示 Stage 1 倾斜 → 问题出在 JOIN
-- 如果 Stage 2 倾斜 → 问题出在 GROUP BY
```

**Step 3：分析 Key 分布**

```scala
// 方法 1：直接统计（数据量 < 1 亿时）
val keyCount = rdd.countByKey()
keyCount.toSeq.sortBy(-_._2).take(20).foreach(println)

// 方法 2：采样统计（数据量大时，采样 10%）
val sampled = rdd.sample(false, 0.1)
sampled.countByKey().toSeq.sortBy(-_._2).take(20).foreach(println)

// 方法 3：Spark SQL
SELECT join_key, count(*) as cnt
FROM table_a
GROUP BY join_key
ORDER BY cnt DESC
LIMIT 20
```

**Step 4：区分两种表现，选择不同修复路径**

| 表现 | 现象 | 根因 | 修复方向 |
|------|------|------|---------|
| Task 执行极慢 | Task 耗时是中位数的 5~100 倍，但最终能完成 | 少数 key 数据量远超其他 key | 打散热点 key（方案 4/7/8） |
| Task OOM | Task 直接失败，报 OOM 或被 YARN kill | 单个 key 的数据超过单 Task 可用内存 | 增大内存 + 打散（先调参再改代码） |

| 优先级 | 方案 | 适用场景 | 改动成本 |
|--------|------|---------|---------|
| 1 | 过滤倾斜 Key | null 值、测试数据等可丢弃的异常 key | 最低 |
| 2 | 提高 Shuffle 并行度 | 整体分区数不足，无单一热点 | 纯参数 |
| 3 | AQE Skew Join | JOIN 倾斜首选（Spark 3.x） | 纯参数 |
| 4 | 两阶段聚合 | GROUP BY / PARTITION BY 聚合倾斜 | SQL 改写 |
| 5 | Broadcast Join | 小表 ≤ 60MB | SQL hint |
| 6 | NULL Key 单独处理 | JOIN Key 含大量 null（> 10%） | SQL 改写 |
| 7 | 热点 Key 单独 JOIN | 少量热点 key（≤ 10 个）且不能过滤 | SQL 改写 |
| 8 | 加盐打散 | 大量热点 key、前七种均无效的兜底方案 | SQL 改写 |

> **经验法则**：优先用纯参数方案（方案 1-3），再考虑 SQL 改写（方案 4-8）。AQE Skew Join 开启方式：`SET spark.sql.adaptive.skewJoin.enabled=true`。

---

#### 方案 0：Hive ETL 预处理（上游聚合）

**适用场景**：数据源是 Hive 表，且数据本身分布不均匀。在 Hive 层提前完成聚合或 JOIN，Spark 直接读预处理后的中间表，从根源上避免 Shuffle。

两种思路：

```sql
-- 思路 1：缩小 key 粒度（预聚合，减少数据量）
-- Hive ETL 中提前按天聚合，Spark 读聚合后的结果
INSERT OVERWRITE TABLE dw.order_daily_agg PARTITION(dt)
SELECT user_id, count(*) as order_cnt, sum(amount) as total_amount, dt
FROM ods.order_detail
GROUP BY user_id, dt;

-- 思路 2：增大 key 粒度（预 JOIN，避免 Spark 中大表 JOIN）
-- Hive ETL 中提前完成大表 JOIN，Spark 读 JOIN 后的宽表
INSERT OVERWRITE TABLE dw.order_user_wide PARTITION(dt)
SELECT a.*, b.user_name, b.city
FROM ods.order_detail a
JOIN dim.user_info b ON a.user_id = b.user_id
WHERE a.dt = '${bizdate}';
```

- 优点：完全规避 Spark 中的数据倾斜，Spark 任务逻辑简化
- 缺点：治标不治本，Hive ETL 中仍然会倾斜；增加存储和链路维护成本

---

#### 方案 4：两阶段聚合（局部聚合 + 全局聚合）

**适用场景**：聚合类 Shuffle（reduceByKey、groupByKey、GROUP BY）出现倾斜。**不适用于 JOIN 类 Shuffle。**

**核心思路**：给 key 加随机前缀打散 → 局部聚合 → 去前缀 → 全局聚合。

```
数值示例：原始数据 (hello,1)(hello,1)(hello,1)(hello,1)
  ↓ 加随机前缀（范围 1~2）
(1_hello,1)(1_hello,1)(2_hello,1)(2_hello,1)
  ↓ 局部聚合（reduceByKey）
(1_hello,2)(2_hello,2)
  ↓ 去前缀
(hello,2)(hello,2)
  ↓ 全局聚合（reduceByKey）
(hello,4)
```

Spark SQL 实现：

```sql
-- 第一阶段：加随机前缀，局部聚合
SELECT split(prefix_key, '_')[1] AS real_key, sum(partial_cnt) AS cnt
FROM (
    -- 局部聚合：加前缀后聚合，将热点 key 打散到多个 Task
    SELECT concat(cast(floor(rand() * 10) as string), '_', group_key) AS prefix_key,
           count(*) AS partial_cnt
    FROM source_table
    GROUP BY concat(cast(floor(rand() * 10) as string), '_', group_key)
) tmp
-- 第二阶段：去前缀，全局聚合
GROUP BY split(prefix_key, '_')[1]
```

---

#### 方案 7：热点 Key 单独 JOIN

**适用场景**：少量热点 key（≤ 10 个）导致 JOIN 倾斜，且这些 key 不能过滤。**不适用于大量 key 都倾斜的情况。**

五步流程：

```scala
// Step 1：采样找出倾斜 key
val skewKeys = rdd.sample(false, 0.1)
  .countByKey()
  .filter(_._2 > threshold)
  .keys.toList  // 例如找到 key = "hot_user_001"

// Step 2：从大表中拆分倾斜 key 数据，打随机前缀
val skewRDD = bigRDD.filter(x => skewKeys.contains(x._1))
  .map(x => (s"${Random.nextInt(100)}_${x._1}", x._2))
val normalRDD = bigRDD.filter(x => !skewKeys.contains(x._1))

// Step 3：从小表中过滤对应 key 的数据，扩容 100 倍
val skewSmallRDD = smallRDD.filter(x => skewKeys.contains(x._1))
  .flatMap(x => (0 until 100).map(i => (s"${i}_${x._1}", x._2)))
val normalSmallRDD = smallRDD

// Step 4：分别 JOIN
val skewResult = skewRDD.join(skewSmallRDD)
  .map { case (k, v) => (k.split("_", 2)(1), v) }  // 去前缀
val normalResult = normalRDD.join(normalSmallRDD)

// Step 5：union 合并结果
val finalResult = skewResult.union(normalResult)
```

---

#### 方案 8：加盐打散（随机前缀扩容 JOIN）

**适用场景**：大量 key 倾斜，方案 7 不适用时的兜底方案。

**核心思路**：一侧打随机前缀（1~N），另一侧扩容 N 倍（打 0~N 顺序前缀），JOIN 后去前缀。

```sql
-- 大表：每条数据打随机前缀（1~10）
SELECT /*+ SHUFFLE_HASH(b) */
    a.id, a.amount, b.user_name
FROM (
    SELECT concat(cast(floor(rand() * 10) as string), '_', user_id) AS salted_key,
           id, amount
    FROM big_table
) a
JOIN (
    -- 小表：每条数据扩容 10 倍（打 0~9 顺序前缀）
    SELECT concat(cast(idx as string), '_', user_id) AS salted_key,
           user_name
    FROM small_table
    LATERAL VIEW explode(array(0,1,2,3,4,5,6,7,8,9)) t AS idx
) b
ON a.salted_key = b.salted_key
```

- 适用：大量 key 倾斜，无法逐一处理
- 局限性：只能缓解不能彻底解决；内存消耗大（小表膨胀 N 倍）；N 值需要调优
- 实际案例：优化前 60 分钟 → 优化后 10 分钟

---

#### 方案 6+8 组合优化（实际项目最常用）

实际项目中，通常只有少数 key 严重倾斜、大部分 key 分布相对均匀。最实用的策略是组合使用：提取倾斜 key 用加盐打散处理，普通 key 正常 JOIN，最后 union。

```sql
-- Step 1：识别倾斜 key（提前统计或采样）
-- 假设已知倾斜 key 列表存入临时表 skew_keys

-- Step 2：倾斜部分——加盐打散 JOIN
SELECT id, amount, user_name
FROM (
    SELECT concat(cast(floor(rand() * 10) as string), '_', user_id) AS salted_key,
           id, amount
    FROM big_table
    WHERE user_id IN (SELECT key FROM skew_keys)
) a
JOIN (
    SELECT concat(cast(idx as string), '_', user_id) AS salted_key, user_name
    FROM small_table
    LATERAL VIEW explode(array(0,1,2,3,4,5,6,7,8,9)) t AS idx
    WHERE user_id IN (SELECT key FROM skew_keys)
) b ON a.salted_key = b.salted_key

UNION ALL

-- Step 3：正常部分——直接 JOIN
SELECT id, amount, user_name
FROM big_table a
JOIN small_table b ON a.user_id = b.user_id
WHERE a.user_id NOT IN (SELECT key FROM skew_keys)
```

这种组合策略的优势：只对倾斜 key 做扩容，避免全量数据膨胀；普通 key 走正常 JOIN 路径，不引入额外开销。

### 6.6 优化效果评估

优化有两个方向——节约资源和加快速度——但它们不总是一致的（比如增加 Executor 可以加速但会消耗更多 CU）。评估标准要根据任务的重要性来定：

```
大 V 任务（核心链路卡点）：以尽快就绪为主，给充足资源，保证下游按时启动
SLA 任务（有承诺就绪时间）：以按时就绪为主，在保证 buffer 的前提下优化资源
普通任务：以节省资源为主，使用默认参数即可
```

### 6.7 Shuffle 底层机制与调优参数

Shuffle 是 Spark 性能的关键分水岭——理解 ShuffleManager 的演进和调优参数，是从"会写 Spark SQL"到"能调优 Spark 任务"的必经之路。前面几节讲了"怎么调"，这一节讲"为什么这么调"。

#### ShuffleManager 演进历史

Spark 的 Shuffle 管理器经历了三代演进，每一代的核心目标都是减少磁盘文件数量、降低随机 I/O。

**第一代：HashShuffleManager（Spark 1.2 以前默认）**

每个 Map Task 为下游每个 Reduce Task 创建一个独立的磁盘文件。文件数量公式：

```
文件数 = Map Task 数 × Reduce Task 数
```

举例：上游 Stage 有 50 个 Map Task，下游 Stage 有 100 个 Reduce Task，则产生 50 × 100 = 5000 个磁盘文件。大量小文件导致频繁的随机磁盘 I/O 和文件句柄开销，是这一代 Shuffle 的核心瓶颈。

**优化版：HashShuffleManager + consolidate 机制**

开启 `spark.shuffle.consolidateFiles=true` 后，引入 shuffleFileGroup 复用机制：同一 Executor 上同一 CPU Core 先后执行的多个 Task 复用同一批磁盘文件。文件数量公式变为：

```
文件数 = 总 Core 数 × Reduce Task 数
（总 Core 数 = Executor 数 × 每个 Executor 的 Core 数）
```

关键区别：未优化时每个 Task 创建一组文件，consolidate 后每个 Core 创建一组文件（同一 Core 上先后执行的多批 Task 共享）。

举例：10 个 Executor，每个 1 个 Core（共 10 个 Core），50 个 Map Task 分 5 批执行，100 个 Reduce Task：

| 对比项 | 未优化 | consolidate 后 |
|--------|--------|----------------|
| 每组文件数 | 100（= Reduce Task 数） | 100（= Reduce Task 数） |
| 每组被几个 Task 使用 | 1 个 Task | 5 个 Task（同 Core 上的 5 批） |
| 每 Executor 文件数 | 5 × 100 = 500 | 1 × 100 = 100 |
| 总文件数 | 10 × 500 = 5000 | 10 × 100 = 1000 |
| 减少比例 | — | 80% |

> **注意**：consolidate 的收益取决于 Map Task 数与总 Core 数的比值（即批次数）。上例中 50 Task / 10 Core = 5 批，所以文件数减少为原来的 1/5。如果 Map Task 数 = 总 Core 数（每个 Core 只跑一个 Task），consolidate 不会减少文件数。

**第二代：SortShuffleManager（Spark 1.2 以后默认）**

每个 Map Task 只产生一个数据文件（data file）和一个索引文件（index file），文件数量大幅减少：

```
文件数 = Map Task 数 × 2（1 个数据文件 + 1 个索引文件）
```

同样以 50 个 Map Task、100 个 Reduce Task 为例：50 × 2 = 100 个文件（对比 HashShuffleManager 的 5000 个，减少 98%）。

SortShuffleManager 有两种运行机制：

**普通机制（Normal Sort Shuffle）**：
1. 每个 Map Task 将数据写入内存数据结构（PartitionedAppendOnlyMap 或 PartitionedPairBuffer）
2. 当内存不足时，按 Partition ID 排序后溢写到磁盘（spill），生成临时文件
3. 所有数据写入完成后，将多个溢写文件 merge 成一个最终数据文件，并生成对应的索引文件
4. 下游 Reduce Task 通过索引文件定位自己对应的数据区间

**bypass 机制（Bypass Merge Shuffle）**：
- 触发条件：Reduce Task 数 ≤ `spark.shuffle.sort.bypassMergeThreshold`（默认 200）且不需要 Map 端聚合
- 与普通机制的区别：跳过排序步骤，直接为每个 Partition 写一个临时文件，最后 merge 成一个输出文件
- 优势：省去排序开销，适合小规模 Shuffle 场景

**三种 ShuffleManager 文件数量对比**：

| ShuffleManager | 文件数量公式 | 示例（50 Map Task, 100 Reduce Task, 10 Executor × 1 Core） | 核心优势 / 问题 |
|----------------|-------------|-----------------------------------------------------------|----------------|
| HashShuffleManager | Map Task 数 × Reduce Task 数 | 50 × 100 = 5000 | 实现简单；磁盘文件爆炸，随机 I/O 严重 |
| HashShuffleManager (consolidate) | 总 Core 数 × Reduce Task 数 | 10 × 100 = 1000 | 减少 80%；仍依赖 Core 数，文件可能仍多 |
| SortShuffleManager | Map Task 数 × 2 | 50 × 2 = 100 | 文件数最少；引入排序开销 |

> **演进本质**：从"每 Task 每分区一个文件"到"每 Core 每分区一个文件"再到"每 Task 一个文件"，每次演进都将文件数量降了一个量级。SortShuffleManager 用"排序 + 索引"的确定性开销换来了文件数量的指数级下降，在工程上几乎总是划算的——这也是它成为默认方案的原因。

#### Shuffle 调优参数

以下参数按 Shuffle Write 侧、Shuffle Read 侧、通用配置三类组织，覆盖日常调优中最常用的 7 个参数。

| 参数 | 默认值 | 说明 | 调优建议 | 预期效果 |
|------|--------|------|---------|---------|
| `spark.shuffle.file.buffer` | 32k | Shuffle Write 阶段每个 Shuffle 文件的内存缓冲区。缓冲区越大，溢写磁盘的次数越少 | 调大到 64k–128k，减少磁盘写次数。对 I/O 密集型任务效果显著 | 磁盘写次数减少 30%–50%，Shuffle Write 耗时降低 10%–20% |
| `spark.reducer.maxSizeInFlight` | 48m | Shuffle Read 阶段每个 Reduce Task 从远程拉取数据的最大并行缓冲区。决定每次能并行拉取多少数据 | 适当调大到 96m 可减少拉取轮次，但会占用更多内存。注意：每对节点间会分配该大小的缓冲区，总内存 = 缓冲区 × 并行连接数 | 拉取轮次减少，Shuffle Read 耗时降低 5%–15% |
| `spark.shuffle.io.maxRetries` | 3 | Shuffle Read 拉取数据失败时的最大重试次数。GC 停顿或网络抖动可能导致拉取失败 | 调大到 5–8，应对 GC 长停顿场景。配合 retryWait 一起调整 | 减少因瞬时故障导致的任务失败，避免整 Stage 重试 |
| `spark.shuffle.io.retryWait` | 5s | 每次重试的等待间隔，给被拉取方恢复的时间 | 适当增大到 10s–15s，避免对端 GC 未恢复就重试 | 配合 maxRetries 使用，显著降低拉取失败率 |
| `spark.shuffle.memoryFraction` | 0.2 | Shuffle 聚合操作（如 reduceByKey 的 combine）可使用的执行内存比例 | 聚合密集型任务可调到 0.3，但需确保留足执行内存。Spark 3.x 统一内存管理下此参数已被 `spark.memory.fraction` 体系取代 | 减少聚合阶段的溢写次数，降低 10%–30% 磁盘 I/O |
| `spark.shuffle.sort.bypassMergeThreshold` | 200 | 当 Reduce 分区数 ≤ 该阈值且无 Map 端聚合时，SortShuffleManager 使用 bypass 机制（跳过排序） | 小规模 Shuffle（分区数 < 200）时，bypass 可省排序开销。若分区数略超阈值，可适当调大到 300–400 | 减少排序 CPU 开销，小任务提升 5%–10% |
| `spark.sql.shuffle.partitions` | 200 | Spark SQL / DataFrame 操作的 Shuffle 分区数（非 RDD API） | 根据数据量调整：每分区 128MB–256MB 为宜。1TB 数据约需 4000–8000 分区。开启 AQE 后可设较大值由 AQE 自动合并 | 分区过多导致调度开销大，分区过少有 OOM 风险。合理设置可提升 10%–30% |

**调优实践要点**：

```
1. 优先调 spark.sql.shuffle.partitions——性价比最高
   - 分区数 = 数据量 / 目标单分区大小（128MB-256MB）
   - 开启 AQE 后，初始分区可设大，让 AQE 自动合并

2. shuffle.file.buffer 和 reducer.maxSizeInFlight 是 I/O 层调优
   - 磁盘 I/O 是瓶颈时优先调 shuffle.file.buffer（Write 侧）
   - 网络拉取是瓶颈时优先调 reducer.maxSizeInFlight（Read 侧）

3. maxRetries 和 retryWait 是稳定性调优，不影响正常性能
   - 只在出现 FetchFailed 异常时才需要调整
   - 常见于 Executor GC 长停顿或集群负载高的场景

4. bypassMergeThreshold 是小任务优化，大任务无需关注
   - 只对分区数 < 200 的小 Shuffle 生效
   - 开启 AQE 后该参数影响减小
```

> **经验总结**：Shuffle 调优的本质是在"磁盘 I/O、网络传输、内存占用"三者间找平衡。没有万能参数，但 `spark.sql.shuffle.partitions` + AQE 的组合能解决 80% 的 Shuffle 性能问题。理解 ShuffleManager 演进的意义在于：知道为什么 SortShuffleManager 是默认的——它用排序的确定性开销换来了文件数量的量级下降，这在工程上几乎总是划算的。

---

[← 返回 Spark 主文档](./04-Spark.md)

