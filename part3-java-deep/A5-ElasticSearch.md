# 附录 A5：ElasticSearch——全文检索与实时分析引擎

> **一句话定位**：MySQL 擅长事务和精确查询，但一旦涉及**全文搜索**（"包含关键词的所有商品"）、**模糊匹配**（纠错、同义词）、**海量数据聚合分析**（实时统计、日志分析），MySQL 的 LIKE '%keyword%' 就力不从心了。ElasticSearch（简称 ES）就是为这些场景而生的分布式搜索与分析引擎。

---

## 一、核心概念——ES 和 MySQL 的类比

| MySQL 概念 | ES 概念 | 说明 |
|-----------|---------|------|
| Database | Index（索引） | 一类数据的集合 |
| Table | Type（7.x 已废弃） | ES 7.x 起一个 Index 只有一个 Type（`_doc`） |
| Row | Document（文档） | 一条数据，JSON 格式 |
| Column | Field（字段） | 文档中的一个属性 |
| Schema | Mapping | 字段名、类型、分词器等定义 |
| SQL 查询 | DSL 查询（JSON） | 用 JSON 描述查询条件 |
| Index（索引） | 倒排索引 | ES 的核心数据结构 |

> **关键认知差异**：MySQL 的"索引"是辅助查找的 B+ 树，ES 的"Index"是数据集合的名字，而 ES 底层用的是**倒排索引**——完全不同的东西。

---

## 二、倒排索引——ES 为什么快

### 2.1 正排索引 vs 倒排索引

```
正排索引（MySQL 的思路）：
文档 → 包含哪些词
  doc1: "Java 并发编程"
  doc2: "Java 集合框架"
  doc3: "Python 并发"

倒排索引（ES 的思路）：
词 → 出现在哪些文档
  "Java"   → [doc1, doc2]
  "并发"   → [doc1, doc3]
  "编程"   → [doc1]
  "集合"   → [doc2]
  "框架"   → [doc2]
  "Python" → [doc3]
```

当你搜索"Java 并发"时，ES 查倒排索引取"Java"的文档列表 `[doc1, doc2]` 和"并发"的文档列表 `[doc1, doc3]`，取交集得 `[doc1]`——两次哈希查找 + 一次交集运算，不需要遍历所有文档。

### 2.2 分析器（Analyzer）与分词器（Tokenizer）

倒排索引的质量取决于**分词**——把文本拆成什么样的词条（Term）。

```
Standard Analyzer:  "Hello World" → ["hello", "world"]（英文按空格分，转小写）
IK Analyzer:        "中华人民共和国" → ["中华人民共和国", "中华", "人民", "共和国"]（中文分词）
```

**Analyzer 和 Tokenizer 不是一回事**——很多中文资料把两者都译作"分词器"，这是混淆的根源。**本书统一采用以下译名，并在正文中优先使用英文原词**：

| 英文原词 | 本书译名 | 职责 | 数量 |
|---------|---------|------|------|
| **Analyzer** | **分析器** | 完整的文本处理流水线（下面三者的组合） | 字段上配置的就是它 |
| **Character Filter** | **字符过滤器** | 切分**前**预处理原始字符串 | 0 个或多个 |
| **Tokenizer** | **分词器** | 真正执行切分，产出 Token | **有且仅有 1 个** |
| **Token Filter** | **词条过滤器** | 对切好的 Token 做加工 | 0 个或多个，**有序** |

也就是说：**"分词器"这个中文词应当专指 Tokenizer；Analyzer 请叫"分析器"**。Analyzer 是整条流水线，Tokenizer 只是其中负责切分的那一环。

```
Analyzer（分析器）= 一条三段式流水线
┌──────────────────────────────────────────────────────────┐
│                                                          │
│  ① Character Filter（字符过滤器）  0 个或多个             │
│     在切分前预处理原始字符串                               │
│     例：去 HTML 标签、字符替换（& → and）                  │
│                        ↓                                 │
│  ② Tokenizer（分词器）           有且仅有 1 个 ← 必需     │
│     真正把字符串切成词条（Token）                          │
│     例：按空格切、按 IK 词典切                             │
│                        ↓                                 │
│  ③ Token Filter（词条过滤器）     0 个或多个              │
│     对切好的词条做加工                                     │
│     例：转小写、去停用词、同义词扩展、词干还原              │
│                                                          │
└──────────────────────────────────────────────────────────┘
                        ↓
                  最终写入倒排索引的 Term（词条）
```

举个完整例子，处理 `"<p>The QUICK brown Foxes</p>"`：

```
原始文本： <p>The QUICK brown Foxes</p>
    ↓ ① char_filter: html_strip（去标签）
           The QUICK brown Foxes
    ↓ ② tokenizer: standard（按词边界切）
           [The] [QUICK] [brown] [Foxes]
    ↓ ③ token_filter: lowercase（转小写）
           [the] [quick] [brown] [foxes]
    ↓ ③ token_filter: stop（去停用词）
           [quick] [brown] [foxes]
    ↓ ③ token_filter: stemmer（词干还原）
           [quick] [brown] [fox]     ← 最终入库的 Term
```

所以「`ik_max_word` 是 analyzer 还是 tokenizer」这个问题的答案是：**两者都是**。IK 插件同时注册了同名的 analyzer 和 tokenizer——`ik_max_word` 作为 analyzer 是"开箱即用的完整流水线"，作为 tokenizer 则是"可被组合的切分组件"。当你需要在 IK 切分基础上再加同义词时，用的就是它的 tokenizer 身份：

```json
"analyzer": {
  "my_analyzer": {
    "char_filter": ["html_strip"],      // ① 可选，可多个
    "tokenizer":   "ik_smart",          // ② 必需，只能一个 ← IK 在这里是 tokenizer
    "filter":      ["lowercase", "my_synonym"]   // ③ 可选，可多个，有序
  }
}
```

> **一句话记忆**：Tokenizer 是"切"，Token Filter 是"改"，Character Filter 是"切之前先洗"，Analyzer 是"把这三件事串起来的那个整体"。**配置 mapping 时字段上写的 `"analyzer": "xxx"` 永远只能填 Analyzer，不能直接填 Tokenizer**（填了会报 `analyzer [xxx] not found`）。

常见的开箱即用 Analyzer（已预先组装好三个环节，可直接在字段上引用）：

| Analyzer | 适用场景 |
|--------|---------|
| Standard | 英文默认，按空格分词 |
| IK（ik_max_word / ik_smart） | 中文分词（最细粒度 / 智能切分） |
| Keyword | 不分词，整个字段作为一个词条（适合 ID、状态码） |
| Whitespace | 按空格分词，不转小写 |

---

## 三、集群架构

### 3.1 核心概念

```
Cluster（集群）
  └── Node（节点）：一个 ES 实例
       └── Index（索引）：一类数据
            └── Shard（分片）：索引的物理分割
                 ├── Primary Shard（主分片）：数据的原始存储
                 └── Replica Shard（副本分片）：主分片的拷贝
```

上面是**逻辑上的包含关系**，但分片真正的价值在于**物理上的分布**。以一个 3 节点集群、索引配置 3 主分片 + 1 副本（共 6 个分片）为例：

```
                        ES Cluster
┌─────────────────┬─────────────────┬─────────────────┐
│     Node-1      │     Node-2      │     Node-3      │
│  ┌───────────┐  │  ┌───────────┐  │  ┌───────────┐  │
│  │  P0  主   │  │  │  P1  主   │  │  │  P2  主   │  │
│  └───────────┘  │  └───────────┘  │  └───────────┘  │
│  ┌───────────┐  │  ┌───────────┐  │  ┌───────────┐  │
│  │  R2  副   │  │  │  R0  副   │  │  │  R1  副   │  │
│  └───────────┘  │  └───────────┘  │  └───────────┘  │
└─────────────────┴─────────────────┴─────────────────┘
   P = Primary（主分片）    R = Replica（副本分片）
   R0 是 P0 的副本，R1 是 P1 的副本，R2 是 P2 的副本
```

这张图有三个关键点：

**① 主副本永不同机**。P0 在 Node-1，它的副本 R0 就一定在 Node-2 或 Node-3。这是 ES 的硬性约束——否则节点一挂，主副本一起丢，副本就失去意义了。

**② 一个文档只属于一个主分片**。写入 `doc_id=1001` 时，`hash(1001) % 3` 算出落到 P1，那这条数据就只在 Node-2 的 P1（和它在别处的副本 R1）里。这也解释了为什么主分片数不可改——改了之后所有老数据的 hash 路由结果全变，等于全部失效。

**③ 查询是分散-聚合（Scatter-Gather）**。搜索请求到达任意节点（该节点成为本次的协调节点），它把请求**并行**发给 P0/P1/P2 各一份（或者它们的副本，走轮询负载均衡），每个分片在本地独立算出 Top N，返回给协调节点做全局归并排序。这就是 ES 能横向扩展的根本原因，也是[深分页](#68-深分页的三种方案)慢的根本原因。

节点故障时的自愈过程：

```
Node-2 宕机
      ↓
① 集群状态变 YELLOW（P1 丢失，但 Node-3 上有 R1）
      ↓
② Master 把 Node-3 的 R1 提升为主分片 P1
      ↓
③ 在 Node-1 上重建一个新的 R1 副本
      ↓
④ 集群状态恢复 GREEN
```

> **集群健康状态**：GREEN = 所有主分片和副本都正常；YELLOW = 主分片都在，但有副本未分配（数据没丢，但少了冗余）；RED = 有主分片丢失（部分数据不可用）。单节点集群配了副本一定是 YELLOW，因为副本无处安放。

**分片策略**：一个 Index 的数据被切分为多个 Shard，分散到不同 Node 上。查询时并行查各分片再合并结果。主分片数量在创建 Index 时确定，**不可更改**（ES 7.x 默认 1 个主分片）；副本数量可以动态调整。副本有双重作用——既是**容灾冗余**，也能**分担读流量**（查询可以走主分片也可以走副本）。

### 3.2 写入流程

```
① 客户端请求发到任意节点（协调节点）
② 协调节点根据 routing（默认 hash(doc_id) % 主分片数）定位目标主分片
③ 主分片写入成功后，转发给副本分片
④ 副本确认后，返回客户端成功
```

### 3.3 近实时（Near Real-Time）原理

ES 写入后**不是立即可搜**，而是有约 1 秒的延迟（`refresh_interval`）。写入流程：数据先进 in-memory buffer → 每秒 refresh 到一个新的 segment（Lucene 可搜索的最小单元）→ 后台 merge 合并小 segment。这就是"近实时"的由来。

---

## 四、DSL 查询入门

### 4.1 查询分类

| 类型 | 说明 | 示例场景 |
|------|------|---------|
| **match** | 全文匹配（先分词再查倒排索引） | 搜索框输入关键词 |
| **term** | 精确匹配（不分词） | 按状态码、ID 过滤 |
| **range** | 范围查询 | 价格区间、日期范围 |
| **bool** | 组合查询（must/should/must_not/filter） | 多条件组合 |
| **aggs** | 聚合分析 | 统计、分组、Top N |

### 4.2 查询示例

```json
// 搜索标题包含"Java"且价格在 50-100 之间的书
{
  "query": {
    "bool": {
      "must": [
        { "match": { "title": "Java" } }
      ],
      "filter": [
        { "range": { "price": { "gte": 50, "lte": 100 } } }
      ]
    }
  },
  "sort": [{ "price": "asc" }],
  "from": 0,
  "size": 10
}
```

> **must vs filter**：`must` 参与相关性评分（`_score`），`filter` 只做过滤不评分且会被缓存。对于不需要评分的条件（如日期范围、状态过滤），用 `filter` 性能更好。

### 4.3 聚合（Aggregation）

```json
// 按品牌分组统计销量，并计算平均价格
{
  "size": 0,
  "aggs": {
    "brands": {
      "terms": { "field": "brand.keyword" },
      "aggs": {
        "avg_price": { "avg": { "field": "price" } },
        "total_sales": { "sum": { "field": "sales" } }
      }
    }
  }
}
```

---

## 五、相关性评分与 BM25

前面讲的都是"能不能搜到"，这一节讲"搜到的怎么排序"——这是搜索引擎和普通数据库最本质的区别。MySQL 的 `WHERE` 只有匹配/不匹配两种状态，而 ES 会给每个匹配的文档算一个 `_score` 相关性分数。

### 5.1 从 TF-IDF 到 BM25

ES 5.x 之后默认的评分算法是 **BM25**（Best Matching 25），它是 TF-IDF 的改进版。理解 BM25 只需要抓住三个直觉：

```
① 词频（TF）：一个词在文档里出现越多，越相关
   但要有"饱和"——出现 100 次不该比出现 10 次相关 10 倍

② 逆文档频率（IDF）：一个词在整个索引里越罕见，越有区分度
   "的""是"到处都有 → 权重极低
   "布隆过滤器"很罕见 → 命中了就很说明问题

③ 文档长度归一化：短文档命中该词，比长文档命中更有意义
   一篇 20 字的标题命中"Java" vs 一篇 5000 字的文章命中"Java"
```

BM25 公式（面试能说清三个因子的作用即可，不要求背）：

```math
\text{score}(D, Q) = \sum_{q_i \in Q} \text{IDF}(q_i) \cdot \frac{f(q_i, D) \cdot (k_1 + 1)}{f(q_i, D) + k_1 \cdot \left(1 - b + b \cdot \frac{|D|}{\text{avgdl}}\right)}
```

其中：

| 符号 | 含义 |
|------|------|
| $f(q_i, D)$ | 词 $q_i$ 在文档 $D$ 中的出现次数（词频 TF） |
| $\lvert D \rvert$ | 文档 $D$ 的长度（词条数） |
| $\text{avgdl}$ | 索引中所有文档的平均长度（average document length） |
| $\text{IDF}(q_i)$ | 逆文档频率，衡量词 $q_i$ 的区分度 |
| $k_1$ | 词频饱和参数，ES 默认 `1.2` |
| $b$ | 文档长度归一化强度，ES 默认 `0.75` |

IDF 部分 Lucene 用的是带平滑的变体，保证结果恒为正数：

```math
\text{IDF}(q_i) = \ln\left(1 + \frac{N - n(q_i) + 0.5}{n(q_i) + 0.5}\right)
```

其中 $N$ 是索引总文档数，$n(q_i)$ 是包含词 $q_i$ 的文档数。**词越罕见，$n(q_i)$ 越小，IDF 越大**——这就是"布隆过滤器"比"的"权重高的数学来源。

把公式拆开看，三个因子各司其职：

```math
\underbrace{\text{IDF}(q_i)}_{\text{词的稀有度}} \cdot \frac{\overbrace{f(q_i,D)}^{\text{词频}}\cdot(k_1+1)}{f(q_i,D) + k_1\cdot\underbrace{\left(1-b+b\cdot\frac{|D|}{\text{avgdl}}\right)}_{\text{长度归一化}}}
```

**为什么这个分式能实现"饱和"**：当 $f(q_i,D) \to \infty$ 时，分子分母同阶，整体趋近于常数 $k_1 + 1$。也就是说**无论一个词出现多少次，单个词的得分都有上限**，这正是 BM25 相比 TF-IDF 最关键的改进。

> **GitHub 数学公式渲染**：上面用的是 GitHub 自 2022 年起原生支持的 LaTeX 语法——块级公式用 ` ```math ` 代码块（或 `$$...$$`），行内公式用 `$...$`，底层由 KaTeX 渲染。本地用 VS Code 预览需要装 Markdown+Math 类插件。

> **"25" 是什么意思**：纯粹是**实验编号**，没有数学含义。BM25 出自 Robertson 与 Spärck Jones 的概率检索模型研究，他们在 TREC 评测中依次试验了 BM1、BM11、BM15…… 其中 **BM11** 是文档长度完全归一化（相当于 $b=1$），**BM15** 是完全不归一化（相当于 $b=0$），而 **BM25 用参数 $b$ 在两者之间做插值**——把 $b=1$ 和 $b=0$ 代入长度归一化因子 $1-b+b\cdot\frac{|D|}{\text{avgdl}}$ 就能验证。第 25 号配置在评测中效果最好，于是成了工业标准。面试时顺口带一句"$b$ 本质是在 BM11 和 BM15 之间插值"，比只说"b 控制长度归一化"显得理解更深。

### 5.2 k1 与 b 两个调优参数

这是 BM25 调优的全部旋钮，也是面试高频追问点：

| 参数 | 默认值 | 控制什么 | 调大的效果 | 调小的效果 |
|------|--------|---------|-----------|-----------|
| **k1** | 1.2 | **词频饱和速度** | 词频影响更大、饱和更慢 | 更快饱和，出现 1 次和 10 次差别小 |
| **b** | 0.75 | **文档长度归一化强度** | 更严厉惩罚长文档 | `b=0` 时完全忽略文档长度 |

```
TF 饱和曲线的直观理解（k1 的作用）：

  TF-IDF：词频线性增长 → 出现 100 次的分数是出现 1 次的 100 倍（不合理）
  BM25：  词频增长会饱和 → 前几次出现贡献大，后面边际递减

  分数
   │        ╭──────────  k1 大（饱和慢）
   │      ╭─╯
   │   ╭──╯──────────    k1 小（很快饱和）
   │ ╭─╯
   └─────────────────→ 词频 TF
```

**"饱和"（Saturation）是什么意思**——这个词借自化学的「饱和溶液」：往水里加盐，加到一定程度就再也溶不进去了，再加多少都没用。BM25 里指的是**一个词出现次数越多，每多出现一次带来的分数增益越小，最终逼近一个上限**。

用具体数字看最清楚。取 $k_1 = 1.2$、$b = 0$（暂时忽略长度归一化），词频项就是：

```math
\frac{f \cdot (k_1+1)}{f + k_1} = \frac{2.2f}{f + 1.2}
```

| 词频 $f$ | TF-IDF（线性） | BM25 得分 | **这一次**出现的边际增益 |
|---------|--------------|----------|---------------------|
| 1 | 1 | 1.00 | +1.0000 |
| 2 | 2 | 1.38 | +0.3750 |
| 3 | 3 | 1.57 | +0.1964 |
| 5 | 5 | 1.77 | +0.0819 |
| 10 | 10 | 1.96 | +0.0231 |
| 100 | 100 | 2.17 | +0.0003 |
| $\infty$ | $\infty$ | **2.20** | 0 |

第 1 次出现值 1.00 分，第 2 次只加 0.375，第 10 次几乎不加了。**上限恒为 $k_1 + 1 = 2.2$，无限逼近但永远达不到。**

**为什么必须饱和**——看不饱和会发生什么：

```
搜索 "Java"

文档 A：《Java 入门教程》，正文 500 字，"Java" 出现 10 次
        TF-IDF 得分 = 10

文档 B：SEO 垃圾页，"Java Java Java..." 重复 1000 次
        TF-IDF 得分 = 1000   ← 排第一

线性计分 = 公开宣告"堆砌关键词就能上首页"
这正是早年搜索引擎被 keyword stuffing 刷爆的根源
```

换成 BM25：A 得 1.96，B 得 2.20，差距从 **100 倍压缩到 1.12 倍**；再叠加长度归一化（B 是超长文档，$\frac{|D|}{\text{avgdl}}$ 极大，被重罚），B 反而排到 A 后面。

这个设计其实符合人的常识：一篇文章提到"布隆过滤器"从 **1 次变 10 次**，性质变了——从"顺带一提"变成"这是主题"；但从 **100 次变 200 次**，只能说明作者话多，不代表更相关。**判断"这篇是不是讲这个的"，前几次出现就够了。**

$k_1$ 正是控制饱和速度的旋钮，同样在 $f = 10$ 时对比：

| $k_1$ | $f=10$ 时得分 | 上限 $k_1+1$ | 含义 |
|------|-------------|------------|------|
| 0 | 1.00 | 1.0 | 极端：只看"有没有"，出现 1 次和 100 次完全等价 |
| 0.5 | 1.43 | 1.5 | 快速饱和，适合短文本 |
| **1.2** | **1.96** | **2.2** | **ES 默认** |
| 2.0 | 2.50 | 3.0 | 饱和慢，词频影响更大 |
| 5.0 | 4.00 | 6.0 | 接近线性，退化回 TF-IDF 的行为 |

**什么时候需要调**：

```
b 调小（甚至设 0）的场景：
  → 字段本身长度差异大但不代表相关性，如"商品标题"
  → 长标题不该被惩罚（"2024新款秋冬加厚保暖男士羽绒服" 不比 "羽绒服" 差)

k1 调小的场景：
  → 文档里关键词堆砌严重（SEO 垃圾内容）
  → 让重复出现的收益快速饱和，压制堆砌
```

```json
// 在 mapping 里为索引自定义 BM25 参数
PUT /products
{
  "settings": {
    "index": {
      "similarity": {
        "my_bm25": {
          "type": "BM25",
          "k1": 1.2,
          "b": 0.3          // 商品标题场景，弱化长度惩罚
        }
      }
    }
  },
  "mappings": {
    "properties": {
      "title": { "type": "text", "similarity": "my_bm25" }
    }
  }
}
```

> **调优前必读**：BM25 参数调优的收益，通常远小于「改进分词」（见 [6.1](#61-分词器深入ik-的工程用法)）和「加 Rerank」（见 [6.10](#610-怎么给-es-加-rerank)）。**先把分词和同义词做好，再考虑动 k1/b**。而且改了 `similarity` 需要重建索引，成本不低。

### 5.3 用 explain 排查评分

排查"为什么这条排在前面"的唯一正确方法：

```json
GET /products/_search
{
  "explain": true,
  "query": { "match": { "title": "无线耳机" } }
}

// 返回的 _explanation 会逐层拆解：
//   weight(title:无线) → idf × tf 各自的值
//   weight(title:耳机) → idf × tf 各自的值
//   最终 score = 各项之和
```

---

## 六、搜索工程实践——集群调优与性能优化

前面几节是"ES 是什么"，这一节是"ES 上生产要注意什么"。这部分内容在搜索平台类岗位的面试里几乎必问。

### 6.1 分词器深入——IK 的工程用法

[2.2](#22-分析器analyzer与分词器tokenizer) 介绍了 IK 的两种模式，生产环境真正的工作量在**词典维护**上。

**`ik_max_word` vs `ik_smart` 的正确用法**——两者不是二选一，而是配合使用：

```json
PUT /articles
{
  "mappings": {
    "properties": {
      "title": {
        "type": "text",
        "analyzer": "ik_max_word",        // 索引时：最细粒度，切出尽可能多的词
        "search_analyzer": "ik_smart"     // 搜索时：智能切分，避免过度扩展
      }
    }
  }
}
```

```
为什么索引和搜索用不同分词器：

  索引 "中华人民共和国国歌" 用 ik_max_word：
    → [中华人民共和国, 中华人民, 中华, 华人, 人民共和国, 人民, 共和国, 国歌]
    → 词条多 = 召回机会多

  搜索 "中华人民共和国" 用 ik_smart：
    → [中华人民共和国]
    → 不过度切分，避免召回大量只匹配"人民"的无关文档
```

**自定义词典**——这是企业场景下投入产出比最高的一环，因为业务黑话、产品名、内部缩写 IK 的默认词典绝对没有：

IK 一共有**四个**配置项，按「词典用途」× 「本地/远程」两两组合：

```xml
<!-- config/IKAnalyzer.cfg.xml -->
<properties>
    <!-- ① 本地扩展词典：值是相对 config 目录的【文件路径】 -->
    <entry key="ext_dict">custom/business.dic</entry>

    <!-- ② 本地扩展停用词典：同样是【文件路径】 -->
    <entry key="ext_stopwords">custom/stopword.dic</entry>

    <!-- ③ 远程扩展词典：值是【HTTP URL】，支持热更新 -->
    <entry key="remote_ext_dict">http://config-server/dict/hot.dic</entry>

    <!-- ④ 远程扩展停用词典：值也是【HTTP URL】 -->
    <entry key="remote_ext_stopwords">http://config-server/dict/hot_stop.dic</entry>
</properties>
```

|  | 扩展词典（要成词的） | 停用词典（要丢弃的） |
|---|---|---|
| **本地**（文件路径，改后需重启） | `ext_dict` | `ext_stopwords` |
| **远程**（HTTP URL，60 秒热更新） | `remote_ext_dict` | `remote_ext_stopwords` |

> **"远程"指的就是 URL**——`ext_dict` / `ext_stopwords` 的值是**节点本地文件路径**（相对 IK 插件的 `config` 目录），改完必须重启节点才生效；带 `remote_` 前缀的两个才是 **HTTP URL**，由 IK 定时轮询、无需重启。两组可以同时配置：稳定的基础词库放本地，高频变动的业务词放远程。
>
> **多个词典文件用英文分号分隔**：`<entry key="ext_dict">custom/a.dic;custom/b.dic</entry>`。

**词典文件长什么样**——结构简单到令人意外：**一行一个词，没有权重、没有词性、没有分隔符**。

```
# custom/business.dic —— 扩展词典
# 以 # 开头的是注释行
布隆过滤器
一致性哈希
熔断降级
美团外卖
青龙系统
SKU
LBS
```

```
# custom/stopword.dic —— 停用词典（Stop Words）
的
了
是
在
和
```

> **"停用词"是什么意思，为什么叫"停"**——这个译名容易让人误以为是"遇到它就停止切分"，**并不是**。Stop Word 指的是**高频出现但几乎不携带检索价值的词**（中文的"的、了、是"，英文的 the、a、is）。IK 的处理方式是：**照常切分，但把命中停用词表的 Token 丢弃，不写入倒排索引**。
>
> ```
> "Java 的并发编程"  →  切分：[java] [的] [并发] [编程]
>                    →  丢弃停用词"的"
>                    →  入库：[java] [并发] [编程]     ← "的"消失了，但切分本身没被打断
> ```
>
> 名字里的 "stop" 来自 1950 年代信息检索先驱 H.P. Luhn 的 **stop list**（停止表）——意为"处理到这些词就**停手，不再往下建索引**"，而不是"停止切分"。所以准确的理解是「**停止收录**」，不是「停止分词」。中文学界固定译作**停用词**（也有译作"停止词"的），本书统一用「停用词（Stop Words）」。
>
> **为什么要去掉**：这些词几乎每篇文档都有，IDF 极低（见 [5.1](#51-从-tf-idf-到-bm25)），对区分文档毫无帮助，却要占用大量倒排索引空间。**但也有代价**——去掉后无法做包含停用词的短语精确匹配，比如搜英文乐队名 "The Who" 会因为两个词都是停用词而检索不到。这也是 ES 的 `standard` analyzer 默认**不启用**英文停用词过滤的原因。

**两个必踩的坑**：

**① 编码必须是 UTF-8 无 BOM。**

**BOM（Byte Order Mark，字节顺序标记）**是文件开头的一段**不可见的特殊字节**，用来向读取程序声明"我是什么编码"。UTF-8 的 BOM 是三个字节 `EF BB BF`。

```
你以为文件是这样：              实际磁盘上是这样：
┌──────────────┐               ┌──────────────────────────┐
│ 布隆过滤器      │               │ EF BB BF 布隆过滤器        │
│ 一致性哈希      │               │ 一致性哈希                 │
└──────────────┘               └──────────────────────────┘
                                  ↑ 三个看不见的字节
```

IK 逐行读取词典时**不会剥离 BOM**，于是第一行被读成 `"\uFEFF布隆过滤器"`——一个前面挂着幽灵字符的词。它和用户真正输入的"布隆过滤器"永远匹配不上，**所以第一行词条永远不生效**。而且 IK 不会报错，只是静默失效，极难排查。

Windows 记事本"另存为 UTF-8"默认就会加 BOM（Win10 1903 后才改为默认不加），这是最常见的来源。

```bash
# 检测：有 BOM 会显示 "UTF-8 Unicode (with BOM) text"
file custom/business.dic

# 看前三个字节，是 efbbbf 就说明有 BOM
head -c 3 custom/business.dic | xxd

# 去除 BOM
sed -i '1s/^\xEF\xBB\xBF//' custom/business.dic
```

> **为什么 UTF-8 其实不需要 BOM**：BOM 的本意是给 UTF-16/UTF-32 区分大端小端（Byte **Order**），而 UTF-8 是单字节序列、根本不存在字节序问题。所以 UTF-8 的 BOM 纯粹是"标记这是 UTF-8"的冗余设计，Unicode 标准也不推荐使用。**Linux/Unix 生态一律不加 BOM**，只有 Windows 系工具爱加。

**② 换行符建议用 LF。** CRLF（Windows 换行 `\r\n`）在部分 IK 版本下会把 `\r` 当作词的一部分，效果和 BOM 类似——每一行的词都多了个看不见的尾巴。

```bash
# 检测：显示 "with CRLF line terminators" 就是 Windows 换行
file custom/business.dic
# 转换
dos2unix custom/business.dic
```

> **最可靠的验证方式**：别只看文件，直接用 `_analyze` 测第一个词能不能被正确切出来——这是端到端的验证，BOM、换行符、路径配错、词典没加载，任何一个环节出问题都会暴露。

**词典的三种加载方式**，对应三种运维成本：

| 方式 | 配置项 | 生效时机 | 适用 |
|------|-------|---------|------|
| **本地词典** | `ext_dict` / `ext_stopwords` | **需重启节点** | 稳定的基础词库，很少变动 |
| **远程词典** | `remote_ext_dict` / `remote_ext_stopwords` | 60 秒内自动生效，**无需重启** | 业务黑话、新品名，需要频繁增补 |
| **同义词** | `synonym_graph` + `updateable` | 调 `_reload_search_analyzers` 立即生效 | 同义改写规则 |

**远程词典的 HTTP 契约**——这是很多人配了不生效的原因，IK 对服务端有硬性要求：

```
IK 每 60 秒发一次请求（HEAD 探测 + GET 拉取）：

  ① 先发 HEAD 请求，读两个响应头：
       Last-Modified: Mon, 03 Aug 2026 10:00:00 GMT
       ETag: "abc123"

  ② 只要其中任意一个与上次不同 → 判定词典有更新
  ③ 再发 GET 请求拉取全量词典内容（不是增量！）
  ④ 重新构建内存里的词典树（DictSegment 字典树）

  ⚠️ 服务端必须返回这两个头中的至少一个，否则 IK 永远认为没更新
  ⚠️ 返回内容的 Content-Type 需为 text/plain，编码 UTF-8
  ⚠️ 每次是全量替换，不是追加——新文件必须包含所有历史词条
```

用 Nginx 托管是最省事的做法，静态文件天然带 `Last-Modified` 和 `ETag`：

```nginx
server {
    listen 80;
    location /dict/ {
        alias /data/es-dict/;
        charset utf-8;
        # 静态文件自动带 Last-Modified / ETag，无需额外配置
    }
}
```

如果用应用服务托管（比如词典存在数据库、需要动态生成），必须自己实现这两个头：

```java
@GetMapping(value = "/dict/hot.dic", produces = "text/plain;charset=UTF-8")
public ResponseEntity<String> hotDict() {
    DictSnapshot snap = dictService.getSnapshot();   // 含内容和版本号
    return ResponseEntity.ok()
            // 这两个头缺一不可，否则 IK 检测不到变更
            .header("Last-Modified", snap.getGmtModified())   // GMT 格式
            .eTag(snap.getVersion())
            .body(snap.getContent());                          // 全量词条
}
```

**完整的词典更新流程**——新词生效不是改完文件就完事：

```
① 运营在词典管理后台新增词条 "青龙系统"
        ↓
② 词典服务更新文件 / 数据库，同时更新 ETag
        ↓
③ 各 ES 节点在 60 秒内检测到变更，重载词典树
        ↓  此时：新写入的文档已能正确切分
        ↓         但历史文档的倒排索引还是旧的切分结果
        ↓
④ 触发历史数据重建（二选一）：
     • 数据量小 → POST /index/_update_by_query?conflicts=proceed
     • 数据量大 → 走 Reindex 到新索引 + 别名切换（不阻塞线上）
        ↓
⑤ 用 _analyze 验证切分结果，用实际 query 验证召回
```

> **远程词典热更新机制**：IK 每 60 秒请求一次远程词典 URL，通过 HTTP 头的 `Last-Modified` 或 `ETag` 判断是否变更。**注意——新词典只对之后写入的文档生效，已索引的老文档不会自动重新分词**，需要 `_update_by_query` 重建。这是最容易踩的坑。
>
> **为什么老文档不会自动更新**：倒排索引是在**写入时**根据当时的分词结果构建的，且 Lucene 的 segment 不可变（见 [6.4](#64-segment-与写入性能)）。词典只影响"分词这个动作"，不会回溯改写已经落盘的索引。所以「加了词还是搜不到」十有八九不是词典没生效，而是**忘了重建历史数据**——先用 `_analyze` 确认词典本身是好的，再去查数据。

**同义词**——搜"手机"要能召回"移动电话"：

```json
PUT /products
{
  "settings": {
    "analysis": {
      "filter": {
        "my_synonym": {
          "type": "synonym_graph",
          "synonyms": ["手机, 移动电话, mobile", "笔记本, laptop"],
          "updateable": true            // 支持热更新
        }
      },
      "analyzer": {
        "syn_analyzer": {
          "tokenizer": "ik_smart",
          "filter": ["my_synonym"]
        }
      }
    }
  }
}
```

> **同义词放在搜索端而非索引端**：如果放在索引端，同义词表一变就要重建全部索引；放在 `search_analyzer` 里，改词表只需 `_reload_search_analyzers`，代价极小。

**调试分词效果**用 `_analyze`：

```json
GET /articles/_analyze
{
  "analyzer": "ik_max_word",
  "text": "布隆过滤器的误判率"
}
```

### 6.2 索引设计与分片规划

**分片数量是 ES 最重要、也最难改的决策**——主分片数创建后不可修改（只能 reindex/split/shrink）。

**这三个操作分别是什么**——它们都是"改变分片数"的手段，但代价和限制差别很大：

| 操作 | 作用 | 分片数变化 | 硬性限制 | 代价 |
|------|------|-----------|---------|------|
| **`_reindex`** | 把数据从旧索引**重新写入**新索引 | 任意 | 无 | **最贵**——等于全量重写，数据量大时以小时计 |
| **`_split`** | 把每个分片**拆成 N 份** | 只能**变多**，且必须是原数量的整数倍 | 源索引须只读 | 中等——底层做硬链接，比 reindex 快 |
| **`_shrink`** | 把多个分片**合并成更少的** | 只能**变少**，且新数量须能整除原数量 | 源索引须只读，且所有主分片要先搬到同一节点 | 中等——同样走硬链接 |

```
原索引 6 个主分片

  _split  → 12 个（×2）、18 个（×3）、24 个（×4）   ✓ 整数倍
          → 8 个                                    ✗ 不是整数倍，报错

  _shrink → 3 个（6÷2）、2 个（6÷3）、1 个（6÷6）    ✓ 能整除
          → 4 个                                    ✗ 6 不能被 4 整除，报错

  _reindex → 任意数量，但要重写全部数据
```

`_split` 和 `_shrink` 之所以比 `_reindex` 快，是因为它们**不重新分析文档**，而是在文件系统层面对 Lucene segment 做**硬链接**（同一块磁盘数据被两个索引共享），只调整分片路由。而 `_reindex` 是把每篇文档取出来、重新走一遍分词和索引流程。

```json
// _shrink 前置条件：设为只读 + 主分片集中到一个节点
PUT /logs-old/_settings
{
  "settings": {
    "index.blocks.write": true,                          // 停止写入
    "index.routing.allocation.require._name": "node-1"   // 主分片全搬到 node-1
  }
}

POST /logs-old/_shrink/logs-small
{
  "settings": { "index.number_of_shards": 1 }
}
```

> **实践中最常用的其实是第四种做法：滚动索引 + 别名**。与其纠结分片数改不了，不如一开始就按时间滚动（`logs-2026.08.01`、`logs-2026.08.02`…），用别名对外提供统一视图。这样新索引可以随时用新的分片数，老索引到期直接删除，**完全绕开了"分片数不可变"这个问题**——这也是时序场景的标准解法。`_shrink` 则常用在 ILM 的 Warm 阶段（见 [6.5](#65-冷热分离与索引生命周期ilm)）：数据不再写入后，把分片数降下来省资源。

```
分片大小经验值：
  单个分片 20~50 GB   ← 日志/时序场景可到 50GB
  单个分片 10~30 GB   ← 搜索场景，追求低延迟取小值

分片数量约束：
  单节点分片数 ≤ 堆内存GB数 × 20
  例：31GB 堆 → 单节点最多约 620 个分片

估算示例：
  预计数据总量 600 GB，搜索场景取 30GB/分片
  → 主分片数 = 600 / 30 = 20 个
  → 副本 1 份 → 总分片数 40 个
```

**过度分片的代价**（比分片不足更常见的错误）：

| 问题 | 说明 |
|------|------|
| 元数据开销 | 每个分片是一个独立 Lucene 实例，占用堆内存和文件句柄 |
| 查询扇出放大 | 一次查询要 fan-out 到所有分片，分片越多协调开销越大 |
| 小分片浪费 | 每个分片有固定开销，1GB 的分片和 30GB 的分片固定成本一样 |

**时序数据用「按时间滚动索引 + 别名」**，这是日志/监控场景的标准做法：

```json
// 写别名指向当前活跃索引，读别名覆盖全部历史索引
POST /_aliases
{
  "actions": [
    { "add": { "index": "logs-2026.07.31", "alias": "logs-write", "is_write_index": true } },
    { "add": { "index": "logs-*",          "alias": "logs-read" } }
  ]
}

// Rollover：满足条件自动切换到新索引
POST /logs-write/_rollover
{
  "conditions": {
    "max_age":   "1d",
    "max_size":  "50gb",
    "max_docs":  100000000
  }
}
```

> **按时间分索引的最大好处是删除成本**。删除 30 天前的日志，如果是单一大索引就要 `delete_by_query`（要逐条标记删除再 merge，极慢且产生大量段碎片）；按天分索引则直接 `DELETE /logs-2026.06.30`，**是元数据操作，秒级完成**。

**Mapping 设计要点**：

| 决策 | 建议 |
|------|------|
| `text` vs `keyword` | 需要分词搜索用 `text`；精确匹配/聚合/排序用 `keyword`；两者都要用 `fields` 多字段 |
| `dynamic` | 生产环境设 `strict`，防止脏字段污染 mapping 导致字段爆炸 |
| 不搜索的字段 | 设 `index: false`，只存不索引，省空间 |
| 不聚合不排序的字段 | 设 `doc_values: false`，省磁盘 |
| `nested` | 谨慎使用——每个 nested 对象是一个独立 Lucene 文档，写放大严重 |

```json
{
  "properties": {
    "title": {
      "type": "text",
      "analyzer": "ik_max_word",
      "fields": { "keyword": { "type": "keyword", "ignore_above": 256 } }
    },
    "raw_html": { "type": "text", "index": false },        // 只存不搜
    "status":   { "type": "keyword" }                       // 精确匹配用 keyword
  }
}
```

**什么是 Doc Values**——上表里 `doc_values: false` 那一条，背后是 ES 一个容易被忽略的设计：**倒排索引只解决"搜索"，解决不了"聚合和排序"，所以 ES 额外存了一份列式数据**。

倒排索引是 `词 → 文档列表` 的映射，天生适合回答"哪些文档包含这个词"：

```
倒排索引（Inverted Index）
  "北京" → [doc1, doc3, doc7]
  "上海" → [doc2, doc5]

  问："哪些文档提到北京？"     → 直接查表，极快 ✓
  问："doc3 的城市是什么？"    → 只能把所有词都翻一遍看谁包含 doc3 ✗
```

但排序和聚合恰恰需要反过来的能力——**给定文档，取出它某个字段的值**。比如"按价格排序"要拿到每篇文档的 price，"按品牌分组统计"要拿到每篇文档的 brand。用倒排索引做这件事等于全表扫描。

所以 ES 在写入时**额外维护一份列式存储**，这就是 Doc Values：

```
Doc Values（正排/列式存储）
  doc1 → "北京"
  doc2 → "上海"
  doc3 → "北京"

  问："doc3 的城市是什么？"  → 按文档号直接定位，极快 ✓
  排序、聚合、脚本取值走的都是这条路
```

| | 倒排索引 | Doc Values |
|---|---|---|
| 结构 | 词 → 文档列表 | 文档 → 字段值 |
| 用途 | `match` / `term` **搜索** | **聚合 / 排序 / 脚本取值** |
| 开关 | `index: true/false` | `doc_values: true/false` |
| 存储位置 | 磁盘，按需载入 | 磁盘（**列式压缩**），靠 OS Page Cache 加速 |

**默认所有非 `text` 字段的 Doc Values 都是开启的**（`text` 字段因为分词后基数太大，默认关闭，要排序聚合得用 `fielddata` 或加 `.keyword` 子字段）。这意味着——**一个你从来不聚合、不排序的 `keyword` 字段，ES 仍然在默默为它写一份列存，白白占磁盘和写入 IO**。

```json
{
  "properties": {
    "trace_id":  {
      "type": "keyword",
      "doc_values": false      // 只用来精确查询，从不聚合排序 → 关掉省磁盘
    },
    "brand": { "type": "keyword" },                        // 要分组统计 → 保持默认开启
    "log_body": { "type": "text", "index": true }          // text 本就不开 doc_values
  }
}
```

> **省多少**：日志类场景关掉不必要字段的 Doc Values，通常能省 **10%~30% 的索引体积**，写入吞吐也会提升。这是低风险高收益的优化。
>
> **代价与坑**：关掉之后，该字段**无法排序、无法聚合、无法用于 `script` 取值、无法作为 `sort` 的 tie-breaker**（这点尤其容易踩——[search_after 深分页](#68-深分页的三种方案)要求排序字段唯一，如果你把兜底字段的 doc_values 关了就用不了）。而且**改这个属性需要重建索引**，属于典型的"设计时就要想清楚"的决策。

**什么是"字段爆炸"（Mapping Explosion）**——这是上表里 `dynamic: strict` 那一条要防的事故，值得单独讲。

ES 默认开启**动态映射（Dynamic Mapping）**：你写入一个 mapping 里没定义过的字段，ES 不报错，而是**自动推断类型并把它永久加进 mapping**。这个特性开发期很方便，生产环境却是定时炸弹：

```
业务代码不小心把用户 ID 当成了字段名：

  写入 { "user_1001_score": 95 }   → mapping 新增字段 user_1001_score
  写入 { "user_1002_score": 87 }   → mapping 新增字段 user_1002_score
  写入 { "user_1003_score": 92 }   → mapping 新增字段 user_1003_score
       ...
  100 万用户 → mapping 里 100 万个字段
```

后果是**集群级别的**，不只是这个索引的问题：

```
① Mapping 属于 Cluster State（集群状态元数据）
       ↓
② Cluster State 常驻每个节点的堆内存，且任何变更都要广播到全集群
       ↓
③ Mapping 膨胀到几十 MB → 每次字段新增都触发全集群同步
       ↓
④ Master 节点 CPU 打满、GC 频繁 → 整个集群响应变慢甚至失联
       ↓
⑤ 每个字段还要额外占用 Lucene 的元数据和文件句柄
```

ES 7.x 之后默认有 `index.mapping.total_fields.limit = 1000` 兜底，超过就拒绝写入。**但很多人遇到报错的第一反应是把这个值调大到 10000**——那只是把爆炸推迟，不是解决。

`dynamic` 有三个取值：

| 取值 | 遇到未定义字段的行为 | 适用 |
|------|-------------------|------|
| `true`（默认） | 自动推断类型并加入 mapping | 开发调试期 |
| `false` | **不加入 mapping，字段能存但搜不到**（`_source` 里有，无法查询/聚合） | 需要保留原始数据但不索引 |
| `strict` | **直接抛错拒绝写入** | **生产环境推荐** |

```json
PUT /orders
{
  "mappings": {
    "dynamic": "strict",              // 未定义字段直接拒绝，倒逼上游规范
    "properties": {
      "order_id": { "type": "keyword" },
      "amount":   { "type": "double" }
    }
  }
}
```

`strict` 的价值在于**把问题暴露在写入时**——脏字段一进来就报错，你立刻知道是哪个上游改了数据结构；而不是等到 mapping 涨到几万个字段、集群开始抖动了才去排查。

> **如果业务确实需要动态字段怎么办**：用 `flattened` 类型。它把整个对象当作**一个字段**存储，内部的所有键值对不会各自成为独立字段，从根本上避免爆炸。代价是只支持精确匹配、不支持分词和数值范围查询。
>
> ```json
> "labels": { "type": "flattened" }    // 无论里面有多少个 key，mapping 里只算 1 个字段
> ```
>
> 另一种思路是**转成 nested 的键值对数组**：`[{"k":"user_1001","v":95}]`，把"字段名"变成"字段值"。这样字段数固定为 2，但要注意 nested 的写放大代价。

### 6.3 filter vs query 与缓存机制

[4.2](#42-查询示例) 提过 `filter` 比 `must` 快，这里讲清楚**为什么**。

```
bool 查询的四个子句：

  must     → 必须匹配，参与评分
  should   → 应该匹配，参与评分（影响 _score）
  must_not → 必须不匹配，不评分，走 filter context
  filter   → 必须匹配，不评分，走 filter context
```

**filter 快的两个原因**：

```
① 跳过评分计算
   不需要算 BM25 的 TF/IDF/长度归一化，纯粹判断"匹配/不匹配"

② 可被缓存为 bitset（核心优势）
   filter 的结果是一个 bitset（每个文档 1 bit，匹配为 1）
   ES 会把高频使用的 filter 结果缓存到 Node Query Cache
   下次同样的 filter 直接复用 bitset，无需重新执行
```

> **缓存的触发条件**：ES 不会缓存所有 filter。它有一个启发式策略——只有在**最近 256 次查询中出现 ≥ 2 次**、且分片文档数超过一定规模时，才会被缓存。这意味着**每次都不同的 filter（如精确到毫秒的时间戳）永远不会命中缓存**，反而白白占用判断开销。

```json
// ✗ 反例：now 精确到毫秒，每次查询的 filter 都不同，缓存永远不命中
{ "range": { "created_at": { "gte": "now-1d" } } }

// ✓ 正例：舍入到小时，同一小时内的查询共享同一个缓存 bitset
{ "range": { "created_at": { "gte": "now-1d/h" } } }
```

**实践准则**：**凡是不需要影响排序的条件，一律放 `filter`**——状态过滤、时间范围、租户 ID、分类 ID、权限过滤。只有真正影响相关性排序的全文匹配才放 `must`。

### 6.4 Segment 与写入性能

理解 segment 是理解 ES 写入性能的关键。回顾 [3.3](#33-近实时near-real-time原理) 的写入流程，补充完整：

```
写入完整链路：

  ① 文档写入 In-memory Buffer + 同时追加到 Translog（保证不丢）
  ② 每隔 refresh_interval（默认 1s）执行 refresh：
       Buffer → 生成新的 Segment（进入 OS Cache，此时可被搜索）
       → 这就是"近实时"的来源
  ③ 每隔 30 分钟或 Translog 满 512MB 执行 flush：
       Segment 真正 fsync 到磁盘，清空 Translog
  ④ 后台持续 merge：把小 Segment 合并成大 Segment
```

**Segment 是不可变的**——这带来两个重要推论：

```
推论 ① 更新 = 标记删除 + 新增
  UPDATE 操作不会原地修改，而是把老文档在 .del 文件里标记为删除，
  再写一个新文档。被标记的文档在 merge 时才真正物理删除。
  → 频繁更新会产生大量"删除但未清理"的文档，膨胀索引体积

推论 ② 段越多，查询越慢
  一次查询要遍历所有 segment 再合并结果
  → merge 是必要的，但 merge 本身消耗 IO 和 CPU
```

**批量导入时的写入优化**（面试高频）：

```json
// 导入前：关闭 refresh、关闭副本
PUT /my_index/_settings
{
  "index": {
    "refresh_interval": "-1",       // 关闭自动 refresh
    "number_of_replicas": 0         // 先不建副本，导完再开
  }
}

// ... 执行 bulk 批量导入（每批 5~15 MB 为宜）...

// 导入后：恢复配置
PUT /my_index/_settings
{
  "index": {
    "refresh_interval": "1s",
    "number_of_replicas": 1
  }
}

// 对不再写入的历史索引做强制合并，提升查询性能
POST /my_index/_forcemerge?max_num_segments=1
```

> **`_forcemerge` 的使用禁忌**：只对**不再写入**的只读索引执行。对活跃写入的索引强制合并会产生巨大的段，后续 merge 极其昂贵，而且合并期间 IO 打满会影响线上查询。这是运维事故的常见来源。

### 6.5 冷热分离与索引生命周期（ILM）

日志/时序场景的数据有明显的**访问衰减**——今天的数据频繁查，三个月前的几乎不查。冷热分离就是让不同热度的数据落在不同硬件上。

```
节点角色分层：

  data_hot   → SSD、高配 CPU     → 承接写入 + 近期数据查询
  data_warm  → SATA SSD / 高转速盘 → 只读，偶尔查询
  data_cold  → 大容量机械盘        → 极少查询，可降副本
  data_frozen→ 对象存储（可搜索快照）→ 几乎不查，成本最低
```

```yaml
# 节点配置：在 elasticsearch.yml 中声明角色
node.roles: [ data_hot, ingest ]     # 热节点
# node.roles: [ data_warm ]          # 温节点
```

**ILM 策略**——让 ES 自动搬迁数据：

```json
PUT _ilm/policy/logs_policy
{
  "policy": {
    "phases": {
      "hot": {
        "actions": {
          "rollover": { "max_age": "1d", "max_primary_shard_size": "50gb" }
        }
      },
      "warm": {
        "min_age": "7d",
        "actions": {
          "allocate":   { "require": { "_tier_preference": "data_warm" } },
          "forcemerge": { "max_num_segments": 1 },
          "shrink":     { "number_of_shards": 1 }
        }
      },
      "cold": {
        "min_age": "30d",
        "actions": {
          "allocate": { "number_of_replicas": 0 }
        }
      },
      "delete": {
        "min_age": "90d",
        "actions": { "delete": {} }
      }
    }
  }
}
```

| 阶段 | 典型动作 | 目的 |
|------|---------|------|
| **Hot** | Rollover | 按大小/时间滚动出新索引 |
| **Warm** | 迁移到温节点、forcemerge、shrink | 降低段数和分片数，省资源 |
| **Cold** | 降副本、迁移到冷节点 | 极致省成本（牺牲可用性） |
| **Delete** | 删除索引 | 到期清理 |

### 6.6 集群调优要点

**JVM 堆内存——最重要的一条规则**：

```
堆内存设置铁律：

  ① 不超过物理内存的 50%
     → 另一半留给 OS Page Cache，Lucene 的段文件靠它加速

  ② 不超过 31 GB
     → 超过 32GB 会失去「压缩指针（Compressed OOPs）」
     → 指针从 4 字节变 8 字节，实际可用对象空间反而变少

  ③ Xms = Xmx（初始堆 = 最大堆）
     → 避免运行时堆伸缩带来的停顿

  推荐：64GB 物理内存的机器 → -Xms31g -Xmx31g
```

**节点角色分离**——中大规模集群必做：

| 角色 | 职责 | 配置侧重 |
|------|------|---------|
| **Master** | 集群元数据管理、分片分配 | 低配即可，但要 3 个（防脑裂） |
| **Data** | 存储数据、执行查询 | 高 CPU + 大内存 + SSD |
| **Coordinating** | 接收请求、分发、合并结果 | 高 CPU + 大内存，无需磁盘 |
| **Ingest** | 数据预处理管道 | 高 CPU |

> **为什么 Master 要 3 个**：ES 用 Quorum 机制选主，`(3/2)+1 = 2` 个节点存活即可选出主节点，容忍 1 台故障。2 个 Master 反而更糟——挂 1 台就无法达成多数派。这和 [3.10 分布式理论](./10-分布式理论与一致性.md) 里 Raft 的选举法定人数是同一个道理。

**熔断器（Circuit Breaker）**——防止 OOM 的最后防线：

```
indices.breaker.total.limit          默认 95% 堆    总熔断
indices.breaker.request.limit        默认 60% 堆    单请求（聚合等）
indices.breaker.fielddata.limit      默认 40% 堆    fielddata
network.breaker.inflight_requests    默认 100% 堆   传输中的请求
```

触发时抛 `CircuitBreakingException`，**这是保护而非故障**——说明查询要的内存超预期，应该优化查询而不是无脑调大阈值。

### 6.7 查询性能优化

| 优化手段 | 说明 |
|---------|------|
| **用 filter 替代 must** | 不需要评分的条件全部放 filter，可缓存（见 6.3） |
| **`_source` 过滤** | 只返回需要的字段，减少网络传输和 IO |
| **`track_total_hits: false`** | 不需要精确总数时关闭，ES 可提前终止扫描 |
| **routing** | 按 key 路由到单分片，把 fan-out 从 N 个分片降到 1 个 |
| **避免 wildcard 前缀通配** | `*keyword` 无法用倒排索引，退化为全扫描 |
| **避免 script 查询/排序** | 每个文档都要执行脚本，极慢 |
| **日期 range 舍入** | `now-1d/h` 而非 `now-1d`，让 filter 缓存生效 |
| **深分页用 search_after** | 见 6.8 |

**routing 的威力**：

```json
// 写入时按租户路由
PUT /docs/_doc/1?routing=tenant_42
{ "tenant_id": "tenant_42", "content": "..." }

// 查询时带上同样的 routing → 只查 1 个分片，而不是全部 20 个
GET /docs/_search?routing=tenant_42
{ "query": { "bool": { "filter": [{ "term": { "tenant_id": "tenant_42" }}] }}}
```

> **routing 的代价是数据倾斜**——如果某个大租户的数据远超其他租户，它所在的分片会成为热点。多租户检索平台需要在"查询扇出小"和"数据分布均匀"之间权衡，超大租户可以单独建索引。

**定位慢查询**用 Profile API 和慢日志：

```json
// Profile API：拆解查询各阶段耗时
GET /my_index/_search
{ "profile": true, "query": { ... } }
```

```json
// 慢日志：超过阈值自动记录
PUT /my_index/_settings
{
  "index.search.slowlog.threshold.query.warn": "5s",
  "index.search.slowlog.threshold.query.info": "2s",
  "index.search.slowlog.threshold.fetch.warn": "1s"
}
```

> **query 阶段慢 vs fetch 阶段慢**：query 慢说明匹配/评分/排序代价大（优化查询结构、加 filter）；fetch 慢说明取 `_source` 的 IO 大（减少返回字段、检查是否返回了大文本字段）。**两者的优化方向完全不同**，慢日志分开设阈值就是为了区分它们。

### 6.8 深分页的三种方案

**"深"指的是什么**——是 `from`（偏移量）深，也就是你要的数据在结果集里位置很靠后。`from: 10, size: 10` 是第 2 页（浅），`from: 10000, size: 10` 是第 1001 页（深）。

慢的根因只有一句话：**数据库和 ES 都没法直接"跳到"第 10000 条，只能从头数过去。** "跳过"在物理上等于"取出来再扔掉"。

MySQL 里已经很浪费了——`LIMIT 10000, 10` 实际要取出 10010 行、排序、扔掉前 10000 行。ES 因为是分布式的，这个浪费还要**再乘以分片数**：

```
from: 10000, size: 10，索引 3 个主分片

              协调节点（Coordinating Node）
                        │
      ┌─────────────────┼─────────────────┐
      ▼                 ▼                 ▼
    ┌────┐            ┌────┐            ┌────┐
    │ P0 │            │ P1 │            │ P2 │
    └────┘            └────┘            └────┘
  各自返回          各自返回          各自返回
  Top 10010         Top 10010         Top 10010
      │                 │                 │
      └─────────────────┼─────────────────┘
                        ▼
         协调节点收到 30030 条 → 全局排序
                        ▼
              丢弃前 10000 条
                        ▼
                  返回 10 条

  实际代价 = (from + size) × 分片数 = 30030 条
  有效产出 = 10 条            浪费率 99.97%
```

> **为什么每个分片不能各返回 10 条就好？** 因为协调节点不知道数据在分片间怎么分布——全局第 10001～10010 名完全可能**全部来自 P1 一个分片**。只有让每个分片都交出自己的前 10010 名，全局排序的结果才是正确的。分片数越多、翻得越深，放大越严重。

这也解释了 ES 为什么默认设 `index.max_result_window = 10000`，超过直接报错：不是不能做，是怕你把协调节点的内存打爆（OOM 发生在归并的那个节点上）。

**选方案前先分清是哪种需求**——深分页不是一个问题，是三个不同的需求：

| 真实需求 | 正确解法 | 说明 |
|---------|---------|------|
| 用户手动翻页翻很深 | **产品层面限制页数** | 先反问：真有用户翻到第 1000 页吗？百度最多 76 页、淘宝最多 100 页。引导用户加筛选条件比技术优化更有效 |
| 连续翻页（下一页…） | `search_after` | 成本恒定，翻到第几页都一样快 |
| 全量导出 / 离线遍历 | `PIT + search_after` | 快照一致性视图，适合报表导出、数据迁移 |

| 方案 | 原理 | 适用 | 限制 |
|------|------|------|------|
| **from + size** | 每个分片返回 from+size 条给协调节点全局排序 | 浅分页（前几页） | `index.max_result_window` 默认上限 10000 |
| **search_after** | 用上一页最后一条的排序值作为游标 | **深分页首选**（用户翻页） | 不能跳页，只能顺序翻 |
| **scroll** | 生成数据快照，游标遍历 | 全量导出（已不推荐） | 占用资源、数据是快照不实时 |
| **PIT + search_after** | Point In Time 保证一致性视图 + 游标 | **ES 7.10+ 全量遍历的正确做法** | 需管理 PIT 生命周期 |

```json
// search_after：用上一页最后一条的 sort 值继续
GET /my_index/_search
{
  "size": 10,
  "query": { "match_all": {} },
  "sort": [
    { "created_at": "desc" },
    { "_id": "asc" }              // 必须有唯一字段兜底，保证排序全局唯一
  ],
  "search_after": [1690000000000, "doc_12345"]
}
```

> **`_id` 兜底为什么必需**：如果只按 `created_at` 排序，多条文档时间戳相同时排序不稳定，翻页会出现重复或遗漏。加一个唯一字段作为最后的 tie-breaker，保证全局有序。这和 MySQL 深分页要用唯一索引做游标是同一个道理。

**`search_after` 为什么能做到成本恒定**——它把"跳过 N 条"换成了"从某个值之后开始"，后者可以直接用索引定位起点，不需要遍历前面的数据：

```
from + size（第 1001 页）              search_after（第 1001 页）
─────────────────────────             ─────────────────────────
每分片交出 Top 10010                   每分片只需交出 10 条
  ↓                                      ↓
协调节点排序 30030 条                   协调节点排序 30 条
  ↓                                      ↓
丢弃 10000 条 → 返回 10 条              直接返回 10 条

代价随 from 线性增长 O(from)           代价恒定 O(size)
```

对应到 MySQL 就是**键集分页（Keyset Pagination）**，思路完全一样：

```sql
-- ✗ 深分页：取 10010 行扔掉 10000 行
SELECT * FROM article ORDER BY create_time DESC LIMIT 10000, 10;

-- ✓ 键集分页：用上一页最后一条的值定位，走索引直接跳到起点
SELECT * FROM article
WHERE create_time < '2026-08-03 10:00:00'   -- 上一页最后一条
ORDER BY create_time DESC LIMIT 10;
```

代价是**只能顺序翻，不能跳页**——你无法从第 3 页直接跳到第 500 页，因为你不知道第 499 页最后一条的排序值。这正是为什么"用户手动跳页到很深"这个需求应该在产品层面砍掉，而不是在技术上硬扛。

### 6.9 ES 8.x 向量检索与混合检索

ES 8.x 原生支持 `dense_vector` 字段和 KNN 检索，底层用的也是 **HNSW**（原理详见 [7.1 RAG 实战 §4.4](../part7-ai-engineering/01-RAG实战.md)）。这让 ES 可以**一个引擎同时做 BM25 + 向量检索**。

```json
// 1. 定义向量字段
PUT /docs
{
  "mappings": {
    "properties": {
      "content":   { "type": "text", "analyzer": "ik_max_word" },
      "embedding": {
        "type": "dense_vector",
        "dims": 1024,
        "index": true,
        "similarity": "cosine",
        "index_options": {
          "type": "hnsw",
          "m": 16,                  // 对应 HNSW 的 M 参数
          "ef_construction": 100
        }
      }
    }
  }
}

// 2. KNN 检索
GET /docs/_search
{
  "knn": {
    "field": "embedding",
    "query_vector": [0.12, -0.03, ...],
    "k": 10,
    "num_candidates": 100,          // 对应 efSearch，越大召回越高越慢
    "filter": [                     // 过滤前置，避免召回塌陷
      { "term": { "tenant_id": "t_42" } }
    ]
  }
}
```

**混合检索**——ES 8.x 支持用 RRF 融合 BM25 和向量结果：

```json
GET /docs/_search
{
  "retriever": {
    "rrf": {
      "retrievers": [
        { "standard": { "query": { "match": { "content": "分布式锁实现" } } } },
        { "knn": { "field": "embedding", "query_vector": [...], "k": 50, "num_candidates": 200 } }
      ],
      "rank_window_size": 50,
      "rank_constant": 20
    }
  }
}
```

> **什么时候用 ES 做向量检索、什么时候上专业向量库**：向量规模在千万级以内、且已有 ES 基建的，直接用 ES 8.x 最省事——一个引擎、一套运维、天然支持混合检索和过滤。到亿级向量或对检索延迟有极致要求时，再引入 Milvus。融合算法 RRF 的原理详见 [7.1 RAG 实战](../part7-ai-engineering/01-RAG实战.md)。

### 6.10 怎么给 ES 加 Rerank

前面多次提到「BM25 调参的收益远小于加 Rerank」，这里讲**具体怎么加**。核心认知是：**检索是两阶段的**——粗排（Recall）负责"快速捞出候选"，精排（Rerank）负责"精确排序"。

```
粗排（ES 负责）                     精排（Rerank 负责）
─────────────────                  ─────────────────
BM25 / 向量检索                     Cross-Encoder 逐对打分
扫描百万级文档                       只处理 Top 20~100 候选
单文档打分，互不影响                  Query 和 Doc 一起进模型，能捕捉细粒度交互
快（毫秒级）                         慢（几十毫秒）
准确率一般                           准确率高
```

ES 侧有三种加 Rerank 的方式，成本和效果递增：

**方式一：`rescore` —— ES 原生，零外部依赖**

对 BM25 的 Top N 结果用一个更贵的查询重新打分。适合"粗排够用，只需微调排序"的场景：

```json
GET /docs/_search
{
  "query": {                          // 粗排：快速召回
    "match": { "content": "分布式锁实现" }
  },
  "rescore": {
    "window_size": 50,                // 只对 Top 50 重新打分（关键：控制代价）
    "query": {
      "rescore_query": {              // 精排：更贵但更准的查询
        "match_phrase": {             // 短语匹配，要求词序连续
          "content": { "query": "分布式锁实现", "slop": 2 }
        }
      },
      "query_weight": 0.7,            // 原始 BM25 分数权重
      "rescore_query_weight": 1.3     // 精排分数权重
    }
  }
}
```

> **`rescore` 的本质是"分层计算"**：昂贵的算法只作用于 `window_size` 条候选，而不是全量文档。这和 Cross-Encoder 精排是同一个思路，只是打分器换成了 ES 内置查询。**注意 `window_size` 必须大于等于 `from + size`**，否则翻页时会出现排序错乱。

**方式二：`text_similarity_reranker` —— ES 8.14+ 原生 Cross-Encoder**

ES 8.14 之后，`retriever` API 支持直接挂载 Rerank 模型，模型通过 Eland 部署到 ES 的 ML 节点，或走 Inference API 调外部服务：

```json
GET /docs/_search
{
  "retriever": {
    "text_similarity_reranker": {
      "retriever": {                        // 内层：混合检索粗排
        "rrf": {
          "retrievers": [
            { "standard": { "query": { "match": { "content": "分布式锁实现" } } } },
            { "knn": { "field": "embedding", "query_vector": [], "k": 50, "num_candidates": 200 } }
          ]
        }
      },
      "field": "content",                   // 用哪个字段和 query 做相关性计算
      "inference_id": "my-rerank-model",    // 已注册的 Rerank 模型
      "inference_text": "分布式锁实现",
      "rank_window_size": 50                // 送入精排的候选数
    }
  }
}
```

这是**最优雅的方案**——召回、融合、精排在一次请求内完成，应用层不用做任何编排。代价是需要 ES 8.14+、需要 ML 节点资源，且模型选择受限于 ES 生态。

**方式三：应用层外挂 Rerank —— 最灵活，生产最常见**

ES 只负责召回，精排在应用层调独立的 Rerank 服务。这是 RAG 场景的主流做法：

```java
// 1. ES 粗排：多召回一些候选（关键：召回数是最终数的 5~10 倍）
SearchResponse resp = esClient.search(s -> s
        .index("docs")
        .size(50)                              // 粗排取 50
        .query(q -> q.match(m -> m.field("content").query(userQuery))), Doc.class);

List<Doc> candidates = extractDocs(resp);

// 2. 调 Rerank 服务精排（Cross-Encoder，batch 推理）
List<Doc> reranked = rerankClient.rerank(userQuery, candidates, /* topN */ 10);
```

> **为什么粗排要多取**：Rerank 只能对召回到的候选重排序，**召回不到的文档它永远救不回来**。典型配比是最终需要 10 条就召回 50~100 条。但候选数直接决定精排延迟（Cross-Encoder 是逐对推理），所以要在召回率和延迟之间权衡。

**三种方式怎么选**：

| 方式 | 效果提升 | 延迟成本 | 依赖 | 适用场景 |
|------|---------|---------|------|---------|
| **`rescore`** | 小（10~20%） | 极低（几 ms） | 无 | 传统搜索，只需微调排序 |
| **`text_similarity_reranker`** | 大 | 中（30~80ms） | ES 8.14+ 和 ML 节点 | ES 版本够新的 RAG 场景 |
| **应用层外挂** | 大 | 中（30~80ms） | 独立 Rerank 服务 | **RAG 主流方案**，模型可自由选型 |

> **Rerank 不是无脑加**：候选数少于 10 条时，Rerank 的延迟收益是负的——粗排本来就没排错什么。Rerank 的价值窗口在候选量 **20~100** 之间。模型选型（`bge-reranker-v2-m3` 等）、延迟预算拆解、批量推理优化详见 [7.1 RAG 实战 §4.6.6](../part7-ai-engineering/01-RAG实战.md)。

---

## 七、ES 与 MySQL 的互补关系

ES 不是要替代 MySQL，而是和 MySQL **各司其职**：

| 维度 | MySQL | ElasticSearch |
|------|-------|---------------|
| 定位 | 事务型数据库（OLTP） | 搜索与分析引擎 |
| 强项 | 事务、关联查询、精确查询 | 全文搜索、模糊匹配、聚合分析 |
| 弱项 | 全文搜索、海量数据聚合 | 事务、关联查询、频繁更新 |
| 数据一致性 | ACID 强一致 | 近实时（~1秒延迟） |
| 典型场景 | 订单表、用户表 | 搜索框、日志分析、商品搜索 |

**常见架构**：MySQL 作为主数据源，通过 Canal/Binlog 同步到 ES，MySQL 负责写和事务性读，ES 负责搜索和分析。

---

## 八、面试深度剖析：ES 高频考点

### 考点 1：ES 为什么比 MySQL 全文搜索快？

> **面试官**：「MySQL 也有 FULLTEXT 索引，为什么还要用 ES？」

MySQL FULLTEXT 索引功能有限（中文分词差、不支持复杂查询、不支持分布式）。ES 基于 Lucene 的倒排索引，原生支持分词、相关性评分、分布式分片并行查询、丰富的查询 DSL，是专门为搜索场景设计的。

### 考点 2：深分页问题

> **面试官**：「ES 的 from + size 深分页有什么问题？」

和 MySQL 类似，`from: 10000, size: 10` 意味着每个分片要返回 10010 条给协调节点，协调节点收集所有分片的结果后全局排序取 Top 10010 再截取最后 10 条。分片越多、from 越大，内存和计算开销越大。ES 用 `index.max_result_window`（默认 10000）硬性拦截这类查询。

解决方案见 [6.8](#68-深分页的三种方案)：用户翻页用 `search_after`（游标分页），全量遍历用 `PIT + search_after`（ES 7.10+ 的正确做法，`scroll` 已不推荐）。

**追问「search_after 为什么要加 `_id` 排序」**：时间戳等字段可能重复，排序不稳定会导致翻页重复或遗漏，必须加唯一字段做 tie-breaker 保证全局有序。

**追问「产品就是要求能跳到第 1000 页，怎么办」**——这题考的是能不能识别伪需求。`search_after` 不支持跳页，硬做只能回到 `from + size` 的老路。正确的回答分三层：先确认真实场景（用户翻到第 1000 页往往是想找特定内容，本质是**检索精度问题而非分页问题**，应该引导加筛选条件或优化相关性）；其次给业界惯例（百度最多 76 页、淘宝最多 100 页，都是产品层面砍需求）；最后给兜底方案（如果是导出需求，改成异步任务 + `PIT + search_after` 全量拉取后生成文件）。**直接把 `max_result_window` 调大到 100 万是错误答案**——那只是把 OOM 的时间点往后推。

### 考点 3：如何保证 MySQL 和 ES 的数据一致性？

> **面试官**：「MySQL 写了数据，ES 怎么保证同步？」

**异步双写**（应用层同时写 MySQL 和 ES）→ 简单但容易不一致。**Canal 监听 Binlog**（推荐）→ MySQL 写入后 Canal 监听 binlog 变更推送到 ES，最终一致。**MQ 中转**→ 业务写 MySQL 后发消息，消费者同步到 ES。

### 考点 4：BM25 的三个因子和两个参数

> **面试官**：「ES 的相关性怎么算的？k1 和 b 分别控制什么？」

三个因子：**词频 TF**（出现越多越相关，但有饱和）、**逆文档频率 IDF**（越罕见的词区分度越高）、**文档长度归一化**（短文档命中更有意义）。

两个参数：**k1（默认 1.2）控制词频饱和速度**——调小则出现 1 次和 10 次差别不大，可压制关键词堆砌；**b（默认 0.75）控制长度惩罚强度**——`b=0` 完全忽略文档长度，商品标题这类长度不代表相关性的场景应调小。

**加分项**：主动说明「BM25 调参的收益通常远小于改进分词和加 Rerank（见 [6.10](#610-怎么给-es-加-rerank)），而且改 `similarity` 要重建索引」——体现你知道优化的优先级，而不是死磕参数。

### 考点 5：分片数怎么定？分片是不是越多越好？

> **面试官**：「一个 600GB 的索引你会设几个主分片？」

**先给方法再给数字**：按单分片 20~50GB 估算（搜索场景取小值追求低延迟，日志场景可到 50GB），600GB 按 30GB/分片算就是 20 个主分片。另一个约束是单节点分片数不超过堆内存 GB 数的 20 倍。

**分片绝不是越多越好**——每个分片是独立的 Lucene 实例，有固定的堆内存和文件句柄开销；查询要 fan-out 到所有分片，分片越多协调节点的合并开销越大。**过度分片是比分片不足更常见的错误。**

**追问「分片数能改吗」**：主分片数创建后不可变，只能 `_reindex` 重建，或用 `_split`（扩大，要求原分片数能整除）/`_shrink`（缩小）。所以时序数据要用**按时间滚动索引 + 别名**，既能规避这个问题，删除历史数据也从 `delete_by_query` 变成秒级的 `DELETE` 索引。

### 考点 6：ES 集群调优你会看哪些点？

> **面试官**：「线上 ES 集群响应变慢，你怎么排查和调优？」

**分层回答**：

**JVM 层**——堆内存不超过物理内存 50%（另一半留给 OS Page Cache，Lucene 靠它加速），且不超过 31GB（超过 32GB 失去压缩指针，可用空间反而变少），`Xms = Xmx` 避免堆伸缩停顿。看 GC 频率和 Old GC 耗时。

**查询层**——开慢日志区分 query 阶段慢还是 fetch 阶段慢，两者优化方向完全不同：query 慢是匹配/评分/排序代价大，用 `filter` 替代 `must`、避免 wildcard 和 script；fetch 慢是取 `_source` 的 IO 大，做 `_source` 字段过滤。再用 Profile API 拆解具体耗时。

**索引层**——检查段数量是否过多（只读索引可 `_forcemerge`），检查是否有大量标记删除的文档撑大索引，检查 mapping 是否有不必要的 `nested`。

**架构层**——节点角色是否分离（Master 3 个防脑裂、Coordinating 节点分担合并压力），是否需要冷热分离，热点分片是否可以用 routing 优化。

**加分项**：提到熔断器触发 `CircuitBreakingException` 是保护而非故障，应该优化查询而不是无脑调大阈值。

### 考点 7：批量写入怎么优化？

> **面试官**：「要往 ES 灌 10 亿条数据，怎么加速？」

**导入前**：`refresh_interval` 设 `-1` 关闭自动刷新（避免频繁生成小 segment），`number_of_replicas` 设 0（副本会让写入量翻倍，导完再开由 ES 自己同步）。

**导入中**：用 `_bulk` 批量接口，每批控制在 5~15MB（太小则请求开销占比高，太大则容易触发熔断）；客户端多线程并发但要控制并发度，观察 bulk 拒绝率（`bulk` 线程池队列满会拒绝）。

**导入后**：恢复 `refresh_interval` 和副本数，对不再写入的索引执行 `_forcemerge?max_num_segments=1`。

**必须说的禁忌**：`_forcemerge` 只能对**不再写入**的只读索引做。对活跃索引强制合并会产生巨大的段，后续 merge 极其昂贵，且合并期间 IO 打满影响线上查询——这是典型的运维事故来源。

### 考点 8：什么是冷热分离？怎么落地？

> **面试官**：「日志数据存 90 天，成本很高，怎么优化？」

利用**访问衰减**规律——今天的数据频繁查，三个月前的几乎不查，让不同热度的数据落在不同硬件上。

节点按角色分层：`data_hot`（SSD，承接写入和近期查询）、`data_warm`（普通 SSD，只读）、`data_cold`（大容量机械盘，降副本）、`data_frozen`（对象存储的可搜索快照）。

用 **ILM 策略**自动搬迁：Hot 阶段做 Rollover 按大小/时间滚动；7 天后进 Warm，迁移到温节点并 `forcemerge` + `shrink` 降低段数和分片数；30 天后进 Cold，副本降为 0；90 天后自动 Delete。

**加分项**：指出 Cold 阶段降副本是**用可用性换成本**的明确权衡——冷数据节点故障时这部分数据会短暂不可查，需要业务方确认能接受。

---

## 九、NoSQL 家族速览

ES 属于 NoSQL 大家族的一员。除了 ES 和 [Redis](./08-缓存与Redis.md)（已在 3.8 详细讲过），还有一个常见的 NoSQL 值得了解：

### 9.1 MongoDB——文档型数据库

**核心理念**：用**JSON-like 文档**（BSON）替代关系表的行和列。一个文档就是一条记录，文档可以嵌套，不需要预定义 Schema。

```json
// MongoDB 中的一个"订单"文档——把订单和订单项嵌套在一起
{
  "_id": ObjectId("..."),
  "user": "张三",
  "total": 299.00,
  "items": [
    { "name": "Java 编程思想", "price": 99.00, "qty": 1 },
    { "name": "深入理解 JVM", "price": 200.00, "qty": 1 }
  ],
  "address": { "city": "北京", "district": "海淀" }
}
```

**vs MySQL**：MySQL 这个场景需要 orders 表 + order_items 表 + 外键关联，查询要 JOIN。MongoDB 把相关数据嵌套在一个文档里，一次读取拿到所有信息，读性能好。但代价是不适合频繁跨文档关联查询，也没有 MySQL 那样的 ACID 事务保证（MongoDB 4.0+ 支持多文档事务，但性能代价较大）。

**适用场景**：内容管理系统（文章/评论结构灵活）、用户画像（字段不固定）、物联网设备数据（海量写入）、游戏存档。

**不适用场景**：强事务需求（如金融交易）、复杂关联查询、需要严格 Schema 约束的业务数据。

> 本书不单独开设 MongoDB 章节，因为对于 Java 后端转全栈的路径来说，MySQL + Redis + ES 覆盖了绝大多数场景，MongoDB 属于"了解定位，按需深入"的级别。

---

[← 附录 A4 SQL 语言与数据处理](./A4-SQL语言与数据处理.md) | [返回本章目录](./README.md) | [附录 A6 代码规范与设计原则 →](./A6-代码规范与设计原则.md)
