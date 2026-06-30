# 7.2 Prompt Engineering——让大模型听懂你的话

> Prompt Engineering（提示工程）不是简单的"提问技巧"，而是一套系统化的方法论，用自然语言"编程"来精确控制大模型的行为。对后端工程师来说，这就是学会调用一个全新的"超级 API"。

---

## 一、Prompt 的本质

### 1.1 Prompt 不是"提问"，而是"编程"

很多人把 Prompt 理解为"向 AI 提问"，但更准确的类比是：**你在用自然语言写"代码"来控制模型行为**。

用后端视角理解：

| 传统编程 | Prompt Engineering |
|---------|-------------------|
| 用 Java/Python 写代码 | 用自然语言写指令 |
| 编译器/解释器执行 | LLM 推理引擎执行 |
| 函数签名定义输入输出 | Prompt 结构定义输入输出 |
| 调试用断点和日志 | 调试用迭代和观察输出 |

**核心类比：Prompt ≈ API 请求参数 + 上下文配置**

就像调用一个 REST API 时，你不只传 query 参数，还要传 header、body、context。Prompt 就是 LLM 这个"超级 API"的入参：

```java
// 传统 REST API 调用
HttpRequest request = HttpRequest.newBuilder()
    .uri(URI.create("https://api.example.com/translate"))
    .header("Content-Type", "application/json")      // ≈ System Prompt
    .POST(HttpRequest.BodyPublishers.ofString(body))  // ≈ User Prompt
    .build();

// LLM "API" 调用
ChatCompletion.create(
    model = "gpt-4",
    messages = [
        {"role": "system", "content": "你是一位专业翻译..."},  // 配置
        {"role": "user", "content": "请翻译以下内容..."}       // 请求
    ]
);
```

### 1.2 三种角色：System / User / Assistant

| 角色 | 作用 | Spring 类比 |
|-----|------|------------|
| System Prompt（系统提示） | 设定模型的全局行为、人设、约束 | `@Configuration` 全局配置 |
| User Prompt（用户提示） | 当前轮次的具体请求 | `@RequestBody` 请求参数 |
| Assistant Prompt（助手提示） | 模型之前的回复，构成对话历史 | `ResponseEntity` 历史响应 |

```json
{
  "messages": [
    {"role": "system", "content": "你是一位资深 Java 架构师，回答简洁专业。"},
    {"role": "user", "content": "Spring Bean 的生命周期是什么？"},
    {"role": "assistant", "content": "Spring Bean 生命周期包括：实例化→属性注入→..."},
    {"role": "user", "content": "那循环依赖怎么解决？"}
  ]
}
```

---

## 二、基础 Prompt 技巧

### 2.1 明确指令：角色 + 任务 + 格式

一个好的 Prompt 应该包含三要素，就像 API 文档要说清楚 Content-Type、请求格式和响应格式：

```text
【角色】你是一位资深数据库工程师，精通 MySQL 和 Hive SQL。
【任务】请将以下 MySQL 查询改写为 Hive SQL，保持语义等价。
【格式】输出格式为：先给出改写后的 SQL，再用列表说明改动点。

MySQL 查询：
SELECT DATE_FORMAT(create_time, '%Y-%m') as month, COUNT(*) 
FROM orders WHERE status = 1 GROUP BY month;
```

### 2.2 Few-shot Learning（少样本学习）

Few-shot Learning 通过在 Prompt 中提供示例，让模型学会你期望的模式。类比单元测试的 **Given-When-Then**：给定输入和输出，模型自己学习映射规则。

```text
请将以下 Java 异常信息翻译为用户友好的中文提示：

示例1：
输入：NullPointerException at UserService.getUser(UserService.java:42)
输出：系统暂时无法获取用户信息，请稍后重试

示例2：
输入：ConnectionTimeoutException: connect to db-master:3306 timed out
输出：数据库连接超时，请检查网络状况或联系管理员

现在请处理：
输入：OutOfMemoryError: Java heap space
```

### 2.3 Zero-shot vs One-shot vs Few-shot

| 策略 | 示例数量 | 适用场景 | 效果 |
|------|---------|---------|------|
| Zero-shot（零样本） | 0 个 | 简单、通用任务 | 依赖模型自身能力 |
| One-shot（单样本） | 1 个 | 需要明确格式但规则简单 | 格式对齐好 |
| Few-shot（少样本） | 2-5 个 | 复杂模式、领域特定任务 | 效果最稳定 |

### 2.4 Temperature 和 Top-p 参数

Temperature（温度）和 Top-p（核采样）控制输出的随机性，类比 `java.util.Random` 的 seed：

| 参数 | 值域 | 低值效果 | 高值效果 | 适用场景 |
|------|------|---------|---------|---------|
| Temperature | 0-2 | 确定性高、保守 | 创造性高、发散 | 0: 代码生成；0.7: 文案创作 |
| Top-p | 0-1 | 只选概率最高的词 | 候选词范围更大 | 通常与 Temperature 配合使用 |

```python
# 代码生成场景：要确定性高
response = openai.chat.completions.create(
    model="gpt-4",
    temperature=0,       # 类似 new Random(固定seed)，输出稳定
    messages=[{"role": "user", "content": "写一个 Java 单例模式"}]
)

# 创意写作场景：要多样性
response = openai.chat.completions.create(
    model="gpt-4",
    temperature=0.9,     # 类似 new Random()，每次不同
    top_p=0.95,
    messages=[{"role": "user", "content": "写一个科幻故事开头"}]
)
```

---

## 三、高级推理策略

### 3.1 Chain-of-Thought（CoT，思维链）

CoT 让模型"分步思考"而非直接给答案。用代码类比：直接 return 结果 vs 先打日志记录中间步骤再返回。

```java
// 不用 CoT：直接返回结果（黑盒）
public int calculate(String problem) {
    return answer; // 你不知道怎么算出来的
}

// 用 CoT：记录推理过程（白盒）
public int calculateWithLog(String problem) {
    log.info("步骤1: 识别已知条件...");
    log.info("步骤2: 确定计算公式...");
    log.info("步骤3: 代入数值计算...");
    return answer; // 每一步都可追溯
}
```

CoT Prompt 示例：

```text
请一步一步思考以下问题：

问题：一个水池有两个进水管和一个出水管。进水管A每小时注入3吨水，
进水管B每小时注入2吨水，出水管每小时排出1吨水。水池容量为40吨，
从空池开始，多久能注满？

思考过程：
1. 计算净进水速率：进水A(3) + 进水B(2) - 出水(1) = 4吨/小时
2. 计算注满时间：40吨 ÷ 4吨/小时 = 10小时

答案：10小时
```

### 3.2 Tree-of-Thought（ToT，思维树）

ToT 是 CoT 的升级版：多条推理路径并行探索，选最优解。**类比：单线程 vs 多线程搜索**。

```text
问题：设计一个高并发订单系统的技术方案

路径A（消息队列）：Kafka 削峰 → 异步处理 → 评估：吞吐高，一致性有延迟
路径B（分布式锁）：Redis 锁 → 同步扣减 → 评估：一致性强，吞吐受限
路径C（预扣库存）：预分配到节点 → 本地扣减 → 异步汇总 → 评估：兼顾性能和一致性

最优选择：路径C（综合评分最高）
```

### 3.3 Self-Consistency（自一致性）

同一问题多次采样，取多数票结果。类比分布式系统的**多数派投票（Quorum）**：

```python
# 类比 Raft/Paxos 的多数派机制
answers = []
for i in range(5):  # 采样5次
    answers.append(llm.generate(prompt, temperature=0.7))
final_answer = majority_vote(answers)  # 3/5 一致则采纳
```

### 3.4 ReAct（Reasoning + Acting）

ReAct 模式交替进行推理（Thought）和行动（Action），类比 Java 的 while 循环 + 条件判断：

```java
// ReAct 模式的 Java 伪代码
while (!taskCompleted) {
    String thought = reason(currentState);     // 思考：分析当前状态
    String action = decideAction(thought);      // 决策：选择下一步行动
    String observation = execute(action);       // 执行：获取反馈
    currentState = update(observation);         // 更新状态
}
```

```text
问题：北京今天的气温比昨天高多少度？

Thought 1: 我需要查询北京今天和昨天的气温
Action 1: search("北京今天气温")
Observation 1: 今天最高气温32°C
Thought 2: 还需要查询昨天的气温
Action 2: search("北京昨天气温")
Observation 2: 昨天最高气温28°C
Thought 3: 现在可以计算差值
Answer: 32-28=4°C
```

---

## 四、结构化输出

### 4.1 为什么需要结构化输出

LLM 默认输出自然语言，但程序无法直接解析自然语言回复。我们需要让 LLM 输出机器可读的格式。

### 4.2 JSON Mode

OpenAI 提供 `response_format` 参数强制 JSON 输出：

```python
response = openai.chat.completions.create(
    model="gpt-4-turbo",
    response_format={"type": "json_object"},
    messages=[
        {"role": "system", "content": "你是一个 JSON 生成器，所有输出必须是合法 JSON。"},
        {"role": "user", "content": "分析这条评论的情感：'这个产品太棒了，物流也很快！'"}
    ]
)
# 输出：{"sentiment": "positive", "score": 0.95, "aspects": ["产品质量", "物流速度"]}
```

### 4.3 Function Calling / Tool Use

Function Calling（函数调用）让 LLM 输出结构化的函数调用意图，由程序实际执行。类比 **RPC 的序列化/反序列化**：LLM 生成调用描述（序列化），程序解析并执行（反序列化）。完整流程：

```python
# 1. 定义函数 Schema（类比 .proto 文件）
tools = [{"type": "function", "function": {
    "name": "query_order_status",
    "description": "查询订单状态",
    "parameters": {
        "type": "object",
        "properties": {
            "order_id": {"type": "string", "description": "订单号"},
            "include_logistics": {"type": "boolean", "description": "是否包含物流信息"}
        },
        "required": ["order_id"]
    }
}}]

# 2. 模型输出调用意图（不执行函数，只输出 JSON）
response = openai.chat.completions.create(
    model="gpt-4",
    messages=[{"role": "user", "content": "查一下订单 ORD-2024-001 的物流"}],
    tools=tools
)
# 返回：{"name": "query_order_status", "arguments": {"order_id": "ORD-2024-001", ...}}

# 3. 程序执行实际函数
result = query_order_status(order_id="ORD-2024-001", include_logistics=True)

# 4. 结果回传模型，生成自然语言回复
messages.append({"role": "tool", "content": json.dumps(result)})
final_response = openai.chat.completions.create(model="gpt-4", messages=messages)
```

### 4.4 用 JSON Schema 约束输出格式

```python
from pydantic import BaseModel
from typing import List

# 定义输出结构（类比定义 Response DTO）
class CodeReviewResult(BaseModel):
    file_path: str
    issues: List[dict]  # {"line": int, "severity": str, "message": str}
    overall_score: float
    suggestion: str

# 在 Prompt 中明确 Schema
prompt = f"""请对以下代码进行 Review，输出必须严格符合以下 JSON Schema：
{CodeReviewResult.model_json_schema()}
"""
```

**后端工程师视角**：这就是在定义 LLM 的"接口契约"，和你定义 REST API 的 Request/Response DTO 一样。

---

## 五、Prompt 工程化实践

### 5.1 Prompt Template：参数化模板

类比 **MyBatis 的 SQL 模板**：把变量部分参数化，固定部分复用。

```python
# MyBatis 风格类比
# SQL: SELECT * FROM users WHERE name = #{name} AND age > #{minAge}
# Prompt 模板同理：

REVIEW_TEMPLATE = """
你是一位资深 {language} 工程师，请对以下代码进行 Code Review。

评审维度：
1. 代码规范性
2. 潜在 Bug
3. 性能问题
4. 安全漏洞

代码内容：
{code}

请以 JSON 格式输出，包含 issues 数组和 overall_score（1-10分）。
"""

# 使用模板
prompt = REVIEW_TEMPLATE.format(
    language="Java",
    code=submitted_code
)
```

### 5.2 Prompt 版本管理

像管理代码一样管理 Prompt——用 Git 做版本控制，建议目录结构：

```text
prompts/
├── code-review/
│   ├── v1.0.txt       # 初版
│   └── v2.0.txt       # 重构为结构化输出
├── sql-generator/
│   └── v1.0.txt
└── prompt-registry.yaml  # 注册表：哪个服务用哪个版本
```

### 5.3 Prompt 评估：自动化测试

类比单元测试——为 Prompt 编写测试用例验证效果：

```python
test_cases = [
    {"input": "NullPointerException at line 42", "expected_contains": "空指针"},
    {"input": "Connection refused: 127.0.0.1:6379", "expected_contains": "Redis"},
    {"input": "disk usage 95%", "expected_contains": "磁盘"},
]

def test_prompt_quality(prompt_template, test_cases):
    passed = 0
    for case in test_cases:
        response = llm.generate(prompt_template.format(input=case["input"]))
        if case["expected_contains"] in response:
            passed += 1
    assert passed / len(test_cases) >= 0.8, "Prompt 准确率低于阈值"
```

### 5.4 常见反模式

| 反模式 | 问题 | 改进方案 |
|--------|------|---------|
| Prompt 过长 | 超出上下文窗口，关键信息被稀释 | 精简无关内容，关键指令放前面 |
| 指令冲突 | "要详细"又"要简洁" | 明确优先级，消除矛盾 |
| 上下文污染 | 多轮对话中早期错误回复影响后续 | 定期清理历史，重置 System Prompt |
| 缺乏约束 | 输出格式不稳定 | 用 JSON Schema 或 Few-shot 约束 |

---

## 六、面试深度剖析

### 考点 1：CoT 和直接回答有什么区别？什么场景用 CoT？

> **面试官问**：Chain-of-Thought 和让模型直接回答有什么区别？什么时候该用 CoT？

**回答**：

CoT 的核心区别在于要求模型**显式输出中间推理步骤**，而非直接给最终答案：

1. **准确性**：多步推理问题（数学、逻辑、代码分析）中，CoT 显著提升准确率
2. **可解释性**：输出可审查，推理错误时可针对性纠正
3. **Token 消耗**：CoT 消耗更多 token，简单问题不需要

适用场景：数学推理、多条件逻辑判断、复杂业务规则。不适用：简单事实查询、翻译、格式转换。

### 考点 2：Function Calling 的工作原理？

> **面试官问**：Function Calling 是怎么工作的？模型真的在执行函数吗？

**回答**：

模型**不执行任何函数**，它只是一个"意图翻译器"：

1. 开发者预先定义函数的 Schema（名称、参数、描述），类似 Swagger/OpenAPI 文档
2. 模型根据用户输入，判断应该调用哪个函数、传什么参数，输出一个**结构化的 JSON 调用描述**
3. **程序侧**收到这个 JSON 后，自己去调用真实函数
4. 函数的返回结果再传回模型，模型组织自然语言回复

本质上是一个两阶段 pipeline：LLM 负责"理解意图 + 参数提取"，程序负责"执行 + 返回结果"。类比 Controller 层只做参数解析和路由，真正的业务逻辑在 Service 层。

### 考点 3：如何保证 LLM 输出的 JSON 格式正确？

> **面试官问**：LLM 输出 JSON 经常格式错误，怎么保证可靠性？

**回答**：

多层防护策略：

1. **Prompt 层**：强调"必须输出合法 JSON"，并给 Few-shot 示例
2. **API 层**：使用 `response_format: {type: "json_object"}` 强制 JSON 输出
3. **Schema 层**：用 JSON Schema 或 Pydantic 定义精确结构
4. **代码层**：解析时 try-catch 兜底，失败则重试（exponential backoff）
5. **校验层**：用 JSON Schema Validator 验证字段完整性

组合使用，不依赖单一手段——就像后端 API 同时做参数校验、类型检查和异常处理。

### 考点 4：Temperature 设为 0 是否就完全确定？

> **面试官问**：把 Temperature 设为 0，模型输出就一定一样吗？

**回答**：

**不完全是**。Temperature=0 意味着每一步都选概率最高的 token（贪心解码），理论上应该确定。但实际中仍有微小差异来源：

1. **浮点精度**：GPU 并行计算中浮点运算顺序不同导致微小差异
2. **Batch 效应**：不同 batch 的上下文可能影响 attention 计算
3. **模型更新**：API 背后的模型可能被静默更新
4. **基础设施差异**：不同 GPU 型号的浮点行为有微小差别

建议：增加结果缓存层、对关键输出做二次校验、使用 `seed` 参数提高可重现性。

---

[← 上一节：7.1 RAG 实战](./01-RAG实战.md) | [返回本章导读](./README.md) | [下一节：7.3 模型部署基础 →](./03-模型部署基础.md)
