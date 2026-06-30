# 7.6 Agent 基础——让大模型使用工具

> **一句话定位**：直接调用 LLM 只能做"一问一答"，Agent（智能体）则让 LLM 拥有"手和脚"——自主决策调用哪些工具、分多步完成任务、保持记忆。对后端工程师来说，LLM 单独用 ≈ 一个纯函数，Agent ≈ 一个微服务编排器，接收请求后自动判断调哪些下游服务、汇总结果、返回响应。

---

## 一、什么是 Agent

### 1.1 从"一问一答"到"自主行动"

Agent 的本质是：**LLM + 工具调用 + 记忆 + 规划**。

```
普通 LLM 调用（纯函数模式）：
  用户提问 → LLM 生成回答 → 结束

Agent（编排器模式）：
  用户提问 → LLM 分析意图 → 调用工具 A → 拿到结果
           → 再分析 → 调用工具 B → 拿到结果
           → 汇总所有结果 → 生成最终回答
```

### 1.2 后端工程师的理解方式

| Agent 概念 | 后端类比 | 说明 |
|-----------|---------|------|
| LLM | API Gateway 的智能路由 | 根据请求内容决定调用哪个下游服务 |
| 工具（Tools） | 下游微服务 | 天气 API、数据库查询、发邮件等 |
| 记忆（Memory） | Session / Redis 缓存 | 保持对话上下文，跨轮次记住信息 |
| 规划（Planning） | 工作流引擎（Camunda / Airflow） | 分解复杂任务为多步执行计划 |

```java
// Agent ≈ 微服务编排器
public Response handleRequest(Request request) {
    Intent intent = analyzeIntent(request);        // LLM 分析意图
    List<ServiceCall> plan = planExecution(intent); // LLM 规划步骤
    List<Result> results = new ArrayList<>();
    for (ServiceCall call : plan) {
        Result result = executeService(call);       // 调用工具
        results.add(result);
        plan = maybeReplan(plan, result);            // LLM 动态调整
    }
    return summarize(results);                      // LLM 汇总回答
}
```

### 1.3 Agent 的四大能力

| 能力 | 含义 | 后端类比 |
|-----|------|---------|
| **感知（Perception）** | 接收并理解用户输入 | Controller 接收请求并解析参数 |
| **推理（Reasoning）** | 分析意图、拆解任务 | Service 层的业务逻辑判断 |
| **行动（Action）** | 调用外部工具执行操作 | Feign / RestTemplate 调用下游 |
| **记忆（Memory）** | 保持上下文、记住历史 | Redis 存 Session，保持用户状态 |

---

## 二、Function Calling / Tool Use

### 2.1 核心机制

Function Calling（函数调用）是 Agent 最关键的底层能力：**LLM 不直接执行代码，而是输出"我想调用某个函数"的结构化意图**，由应用层去实际执行。

类比：Function Calling ≈ **RPC 的服务发现 + 序列化/反序列化**。LLM 相当于"智能路由器"，根据请求自动选择要调哪个服务并序列化参数。

### 2.2 完整流程（四步）

**步骤 1：定义工具的 JSON Schema（≈ 注册服务的接口文档）**

```json
{
  "tools": [{
    "type": "function",
    "function": {
      "name": "get_weather",
      "description": "查询指定城市的天气信息",
      "parameters": {
        "type": "object",
        "properties": {
          "city": { "type": "string", "description": "城市名称" },
          "date": { "type": "string", "description": "日期，YYYY-MM-DD" }
        },
        "required": ["city"]
      }
    }
  }]
}
```

**步骤 2 → 3：用户发请求，LLM 输出调用意图（不执行，只"说"要调什么）**

```json
// 用户："帮我查上海明天的天气"
// LLM 输出：
{
  "tool_calls": [{
    "id": "call_001",
    "function": {
      "name": "get_weather",
      "arguments": "{\"city\": \"上海\", \"date\": \"2025-01-16\"}"
    }
  }]
}
```

**步骤 4：应用层执行 → 结果回传 LLM → 生成最终回答**

```python
# 应用层执行实际函数
weather_result = get_weather(city="上海", date="2025-01-16")
# 返回：{"city": "上海", "temp": "5℃", "condition": "多云"}

# 把结果回传给 LLM
messages.append({
    "role": "tool",
    "tool_call_id": "call_001",
    "content": '{"city": "上海", "temp": "5℃", "condition": "多云"}'
})
# LLM 收到后生成自然语言："上海明天多云，气温 5℃。"
```

### 2.3 LLM 怎么知道调哪个函数

LLM 选函数和生成文本本质相同——**基于概率预测**。把工具定义传入后，模型靠 `description` 字段匹配用户意图，靠参数描述提取参数值。所以**写好工具描述至关重要**，这和 Prompt Engineering 是相通的。

### 2.4 和 7.2 中 Function Calling 的区别

| 维度 | 7.2 中的 Function Calling | 本节的 Function Calling |
|------|--------------------------|------------------------|
| 目的 | 让 LLM 输出结构化 JSON | 让 LLM 驱动外部工具执行 |
| 是否执行 | 不执行，只格式化输出 | 实际调用函数并获取结果 |
| 有无循环 | 单次调用 | 可能多轮：调用 → 拿结果 → 再调用 |
| 应用场景 | 信息提取、格式化 | Agent 工具调用、自动化工作流 |

---

## 三、MCP（Model Context Protocol）

### 3.1 MCP 是什么

MCP（Model Context Protocol，模型上下文协议）是 Anthropic 于 2024 年底提出的开放协议，目标是**标准化 LLM 与外部工具/数据的连接方式**。此前各平台工具接入格式互不兼容，MCP 要解决这个碎片化问题。

### 3.2 JDBC 类比

```
MCP ≈ JDBC/ODBC
JDBC 让 Java 程序连接任何数据库，不需要为每个库写专用代码。
MCP 让任何 LLM 应用连接任何工具，不需要为每个平台写专用适配。
```

| JDBC 世界 | MCP 世界 | 说明 |
|-----------|---------|------|
| Java 应用 | LLM 应用（Host） | 最终用户使用的应用 |
| JDBC Driver | MCP Client | 连接器，负责协议转换 |
| 数据库 | MCP Server | 工具/数据的提供方 |

### 3.3 三个角色与架构

```
┌────────────────────────────────────────┐
│            Host（宿主）                 │
│     例：Claude Desktop、IDE 插件        │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐  │
│  │Client A │ │Client B │ │Client C │  │
│  └────┬────┘ └────┬────┘ └────┬────┘  │
└───────┼───────────┼───────────┼───────┘
   ┌────▼────┐ ┌────▼────┐ ┌────▼────┐
   │ Server  │ │ Server  │ │ Server  │
   │(GitHub) │ │ (数据库) │ │ (Slack) │
   └─────────┘ └─────────┘ └─────────┘
```

### 3.4 MCP Server 实现示例

```typescript
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";

const server = new McpServer({ name: "database-server", version: "1.0.0" });

server.tool(
  "query_orders",
  "根据条件查询订单列表",
  {
    status: z.enum(["pending", "paid", "shipped"]).describe("订单状态"),
    limit: z.number().default(10).describe("返回条数")
  },
  async ({ status, limit }) => {
    const orders = await db.query(
      "SELECT * FROM orders WHERE status = ? LIMIT ?", [status, limit]
    );
    return { content: [{ type: "text", text: JSON.stringify(orders) }] };
  }
);
server.listen();
```

### 3.5 MCP vs Function Calling

| 维度 | Function Calling | MCP |
|------|-----------------|-----|
| 定位 | 一个 LLM 调一组工具的接口规范 | 任何 LLM 连任何工具的通用协议 |
| 绑定关系 | 和特定 LLM 平台绑定 | 平台无关，通用标准 |
| 工具发现 | 手动传入工具定义 | 工具自注册、自描述 |
| 传输方式 | HTTP 请求的一部分 | 支持 stdio、HTTP SSE 等 |
| 类比 | 私有 API 接口 | JDBC/ODBC 开放标准 |

> **要点**：Function Calling 是调用工具的"动作"，MCP 是让这个动作标准化、可复用的"协议层"。两者是不同层次，不是替代关系。

---

## 四、Agent 设计模式

### 4.1 ReAct（Reasoning + Acting）

最经典的模式，核心是**交替推理和行动**。

```
用户：苹果公司最新市值折合人民币多少？

Thought 1: 需要先查市值
Action 1:  search_web("Apple Inc market cap")
Observation 1: $3.45 trillion

Thought 2: 需要查汇率做换算
Action 2:  get_exchange_rate("USD", "CNY")
Observation 2: 1 USD = 7.28 CNY

Thought 3: 3.45万亿 × 7.28 = 25.12万亿，可以回答了
Final Answer: 约 3.45 万亿美元，折合约 25.12 万亿人民币。
```

类比：`while(true) { 分析问题(); 调用工具(); 检查结果(); if(满足) break; }`

### 4.2 Planning（规划优先）

**先制定完整计划，再逐步执行**。类比：项目管理中"先写设计文档，评审通过后再编码"。

```
用户：帮我写一篇 Redis 7.0 新特性的技术博客

Plan:
  1. 搜索 Redis 7.0 Release Notes
  2. 整理核心新特性（Functions、Multi-part AOF 等）
  3. 为每个特性编写示例代码
  4. 组织博客结构，检查技术准确性

Execute Step 1: search_web("Redis 7.0 release notes") → ...
```

### 4.3 Reflection（反思改进）

执行后增加**自我审查**，发现错误就修正。类比：Code Review + 单元测试。

### 4.4 Multi-Agent（多智能体协作）

多个 Agent 各司其职，类比微服务架构。常见协作模式：

| 模式 | 描述 | 类比 |
|------|------|------|
| **顺序执行（Pipeline）** | A → B → C 依次处理 | 工厂流水线 |
| **层级委托（Hierarchical）** | Manager 分配任务给 Workers | 经理分活给组员 |
| **辩论协作（Debate）** | 多 Agent 给不同观点 | 技术评审会 |
| **竞争选优（Competitive）** | 各自完成，选最优 | A/B 测试 |

### 4.5 四种模式对比

| 模式 | 核心思想 | 优势 | 劣势 | 适合场景 |
|------|---------|------|------|---------|
| ReAct | 边推理边行动 | 灵活适应 | 可能死循环 | 通用工具调用 |
| Planning | 先规划后执行 | 可控可预测 | 计划可能脱离实际 | 复杂多步任务 |
| Reflection | 执行后自审 | 输出质量高 | 增加时间开销 | 代码生成、创作 |
| Multi-Agent | 多角色协作 | 专业分工 | 协调成本高 | 复杂业务流程 |

---

## 五、Agent 框架对比

| 维度 | LangChain | LlamaIndex | AutoGen | CrewAI |
|------|-----------|------------|---------|--------|
| **核心定位** | 通用 LLM 应用框架 | 数据检索 + Agent | Multi-Agent 对话 | 角色化 Multi-Agent |
| **优势** | 生态最丰富、社区活跃 | RAG + Agent 无缝集成 | 对话式协作先驱 | 角色定义直观 |
| **劣势** | 抽象层过多（"胶水代码"） | Agent 能力较弱 | 对话来回次数多 | 灵活性不够 |
| **适合场景** | 通用 Agent 开发 | RAG 密集型应用 | 多角色协作任务 | 流程化团队任务 |
| **后端类比** | Spring 全家桶 | MyBatis（数据层强） | Akka Actor | Camunda（流程编排） |

```python
# LangChain Agent 示例
from langchain.agents import create_tool_calling_agent
from langchain_openai import ChatOpenAI

llm = ChatOpenAI(model="gpt-4")
tools = [search_tool, calculator_tool, database_tool]
agent = create_tool_calling_agent(llm, tools, prompt)
result = agent.invoke({"input": "上海到北京高铁票价折合多少美元"})
```

```python
# AutoGen Multi-Agent 示例
from autogen import AssistantAgent, UserProxyAgent

coder = AssistantAgent("coder", llm_config=llm_config)
reviewer = AssistantAgent("reviewer", llm_config=llm_config)
executor = UserProxyAgent("executor", code_execution_config=exec_config)
# coder 写代码 → executor 执行 → reviewer 审查 → 循环直到通过
executor.initiate_chat(coder, message="写一个 Python 爬虫抓取新闻标题")
```

---

## 六、NL2SQL 实战案例

NL2SQL（Natural Language to SQL，自然语言转 SQL）是 Agent 最经典的落地场景之一。

### 6.1 整体流程

```
用户："上个月销售额最高的前 5 个城市是哪些？"
        │
        ▼
  NL2SQL Agent：
  1. 理解意图 → 查询销售额 Top 5 城市
  2. 获取表结构 → orders(city, amount, date)
  3. 生成 SQL →
     SELECT city, SUM(amount) as total FROM orders
     WHERE date >= '2024-12-01' AND date < '2025-01-01'
     GROUP BY city ORDER BY total DESC LIMIT 5
  4. 执行 SQL → 拿到结果
  5. 格式化 → "1. 上海 1,234万  2. 北京 1,180万 ..."
```

### 6.2 实现要点

```python
# 工具 1：获取表结构，供 LLM 理解数据库 schema
def get_table_schema(table_name: str) -> str:
    return db.execute(f"SHOW CREATE TABLE {table_name}").fetchone()

# 工具 2：执行 SQL（带安全检查，禁止写操作）
def execute_sql(sql: str) -> list:
    if any(kw in sql.upper() for kw in ["DROP", "DELETE", "UPDATE", "INSERT"]):
        return "拒绝执行：只允许 SELECT 查询"
    return db.execute(sql).fetchall()
```

### 6.3 核心挑战

| 挑战 | 应对策略 |
|------|---------|
| **表结构理解**——LLM 不了解业务表 | DDL + 字段注释 + 样例数据放进上下文 |
| **复杂 JOIN**——多表关联易出错 | 提供表关系说明，限制 JOIN 层数 |
| **SQL 注入**——生成的 SQL 可能被利用 | 只读权限 + 白名单 + 参数化 |
| **语义歧义**——"上个月"指什么？ | Prompt 中明确时间计算规则 |
| **结果验证**——SQL 逻辑可能不对 | 先 EXPLAIN，检查行数是否合理 |

### 6.4 和后端工程师的关联

NL2SQL 不替代你的查询服务，而是在其上增加自然语言接口。你的存储过程、查询优化经验在设计 NL2SQL 系统时更有价值——因为你知道哪些查询高效、哪些表结构合理。

---

## 七、面试深度剖析

### 考点 1：Agent 和普通 LLM 调用有什么区别？

普通 LLM 调用是无状态的一问一答，不能和外部世界交互。Agent 增加了三个能力：**工具调用**（驱动外部系统）、**记忆**（短期对话历史 + 长期向量存储）、**规划**（多步分解、动态调整）。后端类比：普通调用像无状态 `@GetMapping`，Agent 像有状态的工作流引擎。

### 考点 2：Function Calling 的工作原理？LLM 怎么知道调哪个函数？

在 API 请求中把工具的 JSON Schema 传给 LLM，模型靠工具的 `description` 匹配用户意图、靠参数描述提取参数值，输出结构化的调用意图而非纯文本。关键点：**LLM 只输出意图，不执行**，实际执行由应用层完成，保证安全性和灵活性。

### 考点 3：Multi-Agent 的协作模式有哪些？

四种模式：顺序执行（Pipeline，适合流程明确的任务）、层级委托（Hierarchical，适合可拆分的大任务）、辩论协作（Debate，适合多视角评估）、竞争选优（Competitive，适合有明确评价标准的场景）。核心挑战是协调成本——Agent 间通信可能导致延迟和 token 消耗剧增。

### 考点 4：Agent 的可靠性问题怎么解决？

| 问题 | 解决方案 |
|------|---------|
| 幻觉导致错误工具调用 | 参数校验 + 工具白名单 + 重试 |
| 死循环 | 最大迭代次数 + 超时中断 + 循环检测 |
| 目标偏离 | 中间审核 + 约束明确的 System Prompt |
| 安全风险（如删库） | 最小权限 + 操作审批 + 只读模式 |

工程最佳实践：**防御性设计**（参数校验如 `@Valid`）、**可观测性**（记录每步 Thought/Action/Observation，类似链路追踪）、**兜底策略**（无法完成时优雅降级为人工处理）。

---

[← 上一节：7.5 微调入门](./05-微调入门.md) | [返回本章导读](./README.md) | [下一节：7.7 分布式训练 →](./07-分布式训练.md)
