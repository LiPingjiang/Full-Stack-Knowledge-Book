# 4.2 Java → JS/TS：从名义类型到结构类型

> 对 Java 工程师来说，TypeScript 是**最容易上手**的语言——它带类型，写起来有「家」的感觉。
> 但有几个关键差异会让你「水土不服」。这一节兑现 [3.4 类型系统](../part3-java-deep/04-类型系统.md) 和 [1.3 类型光谱](../part1-mindset-shift/03-从强类型到类型光谱.md) 埋下的钩子。

---

## 一、先建立锚点：TS ≈ 带类型的 JS

一句话定位：**JavaScript 是动态弱类型语言，TypeScript 是给它加了一套编译期类型系统的「超集」。** 你写的 TS 经编译（类型检查后）会**擦除所有类型**变成纯 JS 运行——这一点 [1.3](../part1-mindset-shift/03-从强类型到类型光谱.md) 已经埋过伏笔，下面详谈。

对你而言，TS 的类型注解语法和 Java 几乎一一对应：

```typescript
// TS：类型写在变量/参数后面（冒号），其余和 Java 直觉一致
function greet(name: string, age: number): string {
  return `${name} is ${age}`;
}

interface User {        // 像 Java 的 interface / record
  id: number;
  name: string;
  email?: string;       // ? 表示可选字段（Java 没有这个简洁写法）
}

class UserService {     // class 写法也和 Java 类似
  private users: User[] = [];
  addUser(u: User): void { this.users.push(u); }
}
```

看着是不是很亲切？这就是 TS 对 Java 工程师友好的地方。但接下来是**三个会绊倒你的关键差异**。

---

## 二、差异一：结构类型（鸭子类型的静态版）

这是从 Java 转 TS **最大的认知冲击**。回顾 [3.4](../part3-java-deep/04-类型系统.md)：Java 是**名义类型**——类型兼容看「是否显式声明了继承/实现关系」。TS 是**结构类型**——**「长得一样就是兼容的」**，不管你叫什么名字、有没有声明关系。

```typescript
interface Flyable {
  fly(): void;
}

class Bird {
  fly() { console.log('flap'); }   // 注意：没有 implements Flyable
}

const f: Flyable = new Bird();   // ✅ 在 TS 里完全合法！
                                 // 因为 Bird 的"结构"满足 Flyable（有 fly 方法）
```

对比 [3.4](../part3-java-deep/04-类型系统.md) 里那个**完全相同的例子在 Java 里编译报错**——Java 要求 `Bird` 必须显式 `implements Flyable`。TS 不要求，只看结构。

这带来巨大的灵活性，但也要转变思维：

```typescript
// 任何"结构匹配"的对象都能传，哪怕它来自完全不相干的地方
function printName(obj: { name: string }) {
  console.log(obj.name);
}

printName({ name: '李先生', age: 30 });   // ✅ 有 name 就行，多余的 age 无所谓
printName(new Bird());                    // ❌ Bird 没有 name，结构不匹配
```

**给后端大脑的转换提示**：别再找「这个类 implements 了什么」，而要问「这个对象的结构满足要求吗」。这是 [1.3](../part1-mindset-shift/03-从强类型到类型光谱.md) 提到的结构 vs 名义类型的实战体现。

---

## 三、差异二：类型完全擦除，运行时「裸奔」

回顾 [3.4](../part3-java-deep/04-类型系统.md)：Java 泛型有类型擦除，但**类的类型信息运行时还在**（你能 `getClass()`、能 `instanceof SomeClass`）。TS 更极端——**所有类型注解在编译成 JS 后完全消失，运行时一点都不剩**：

```typescript
interface User { id: number; name: string; }

function process(data: User) {
  // ❌ 编译错误！运行时根本没有 User 这个东西，没法 instanceof
  if (data instanceof User) { }
}
```

这意味着：**TS 的类型只在「写代码 + 编译」时保护你，运行时毫无防护。** 一个从网络拿到的 JSON，你标注成 `User` 类型，但如果后端实际返回的数据结构不对，**运行时不会报错**，类型欺骗了你。

```typescript
// 危险：这个断言在编译期骗过了类型检查，运行时数据可能根本不是 User
const data = JSON.parse(response) as User;   // as 是"类型断言"，运行时不验证
console.log(data.name.toUpperCase());        // 如果 name 不存在，运行时炸
```

**实战准则**：对**外部数据**（API 响应、用户输入、配置文件），不要只靠 TS 类型「嘴上保证」，要用**运行时校验库**（如 Zod）真正验证：

```typescript
import { z } from 'zod';

const UserSchema = z.object({ id: z.number(), name: z.string() });
const data = UserSchema.parse(JSON.parse(response));   // 运行时真的会校验，不符就抛错
```

这相当于你后端用 `@Valid` + Bean Validation 校验入参——**类型注解管编译期，运行时校验管真实数据**，两者都要。

---

## 四、差异三：单线程 async/await（其实你已经懂了）

JS/TS 的并发模型在 [4.1](./01-高并发HTTP服务对比.md) 和 [1.1](../part1-mindset-shift/01-从请求响应到用户交互.md) 已经讲透——**单线程事件循环 + async/await**。这里只强调对 Java 工程师的关键提醒：

```typescript
// async 函数返回 Promise（类比 Java 的 CompletableFuture）
async function getUser(id: number): Promise<User> {
  const resp = await fetch(`/api/users/${id}`);   // await 让出，不阻塞线程
  return resp.json();
}
```

- `Promise<T>` ≈ Java 的 `CompletableFuture<T>`，但语法糖 `async/await` 让它写起来像同步代码。
- **没有 Java 的 `synchronized`、没有锁**——因为单线程，根本不存在多线程数据竞争（[3.2 JMM](../part3-java-deep/02-内存模型JMM.md) 那套问题在单线程的 JS 里几乎不存在）。这是单线程模型的一大「红利」：**你不用操心并发安全**。
- 但**别在 async 里写 CPU 密集的同步死循环**——会卡死唯一的线程。CPU 密集任务要丢给 Worker 线程或拆成异步分片。

---

## 五、生态对照表

帮你把 Java 生态的工具对应到 JS/TS 世界：

| 用途 | Java | JS/TS |
|------|------|-------|
| Web 框架 | Spring Boot | Express / NestJS / Fastify |
| 依赖注入 | Spring IoC | NestJS（理念最像 Spring） |
| ORM | MyBatis / Hibernate | Prisma / TypeORM |
| 构建工具 | Maven / Gradle | npm + Vite（见 [2.4](../part2-frontend-core/04-工程化.md)） |
| 单元测试 | JUnit | Vitest / Jest |
| 校验 | Bean Validation | Zod |
| HTTP 客户端 | OkHttp / RestTemplate | fetch / axios |

> **特别推荐 NestJS**：它的架构（模块、依赖注入、装饰器、分层）几乎是「TypeScript 版 Spring Boot」，是 Java 后端转 Node 后端**最丝滑的桥梁**。

---

## 六、给后端大脑的「翻译词典」

| TS 概念 | Java 类比 | 关键差异 |
|---------|----------|---------|
| `interface` | interface / record | TS 是结构类型，不需 implements |
| 类型注解 | 类型声明 | 运行时完全擦除 |
| `as` 类型断言 | 强制类型转换 | 运行时**不验证**，可能骗你 |
| `Promise<T>` | `CompletableFuture<T>` | 配 async/await 写起来像同步 |
| 单线程模型 | — | 无锁、无 JMM 问题，但怕 CPU 密集 |
| Zod 校验 | `@Valid` | 给运行时数据补上真实校验 |

---

## 本章小结

- TS = 带编译期类型的 JS 超集，类型语法和 Java 高度相似，上手最快。
- **三大关键差异**：(1) **结构类型**（长得像就兼容，不看声明，兑现 [3.4](../part3-java-deep/04-类型系统.md) 钩子）；(2) **类型完全擦除**（运行时裸奔，外部数据务必用 Zod 等运行时校验）；(3) **单线程 async/await**（无锁红利，但怕 CPU 密集）。
- 生态上 **NestJS** 是 Java 后端转 Node 最丝滑的桥梁（TS 版 Spring Boot）。
- 核心转换：从「找 implements 关系」转向「看结构是否匹配」，从「信任类型」转向「编译期信类型 + 运行时校验数据」。

---

[← 上一节：4.1 高并发对比](./01-高并发HTTP服务对比.md) | [下一节：4.3 Java → Go →](./03-Java到Go.md)
