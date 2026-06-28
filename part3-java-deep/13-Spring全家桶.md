# 3.13 Spring 全家桶：为什么 Java 后端绕不开它

> 「Spring 和 Spring Boot 什么关系？」「IOC 到底在干嘛？」「Bean 的生命周期说一遍？」——Java 后端面试里，Spring 是出镜率最高的框架。不是因为它最新最炫，而是它用**控制反转 + 约定大于配置**把整个 Java 后端生态黏合成了一个整体。理解 Spring，你才能真正看懂 Java 后端项目的骨架。

---

## 一、Spring 生态全景

先建立一张地图：Spring 不是单一框架，而是一组**从底层容器到上层微服务**的分层体系。

```mermaid
flowchart TD
    A["Spring Cloud<br/>微服务组件集：注册发现 / 配置中心 / 网关 / 熔断"]
    B["Spring Boot<br/>自动配置 + Starter 机制，开箱即用"]
    C["Spring Framework<br/>IOC 容器 + AOP + MVC + 事务管理（最底层基石）"]

    A -->|基于| B -->|基于| C

    style A fill:#e1f5fe,stroke:#0288d1
    style B fill:#e8f5e9,stroke:#388e3c
    style C fill:#fff3e0,stroke:#f57c00
```

| 层级 | 解决什么问题 | 一句话 |
|------|-------------|--------|
| **Spring Framework** | 对象管理、依赖注入、切面编程 | 骨架 |
| **Spring Boot** | 自动配置、内嵌容器、约定优先 | 省心 |
| **Spring Cloud** | 微服务治理：注册/配置/网关/熔断 | 分布式全家桶 |

### 为什么 Java 后端 ≈ Spring？

1. **生态垄断**：几乎所有主流中间件都提供了 Spring 集成（MyBatis-Spring、Spring Data Redis、Spring Kafka……），不用 Spring 你就等着自己造轮子。
2. **约定大于配置**：Boot 把 80% 的模板配置默认好了，你只需要关注业务代码。
3. **开箱即用**：一个 `@SpringBootApplication` + `main` 方法，服务就跑起来了，不需要额外的 Tomcat 部署。
4. **社区/招聘市场**：后端岗位 JD 里"熟练使用 Spring / Spring Boot"是标配。

---

## 二、IOC 容器（面试必问）

### 2.1 什么是控制反转 / 依赖注入

大白话：以前你自己 `new` 对象，现在把对象的「创建权」交给 Spring 容器，你只声明「我要什么」，容器帮你注进来。

```java
// ❌ 传统写法：你亲手 new，耦合死
public class OrderService {
    private UserDao userDao = new UserDaoMysqlImpl(); // 想换实现？改代码重新编译
}

// ✅ Spring IOC：声明依赖，容器注入
@Service
public class OrderService {
    private final UserDao userDao; // 接口类型

    @Autowired // 容器帮你找到 UserDao 的实现注进来
    public OrderService(UserDao userDao) {
        this.userDao = userDao;
    }
}
```

| 维度 | 传统 new | IOC 注入 |
|------|---------|---------|
| 依赖关系 | 硬编码在类里 | 外部（容器）管理 |
| 切换实现 | 改源码重新编译 | 改配置或换 @Qualifier |
| 可测试性 | 难 mock | 构造器传 mock 即可 |
| 谁控制对象生命周期 | 调用方 | Spring 容器 |

### 2.2 BeanFactory vs ApplicationContext

| 特性 | BeanFactory | ApplicationContext |
|------|-------------|-------------------|
| 定位 | Spring 最底层容器接口 | BeanFactory 的超集（日常用这个） |
| Bean 加载时机 | 懒加载（getBean 时才创建） | 预加载（启动时创建所有单例 Bean） |
| 额外功能 | 无 | 事件发布、国际化、AOP 自动代理、Environment 等 |
| 适用场景 | 资源极受限的嵌入式 | 99.9% 的生产应用 |

> 面试简答：「ApplicationContext 继承了 BeanFactory，在其基础上增加了 AOP、事件、国际化等企业级功能，并在启动时预创建单例 Bean。」

### 2.3 Bean 的作用域

| 作用域 | 实例数量 | 使用场景 |
|--------|---------|---------|
| **singleton**（默认） | 容器内唯一 | 无状态 Service / DAO |
| **prototype** | 每次 getBean 新建 | 有状态对象 |
| **request** | 每个 HTTP 请求一个 | Web 应用的请求级数据 |
| **session** | 每个 HTTP Session 一个 | 用户会话数据 |

> 面试坑：把有状态 Bean 设为 singleton → 并发数据污染。

### 2.4 三种注入方式

```java
// ① 构造器注入（官方推荐）
@Service
public class OrderService {
    private final UserDao userDao;

    public OrderService(UserDao userDao) { // 不可变 + 完整性强
        this.userDao = userDao;
    }
}

// ② Setter 注入
@Service
public class OrderService {
    private UserDao userDao;

    @Autowired
    public void setUserDao(UserDao userDao) {
        this.userDao = userDao;
    }
}

// ③ 字段注入（最简洁但不推荐）
@Service
public class OrderService {
    @Autowired
    private UserDao userDao; // 无法 final，难单测
}
```

| 方式 | 优点 | 缺点 | 推荐度 |
|------|------|------|--------|
| **构造器注入** | 不可变、强制完整性、易单测 | 参数多时构造器长 | ⭐⭐⭐⭐⭐ |
| Setter 注入 | 可选依赖友好 | 对象可能处于不完整状态 | ⭐⭐⭐ |
| 字段注入 | 代码少 | 无法 final、难 mock、隐藏依赖 | ⭐⭐ |

> **结论**：Spring 官方从 4.3 起推荐构造器注入；字段注入只剩「写得少」这一个优点，面试别说"我用 @Autowired 加字段"。

<details>
<summary><b>展开：循环依赖怎么解决？三级缓存是什么？</b></summary>

**场景**：A 依赖 B，B 又依赖 A。如果都是构造器注入 → 直接报错（Spring 无法解决构造器循环依赖）。

**Spring 对 singleton + 字段/Setter 注入的解决方案——三级缓存**：

| 缓存 | 名称 | 存什么 |
|------|------|--------|
| 一级缓存 | `singletonObjects` | 完全初始化好的 Bean |
| 二级缓存 | `earlySingletonObjects` | 提前暴露的半成品 Bean（属性还没填） |
| 三级缓存 | `singletonFactories` | Bean 的 ObjectFactory（可生成代理） |

**流程简述**：
1. 创建 A → 实例化 A（空壳）→ 把 A 的 ObjectFactory 放入三级缓存
2. 填充 A 属性 → 发现需要 B → 去创建 B
3. 创建 B → 实例化 B → 填充 B 属性 → 发现需要 A
4. 从三级缓存拿到 A 的 ObjectFactory → 调用它拿到 A 的早期引用（可能是代理）→ 放入二级缓存
5. B 拿到 A 的引用 → B 创建完成 → 放入一级缓存
6. 回来继续填充 A → A 创建完成

**面试答题口诀**：「三级缓存的核心目的是在 Bean 还没初始化完时就把它的引用暴露出来；三级缓存（而不是二级）的额外价值是延迟代理对象的创建。」

> 注意：Spring Boot 2.6+ 默认**禁止循环依赖**（`spring.main.allow-circular-references=false`），官方立场是从设计上消灭循环依赖。

</details>

---

## 三、AOP（面向切面编程）

### 3.1 什么问题用 AOP 解决

想象每个 Service 方法都要加日志、事务、权限校验——这些逻辑跟业务无关却散落各处，就是**横切关注点（Cross-Cutting Concerns）**。

AOP 的思路：把横切逻辑**抽到切面里**，运行时自动织入目标方法，业务代码不感知。

```mermaid
flowchart LR
    subgraph 横切关注点
        L["日志打印<br/>每个方法都要记"]
        T["事务管理<br/>方法开始开事务，结束提交/回滚"]
        P["权限校验<br/>调用前检查"]
        M["性能监控<br/>计时"]
    end

    S1["ServiceA.method()"] -.-> L & T & P & M
    S2["ServiceB.method()"] -.-> L & T & P & M
    S3["ServiceC.method()"] -.-> L & T & P & M

    style L fill:#fff3e0,stroke:#f57c00
    style T fill:#fff3e0,stroke:#f57c00
    style P fill:#fff3e0,stroke:#f57c00
    style M fill:#fff3e0,stroke:#f57c00
```

### 3.2 核心概念

| 概念 | 英文 | 大白话 |
|------|------|--------|
| **切面 Aspect** | Aspect | 横切逻辑的模块（一个类） |
| **切点 Pointcut** | Pointcut | "在哪些方法上切"（表达式） |
| **通知 Advice** | Advice | "在切点处做什么"（前置/后置/环绕…） |
| **织入 Weaving** | Weaving | 把通知应用到目标对象的过程 |
| **连接点 JoinPoint** | JoinPoint | 程序执行中可以插入切面的点（Spring 只支持方法级） |

通知类型：

| 注解 | 时机 | 典型用途 |
|------|------|---------|
| `@Before` | 方法执行前 | 参数校验、权限检查 |
| `@After` | 方法执行后（无论是否异常） | 资源清理 |
| `@AfterReturning` | 方法正常返回后 | 结果日志 |
| `@AfterThrowing` | 方法抛异常后 | 异常告警 |
| `@Around` | 环绕（最强大，可控制是否执行目标方法） | 事务、耗时统计 |

### 3.3 JDK 动态代理 vs CGLIB 代理

| 维度 | JDK 动态代理 | CGLIB |
|------|-------------|-------|
| 原理 | 基于接口，生成实现了同接口的代理类 | 基于继承，生成目标类的子类 |
| 要求 | 目标类必须实现接口 | 目标类不能是 final |
| 性能 | 调用略快（JDK 新版已优化） | 生成代理略慢，方法调用通过 FastClass |
| Spring 默认 | 有接口用 JDK 代理 | 无接口用 CGLIB |
| Spring Boot 2.x+ 默认 | — | **统一使用 CGLIB**（`proxyTargetClass=true`） |

> 面试简答：「Spring Boot 默认 CGLIB。因为 JDK 代理要求接口，开发者经常忘了写接口导致注入失败，统一 CGLIB 更省心。」

### 3.4 @Transactional 就是 AOP

Spring 的声明式事务本质是通过 AOP 在方法前后织入 `开启事务 / 提交 / 回滚` 逻辑：

```java
@Service
public class OrderService {

    @Transactional(rollbackFor = Exception.class)
    public void createOrder(OrderDTO dto) {
        // 1. 扣库存
        // 2. 创建订单
        // 3. 扣余额
        // 任何一步抛异常 → 自动回滚
    }
}
```

<details>
<summary><b>展开：@Transactional 失效的常见场景</b></summary>

| 场景 | 原因 | 解决 |
|------|------|------|
| **同类内部调用** | `this.method()` 不走代理 | 注入自身 / 抽出到另一个 Bean |
| **方法不是 public** | Spring AOP 默认只代理 public 方法 | 改为 public |
| **异常被 catch 吞掉** | 代理看不到异常，不触发回滚 | 抛出去或手动 `setRollbackOnly` |
| **rollbackFor 没配** | 默认只回滚 RuntimeException | 加 `rollbackFor = Exception.class` |
| **数据库引擎不支持事务** | 如 MySQL 的 MyISAM | 换 InnoDB |
| **多数据源/非 Spring 管理的连接** | 事务管理器不匹配 | 指定 `transactionManager` |

**面试必背的第一条**：同类调用失效——因为 `@Transactional` 是 AOP 实现，AOP 通过代理对象拦截，`this` 调用绕过了代理。

</details>

---

## 四、Bean 生命周期（面试高频）

### 4.1 完整流程

```
实例化(Instantiation)
    ↓
属性填充(Populate Properties) —— 依赖注入
    ↓
Aware 接口回调 —— BeanNameAware / BeanFactoryAware / ApplicationContextAware
    ↓
BeanPostProcessor#postProcessBeforeInitialization
    ↓
初始化 —— @PostConstruct / InitializingBean#afterPropertiesSet / init-method
    ↓
BeanPostProcessor#postProcessAfterInitialization  ← AOP 代理在这里生成！
    ↓
Bean 就绪，放入容器供使用
    ↓
销毁 —— @PreDestroy / DisposableBean#destroy / destroy-method
```

### 4.2 各阶段做什么（对比表）

| 阶段 | 做什么 | 你能介入的接口/注解 |
|------|--------|-------------------|
| 实例化 | 通过构造器创建对象（空壳） | 构造方法 |
| 属性填充 | 把依赖 Bean 注入进来 | @Autowired / @Value |
| Aware 回调 | 让 Bean 感知容器信息 | BeanNameAware, ApplicationContextAware |
| 前置处理 | 初始化之前拦截 | BeanPostProcessor#postProcessBeforeInitialization |
| 初始化 | 自定义初始化逻辑 | @PostConstruct / InitializingBean / init-method |
| 后置处理 | 初始化之后拦截（**AOP 代理生成点**） | BeanPostProcessor#postProcessAfterInitialization |
| 使用 | 业务调用 | — |
| 销毁 | 释放资源 | @PreDestroy / DisposableBean / destroy-method |

<details>
<summary><b>展开：BeanPostProcessor 能用来做什么？AOP 是在哪个阶段生成代理的？</b></summary>

**BeanPostProcessor 能做什么**：

- `AbstractAutoProxyCreator`（AOP 代理创建）：在 `postProcessAfterInitialization` 阶段对需要代理的 Bean 生成代理对象
- `AutowiredAnnotationBeanPostProcessor`：处理 `@Autowired` / `@Value` 注入
- `CommonAnnotationBeanPostProcessor`：处理 `@PostConstruct` / `@PreDestroy` / `@Resource`
- 自定义：监控 Bean 创建耗时、动态修改 Bean 属性、实现自定义注解处理

**AOP 代理在哪个阶段生成？**

在 `postProcessAfterInitialization`（后置处理阶段）。具体实现类是 `AbstractAutoProxyCreator`，它检查 Bean 是否匹配任何 Advisor 的切点，如果匹配就创建代理对象返回（替代原始 Bean）。

**特殊情况——循环依赖**：如果发生循环依赖，代理会提前在三级缓存的 `getEarlyBeanReference` 中创建（通过 `SmartInstantiationAwareBeanPostProcessor#getEarlyBeanReference`），不再等到后置处理阶段。

</details>

---

## 五、Spring Boot 自动配置原理

### 5.1 @SpringBootApplication 拆解

```java
@SpringBootApplication
// 等价于：
@SpringBootConfiguration    // 本质就是 @Configuration
@ComponentScan              // 扫描当前包及子包
@EnableAutoConfiguration    // 核心：开启自动配置
public class Application {
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
```

### 5.2 自动配置加载流程

```
@EnableAutoConfiguration
    ↓
@Import(AutoConfigurationImportSelector.class)
    ↓
读取 META-INF/spring.factories（Spring Boot 2.x）
或 META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports（3.x）
    ↓
获取所有自动配置类的全限定名列表
    ↓
通过条件注解过滤（@ConditionalOnXxx）
    ↓
满足条件的配置类被加载 → 注册 Bean
```

### 5.3 条件装配注解

| 注解 | 含义 |
|------|------|
| `@ConditionalOnClass` | 类路径上有指定 class 才生效 |
| `@ConditionalOnMissingBean` | 容器中没有该 Bean 才创建（让你可以覆盖默认） |
| `@ConditionalOnProperty` | 配置文件中有指定属性才生效 |
| `@ConditionalOnWebApplication` | 当前是 Web 应用才生效 |
| `@ConditionalOnBean` | 容器中已有指定 Bean 才生效 |

### 5.4 Starter 机制

Starter = 依赖聚合 + 自动配置。引入一个 jar 就配好一切：

```xml
<!-- 引入 starter-web：自动配好 Tomcat + DispatcherServlet + Jackson -->
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-web</artifactId>
</dependency>
```

| Starter | 自动配好什么 |
|---------|-------------|
| `starter-web` | 内嵌 Tomcat + Spring MVC + Jackson |
| `starter-webflux` | 内嵌 Netty + WebFlux（响应式） |
| `starter-data-redis` | RedisTemplate + Lettuce 连接池 |
| `starter-data-jpa` | Hibernate + DataSource + EntityManager |

> 呼应 [3.6 网络 IO 模型](./06-网络IO模型.md)：`starter-web` 底层是 Servlet（BIO/NIO 同步模型），`starter-webflux` 底层是 Reactor + Netty（全异步非阻塞）。选哪个取决于你的 IO 模型诉求。

---

## 六、Spring MVC 请求处理流程

### 6.1 核心流程图

```
客户端请求
    ↓
DispatcherServlet（前端控制器，入口）
    ↓
HandlerMapping（找到哪个 Controller 处理这个 URL）
    ↓
HandlerAdapter（适配调用，处理参数绑定）
    ↓
Controller 方法执行 → 返回结果
    ↓
ViewResolver（视图解析，前后端分离时直接返回 JSON）
    ↓
响应客户端
```

### 6.2 与 Servlet 规范的关系

`DispatcherServlet` 本质是一个 `HttpServlet`（继承自 `FrameworkServlet` → `HttpServletBean` → `HttpServlet`），它注册在 Servlet 容器（Tomcat）里，接管所有请求后按 Spring MVC 的策略分发。

所以 Spring MVC = Servlet 规范上的**更高层抽象**。你用 `@RequestMapping` 写的 Controller，最终还是跑在 Servlet 容器里。

### 6.3 常用注解速查

| 注解 | 作用 | 示例 |
|------|------|------|
| `@Controller` | 标记 MVC 控制器（返回视图名） | 传统 JSP 项目 |
| `@RestController` | `@Controller` + `@ResponseBody`（返回 JSON） | REST API |
| `@RequestMapping` | 映射 URL 到方法 | `@RequestMapping("/api/orders")` |
| `@GetMapping` / `@PostMapping` | 语义化的 HTTP 方法映射 | `@GetMapping("/{id}")` |
| `@RequestBody` | 把请求体 JSON 反序列化为对象 | `public R create(@RequestBody OrderDTO dto)` |
| `@PathVariable` | 取 URL 路径变量 | `@GetMapping("/{id}")` → `@PathVariable Long id` |
| `@RequestParam` | 取查询参数 | `?page=1` → `@RequestParam int page` |
| `@ResponseBody` | 方法返回值直接写入响应体 | 返回 JSON 而不是视图 |

---

## 七、Spring Cloud 核心组件

> 这一节不深展开实现细节，重在让你知道微服务治理有哪些领域、每个领域主流选型是什么，面试时能画出全景图。

### 7.1 全景对比

| 治理领域 | Netflix 时代（已停更） | 当前主流 | 作用 |
|---------|----------------------|---------|------|
| 服务注册发现 | Eureka | **Nacos** | 服务实例注册、发现、健康检查 |
| 配置中心 | Spring Cloud Config | **Nacos Config** / Apollo | 统一管理配置、热更新 |
| API 网关 | Zuul 1.x | **Spring Cloud Gateway** | 路由、鉴权、限流、灰度 |
| 负载均衡 | Ribbon | **Spring Cloud LoadBalancer** | 客户端负载均衡策略 |
| 熔断限流 | Hystrix | **Sentinel** / Resilience4j | 熔断降级、流控、系统保护 |
| 链路追踪 | Sleuth + Zipkin | **Micrometer Tracing** + Zipkin / SkyWalking | 分布式调用链可视化 |
| 远程调用 | Feign | **OpenFeign** | 声明式 HTTP 客户端 |
| 分布式事务 | — | **Seata** | 跨服务事务一致性 |

### 7.2 各组件一句话说明

- **Nacos**：既做注册中心又做配置中心，阿里开源，国内使用率最高。
- **Spring Cloud Gateway**：基于 WebFlux（Netty），非阻塞，替代了阻塞式的 Zuul 1.x。
- **Sentinel**：阿里开源的流控组件，支持熔断/限流/系统自适应保护，控制台可视化配置。与 [3.7 高可用架构](./07-高可用架构.md) 中的熔断限流思路一脉相承。
- **OpenFeign**：像调本地方法一样调远程 HTTP 接口，底层自动集成负载均衡。
- **Seata**：分布式事务中间件，支持 AT/TCC/Saga 等模式。

<details>
<summary><b>展开：微服务选型决策树（国内 Java 后端）</b></summary>

```mermaid
flowchart TD
    R["注册中心？"] -->|已有 ZK 且不想换| R1["ZK + Dubbo 体系"]
    R -->|新项目| R2["Nacos（注册 + 配置一站式）"]

    G["网关？"] --> G1["Spring Cloud Gateway<br/>（不要再用 Zuul 1.x）"]

    F["熔断限流？"] -->|简单场景| F1["Resilience4j（轻量、函数式）"]
    F -->|需要控制台 + 动态推送| F2["Sentinel"]

    T["链路追踪？"] -->|已有基础设施| T1["SkyWalking（字节码增强，无侵入）"]
    T -->|Spring 生态优先| T2["Micrometer Tracing + Zipkin"]

    C["远程调用？"] -->|Spring Cloud 体系| C1["OpenFeign"]
    C -->|高性能 RPC| C2["Dubbo / gRPC"]

    style R fill:#e1f5fe,stroke:#0288d1
    style G fill:#e1f5fe,stroke:#0288d1
    style F fill:#e1f5fe,stroke:#0288d1
    style T fill:#e1f5fe,stroke:#0288d1
    style C fill:#e1f5fe,stroke:#0288d1
```

</details>

---

## 本篇小结

| 知识点 | 面试频率 | 关键词 |
|--------|---------|--------|
| IOC / DI | ⭐⭐⭐⭐⭐ | 控制反转、构造器注入、三级缓存 |
| AOP | ⭐⭐⭐⭐⭐ | 动态代理、CGLIB、@Transactional 失效 |
| Bean 生命周期 | ⭐⭐⭐⭐⭐ | 八步流程、BeanPostProcessor、代理生成时机 |
| 自动配置原理 | ⭐⭐⭐⭐ | spring.factories、条件装配、Starter |
| Spring MVC 流程 | ⭐⭐⭐⭐ | DispatcherServlet → HandlerMapping → Controller |
| Spring Cloud 组件 | ⭐⭐⭐ | Nacos、Gateway、Sentinel、Seata |

> 掌握前四个（IOC + AOP + 生命周期 + 自动配置）足以应对 90% 的 Spring 面试题。Spring Cloud 部分知道全景、能说出选型理由即可。

---

## 相关章节链接

- [3.6 网络 IO 模型](./06-网络IO模型.md)——starter-web（Servlet 同步）vs starter-webflux（Reactor 异步）的底层 IO 差异
- [3.7 高可用架构](./07-高可用架构.md)——Sentinel 熔断限流的设计思路与实践
- [3.10 分布式理论与一致性](./10-分布式理论与一致性.md)——Seata 分布式事务的理论基础（CAP / BASE）
