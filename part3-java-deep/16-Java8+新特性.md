# 3.16 Java 8+ 新特性：从 Lambda 到虚拟线程的现代化之路

> **一句话定位**：Java 8 是 Java 历史上最大的一次变革——Lambda、Stream、Optional 让 Java 从"纯面向对象"迈入了"面向对象 + 函数式"的双范式时代。之后的 JDK 9-21 又陆续带来了模块化、var、Record、Sealed Class、Pattern Matching、虚拟线程等重要特性。本章按"你每天都在用 → 你应该用但可能没用 → 你需要了解的趋势"的顺序展开。

---

## 一、Lambda 与函数式接口——让代码说人话

### 1.1 Lambda 到底是什么？

Lambda 表达式本质上是**匿名函数的语法糖**——让你可以把"一段行为"当作参数传来传去，而不是非要创建一个匿名内部类。

```java
// Java 7：匿名内部类
Collections.sort(list, new Comparator<String>() {
    @Override
    public int compare(String a, String b) {
        return a.length() - b.length();
    }
});

// Java 8：Lambda
Collections.sort(list, (a, b) -> a.length() - b.length());

// 更简洁：方法引用
Collections.sort(list, Comparator.comparingInt(String::length));
```

### 1.2 函数式接口（Functional Interface）

Lambda 需要一个"载体"——只有**一个抽象方法**的接口称为函数式接口，用 `@FunctionalInterface` 标注。

| 接口 | 抽象方法 | 用途 | 典型场景 |
|------|---------|------|---------|
| `Function<T, R>` | `R apply(T t)` | 转换：输入 T 输出 R | `map()` |
| `Predicate<T>` | `boolean test(T t)` | 判断：输入 T 返回布尔 | `filter()` |
| `Consumer<T>` | `void accept(T t)` | 消费：输入 T 无返回 | `forEach()` |
| `Supplier<T>` | `T get()` | 生产：无输入返回 T | 延迟创建、工厂 |
| `BiFunction<T, U, R>` | `R apply(T t, U u)` | 双参数转换 | `reduce()` |
| `UnaryOperator<T>` | `T apply(T t)` | 输入输出同类型的 Function | `replaceAll()` |

### 1.3 Lambda 的实现原理——不是匿名内部类

Lambda 看起来像匿名内部类的语法糖，但实现完全不同。编译器不会为每个 Lambda 生成一个 `.class` 文件，而是使用 `invokedynamic` 指令 + `LambdaMetafactory` 在**运行时动态生成**实现类。好处是减少类文件数量、启动更快、JIT 优化空间更大。

---

## 二、Stream API——声明式数据处理管道

### 2.1 核心思想：描述"做什么"而非"怎么做"

```java
// 命令式：怎么做
List<String> result = new ArrayList<>();
for (Order order : orders) {
    if (order.getAmount() > 1000) {
        result.add(order.getCustomerName());
    }
}
Collections.sort(result);

// 声明式：做什么
List<String> result = orders.stream()
    .filter(o -> o.getAmount() > 1000)
    .map(Order::getCustomerName)
    .sorted()
    .collect(Collectors.toList());
```

### 2.2 Stream 三段式

```
数据源 → 中间操作（惰性） → 终端操作（触发执行）
```

| 阶段 | 常用操作 | 特点 |
|------|---------|------|
| 中间操作 | `filter`、`map`、`flatMap`、`sorted`、`distinct`、`limit`、`skip`、`peek` | **惰性**：不调用终端操作就不执行 |
| 终端操作 | `collect`、`forEach`、`reduce`、`count`、`findFirst`、`anyMatch`、`toArray` | **触发执行**：整个管道开始流动 |

> **面试重点**：Stream 是**惰性求值**的——中间操作只是在构建管道描述，真正的遍历和计算在终端操作调用时才发生，而且只遍历一次（不是每个操作遍历一次）。

### 2.3 flatMap——打平嵌套结构

```java
// 每个订单有多个商品，要拿到所有商品的列表
List<Item> allItems = orders.stream()
    .flatMap(order -> order.getItems().stream())  // Stream<Order> → Stream<Item>
    .collect(Collectors.toList());
```

`map` 是一对一变换，`flatMap` 是一对多变换后打平。

### 2.4 Collectors 常用收集器

| 收集器 | 效果 |
|--------|------|
| `toList()` / `toSet()` | 收集到 List / Set |
| `toMap(keyMapper, valueMapper)` | 收集到 Map |
| `groupingBy(classifier)` | 按条件分组（返回 `Map<K, List<V>>`） |
| `partitioningBy(predicate)` | 按布尔条件二分（返回 `Map<Boolean, List<V>>`） |
| `joining(delimiter)` | 字符串拼接 |
| `counting()` / `summingInt()` / `averagingInt()` | 统计 |

<details>
<summary><b>展开：parallelStream 的陷阱——不是加了 parallel 就更快</b></summary>

`parallelStream()` 底层使用 `ForkJoinPool.commonPool()`（默认线程数 = CPU 核心数 - 1），将数据拆分后并行处理。但有几个常见陷阱：

**陷阱一：数据量不够大时更慢**。并行化有开销（线程调度、数据分割、结果合并），数据量小于 1 万时通常不值得。

**陷阱二：共享 ForkJoinPool**。所有 `parallelStream` 共用同一个 commonPool，如果某个流操作很慢（比如涉及 IO），会阻塞其他 parallelStream。

**陷阱三：线程安全问题**。如果 Lambda 中有副作用（修改外部变量），并行执行会导致数据竞争。

**陷阱四：有些数据源不适合并行**。`LinkedList` 无法高效分割（只能顺序遍历），`ArrayList`、`IntStream.range()` 适合并行（支持随机分割）。

**经验法则**：只在"数据量大（万级以上）+ 操作是纯 CPU 计算 + 数据源支持高效分割"时使用 parallelStream。

</details>

---

## 三、Optional——优雅处理 null

### 3.1 设计初衷

`Optional` 是一个**容器对象**，要么包含一个非 null 值（`present`），要么为空（`empty`）。它的设计初衷是作为**方法返回值**来明确表示"这个方法可能不返回结果"，从而在 API 层面强迫调用者处理"无值"的情况，减少 `NullPointerException`。

### 3.2 正确用法 vs 反模式

```java
// ✅ 正确：作为方法返回值
public Optional<User> findById(Long id) {
    return Optional.ofNullable(userMap.get(id));
}

// ✅ 正确：链式调用
String city = findById(1L)
    .map(User::getAddress)
    .map(Address::getCity)
    .orElse("未知");

// ❌ 反模式：作为方法参数
public void process(Optional<String> name) { }  // 不要这样

// ❌ 反模式：作为字段
private Optional<String> name;  // 不要这样（Optional 未实现 Serializable）

// ❌ 反模式：isPresent() + get()
if (opt.isPresent()) { return opt.get(); }  // 和 null 检查没区别
```

### 3.3 orElse vs orElseGet

```java
// orElse：无论 Optional 是否有值，都会执行参数表达式
opt.orElse(createDefaultUser());     // createDefaultUser() 总是被调用！

// orElseGet：只在 Optional 为空时才执行
opt.orElseGet(() -> createDefaultUser());  // 只在需要时调用
```

> **面试考点**：当默认值的创建成本高（比如数据库查询、远程调用）时，必须用 `orElseGet` 而不是 `orElse`。

---

## 四、新日期 API（java.time）

Java 8 之前的 `Date` 和 `Calendar` 设计糟糕：可变、线程不安全、API 混乱（月份从 0 开始）。`java.time` 包（基于 Joda-Time）彻底重设计。

| 类 | 含义 | 是否包含时区 |
|----|------|------------|
| `LocalDate` | 日期（年月日） | 否 |
| `LocalTime` | 时间（时分秒纳秒） | 否 |
| `LocalDateTime` | 日期+时间 | 否 |
| `ZonedDateTime` | 日期+时间+时区 | 是 |
| `Instant` | 时间戳（UTC 纪元秒） | 是（UTC） |
| `Duration` | 两个时间点的间隔 | — |
| `Period` | 两个日期的间隔（年月日） | — |

**核心设计原则**：所有类都是**不可变的**（immutable）且**线程安全**。修改操作（`plusDays`、`withMonth`）返回新对象，原对象不变。

---

## 五、JDK 9-21 关键特性速览

Java 从 JDK 9 开始每 6 个月发一个版本，特性碎片化分布在各版本中。下面按"实用程度"梳理最重要的：

### 5.1 var——局部变量类型推断（JDK 10）

```java
var list = new ArrayList<String>();   // 编译器推断为 ArrayList<String>
var stream = list.stream();           // 编译器推断为 Stream<String>
```

`var` 只能用于**局部变量**，不能用于字段、方法参数、返回值。它是编译期语法糖，字节码和显式声明完全一样。

### 5.2 Record——不可变数据载体（JDK 16）

```java
// 传统写法：POJO 需要大量样板代码
public class Point {
    private final int x, y;
    public Point(int x, int y) { this.x = x; this.y = y; }
    public int x() { return x; }
    public int y() { return y; }
    // 还要 equals(), hashCode(), toString()...
}

// Record：一行搞定
public record Point(int x, int y) { }
// 自动生成：全参构造器、getter（x()、y()）、equals、hashCode、toString
```

Record 是 `final` 类，字段自动 `private final`，本质是**不可变数据载体**。适合 DTO、值对象。

### 5.3 Sealed Class——限制继承（JDK 17）

```java
public sealed interface Shape permits Circle, Rectangle, Triangle { }
public record Circle(double radius) implements Shape { }
public record Rectangle(double w, double h) implements Shape { }
public record Triangle(double a, double b, double c) implements Shape { }
// 其他类无法实现 Shape
```

sealed 让继承关系成为**封闭集合**，编译器可以进行穷举检查。配合 Pattern Matching 使用效果最好。

### 5.4 Pattern Matching——模式匹配（JDK 16-21 逐步完善）

```java
// JDK 16：instanceof 模式匹配
if (obj instanceof String s) {
    System.out.println(s.length());  // 不需要强转
}

// JDK 21：switch 模式匹配（正式版）
String describe(Shape shape) {
    return switch (shape) {
        case Circle c -> "圆，半径 " + c.radius();
        case Rectangle r -> "矩形，面积 " + r.w() * r.h();
        case Triangle t -> "三角形";
    };
    // sealed + switch = 编译器检查穷举，漏掉一个就报错
}
```

### 5.5 虚拟线程（JDK 21 正式版）

虚拟线程（Virtual Thread）是 Project Loom 的核心成果，在 [3.1 并发体系](./01-并发体系.md) 中已详细介绍。核心价值：**用同步写法获得异步性能**，一个 JVM 可以轻松创建百万级虚拟线程。

```java
// 创建虚拟线程
Thread.startVirtualThread(() -> {
    var result = httpClient.send(request, bodyHandler);  // 阻塞时自动让出载体线程
});

// 虚拟线程池
try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
    executor.submit(() -> handleRequest(request));
}
```

### 5.6 版本特性总览表

| 版本 | 关键特性 | LTS |
|------|---------|-----|
| JDK 8 | Lambda、Stream、Optional、新日期 API、默认方法 | ✅ |
| JDK 9 | 模块化（JPMS）、JShell、集合工厂方法 `List.of()` | |
| JDK 10 | `var` 局部变量类型推断 | |
| JDK 11 | `String.isBlank()`、`HttpClient`、单文件 java 运行 | ✅ |
| JDK 14 | `switch` 表达式（正式版）、`NullPointerException` 增强信息 | |
| JDK 16 | Record（正式版）、`instanceof` 模式匹配 | |
| JDK 17 | Sealed Class（正式版）、移除 Applet/RMI | ✅ |
| JDK 21 | 虚拟线程（正式版）、`switch` 模式匹配（正式版）、Sequenced Collections | ✅ |

---

## 六、面试深度剖析：大厂高频考点

### 考点 1：Stream 和 for 循环谁快？

> **面试官**：「用 Stream 比 for 循环快吗？」

大多数情况下 **for 循环更快**（少了 Stream 管道构建和 Lambda 调用的开销），但差距通常很小。Stream 的优势不在性能，而在**可读性和表达力**。只有在数据量大 + 纯 CPU 计算时，`parallelStream` 才有明显的性能优势。

### 考点 2：Lambda 能捕获外部变量吗？

> **面试官**：「Lambda 里能修改外部的局部变量吗？」

不能。Lambda 可以**捕获**外部局部变量，但该变量必须是 `final` 或 **effectively final**（声明后没有被重新赋值）。这是因为 Lambda 捕获的是变量的值的副本，不是引用——如果允许修改，Lambda 内外看到的值就不一致了。

要"修改"外部变量，可以用 `AtomicInteger`、数组等引用类型做间接修改（修改的是对象内容，不是变量本身）。

### 考点 3：Optional 的正确使用姿势

> **面试官**：「Optional 应该怎么用？不应该怎么用？」

**应该**：作为方法返回值表示"可能无值"；用链式调用（`map`/`flatMap`/`orElse`）替代嵌套 null 检查。

**不应该**：作为方法参数（增加调用方复杂度）；作为类字段（不可序列化）；用 `isPresent()` + `get()` 组合（和 null 检查没区别，而且 `get()` 在空时抛 `NoSuchElementException`）。

---

## 七、多语言钩子

Java 8+ 的函数式编程特性在其他语言中早已是原生能力：

- **JavaScript/TypeScript**：箭头函数、`Array.map/filter/reduce` 比 Java Stream 更早、更简洁 → [4.2 Java→JS/TS](../part4-multilang-compare/02-Java到JS-TS.md)
- **Go**：没有 Lambda 语法糖，用函数变量和闭包实现类似效果；没有 Stream API → [4.3 Java→Go](../part4-multilang-compare/03-Java到Go.md)
- **Rust**：迭代器（Iterator）+ 闭包 + `map/filter/collect`，思路和 Stream 几乎一样，但零成本抽象 → [4.4 Java→Rust](../part4-multilang-compare/04-Java到Rust.md)
- **Python**：列表推导式 + `map/filter/reduce`，一直是函数式风格，但 Lambda 只能写单行表达式 → [4.5 Java→Python](../part4-multilang-compare/05-Java到Python.md)

---

[← 3.15 Java 集合框架](./15-Java集合框架.md) | [返回本章目录](./README.md) | [3.17 设计模式 →](./17-设计模式.md)
