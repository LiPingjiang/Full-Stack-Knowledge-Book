# 4.3 Java → Go：少即是多的并发哲学

> Go 是 Java 后端工程师转型**最平滑**的系统语言之一——同样是静态编译、强类型、为服务端而生。
> 但 Go 用一套截然不同的哲学解决并发与设计问题。这一节兑现 [3.1 并发](../part3-java-deep/01-并发体系.md)、[3.2 JMM](../part3-java-deep/02-内存模型JMM.md)、[3.5 异常](../part3-java-deep/05-异常体系.md) 的钩子。

---

## 一、Go 的核心哲学：少即是多

Go 由 Google 设计（2009），出发点是「**让大型团队能快速写出可维护的并发服务**」。它的设计哲学和 Java 形成鲜明对比：

| 维度 | Java | Go |
|------|------|-----|
| 语言特性 | 丰富（泛型/注解/继承/异常...） | 极简（刻意少特性） |
| 继承 | 类继承 | **没有继承**，只有组合 |
| 异常 | try-catch | **没有异常**，用 error 返回值 |
| 并发 | 线程/虚拟线程 | goroutine + channel（语言内置） |
| 编译产物 | 字节码 + JVM | 单个原生二进制（无依赖） |
| 语法繁简 | 较繁 | 极简（连 while 都没有，只有 for） |

**Go 的精神**：宁可让你多写几行直白的代码，也不要花哨的抽象。这对习惯了 Spring「魔法」的 Java 工程师，一开始会不适应，但很快会爱上它的「**所见即所得**」。

---

## 二、并发：goroutine + channel（CSP 模型）

这是 Go 最闪光的地方，也是兑现 [3.1](../part3-java-deep/01-并发体系.md) 钩子的重点。

**goroutine**：用 `go` 关键字就能启动一个并发任务，极其轻量（几 KB 栈起步，对比 Java 传统线程的 1MB）：

```go
go doSomething()   // 就这么简单，启动一个 goroutine 并发执行

// 一口气开十万个 goroutine，毫无压力（Java 传统线程做不到）
for i := 0; i < 100000; i++ {
    go worker(i)
}
```

这和 [3.1](../part3-java-deep/01-并发体系.md) 讲的 Java **虚拟线程**思想几乎一致——用户态调度的轻量协程、阻塞自动让出。区别是 Go 从 2009 年就这么干了，而 Java 2023 才补上。

**channel**：goroutine 之间通过 **channel** 通信，这是 Go 最核心的并发哲学——**CSP（Communicating Sequential Processes）**：

```go
ch := make(chan int)        // 创建一个 channel

go func() {
    ch <- 42                // 往 channel 发送数据
}()

result := <-ch              // 从 channel 接收数据（会阻塞直到有数据）
```

Go 有一句著名格言，直接挑战了 Java 的并发哲学：

> **"Do not communicate by sharing memory; instead, share memory by communicating."**
> 不要用共享内存来通信，而要用通信来共享内存。

这正是兑现 [3.2 JMM](../part3-java-deep/02-内存模型JMM.md) 埋的钩子！回顾一下：Java 的并发是「**多个线程共享可变内存 + 加锁保护**」，于是要面对 JMM、可见性、volatile、happens-before 那一整套复杂性。Go 的主张是**反过来**——**别共享可变状态，而是用 channel 把数据「传递」给需要的 goroutine**，从根上回避了数据竞争：

```go
// Java 思路：多个线程共享一个 counter，加锁保护（要操心 JMM）
// Go 思路：让一个 goroutine 独占 counter，别的 goroutine 通过 channel 请求它操作

func counter(requests chan int, results chan int) {
    count := 0                    // 这个状态只有这一个 goroutine 碰，天然安全
    for range requests {
        count++
        results <- count
    }
}
```

> goroutine 调度与 CSP 模型的深层原理 → [Go goroutine 与 CSP](../concurrency-models/go-goroutine-csp.md)
> 当然，Go 也提供 `sync.Mutex` 等传统锁，共享内存时用。但 channel 是它推崇的「Go 风格」。

---

## 三、错误处理：error 是普通返回值

兑现 [3.5 异常体系](../part3-java-deep/05-异常体系.md) 的钩子。Go **没有异常**，错误是**普通的返回值**，约定放在多返回值的最后一个：

```go
// 约定：可能出错的函数，最后返回一个 error
func queryUser(id string) (User, error) {
    if id == "" {
        return User{}, errors.New("id 不能为空")   // 返回错误
    }
    return User{Id: id}, nil   // nil 表示没出错
}

// 调用者必须显式检查 error —— 这是 Go 最有特色（也最有争议）的写法
user, err := queryUser("123")
if err != nil {
    // 处理错误
    return err
}
// 没出错才继续用 user
fmt.Println(user)
```

这就是 Java 受检异常争议（[3.5](../part3-java-deep/05-异常体系.md)）的「另一种答案」：

- Java 受检异常想「强制处理错误」，但用 try-catch 实现，导致样板和传染。
- Go 把错误降级成**普通值**，用 `if err != nil` 强制你**显式面对每个错误**。它确实啰嗦（你会写无数个 `if err != nil`），但**错误路径一目了然、没有隐藏的控制流跳转**。

Go 的拥趸认为「异常是隐藏的 goto，难以推理」，所以宁愿啰嗦也要显式。你会发现 Go 代码里到处是 `if err != nil`——这是它的特色，不是 bug。

---

## 四、没有继承：用组合和接口

兑现「组合优于继承」。Go **没有类继承**，用**结构体嵌入（组合）** 和**隐式接口**实现复用与多态：

```go
// 组合：把一个结构体"嵌入"另一个，获得它的字段和方法
type Animal struct { Name string }
func (a Animal) Eat() { fmt.Println(a.Name, "eating") }

type Dog struct {
    Animal      // 嵌入 Animal（组合，不是继承）
    Breed string
}

d := Dog{Animal{"旺财"}, "柴犬"}
d.Eat()   // 可以直接调用嵌入来的方法

// 接口是隐式实现的（又是结构类型思想！呼应 [3.4]）
type Speaker interface { Speak() string }

type Cat struct{}
func (c Cat) Speak() string { return "喵" }   // 没有 implements 声明

var s Speaker = Cat{}   // ✅ Cat 有 Speak 方法，就自动满足 Speaker 接口
```

注意 Go 的接口是**隐式实现**的——和 [3.4](../part3-java-deep/04-类型系统.md)/[4.2](./02-Java到JS-TS.md) 讲的结构类型一个思路：**有对应方法就自动满足接口，不用显式声明 implements**。这又是一个「Java 名义类型 vs 其他语言结构类型」的体现。

---

## 五、内存：有 GC，但更轻量

兑现 [3.3 JVM](../part3-java-deep/03-JVM运行时.md) 钩子。Go 和 Java 一样**有 GC**（不用手动管内存），但：

- Go 编译成**原生机器码**，没有 JVM、没有解释/JIT 预热，**启动即全速**（Java 要等 JIT 热身）。
- Go 的 GC 专为**低延迟**设计（并发标记清除），STW 停顿通常在亚毫秒级。
- Go 有**逃逸分析**，尽量把对象分配在栈上（自动回收），减少 GC 压力。

所以 Go 服务通常**启动快、内存占用低、延迟稳定**，特别适合云原生 / 微服务 / CLI 工具（一个静态二进制丢上去就能跑，无需装运行时）。

---

## 六、生态对照与给后端大脑的「翻译词典」

| 用途 | Java | Go |
|------|------|-----|
| Web 框架 | Spring Boot | Gin / Echo / 标准库 net/http |
| 并发原语 | Thread / 线程池 | goroutine（`go`） |
| 线程通信 | 共享内存 + 锁 | channel |
| 错误处理 | 异常 | `error` 返回值 |
| 继承复用 | extends | 结构体嵌入（组合） |
| 多态 | interface + implements | 隐式接口 |
| 包管理 | Maven | go mod |
| 部署产物 | jar + JVM | 单个静态二进制 |

---

## 本章小结

- Go 的哲学是**「少即是多」**：刻意极简，宁可多写直白代码也不要花哨抽象。
- **并发**用 goroutine（轻量协程，似 Java 虚拟线程）+ channel（CSP），核心格言「不要用共享内存通信，要用通信共享内存」——正是对 [3.2 JMM](../part3-java-deep/02-内存模型JMM.md) 复杂性的「绕开」式回答。
- **错误处理**用 `error` 返回值 + `if err != nil`，是 [3.5 受检异常](../part3-java-deep/05-异常体系.md) 争议的另一种答案：啰嗦但显式、无隐藏控制流。
- **没有继承**，用组合 + 隐式接口（又是结构类型）。
- **有 GC 但更轻量**（原生编译、低延迟 GC、启动快），适合云原生/微服务/CLI。
- 对 Java 后端而言，Go 是转型最平滑的系统语言，**几天就能上手写生产服务**。

---

[← 上一节：4.2 Java → JS/TS](./02-Java到JS-TS.md) | [下一节：4.4 Java → Rust →](./04-Java到Rust.md)
