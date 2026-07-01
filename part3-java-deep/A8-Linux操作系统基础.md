# 附录 A8：Linux 操作系统基础——内核机制与资源隔离

> **一句话定位**：JVM 跑在 Linux 上、YARN 容器隔离用 cgroup、Docker 底层也是 cgroup + namespace、线上排查问题必须懂 Linux 进程模型——这些是 Java 后端工程师的"基础设施知识"。本附录不讲 Linux 命令（命令速查见 [A7 开发工具链](./A7-开发工具链.md)），而是讲 Linux 内核的**运行机制和原理**，帮你理解"为什么"而不只是"怎么敲"。

---

## 一、进程与线程——Linux 内核视角

### 1.1 进程是什么

在 Linux 内核中，进程是一个 `task_struct` 结构体——内核用这个结构体记录进程的一切信息：PID、内存映射、打开的文件、CPU 时间、调度优先级等。当你执行 `java -jar app.jar`，内核做了以下事情：

```
用户执行 java -jar app.jar

① fork()：内核创建一个新进程（复制当前 shell 进程的 task_struct）
   → 新进程获得独立的 PID
   → 继承父进程的文件描述符表、环境变量等

② exec()：内核把新进程的内存空间替换为 java 可执行文件
   → 加载 JVM 的 .so 库到内存
   → 开始执行 JVM 的 main 函数
   → 原来的 shell 代码被完全替换

从内核视角看，进程就是：
  task_struct {
      pid: 12345
      mm: 内存映射（页表，指向物理内存页）
      files: 打开的文件描述符表
      fs: 当前工作目录
      signal: 信号处理表
      scheduler: 调度信息（优先级、CPU 时间统计）
      ...
  }
```

> **类比**：进程就像一个"工位"——有自己的桌面（内存）、抽屉（文件描述符）、工牌（PID）。fork() 是复制一个工位，exec() 是把桌面上的东西全换成新的。

### 1.2 线程是什么——和进程的关键区别

Linux 内核中，线程和进程的本质**都是 `task_struct`**，区别在于是否共享内存空间：

```
进程（不共享内存）：
  fork() → 新 task_struct + 新页表（内存独立）
  两个进程的虚拟地址 0x4000 指向不同的物理页

线程（共享内存）：
  clone(CLONE_VM | CLONE_FS | CLONE_FILES | CLONE_SIGHAND)
  → 新 task_struct + 复用同一个页表（内存共享）
  两个线程的虚拟地址 0x4000 指向同一个物理页
```

| 维度 | 进程 | 线程 |
|------|------|------|
| **内存** | 独立页表，互不可见 | 共享页表，同一地址空间 |
| **创建开销** | 大（复制页表、文件描述符等） | 小（只创建 task_struct，复用页表） |
| **切换开销** | 大（切换页表，TLB 失效） | 小（不切换页表，TLB 保持） |
| **通信方式** | 管道、信号、共享内存、Socket | 直接读写共享变量 |
| **隔离性** | 强（一个崩了不影响另一个） | 弱（一个线程踩坏内存，整个进程崩） |
| **内核表示** | 独立 task_struct | 独立 task_struct（但共享 mm_struct） |

> **关键**：Linux 内核不区分"进程"和"线程"——都是 task_struct，调度器一视同仁。区别只在于创建时是否设置了 `CLONE_VM`（共享内存）。这就是为什么 Linux 线程也叫"轻量级进程"（LWP）。

### 1.3 这跟 Java / Spark 有什么关系

```
Java 线程：
  new Thread().start()
  → JVM 调用 Linux 的 clone(CLONE_VM | ...)
  → 内核创建一个新的 task_struct（轻量级进程）
  → Java 线程 = OS 线程（1:1 模型，每个 Java 线程对应一个内核线程）

Spark Executor：
  java -cp ... ExecutorMain
  → 一个 JVM 进程（一个 task_struct，有独立页表）
  → 内部多个 Task 线程（多个 task_struct，共享同一个页表）
  → 多个 Task 线程可以直接共享 JVM 堆内存中的 RDD 缓存
```

<details>
<summary><b>展开：JDK 21 虚拟线程为什么不走 1:1 模型了</b></summary>

JDK 21 之前，Java 线程和 OS 线程是 1:1——每创建一个 Java 线程，内核就创建一个 task_struct。线程太多时，内核调度开销和内存占用（每个线程的栈空间）成为瓶颈。

虚拟线程（Virtual Thread）改为 M:N 模型——多个虚拟线程映射到少量 OS 线程（Carrier Thread）上。虚拟线程的调度由 JVM 在用户态完成，不需要内核参与。只有在执行阻塞 I/O 时，虚拟线程才让出 Carrier Thread 给其他虚拟线程使用。详见 [3.1 并发体系](./01-并发体系.md)。

</details>

---

## 二、cgroup——内核级资源隔离

### 2.1 cgroup 是什么

cgroup（Control Groups）是 **Linux 内核功能**，不是系统调用（虽然它通过文件操作来配置），也不是用户态软件。它是 Linux 内核 2.6.24（2008 年）引入的资源隔离机制，Docker 容器技术的底层基石就是 cgroup + namespace。

cgroup 的作用是：**限制一组进程能使用的 CPU、内存、磁盘 I/O 等资源**。

```
没有 cgroup：
  物理机上 3 个进程同时跑
  进程 A 疯狂吃 CPU → 进程 B 和 C 被挤慢
  进程 A 内存泄漏 → 整台机器 OOM，B 和 C 一起死

有 cgroup：
  cgroup A 限制进程 A 最多用 4 核 CPU、8GB 内存
  cgroup B 限制进程 B 最多用 2 核 CPU、4GB 内存
  cgroup C 限制进程 C 最多用 2 核 CPU、4GB 内存
  → 进程 A 疯狂吃 CPU 也不影响 B 和 C（CPU 被限流）
  → 进程 A 内存泄漏只死自己（被 OOM Kill），B 和 C 不受影响
```

### 2.2 cgroup 怎么用——虚拟文件系统

cgroup 不是 shell 命令，它通过**虚拟文件系统**暴露给用户态。内核挂载了一个特殊目录 `/sys/fs/cgroup/`，你对这个目录里的文件做"创建目录""写入数字"这些操作，内核就会实时生效。

```
cgroup 的使用方式——本质是文件操作：

# 1. 创建一个 cgroup（就是创建一个目录）
mkdir /sys/fs/cgroup/cpu/yarn_container_123
mkdir /sys/fs/cgroup/memory/yarn_container_123

# 2. 设置 CPU 限制（往文件里写数字）
echo 400000 > /sys/fs/cgroup/cpu/yarn_container_123/cpu.cfs_quota_us
# 含义：每 100000 微秒周期内，最多用 400000 微秒 CPU = 4 核

# 3. 设置内存限制
echo 8589934592 > /sys/fs/cgroup/memory/yarn_container_123/memory.limit_in_bytes
# 含义：最多用 8GB 内存

# 4. 把 Executor JVM 进程关进这个 cgroup
echo <JVM_PID> > /sys/fs/cgroup/cpu/yarn_container_123/tasks
echo <JVM_PID> > /sys/fs/cgroup/memory/yarn_container_123/tasks
# 从此刻起，这个 JVM 进程及其所有线程都受上面的限制

# 5. 启动 Executor（普通的 java 命令，但已经被关进笼子了）
java -cp spark.jar -Xmx8G org.apache.spark.executor.CoarseGrainedExecutorBackend ...
```

### 2.3 cgroup 文件的权限模型

cgroup 的虚拟文件系统挂载在 `/sys/fs/cgroup/`，这个目录的权限取决于谁挂载的。在 YARN 集群中，NodeManager 进程以 `yarn` 用户运行：

```
cgroup v1（传统模式，YARN 默认）：
  /sys/fs/cgroup/cpu/                ← root 拥有，只读
  /sys/fs/cgroup/cpu/yarn/           ← yarn 用户可写
  /sys/fs/cgroup/memory/yarn/        ← yarn 用户可写

  NodeManager（yarn 用户）可以：
  - 在 yarn/ 子目录下创建 container 目录
  - 写入 cpu.cfs_quota_us、memory.limit_in_bytes 等文件
  - 把进程 PID 写入 tasks 文件

  需要 root 权限的操作：
  - 挂载 cgroup 文件系统（mount -t cgroup）
  - 修改顶层 cgroup 的全局配置
  → 这些在机器初始化时由 root 做好，运行时不需要

cgroup v2（统一层级模式，Linux 4.5+，较新）：
  /sys/fs/cgroup/yarn/               ← 统一目录
  /sys/fs/cgroup/yarn/container_001/
    ├── cpu.max                       ← CPU 限制（合并了 v1 的两个文件）
    ├── memory.max                    ← 内存限制
    └── cgroup.procs                  ← 进程 PID

  权限模型更严格，需要委托（delegation）机制
  把 /sys/fs/cgroup/yarn/ 目录的写权限交给 yarn 用户
```

### 2.4 cgroup 能做什么——五大功能

```
① CPU 限制：限制一组进程最多用多少 CPU 时间片
   例：cgroup 规定 Container 最多用 4 核 → 里面所有线程的 CPU 总和不超过 4 核

② 内存限制：限制一组进程最多用多少内存
   例：cgroup 规定 Container 最多用 8GB → 超过就被 OOM Kill（YARN kill 的底层原因）

③ 磁盘 I/O 限制：限制读写带宽
   例：限制 Container 磁盘吞吐 100MB/s

④ 网络限制：限制网络带宽（较少用于 YARN，更多用于 Docker）

⑤ 设备访问控制：禁止访问某些硬件设备
```

> **"资源分配单位"是什么意思？** 说 YARN Container 是"资源分配单位"，意思是 YARN 在调度时以 Container 为粒度分配资源——"给你一个 Container，里面有 4 vcore、8GB 内存"。这个"分配"不是凭空造出来的，而是通过 cgroup 在内核层面强制执行：Container 里的进程（Executor JVM）如果试图使用超过配额的 CPU，会被内核限流；如果使用超过配额的内存，会被内核 OOM Kill。所以"资源分配单位"不是一个抽象概念，而是有 cgroup 在底层硬执行的物理约束。

### 2.5 YARN 怎么用 cgroup 控制 Executor

```
YARN 用 cgroup 控制 Executor 的完整流程：

① Driver 向 YARN ResourceManager 申请资源
   "我需要 3 个 Container，每个 4 vcore、8GB 内存"

② ResourceManager 找到有空闲资源的 NodeManager
   "NodeManager A，给我启动一个 Container，4 vcore 8GB"

③ NodeManager 在本机创建 cgroup（文件操作，不需要特殊权限）
   mkdir /sys/fs/cgroup/cpu/yarn/container_001
   echo 400000 > .../cpu.cfs_quota_us        ← 限制 4 核
   mkdir /sys/fs/cgroup/memory/yarn/container_001
   echo 8589934592 > .../memory.limit_in_bytes  ← 限制 8GB

④ NodeManager 启动 JVM 进程
   java -cp ... -Xmx8G ... ExecutorMain

⑤ NodeManager 把 JVM 的 PID 写入 cgroup 的 tasks 文件
   echo 12345 > /sys/fs/cgroup/cpu/yarn/container_001/tasks
   echo 12345 > /sys/fs/cgroup/memory/yarn/container_001/tasks

⑥ 从此刻起，内核强制限制这个 JVM：
   - CPU 超过 4 核 → 内核限流，线程被强制 sleep
   - 内存超过 8GB → 内核 OOM Kill，JVM 进程被杀（YARN 报 Container killed by framework）

⑦ Application 结束，NodeManager 清理 cgroup
   rmdir /sys/fs/cgroup/cpu/yarn/container_001
   rmdir /sys/fs/cgroup/memory/yarn/container_001
```

从 Linux 内核视角看 YARN Container 的资源隔离：

```
物理机（32 核 CPU，128GB 内存）
  ├── cgroup A（限制：4 vcore，8GB 内存）
  │     └── Executor 1（JVM 进程）→ 最多用 4 核 CPU、8GB 内存
  ├── cgroup B（限制：4 vcore，8GB 内存）
  │     └── Executor 2（JVM 进程）→ 最多用 4 核 CPU、8GB 内存
  └── cgroup C（限制：2 vcore，4GB 内存）
        └── 其他任务的 Executor

三个 Executor 互不影响：
  - Executor 1 OOM 不会影响 Executor 2（内存隔离）
  - Executor 1 跑满 CPU 不会拖慢 Executor 2（CPU 隔离）
  - 这是通过内核级 cgroup 强制执行的，不是应用层协议
```

> **cgroup 限制 vs JVM -Xmx 的区别**：`-Xmx` 是 JVM 自己的堆内存限制，JVM 自己遵守。cgroup 限制的是整个进程的物理内存（堆 + 堆外 + 线程栈 + native 内存），由内核强制执行。JVM 根本不知道自己被 cgroup 限制着——它只知道"我要分配内存"，如果超了，内核直接 OOM Kill。这就是为什么 YARN kill（exitCode 143）和 JVM OOM（OutOfMemoryError）是两种不同的错误。

---

## 三、VFS——为什么 cgroup 文件写一下就生效

### 3.1 cgroup 文件不是普通文件

上一节说"往 cgroup 文件里写数字就能限制资源"，这引出一个问题：内核怎么知道你写了数字？是有一个进程在监控这些文件吗？

**不是。** cgroup 文件的底层实现跟普通磁盘文件完全不同。普通文件是"存储容器"，cgroup 文件是"内核接口"。

```
普通文件（比如 /home/user/data.txt）：
  echo "hello" > data.txt
  
  内核的文件系统层（VFS）处理这个写操作：
  → 找到 data.txt 对应的磁盘块
  → 把 "hello" 写入磁盘块
  → 更新文件的元数据（大小、修改时间）
  → 完成
  
  数据最终落在物理磁盘上，文件就是一个"存储容器"

cgroup 虚拟文件（比如 cpu.cfs_quota_us）：
  echo 400000 > /sys/fs/cgroup/cpu/yarn/cpu.cfs_quota_us
  
  内核的文件系统层（VFS）处理这个写操作：
  → 识别出这是 cgroup 文件系统（cgroupfs），不是磁盘文件系统
  → 不找磁盘块，不写磁盘
  → 调用 cgroup 子系统注册的"写回调函数"
  → 回调函数把 400000 写入内核内存中的调度参数变量
  → 完成
  
  数据最终落在内核内存中，文件就是一个"内核接口"
```

### 3.2 VFS——一切皆文件的设计

Linux 的 **VFS（Virtual File System，虚拟文件系统）** 是所有文件操作的统一入口。VFS 定义了一套接口（`file_operations` 结构体），不同的文件系统各自实现这套接口：

```
Linux VFS 架构（一切皆文件的设计）：

用户写文件：echo 400000 > /sys/fs/cgroup/cpu/.../cpu.cfs_quota_us
     ↓
VFS 层（统一接口，不关心底层是什么文件系统）
     ↓ 判断文件属于哪个文件系统
     ├── 磁盘文件系统（ext4/xfs）
     │     → 调用 ext4 的 .write 函数 → 写磁盘块
     │
     ├── cgroup 文件系统（cgroupfs）
     │     → 调用 cgroup 注册的 .write 回调 → 修改内核参数
     │
     ├── 进程信息文件系统（procfs，/proc/）
     │     → 调用 proc 的 .write 回调 → 修改进程参数
     │
     └── 设备文件（/dev/null, /dev/sda1）
           → 调用设备驱动的 .write 回调 → 操作硬件
```

每个文件系统在 VFS 中注册自己的 `file_operations`，其中 `.write` 函数指针指向自己的实现。当你写 cgroup 文件时，VFS 根据文件路径识别出这是 cgroupfs，调用 cgroup 的 `.write` 回调函数，这个函数直接修改内核内存中的调度参数。**全程没有任何进程在监控，也没有轮询，就是一次函数调用。**

### 3.3 从代码层面理解

```c
// VFS 层：统一的写文件接口（简化伪代码）
ssize_t vfs_write(struct file *file, const char *buf, size_t count) {
    const struct file_operations *ops = file->f_op;  // 获取文件的操作函数表
    
    // VFS 不关心底层做什么，只负责调用正确的函数
    return ops->write(file, buf, count);
}

// ext4 文件系统的 write 实现
ssize_t ext4_file_write(struct file *file, const char *buf, size_t count) {
    // 找磁盘块 → 写磁盘 → 更新元数据
    write_to_disk_block(file, buf, count);
    return count;
}

// cgroup 文件系统的 write 实现（内核代码，不是用户态）
ssize_t cgroup_file_write(struct file *file, const char *buf, size_t count) {
    // 解析用户写入的值
    int quota = parse_int(buf);  // 400000
    
    // 直接修改内核调度参数（内存操作，立刻生效）
    struct cgroup *cg = file->private_data;
    cg->cpu_quota = quota;  // 内核内存变量，立刻生效
    
    return count;
}
```

> **类比**：cgroup 文件就像一个"旋钮"——旋钮的外观是一个文件（可以 echo 写入），但旋钮的内部不是存储空间，而是直接连着一根电线（回调函数），你转动旋钮的瞬间，电流立刻通过电线传到设备（内核参数更新）。没有人需要定时检查旋钮的位置，也没有进程在监控——转动本身就是触发。普通文件像一个"储物箱"，你往里面放东西，东西就存在那里，谁需要谁来取。

### 3.4 限制是怎么执行的——不是轮询，是事件驱动

cgroup 的机制分为两个阶段：

```
阶段 1：配置时（写入文件）—— 内核立即生效

  echo 400000 > cpu.cfs_quota_us
  ↓
  内核的 cgroup 文件写回调函数被触发（同步调用，不是轮询！）
  ↓
  内核更新这个 cgroup 的 CPU 带宽配额（内存中修改，立刻生效）
  ↓
  后续所有被关进这个 cgroup 的进程都受新配额约束

阶段 2：运行时（执行限制）—— 内核调度器实时执行

  CPU 限制的实现：
    内核的 CFS（Completely Fair Scheduler）调度器在每个调度周期检查：
    "这个 cgroup 在本周期内已用 CPU 时间是否超过配额？"
    超过 → 该 cgroup 内的所有线程被标记为 throttled（限流）
    线程尝试运行时，调度器直接不分配 CPU 时间片给它
    直到下一个周期重置计数
    
    这部分是调度器的正常逻辑，不是额外轮询
    调度器本来就要决定"下一个跑哪个线程"，顺便检查 cgroup 配额

  内存限制的实现：
    内核在分配物理内存页时（page fault 处理路径），检查 cgroup 内存配额
    超过 → 触发 OOM Killer 或拒绝分配
    这是内存分配的同步检查，不是轮询
```

完整的工作流程：

```
写入 cgroup 文件（配置阶段）
  → 内核写回调函数被触发（同步，不是轮询）
  → 内核内存中更新 cgroup 配额参数
  → 立刻生效

进程运行时（执行阶段）
  → CPU 调度器在正常调度流程中检查 cgroup 配额（附带检查，非额外开销）
  → 内存分配器在 page fault 路径中检查 cgroup 配额（同步检查）
  → 超限则限流或 kill
```

> **类比**：cgroup 文件就像"开关面板"——你拨动开关（写文件），电路（内核）立刻响应。不需要有人定时去检查开关状态（轮询）。限制执行时，也不是保安定时巡逻，而是门禁系统——每个线程要 CPU 时间片或内存页时，门禁当场刷卡检查，不通过就拦住。

---

## 四、namespace——容器隔离的另一块基石

cgroup 管的是"能用多少资源"，namespace 管的是"能看到什么"。Docker 容器 = cgroup（资源限制）+ namespace（视图隔离）。

```
namespace 做的事——让进程以为自己是系统里唯一的：

PID namespace：
  容器内的进程看到的 PID 从 1 开始
  容器内的 PID 1 在宿主机上实际是 PID 12345
  → 容器内看不到宿主机的其他进程

Mount namespace：
  容器内有自己的文件系统挂载树
  容器看不到宿主机的 /home、/etc 等
  → 容器内看到的是一个完整的 Linux 根目录

Network namespace：
  容器有自己的网卡、IP 地址、路由表、端口空间
  容器内的 8080 端口和宿主机的 8080 端口互不冲突
  → Docker 端口映射（-p 8080:8080）就是连接两个网络 namespace

UTS namespace：
  容器有自己的 hostname
  → 容器内 hostname 显示为容器 ID，不是宿主机名

IPC namespace：
  容器有自己的消息队列、共享内存、信号量
  → 容器间不共享 IPC

User namespace：
  容器内的 root 用户映射到宿主机的普通用户
  → 容器内看似 root，实际在宿主机上没有特权
```

> **YARN Container vs Docker Container**：YARN 的 Container 主要用 cgroup 做资源限制，不使用 namespace 做视图隔离——YARN Container 里的 Executor JVM 能看到宿主机上的所有进程和文件。Docker Container 则同时使用 cgroup + namespace，实现了更强的隔离。这就是为什么 YARN 叫"Container"但不是真正的容器——它只是借用了资源隔离的概念。

---

## 五、进程间通信（IPC）——为什么需要它

进程之间内存隔离，不能直接读写对方的内存。当进程需要协作时，必须通过内核提供的 IPC 机制：

| 机制 | 原理 | 类比 | 典型场景 |
|------|------|------|---------|
| **管道（Pipe）** | 内核缓冲区，一端写一端读 | 管子两头，一头塞一头取 | Shell 的 `\|`，父子进程通信 |
| **命名管道（FIFO）** | 管道但有关联的文件路径 | 管子上贴了门牌号 | 不相关进程间通信 |
| **消息队列** | 内核维护的链表，按消息为单位 | 公告板，按条留言 | 异步通知 |
| **共享内存** | 两个进程的页表指向同一块物理页 | 两个人看同一块白板 | 大数据量传输（最快） |
| **信号量（Semaphore）** | 内核计数器，控制共享资源的访问 | 停车场的车位计数器 | 共享内存的同步保护 |
| **信号（Signal）** | 内核向进程发送的异步通知 | 紧急广播 | `kill -9`、`Ctrl+C` |
| **Socket** | 网络通信的通用接口，也可用于本机 | 电话 | 跨机器通信、RPC |

> **跟 Java 的关系**：Java 的 `Thread` 通信走的是共享内存（同一个 JVM 内，多个线程共享堆）。但如果是两个 JVM 进程通信，就得走 Socket（如 RPC）或共享内存（如 off-heap）。AQS 中的 `volatile` + CAS 本质上就是利用了 CPU 缓存一致性协议来保证共享变量的可见性（详见 [3.1 并发体系](./01-并发体系.md)）。

---

## 六、面试深度剖析

### 考点 1：cgroup 和 JVM -Xmx 的区别

> **面试官**：「YARN 上 Spark Executor 的内存限制，cgroup 和 -Xmx 各管什么？」

两层限制，作用范围不同。`-Xmx` 是 JVM 自己的堆内存上限，JVM 主动遵守，超出抛 OutOfMemoryError，JVM 还活着可以 try-catch。cgroup 是内核限制整个进程的物理内存（堆 + 堆外 + 线程栈 + native），JVM 不知道自己被限制，超出时内核直接 OOM Kill 进程，JVM 来不及响应。这就是为什么 YARN kill（exitCode 143）和 JVM OOM 是两种不同错误——前者是 cgroup 杀的，后者是 JVM 自己抛的。

### 考点 2：为什么 Spark Task 用线程而不是进程

> **面试官**：「Spark 的 Task 为什么设计成线程而不是像 MapReduce 那样每个 Task 一个进程？」

三个原因。第一，启动开销——JVM 启动需要几秒（加载类、初始化），如果每个 Task 都启动 JVM，短任务的启动比计算还慢。Spark 的 Executor 常驻，Task 以线程方式运行，启动几乎零开销。第二，内存共享——同一个 Executor 内的 Task 线程共享 JVM 堆，RDD 缓存可以被多个 Task 复用，不需要每个 Task 重新加载。第三，数据本地性——Shuffle 数据在同一个 JVM 内可以直接内存传递，不需要序列化走网络。

### 考点 3：cgroup 文件写一下就生效的原理

> **面试官**：「cgroup 通过文件来配置资源限制，内核是怎么知道文件被修改的？是轮询吗？」

不是轮询，是 VFS 的回调机制。cgroup 挂载的是虚拟文件系统 cgroupfs，不是磁盘文件系统。当用户 `echo 400000 > cpu.cfs_quota_us` 时，VFS 层识别出这是 cgroupfs 文件，调用 cgroup 子系统注册的 `.write` 回调函数，这个函数直接修改内核内存中的调度参数。整个过程是一次同步函数调用，没有进程监控，没有轮询。限制的执行也不是轮询——CPU 限制由调度器在正常调度流程中附带检查，内存限制在 page fault 路径中同步检查。

### 考点 4：Docker 容器和 YARN Container 的区别

> **面试官**：「YARN 也叫 Container，Docker 也叫 Container，它们有什么区别？」

YARN Container 只用了 cgroup 做资源限制（CPU + 内存配额），没有用 namespace 做视图隔离——YARN 上的 Executor 能看到宿主机的所有进程和文件系统。Docker Container 同时使用 cgroup（资源限制）+ namespace（视图隔离），容器内的进程看到的是独立的 PID 空间、文件系统、网络栈，隔离更彻底。所以 YARN 叫 Container 是借用"资源隔离"的概念，但不是真正的容器技术。

---

**相关章节**：

- [3.1 并发体系](./01-并发体系.md)——Java 线程与 OS 线程的 1:1 模型、虚拟线程的 M:N 模型
- [3.3 JVM 运行时](./03-JVM运行时.md)——JVM 内存结构、GC、线上 OOM/CPU 排查
- [3.6 网络 IO 模型](./06-网络IO模型.md)——epoll 多路复用、Reactor 模式
- [A7 开发工具链](./A7-开发工具链.md)——Linux 运维命令速查（文件/进程/网络/线上排查组合拳）
- [6.4 Spark](../part6-bigdata/04-Spark.md)——Executor/Task/Container 在 Spark 中的具体应用

---

[← 返回本章目录](./README.md) | [返回全书目录](../README.md)
