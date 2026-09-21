# 第 4 课 Partition 复盘：我遇到的问题与犯的错误

## 一、编译层错误（3 个）

### 1. 手写匿名类实现 ReadWriteLock，还返回 null
- **现象**：构造器里 new 了一个匿名内部类实现 ReadWriteLock，readLock()/writeLock() 都 return null
- **根因**：把"接口"当成了"要自己实现的抽象任务"。ReadWriteLock 只是持有读写两把锁的**容器接口**，JDK 有现成实现
- **正解**：`new ReentrantReadWriteLock()` 一行
- **带走**：拿到一个接口先问"JDK 有没有现成实现"，而不是"我要怎么实现它"

### 2. 构造器签名暴露内部状态
- **现象**：`Partition(int id, AtomicLong nextOffset, List<SegmentInfo> segments, String baseDir)`
- **根因**：把内部状态（发号器、段列表）当成外部输入。调用方只应该告诉我"你是谁、存哪里"，不该替我造内脏
- **隐藏雷**：`List<SegmentInfo>` 里是 private 内部类，外部根本 new 不出来——签名和实现自相矛盾
- **正解**：`(int id, int leader, List<Integer> followers, String baseDir)`，内部状态自己 new

### 3. 字段未初始化就使用
- **现象**：构造器里 segments 还是 null 就调 initialize()（initialize 里要 segments.add）
- **根因**：初始化顺序没有"先备好容器，再跑逻辑"的意识
- **带走**：构造器里先让所有字段就位，最后一句才调 initialize()

## 二、逻辑 bug(帮助我)（4 个）

### 4. findPositionForOffset 三连击：泄漏 + O(n) + 缺兜底
- 裸 new RandomAccessFile 没有 try-with-resources → 句柄泄漏
- 顺序扫描整个索引 → 把 O(1) 直达的索引用成了 O(n)（忘了"第 N 项 × 16 字节可以直接 seek"）
- 缺 relativeOffset >= entryCount 的兜底分支（账本落后时用最后一项位置降级，而不是失败）
- **带走**：索引的意义就是定长表格随机访问，用循环扫等于白造索引

### 5. findSegmentForOffset 缺越界防线
- 消费者要的 offset ≥ nextOffset（比所有消息都新）时应该返回 null，我漏了这层判断
- **带走**：所有"查找"函数先写越界/空集合的出口，再写正常逻辑

### 6. getLogEndOffset 坐标系混淆 ★本课代表性错误
- **现象**：`baseOffset + activeLogChannel.size()`——把最后一段的**字节大小**当**消息条数**加
- **根因**：offset（逻辑坐标，第几条）和字节位置（物理坐标，第几字节）两个坐标系在我脑子里没分开。书架长 4821 厘米 ≠ 有 4821 本书
- **正解**：`nextOffset.get()`——发号器本身就是日志末尾
- **连带**：private + 包装类型 Long，第 5 课 broker 调不到。改成 public long

### 7. 不可变对象加了 setter
- SegmentInfo 生来"信息一次确定、永不改变"（和第 1 课 BrokerInfo 同理），我加了一整套 setter 给未来埋雷
- **带走**：值对象 = final 字段 + getter，没有 setter

## 三、运行期 bug(帮助我)：全场最佳 ★★★

### 8. updateIndex 覆盖写：RandomAccessFile 的 "rw" 游标在 0
- **现象**：.index 永远只有 16 字节、只剩最后一条账 → readMessages(0) 被引到错误字节 → 只读回 1 条消息、get(2) 越界崩溃
- **根因**：`new RandomAccessFile(path, "rw")` 打开后游标在**文件开头**，不会自动追加。必须先 `channel.position(channel.size())` 再写。每次记账都盖掉上一条
- **为什么可怕**：全程**静默**——append 返回值全对、.log 完美、零报错；错的是索引，症状爆发在遥远的 readMessages。"写入侧的小错，读取侧的大炸"
- **诊断方法**：od 十六进制转储直接看文件——代码看百遍不如字节看一眼
- **带走**："rw 打开默认从头，想追加必须自己跳到尾"（openSegmentForAppend 里那句 position(size()) 是同一个知识点的另一个化身）

### 9. 修完 bug(帮助我) 没清污染现场
- **现象**：修复后重跑，第一行日志 nextOffset=3（应为 0），八行输出全部错位
- **根因**：上一轮的腐败索引文件还躺在 testdata 里，恢复逻辑信任磁盘旧账 → 腐败跨重启存活并传染。老师明确要求先删 testdata，我漏做了
- **带走**：**测试的前置条件和代码同等重要**——脏环境会让正确的代码表现出错误的行为。每次验证前问自己"现场干净吗"

### 10. Shell 方言踩坑（小）
- 在 PowerShell 敲了 Git Bash 的 `rm -rf` → Remove-Item : 找不到参数"rf"
- PowerShell: `Remove-Item -Recurse -Force`；Bash: `rm -rf`

### 11. 日志中文乱码（非错误）
- UTF-8 日志被 PowerShell 按 GBK 解读成"鎭㈠瀹屾垚"。治法：`chcp 65001` 或 `-Dfile.encoding=UTF-8`

## 四、我的三个系统性错误模式（跨课复发，重点防）

### 模式 A：IDE 自动补全依赖症（第 2/3/4 课连续发作）
- 第 2 课 import 了 Netty 的 SocketChannel、第 3 课用错 AsyncCallback.ChildrenCallback 和 EventType.SyncConnected、第 4 课手写 ReadWriteLock
- **共性**：遇到编译错误先让 IDE 猜，不先问"正确的类是谁、住在哪个包"
- **预防**：import 出现 io.netty / org.apache.zookeeper 等陌生包、或 IDE 提示实现接口时——停下来，查 JDK 自带的是什么

### 模式 B：坐标系/单位混淆
- getLogEndOffset 字节当条数、第 2 课 encodeReplicateRequest 的 maxBytes 当 msgLen
- **共性**：看到一个数字没问"它的单位/坐标系是什么"
- **预防**：变量出现的瞬间标注语义：offset（第几条）？position（第几字节）？count（几条）？size（几字节）？

### 模式 C：资源与状态的生命周期意识弱
- 文件打开不关（泄漏）、游标在哪不管（覆盖写）、旧文件不删（污染）
- **共性**：只关心"做什么"，不关心"从哪开始、到哪结束、留下什么"
- **预防**：每次 open 三连问：游标在哪？什么时候关？下次跑会不会踩到这次留下的东西？

## 五、这课我提出的好问题（理解资产，不是错误）

1. ReadWriteLock/AtomicLong 为什么需要 → 写写互斥、读读共享；无锁读要原子性
2. offset 7 的字节位置 4821 从哪来 → 逻辑坐标 vs 物理坐标，索引是翻译官，写时记账读时查账
3. activeLogFile/activeLogChannel/baseDir 各管什么 → 住址、账本、笔尖
4. SegmentInfo 路径从哪来 → 两个出生地：新建时拼出来、恢复时从文件名反解
5. new File 后为什么还要 exists() → File 只是路径名片，exists 才碰磁盘；孤儿 .log 不进账
6. relativeOffset >= entryCount 兜底分支 → 账本落后时降级：拿最近已知位置慢查，不失败
7. 跨段读取怎么写 → 外层换文件、内层读消息的双层循环；position=0 是灵魂
8. readMessages 流程图 → 三个出口的分工，finally 兜住资源

## 六、最终战果

- 一个能持久化、能恢复、能跨段读、带索引的日志存储引擎
- 亲手修掉：覆盖写索引、游标位置、环境污染三个真事故级 bug(帮助我)
- 顺带修掉原作都有的跨段读取 ClosedChannelException 缺陷
