# 第 5、6 课 问题与解答汇总

## 第 5 课 SimpleKafkaBroker

### 一、字段与设计

#### Q1：zkPort 是什么
- **答**：ZooKeeper 服务器的监听端口（默认 2181）。broker 用它连"公告栏"。三个 broker 的 zkPort 都一样（连同一个 ZK），服务端口各不相同（9091/9092/9093）。
- **认知**：两个端口服务两种对话——9091 是 broker 和外部（客户端/兄弟）说话的嘴；2181 是 broker 和 ZK 说话的耳朵。

#### Q2：zkClient 已经是字段了，为什么还需要 zkPort
- **答**：zkPort 不是平行于 zkClient 的另一份数据，是 zkClient 的"出生参数"。构造器里 `new ZookeeperClient("localhost", zkPort)` 消耗掉 zkPort，之后 broker 只跟 zkClient 说话。
- **判定法**：构造器之后还有时刻直接用它吗？没有 → 局部参数用完即弃；有 → 升级为字段。

#### Q3：BrokerInfo 的 host 和 broker 自己的 brokerHost 是什么关系
- **答**：同一份数据的三种形态。① brokerHost（broker 自己的字段：原料）→ ② 字符串 "localhost:9091"（写进 ZK）→ ③ BrokerInfo 对象（住进**别人**的 clusterMetadata）。
- **认知**：BrokerInfo 是"别人眼中的我"——在集群里流动的值对象，第 1 课的 equals/hashCode 保证它在 Map 里正确工作。

#### Q4：怎么知道构造器传哪些参数、哪些 new、哪些 initialize
- **答**：三分法。
  - **身份证**（调用方知道、各不相同）→ 参数
  - **内脏**（所有实例一样）→ 构造器里 new
  - **开机自检**（慢、可能失败、碰外部世界）→ initialize / 显式 start
- **三连问**：①调用方知道吗？②十个对象每次都不同吗？③要碰磁盘/网络可能失败吗？

#### Q5：executor 是线程池吗，用于处理多个请求
- **答**：对。newFixedThreadPool(10) = 养 10 个工人。submit = 托付不等待（异步）。
- **四类活**：①站大门（accept 循环）②每连接一工人（会话）③复制（异步不等 ACK）④通知兄弟（fire-and-forget）。
- **心智**：1 门卫 + N 管家 + 临时工，全从同一个 10 人池出。

#### Q6：所有成员变量用进化思维和实际流程推出
- **答**：按时刻线推导：
  - 时刻0（main 读命令行）→ brokerId/Host/Port（身份证）
  - 时刻1（构造器备工具箱）→ topics/clusterMetadata/executor/isRunning/isController/zkClient/serverChannel（内脏）
  - 时刻2（start 注册+选举）→ zkClient 连 ZK、isController 定角色
  - 时刻3（loadTopics 恢复）→ topics 表从 ZK 读档案重建
  - 时刻4（营业）→ 每条请求查 topics/clusterMetadata/isController
  - 时刻5（兄弟死了）→ onBrokersChanged 更新 clusterMetadata + rebalance
  - 时刻6（stop）→ isRunning 降旗，反向收摊

#### Q7：DATA_DIR 在构造器里构造，其他地方能调用吗
- **答**：变量死了（局部），效果永存（磁盘目录）。其他地方要重算路径：`DATA_DIR + brokerId + topic`。

#### Q8：为什么用 ReentrantReadWriteLock 而不是手写
- **答**：ReadWriteLock 是接口，ReentrantReadWriteLock 是 JDK 唯一实现。手写匿名类返回 null = 编译错误。字段声明接口、创建时用实现类 = 面向接口编程。

### 二、启动与生命周期

#### Q9：start 要做什么，怎么分析得来的
- **答**：反向归谬法。每步问"不做会怎样"：
  - 不 bind → 客户端连不上
  - 不注册到 ZK → 别人不知道连哪
  - 注册先于选举 → 选举时通讯录空
  - 不选举 → 没人管建 topic/rebalance
  - 不 loadTopics → 重启失忆，PRODUCE 全错
  - accept 在最后 → 半成品状态收到请求
- **通用方法**：列对象 start 前状态 + 外部世界状态 → 问"第一条请求要哪些状态就位" → 倒推出步骤和顺序。

#### Q10：stop 应该做什么
- **答**：start 的逆运算，从外往内收：
  1. isRunning.set(false) 降旗
  2. serverChannel.close() 摘招牌
  3. partition.close() 收笔尖
  4. executor.shutdown + await 解散工人
  5. zkClient.close() 挂断 ZK
- **纪律**：从外往内收——先切断外部输入源，再停内部状态。stop 和 start 六对动作严格镜像。

### 三、网络与连接

#### Q11：acceptConnections 怎么实现
- **答**：看旗子循环 + 非阻塞 accept + null 判断 + sleep + 派工人。
  - 非阻塞模式：没人敲门 accept 立刻返回 null
  - sleep 防空转
  - catch 里判 isRunning 区分停机和故障
  - executor.submit 瞬间返回，门卫继续守门

#### Q12：null 是判断 clientChannel 是否有连接吗
- **答**：方向反了。broker 是被动方（楼），客户端是主动方（客）。accept 是"瞥一眼门外"，null = 此刻没人敲门，不是"申请失败"。

#### Q13：没连接时返回 null，有连接时返回这个连接？
- **答**：对。null 和非 null 的唯一区别就是"这一刻有没有新的人来敲门"。

#### Q14：null 的时候我"占用"这个 channel 吗，别人申请是不是获得我这个 channel
- **答**：三个误解拆解：
  - null 表示没拿到任何 channel，手上空空
  - 每条连接独立，不会被两个人同时拿
  - channel 不是"占有/释放"，是"持有引用 + 最终 close"
  - close 必须显式写（finally 里），不会自动释放

#### Q15：executor 到底是什么
- **答**：工人池管理员，养工人 + 派活。和 serverChannel 完全独立，彼此不知道对方存在。
  - accept 返回 null 由 OS + 非阻塞模式决定
  - executor 派活由池决定
  - 三者各管一摊：serverChannel 收连接、configureBlocking 设 read 行为、executor 派工人

### 四、请求处理

#### Q16：handleClient / processClientMessage 怎么实现
- **答**：handleClient 是 acceptConnections 的双胞胎——看旗子循环 + 非阻塞 read + sleep + finally close。processClientMessage 读第一个字节（类型）按 switch 分发到各 handler。

#### Q17：为什么没有 handleProduceNotification
- **答**：PRODUCE 的通知机制是"复制"（推给副本）+ "消费者拉取"（拉给消费者），不需要全集群广播。
  - 结构变化（CREATE_TOPIC）→ 影响全员 → 广播
  - 数据变化（PRODUCE）→ 只影响副本 → 复制
  - 通知机制的收件人范围由变化的影响范围决定

### 五、集群协作

#### Q18：controller 是记录各个 topic 的 leader 吗
- **答**：不是。controller 是一个"角色"——拥有修改元数据权限的那台 broker。
  - /topics/.../partitions/0 的 data "1;2,3," 是档案（存 leader 信息）
  - /controller 的 data "1" 只记录"谁是管理员"
  - 两个 leader 概念：partition leader（每分区一个，消息写入入口）vs controller（全集群一个，行政大脑）

#### Q19：/topics/order/partitions/0 data "1;2,3," 是什么意思
- **答**：topic order 的分区 0，leader 是 broker 1，followers 是 broker 2 和 3。三台机器各有一份内容相同的日志，只有 leader 接客。

#### Q20：临时节点 data 不会变化吗，比如端口变了
- **答**：技术上允许 setData，但不做。身份（host:port）是"出生时写一次、到死不变"——要换地址就重启（旧节点消失→新节点出现），通过两次 watch 通知让集群知晓。

#### Q21：electController 怎么实现
- **答**：四步：
  1. 骨架清理（/controller 是持久空节点，不删掉永远抢不到）
  2. createEphemeralNode 抢位子
  3. 抢到→isController=true+rebalance；没抢到→认命
  4. 失败→后台线程睡 2 秒重试
- **雷区兑现**：①骨架清理舞步 ②落选分支不调 watchNode（你的 wrapper 自续期版）

#### Q22：rebalancePartitions 在干什么
- **答**：大脑巡查——遍历全部分区，leader 死了（不在 clusterMetadata 里）就从幸存者提拔新 leader，写回 ZK 档案。被 onBrokersChanged 和 electController 触发。

#### Q23：handleTopicNotification 是让重新更新 topic 数据吗
- **答**：对——controller 建完 topic 后通知兄弟"有新 topic"，兄弟收到后调 loadTopic 读 ZK 档案建 Partition 对象。它本身不干活，是 loadTopic 的网络入口。

#### Q24：onBrokersChanged 里 removeIf 应该放到判断后吗
- **答**：顺序对的（先更新通讯录再判断角色），但应该分层写清晰：第一步全做通讯录更新，第二步全做角色判断。让"先采集事实后做决策"的因果一目了然。

### 六、其他

#### Q25：两个 bid（for 循环里的和 lambda 参数）冲突吗
- **答**：不冲突——同名巧合，两个作用域不嵌套。for 的 bid 是循环变量，lambda 的 bid 是形参，叫什么都行。

#### Q26：clusterMetadata.keySet().removeIf 是循环遍历吗
- **答**：对——遍历+条件删除，等价于 Iterator + while + it.remove()。removeIf 是 JDK 8 封装的安全删除方法，一行顶五行的迭代器循环，不会抛 ConcurrentModificationException。

---

## 第 6 课 SimpleKafkaClient + Producer + Consumer

### 一、设计哲学

#### Q1：为什么 Consumer 绑定 partition，Producer 不用
- **答**：两个协议不对称——PRODUCE 可以不指定分区（随机选，负载均衡），FETCH 必须指定（offset 是分区内坐标，不绑不知道从哪读）。
- **本质**：写是"路由"（每次选），读是"立场"（站在哪）。Consumer 的 currentOffset 必须属于一个分区才有意义。
- **真 Kafka**：消费组协调器把分区分配给各 Consumer，教育版让用户自己开多个 Consumer 各管一个分区。

#### Q2：一个 topic 的若干 partition 数据不应该相同吗
- **答**：分片 vs 副本——partition 是分片（数据不同，水平扩展），replica 是副本（数据相同，容灾）。Producer 发一条只进一个 partition，同一 partition 的多个副本数据相同。

#### Q3：Producer 随机选分区，Consumer 怎么找到消息
- **答**：Consumer 不"找"消息，它"订阅"流——绑一个分区从头到尾读。要全量消费就开多个 Consumer 各管一个分区。
- **流式 vs 查询**：Kafka 是流式系统不是数据库，消费者不是"查某条消息"是"订阅一条消息流"。

#### Q4：Producer 的分区策略
- **答**：无 key 随机（负载均衡，不保证顺序）；有 key hash（同一 key 永远进同一分区，保证顺序）。教育版默认随机。

### 二、字段与技术细节

#### Q5：AtomicBoolean 和 boolean 的区别
- **答**：boolean 是值，AtomicBoolean 是线程安全的值的容器。普通 boolean 跨线程没有可见性保证（CPU 缓存），Atomic 保证改动立刻对其他线程可见 + 原子操作（compareAndSet）。
- **何时用 Atomic**：字段被多线程碰（如 Consumer 的 running 被 main 和轮询线程碰）；单线程内用普通 boolean。

#### Q6：1MB 在 int 类型设置什么
- **答**：`1024 * 1024` = 1048576。写 `1024 * 1024` 不写 `1048576` 是可读性——编译期常量折叠，运行时无差别。
- **坑**：`2 * 1024 * 1024 * 1024`（2GB）溢出 int，要加 L：`2L * 1024 * 1024 * 1024`。

#### Q7：Producer 和 Consumer 都固定绑定一个 topic 吗
- **答**：对——教育版简化设计，一个对象代表"一条消息流"。真 Kafka 的 Producer 可以每次指定 topic，Consumer 通过订阅 + 消费组协调。

### 三、内部类

#### Q8：内部类不能直接用 Protocol 中的类吗
- **答**：能用，但客户端有自己的类是为了：①缓存安全（返回副本防篡改）②未来解耦（协议变化不波及客户端）。教育版略过度但和原作对齐方便排错。
- **必须 static**：非 static 内部类持有外部实例引用，不能独立 new。值对象必须是 static 内部类或独立类。

### 四、实现问题

#### Q9：SimpleKafkaClient 审查发现的问题
- **bootstartPort 笔误**：应该是 bootstrapPort
- **第 17 行多余 static import**：ERROR_RESPONSE 没用到（代码里写了 Protocol.ERROR_RESPONSE）
- **createTopic 协议解析错**：CREATE_TOPIC_RESPONSE 是 [1B 类型][1B status]，不是 [1B 类型][4B status]
- **send 参数**：应该收 byte[] 不是 String（字节层面不绑死编码）
- **getLeader 的 stream**：findFirst().get() 找不到会抛 NoSuchElementException，用 for 循环更安全
- **refreshMetadata 不清空旧缓存**：死 broker 的旧条目会残留，刷新前先 clear()

#### Q10：Consumer 的 startConsuming offset 传错
- **现象**：poll 内部 currentOffset += messages.size()，for 循环传的全是"读完后"的值
- **修法**：回退算起始 offset，for 里递增

#### Q11：Consumer 空列表判断
- **现象**：poll 返回空列表（没新消息）时 messages != null 为 true，永远不进 sleep → CPU 空转
- **修法**：判 `messages != null && !messages.isEmpty()`

#### Q12：Producer 构造器链
- **现象**：三参数构造器没设 createTopicIfNotExist（默认 false），应该委托给四参数版
- **修法**：`this(broker, port, topic, true)`

#### Q13：currentOffset 跨线程可见性
- **答**：被 poll（轮询线程）和 seek（main 线程）碰，普通 long 跨线程可能读到撕裂值。用 volatile long 或 AtomicLong。

---

## 贯穿两课的核心认知

### 1. 生命周期对称性
start 是"从空补到满"，stop 是"从满减到空"，顺序相反。开的顺序倒过来就是关的顺序——因为关的前提是"它的上游已没人用"。

### 2. 回调铁律
回调跑在别人的线程上（ZK 事件线程、executor 工人线程），必须 catch + log，永不 throw——线程被异常炸死 = 整个系统变聋。

### 3. 推 vs 拉的取舍
- 结构变化（全员影响）→ 推（controller 广播）
- 数据变化（副本影响）→ 推给副本（复制）
- 数据消费（消费者要）→ 拉（消费者主动 FETCH）

### 4. 流式 vs 查询
Kafka 是流式系统：Consumer 不"找"消息，它"订阅"流。消息被写进哪个分区是 Producer 的决定，Consumer 只管"把某个分区的所有消息读出来"。

### 5. 分片 vs 副本
- Partition = 分片（数据不同，水平扩展）
- Replica = 副本（数据相同，容灾）
- 两者正交，Kafka 全部存储布局 = 分片 × 副本的笛卡尔积

### 6. 三分法设计
- 身份证（调用方知道、各不相同）→ 参数
- 内脏（所有实例一样）→ 构造器 new
- 开机自检（慢、可能失败）→ initialize / 显式 start
