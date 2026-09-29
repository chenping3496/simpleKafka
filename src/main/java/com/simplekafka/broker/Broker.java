package com.simplekafka.broker;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;

import java.io.File;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Broker {
    String DATA_DIR = "data";
    private final int brokerId;
    private final String host;
    private final int port;
    private static final Logger LOGGER = Logger.getLogger(Broker.class.getName());


    ServerSocketChannel serverChannel;//网络上连接
    Map<String, List<Partition>> topics;    // 存储topic和partition相关数据
    Map<Integer, BrokerInfo> clusterMetadata;   // brokerId → 第1课的 BrokerInfo
    ZookeeperClient zkClient;      // 注册自己、选举、watch 兄弟
    AtomicBoolean isController;    // 我现在的角色：普通 broker 还是大脑
    ExecutorService executor;            // 线程池：处理请求、选举、watch 等
    AtomicBoolean isRunning;    // 是否还在运行

    public Broker(int brokerId, String host, int port, ZookeeperClient zkClient) throws IOException {
        this.brokerId = brokerId;
        this.host = host;
        this.port = port;
        this.zkClient = zkClient;
        this.isController = new AtomicBoolean(false);
        this.isRunning = new AtomicBoolean(false);
        this.executor = Executors.newFixedThreadPool(10);   // 构造器里 new 的内脏
        this.topics = new ConcurrentHashMap<>();
        this.clusterMetadata = new ConcurrentHashMap<>();
        this.serverChannel = ServerSocketChannel.open();
        File dataDir = new File(DATA_DIR + File.separator + brokerId);
        if (!dataDir.exists()) dataDir.mkdirs();
    }
    public void start() throws IOException {
        if (isRunning.compareAndSet(false, true)) {        // 防重复启动
            serverChannel.socket().bind(new InetSocketAddress(host, port));//将ServerSocketChannel绑定到指定的Host和Port
            serverChannel.configureBlocking(false);//设置ServerSocketChannel为非阻塞模式
            registerWithZookeeper();    // ① 让集群看见我（/brokers/N + watch兄弟）
            electController();          // ② 抢大脑（或认老大）
            loadTopics();               // ③ 恢复历史 topic（读ZK → 重建 topics 表 → Partition对象接管磁盘）
            executor.submit(this::acceptConnections);   // ④ 最后才开门迎客
        }
    }


    public void stop() {
        if (isRunning.compareAndSet(true, false)) {   // 已停就不再重复停
            try {
                serverChannel.close();
                for (List<Partition> ps : topics.values())
                    for (Partition p : ps) p.close();
                executor.shutdown();
                executor.awaitTermination(5, TimeUnit.SECONDS);
                zkClient.close();
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Error stopping broker", e);
            }
        }
    }

    //===============网络================
    /**
     * 接受客户端连接的方法
     * 该方法会持续运行，接受新的客户端连接，并为每个连接分配处理线程
     * @throws IOException 如果在接受连接过程中发生I/O错误
     */
    public void acceptConnections() {
        while (isRunning.get()) {  // 只要服务在运行，就持续接受连接
            try {
                SocketChannel clientChannel = serverChannel.accept();   // ② 收一个连接
                if (clientChannel != null) {                             // ③ 非空才处理
                    clientChannel.configureBlocking(true);   // ★ 客户端通道用阻塞模式，避免 partial read
                    LOGGER.info("Accepted connection from " + clientChannel.getRemoteAddress());
                    executor.submit(() -> handleClient(clientChannel));  // ④ 派工人伺候
                }
                Thread.sleep(100);                                  // ⑤ 让出CPU，避免忙等待
            } catch (Exception e) {
                if (isRunning.get()) {                              // ⑥ 区分“正常停机”还是“真出事”
                    LOGGER.log(Level.SEVERE, "Error accepting connection", e);
                }
            }
        }
    }

/**
 * 处理客户端连接的方法
 * @param clientChannel 客户端SocketChannel通道
 */
    private void handleClient(SocketChannel clientChannel) {
        System.out.println("[DEBUG] handleClient STARTED for " + clientChannel);
        try {
            ByteBuffer buffer = ByteBuffer.allocate(1024);

            while (clientChannel.isOpen() && isRunning.get()) {
                buffer.clear();
                int bytesRead = clientChannel.read(buffer);
                System.out.println("[DEBUG] read returned: " + bytesRead);

                if (bytesRead > 0) {
                    buffer.flip();
                    System.out.println("[DEBUG] processing message, type byte=" + buffer.get(0));
                    try {
                        processClientMessage(clientChannel, buffer);
                    } catch (Exception e) {
                        // ★ 处理失败也要给客户端回个响应，否则客户端永远等
                        LOGGER.log(Level.SEVERE, "Error processing client message", e);
                        try {
                            Protocol.sendErrorResponse(clientChannel, "Internal server error");
                        } catch (IOException ioe) {
                            LOGGER.log(Level.WARNING, "Failed to send error response", ioe);
                        }
                    }
                } else if (bytesRead < 0) {
                    // 客户端断开连接，关闭channel并退出循环
                    clientChannel.close();
                    break;
                }

            // 添加短暂休眠，防止CPU空转
            // 注意：这里固定50ms可能不是最优值，应根据实际场景调整
                Thread.sleep(50);                              // ★ 坑2：非阻塞防CPU空转
            }
        } catch (Exception e) {
        // 如果服务仍在运行，记录错误日志
            if (isRunning.get()) {
                LOGGER.log(Level.SEVERE, "Error handling client", e);
            }
        } finally {
        // 确保无论如何都要关闭客户端连接
            try {
                if (clientChannel.isOpen()) clientChannel.close();   // 铁律：finally必关
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Error closing client channel", e);
            }
        }
    }

/**
 * 处理客户端消息的方法
 * @param clientChannel 客户端通道，用于与客户端通信
 * @param buffer 包含客户端消息的缓冲区
 * @throws IOException 如果发生I/O错误
 */
    private void processClientMessage(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        byte messageType = buffer.get();    // 消费第1个字节：消息类型

    // 根据消息类型将请求分发给不同的处理方法
        switch (messageType) {
            case Protocol.PRODUCE:
                handleProduceRequest(clientChannel, buffer);    // 处理生产请求
                break;
            case Protocol.FETCH:
                handleFetchRequest(clientChannel, buffer);    // 处理获取请求
                break;
            case Protocol.METADATA:
                handleMetadataRequest(clientChannel, buffer);    // 处理元数据请求
                break;
            case Protocol.CREATE_TOPIC:
                handleCreateTopicRequest(clientChannel, buffer);    // 处理创建主题请求
                break;
            case Protocol.REPLICATE:
                handleReplicateRequest(clientChannel, buffer);    // 处理复制请求，即produce后的同步
                break;
            case Protocol.TOPIC_NOTIFICATION:
                handleTopicNotification(clientChannel, buffer);    // 处理主题通知请求，即create_topic后的通知
                break;
            default:
            // 记录未知消息类型的警告日志并发送错误响应
                LOGGER.warning("Unknown message type: " + messageType);
                Protocol.sendErrorResponse(clientChannel, "Unknown message type");
        }
    }

    //===============处理请求================


/**
 * 处理生产者请求的方法，用于接收客户端发送的消息并存储到相应的主题分区中
 * @param clientChannel 客户端Socket通道，用于与客户端通信
 * @param buffer 包含请求数据的缓冲区，包含主题、分区号、消息长度和消息内容
 * @throws IOException 如果发生I/O错误
 */
    private void handleProduceRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
    // 从缓冲区中读取主题名称（短字符串格式）
        String topic = readShortString(buffer);
    // 从缓冲区中读取分区号
        int partition = buffer.getInt();
    // 从缓冲区中读取消息长度
        int msgLen = buffer.getInt();
    // 根据消息长度创建字节数组并读取消息内容
        byte[] message = new byte[msgLen];
        buffer.get(message);

        // 决策树：topic存在? 分区存在? 我是leader?
    // 检查主题是否存在，如果不存在则发送错误响应并返回
        if (!topics.containsKey(topic)) {
            Protocol.sendErrorResponse(clientChannel, "Topic does not exist"); return;
        }
    // 查找指定的分区，如果不存在则发送错误响应并返回
        Partition p = findPartition(topic, partition);
        if (p == null) {
            Protocol.sendErrorResponse(clientChannel, "Partition does not exist"); return;
        }
    // 检查当前broker是否是该分区的leader，如果不是则将请求转发给leader
        if (p.getLeader() != brokerId) {
            forwardProduceToLeader(clientChannel, topic, partition, message, p.getLeader()); return;
        }

    // 如果当前broker是分区leader，则将消息追加到分区日志中
        long offset = p.append(message);
        // acks=all：等所有 follower 都 ack 才算成功，否则回失败状态
        boolean allAcked = replicateToFollowers(topic, p, message, offset);

        // 回响应：[1B][8B offset][1B status]  status=0 成功(所有副本已确认) / 1 失败
        ByteBuffer resp = ByteBuffer.allocate(10);
        resp.put(Protocol.PRODUCE_RESPONSE);
        resp.putLong(offset);
        resp.put((byte)(allAcked ? 0 : 1));
        resp.flip();
        clientChannel.write(resp);
    }
    /**
     * 处理客户端的获取请求，从指定主题和分区中读取消息
     * @param clientChannel 客户端Socket通道，用于发送响应
     * @param buffer 包含请求内容的缓冲区
     * @throws IOException 如果发生I/O错误
     */
    private void handleFetchRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        // 从缓冲区读取主题长度和主题名称
        short topicLen = buffer.getShort();
        byte[] topicBytes = new byte[topicLen];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);

        // 读取分区号、偏移量和最大字节数
        int partition = buffer.getInt();
        long offset = buffer.getLong();
        int maxBytes = buffer.getInt();

        // 记录请求信息
        LOGGER.info("Fetch request for topic: " + topic + ", partition: " + partition +
                ", offset: " + offset + ", maxBytes: " + maxBytes);

        // 获取主题对应的分区列表
        List<Partition> partitions = topics.get(topic);
        if (partitions == null) {
            Protocol.sendErrorResponse(clientChannel, "Topic does not exist");
            return;
        }
        // 检查分区是否存在
        if (partition >= partitions.size()) {
            Protocol.sendErrorResponse(clientChannel, "Partition does not exist");
            return;
        }

        // 获取指定分区
        Partition p = partitions.get(partition);

        // 越界检查：offset >= 末尾 → 返回0条消息
        if (offset >= p.getLogEndOffset()) {
            ByteBuffer response = ByteBuffer.allocate(5);
            response.put(Protocol.FETCH_RESPONSE);
            response.putInt(0);   // 0条消息
            response.flip();
            clientChannel.write(response);
            return;
        }

        // 读取指定偏移量和最大字节数范围内的消息
        List<byte[]> messages = p.readMessages(offset, maxBytes);

        // 打包响应：[1B][4B count] + 每条 [8B offset][4B len][msg]
        int totalSize = 5;
        for (byte[] msg : messages) totalSize += 12 + msg.length;

        // 创建响应缓冲区
        ByteBuffer response = ByteBuffer.allocate(totalSize);
        response.put(Protocol.FETCH_RESPONSE);
        response.putInt(messages.size());

        // 写入每条消息：包括偏移量、长度和消息内容
        long currentOffset = offset;
        for (byte[] msg : messages) {
            response.putLong(currentOffset);
            response.putInt(msg.length);
            response.put(msg);
            currentOffset++;
        }
        response.flip();
        clientChannel.write(response);
    }

    private void handleCreateTopicRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        String topic = readShortString(buffer);
        int numPartitions = buffer.getInt();
        short replicationFactor = buffer.getShort();

        if (topics.containsKey(topic)) {
            Protocol.sendErrorResponse(clientChannel, "Topic already exists"); return;
        }
        if (numPartitions <= 0 || replicationFactor <= 0 || replicationFactor > clusterMetadata.size()) {
            Protocol.sendErrorResponse(clientChannel, "Invalid topic configuration"); return;
        }

        if (isController.get()) {
            System.out.println("[DEBUG-CT] before createTopic, topics has " + topic + "? " + topics.containsKey(topic));
            createTopic(topic, numPartitions, replicationFactor);
            System.out.println("[DEBUG-CT] after createTopic, topics has " + topic + "? " + topics.containsKey(topic));
            ByteBuffer resp = ByteBuffer.allocate(2);
            resp.put(Protocol.CREATE_TOPIC_RESPONSE);
            resp.put((byte)0);
            resp.flip();
            clientChannel.write(resp);
            System.out.println("[DEBUG-CT] response sent to client");
        } else {
            forwardCreateTopicToController(clientChannel, topic, numPartitions, replicationFactor);
        }
    }

    private void handleMetadataRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        int size = 5;
        for (var e : topics.entrySet()) {
            size += 6 + e.getKey().length();
            size += e.getValue().size() * 12;
            for (Partition p : e.getValue()) size += p.getFollowers().size() * 4;
        }
        size += 4 + clusterMetadata.size() * 10;
        for (BrokerInfo b : clusterMetadata.values()) size += b.getHost().length();

        ByteBuffer resp = ByteBuffer.allocate(size);
        resp.put(Protocol.METADATA_RESPONSE);
        // brokers
        resp.putInt(clusterMetadata.size());
        for (BrokerInfo b : clusterMetadata.values()) {
            resp.putInt(b.getId());
            resp.putShort((short)b.getHost().length());
            resp.put(b.getHost().getBytes());
            resp.putInt(b.getPort());
        }
        // topics
        resp.putInt(topics.size());
        for (var e : topics.entrySet()) {
            resp.putShort((short)e.getKey().length());
            resp.put(e.getKey().getBytes());
            resp.putInt(e.getValue().size());
            for (Partition p : e.getValue()) {
                resp.putInt(p.getId());
                resp.putInt(p.getLeader());
                resp.putInt(p.getFollowers().size());
                for (int f : p.getFollowers()) resp.putInt(f);
            }
        }
        resp.flip();
        clientChannel.write(resp);
    }

/**
 * 将生产请求转发给主题的leader broker
 * @param clientChannel 客户端通道，用于返回响应
 * @param topic 主题名称
 * @param partition 分区号
 * @param message 消息内容
 * @param leaderId leader broker的ID
 * @throws IOException 如果发生I/O错误
 */
    private void forwardProduceToLeader(SocketChannel clientChannel, String topic, int partition,
                                        byte[] message, int leaderId) throws IOException {
        // 从集群元数据中获取leader broker的信息
        BrokerInfo leader = clusterMetadata.get(leaderId);
        if (leader == null) {
            // 如果leader不可用，发送错误响应给客户端并返回
            Protocol.sendErrorResponse(clientChannel, "Leader broker not available"); return;
        }
        try (SocketChannel ch = SocketChannel.open()) {
            // 连接到leader broker
            ch.connect(new InetSocketAddress(leader.getHost(), leader.getPort()));
            // 发送生产请求给leader
            ch.write(Protocol.encodeProduceRequest(topic, partition, message));
            // 分配缓冲区并读取leader的响应
            ByteBuffer resp = ByteBuffer.allocate(1024);
            ch.read(resp); resp.flip();
            clientChannel.write(resp);   // 原样转发 leader 的响应给客户端
        } catch (IOException e) {
            Protocol.sendErrorResponse(clientChannel, "Failed to forward to leader");
        }
    }

    private void forwardCreateTopicToController(SocketChannel clientChannel, String topic,
                                                int numPartitions, short replicationFactor) throws IOException {
        int controllerId;
        try {
            controllerId = Integer.parseInt(zkClient.getData("/controller"));
        } catch (Exception e) {
            Protocol.sendErrorResponse(clientChannel, "Controller not available"); return;
        }
        BrokerInfo controller = clusterMetadata.get(controllerId);
        if (controller == null) {
            Protocol.sendErrorResponse(clientChannel, "Controller broker not available"); return;
        }
        try (SocketChannel ch = SocketChannel.open()) {
            ch.connect(new InetSocketAddress(controller.getHost(), controller.getPort()));
            ch.write(Protocol.encodeCreateTopicRequest(topic, numPartitions, replicationFactor));
            ByteBuffer resp = ByteBuffer.allocate(1024);
            ch.read(resp); resp.flip();
            clientChannel.write(resp);
        } catch (IOException e) {
            Protocol.sendErrorResponse(clientChannel, "Failed to forward to controller");
        }
    }

    //================复制========================
    /**
     * 将消息复制到主题的所有从节点（follower）上 —— acks=all 语义。
     * <p>
     * leader 本地 append 之后，必须等所有 follower 都回 REPLICATE_ACK 才算成功；
     * 任一 follower 失败/超时 → 返回 false，produce 响应标记失败。
     * 仍用线程池并行复制，但用 CountDownLatch 同步等待全部完成（带 5s 超时，
     * 避免某个 follower 挂死把 producer 拖死）。
     *
     * @param topic 主题名称
     * @param partition 分区对象
     * @param message 要复制消息的字节数组
     * @param offset 消息的偏移量
     * @return true=所有 follower 都 ack；false=至少一个 follower 失败/超时
     */
    private boolean replicateToFollowers(String topic, Partition partition, byte[] message, long offset) {
        // 统计需要等待的 follower 数（跳过自己和不在集群的）
        int pending = 0;
        for (int followerId : partition.getFollowers()) {
            if (followerId == brokerId) continue;
            if (clusterMetadata.get(followerId) == null) continue;
            pending++;
        }
        if (pending == 0) {
            return true;   // 没有需要复制的 follower，视为全部 ack
        }

        CountDownLatch latch = new CountDownLatch(pending);
        AtomicBoolean allAcked = new AtomicBoolean(true);

        for (int followerId : partition.getFollowers()) {
            if (followerId == brokerId) continue;
            BrokerInfo follower = clusterMetadata.get(followerId);
            if (follower == null) continue;

            executor.submit(() -> {   // 并行复制，但主线程会 await 全部完成
                try (SocketChannel ch = SocketChannel.open()) {
                    ch.connect(new InetSocketAddress(follower.getHost(), follower.getPort()));
                    ch.write(Protocol.encodeReplicateRequest(topic, partition.getId(), offset, message));
                    ByteBuffer resp = ByteBuffer.allocate(1);
                    ch.read(resp); resp.flip();
                    byte ack = resp.get();
                    if (ack == Protocol.REPLICATE_ACK) {
                        LOGGER.info("Replication to " + followerId + " ok");
                    } else {
                        allAcked.set(false);
                        LOGGER.warning("Replication to " + followerId + " nack=" + ack);
                    }
                } catch (IOException e) {
                    allAcked.set(false);
                    LOGGER.log(Level.SEVERE, "Replication to " + followerId + " failed", e);
                } finally {
                    latch.countDown();
                }
            });
        }

        try {
            // acks=all：等所有 follower ack；超时则视为有副本未确认
            if (!latch.await(5, TimeUnit.SECONDS)) {
                LOGGER.warning("Replication timed out (5s), not all followers acked");
                return false;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
        return allAcked.get();
    }

/**
 * 处理来自客户端的复制请求方法
 * @param clientChannel 客户端通道，用于与客户端通信
 * @param buffer 包含请求数据的缓冲区
 * @throws IOException 如果发生I/O错误
 */
    private void handleReplicateRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        // 从缓冲区中读取主题名称（短字符串）
        String topic = readShortString(buffer);
        // 从缓冲区中读取分区ID
        int partitionId = buffer.getInt();
        // 从缓冲区中读取偏移量（leader 指定的权威 offset）
        long leaderOffset = buffer.getLong();
        // 从缓冲区中读取消息长度
        int msgLen = buffer.getInt();
        // 根据消息长度创建字节数组
        byte[] message = new byte[msgLen];
        // 从缓冲区中读取消息内容到字节数组
        buffer.get(message);

        // 查找指定主题和分区的Partition对象
        Partition p = findPartition(topic, partitionId);
        // 如果找不到对应的分区，返回失败响应
        if (p == null) {
            ByteBuffer resp = ByteBuffer.allocate(1); resp.put((byte)0); resp.flip();
            clientChannel.write(resp); return;
        }

        // ★ 偏移量一致性：follower 必须用 leader 的 offset 写入，而非本地 nextOffset 重编号。
        // 仅当 leaderOffset == 本地 nextOffset（follower 已追平、与 leader 同步）才写入；
        // 否则说明 follower 失步（漏收前序消息，或重复收到已写过的 offset），
        // 拒绝写入并回失败 ack —— 避免静默重编号导致 failover 后按 offset 取错消息。
        // 真正的 Kafka 这时触发 follower 从 leader 按 nextOffset 重新拉取追平（HW/ISR）。
        byte ack;
        if (leaderOffset == p.getNextOffset().get()) {
            long written = p.append(message);   // 内部用 nextOffset，与 leaderOffset 相等
            ack = (written == leaderOffset) ? Protocol.REPLICATE_ACK : (byte)0;
        } else {
            LOGGER.warning("Follower out of sync: topic=" + topic + " partition=" + partitionId
                    + " leaderOffset=" + leaderOffset + " localNextOffset=" + p.getNextOffset().get()
                    + " — 拒绝复制，需从 leader 重新追平");
            ack = 0;   // 失步，拒绝
        }

        ByteBuffer resp = ByteBuffer.allocate(1);
        resp.put(ack);
        resp.flip();
        clientChannel.write(resp);
    }

    //=================集群====================
    private void registerWithZookeeper() {
        try {
            zkClient.connect();
            zkClient.createEphemeralNode("/brokers/" + brokerId, host + ":" + port);
            clusterMetadata.put(brokerId, new BrokerInfo(brokerId, host, port));
            zkClient.watchChildren("/brokers", this::onBrokersChanged);
            zkClient.watchNode("/controller", this::onControllerChange);   // ★ 唯一一次，永远不重复
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to register with ZooKeeper", e);
        }
    }

    private void onBrokersChanged(List<String> brokerIds) {
        // 更新通讯录：加新的、删死的
        for (String id : brokerIds) {
            try {
                int bid = Integer.parseInt(id);
                if (!clusterMetadata.containsKey(bid)) {
                    String data = zkClient.getData("/brokers/" + id);
                    String[] hp = data.split(":");
                    if (hp.length < 2) {
                        LOGGER.warning("Bad broker data for " + id + ": " + data);
                        continue;
                    }
                    clusterMetadata.put(bid, new BrokerInfo(bid, hp[0], Integer.parseInt(hp[1])));
                }
            } catch (Exception e) { LOGGER.log(Level.WARNING, "Failed to process broker info", e); }
        }
        clusterMetadata.keySet().removeIf(bid -> !brokerIds.contains(String.valueOf(bid)));//循环遍历删除死的

        // 角色逻辑
        if (!brokerIds.contains(String.valueOf(brokerId)) && isController.get()) {
            isController.set(false);   // 我自己掉了，让出大脑
        } else if (isController.get()) {
            rebalancePartitions();    // 我是大脑，重平衡
        } else {
            electController();        // 我是平民，试试抢（可能前任死了）
        }
    }

/**
 * 执行控制器选举方法
 * 该方法尝试在ZooKeeper中创建一个临时节点来成为控制器
 * 如果创建失败，则获取当前控制器的信息
 * 如果发生异常，会延迟2秒后重试选举
 */
    private void electController() {
        try {
        // 定义控制器在ZooKeeper中的路径
            String path = "/controller";
        // 检查路径是否存在，如果存在则检查节点数据是否为空
            if (zkClient.exists(path)) {
            // 获取节点数据
                String data = zkClient.getData(path);
            // 如果数据为null或空字符串，则删除该节点
                if (data == null || data.trim().isEmpty()) {
                    zkClient.deleteNode(path);
                }
            }
            // 抢位子 - 尝试创建临时节点成为控制器
            if (zkClient.createEphemeralNode(path, String.valueOf(brokerId))) {
            // 如果创建成功，设置当前broker为控制器
                isController.set(true);
                LOGGER.info("I am the controller");
            // 重新平衡分区
                rebalancePartitions();
            } else {
            // 创建失败，说明已经有其他broker成为控制器
                String controllerId = zkClient.getData(path);
                // ★ 关键修复：如果现有 controller 就是我自己，不要降级！
                if (controllerId != null && controllerId.equals(String.valueOf(brokerId))) {
                    isController.set(true);
                    LOGGER.info("I am still the controller");
                } else {
                    isController.set(false);
                    LOGGER.info("Controller is broker " + controllerId);
                }
            }
        } catch (Exception e) {
        // 记录选举失败的严重错误日志
            LOGGER.log(Level.SEVERE, "Controller election failed", e);
        // 创建新线程延迟2秒后重新尝试选举
            new Thread(() -> {
                try { Thread.sleep(2000); electController(); }
                catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }).start();
        }
    }

    private void onControllerChange() {
        LOGGER.info("Controller changed, initiating new election");
        electController();
    }
/**
 * 重新平衡分区的方法，用于在集群状态变化时调整分区领导者和追随者
 * 只有当当前实例是控制器时才会执行此操作
 */
    private void rebalancePartitions() {
        // 检查当前实例是否是控制器，如果不是则直接返回
        if (!isController.get()) return;
        // 遍历所有主题
        for (var entry : topics.entrySet()) {
            String topic = entry.getKey();
            // 遍历当前主题的所有分区
            for (Partition p : entry.getValue()) {
                // 检查分区是否有领导者，或者领导者是否不在当前集群元数据中
                if (p.getLeader() == -1 || !clusterMetadata.containsKey(p.getLeader())) {
                    // 获取当前集群中所有代理的列表
                    List<Integer> brokers = new ArrayList<>(clusterMetadata.keySet());
                    // 如果集群中存在可用的代理
                    if (!brokers.isEmpty()) {
                        // 选择第一个代理作为新的领导者
                        int newLeader = brokers.get(0);
                        p.setLeader(newLeader);
                        // 选择最多2个其他代理作为追随者（总共3个副本）
                        List<Integer> followers = new ArrayList<>();
                        for (int i = 1; i < Math.min(brokers.size(), 3); i++) followers.add(brokers.get(i));
                        p.setFollowers(followers);
                        updatePartitionMetadata(topic, p);
                    }
                }
            }
        }
    }

    private void updatePartitionMetadata(String topic, Partition partition) {
        try {
            String path = "/topics/" + topic + "/partitions/" + partition.getId();
            String data = partition.getLeader() + ";";
            for (int f : partition.getFollowers()) data += f + ",";
            if (zkClient.exists(path)) zkClient.setData(path, data);
            else zkClient.createPersistentNode(path, data);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to update partition metadata", e);
        }
    }

    private void createTopic(String topic, int numPartitions, short replicationFactor) {
        try {
            String topicDir = DATA_DIR + File.separator + brokerId + File.separator + topic;
            new File(topicDir).mkdirs();
            String topicPath = "/topics/" + topic;
            if (!zkClient.exists(topicPath)) {
                zkClient.createPersistentNode(topicPath, "");
                zkClient.createPersistentNode(topicPath + "/partitions", "");
            }
            List<Partition> partitions = new ArrayList<>();
            List<Integer> brokerIds = new ArrayList<>(clusterMetadata.keySet());
            for (int i = 0; i < numPartitions; i++) {
                int leaderIndex = i % brokerIds.size();
                int leaderId = brokerIds.get(leaderIndex);
                List<Integer> followers = new ArrayList<>();
                for (int j = 1; j < replicationFactor; j++)
                    followers.add(brokerIds.get((leaderIndex + j) % brokerIds.size()));

                String partitionDir = topicDir + File.separator + i;
                new File(partitionDir).mkdirs();
                Partition p = new Partition(i, leaderId, followers, partitionDir);
                partitions.add(p);

                String data = leaderId + ";";
                for (int f : followers) data += f + ",";
                zkClient.createPersistentNode(topicPath + "/partitions/" + i, data);
            }
            topics.put(topic, partitions);
            for (int id : brokerIds) if (id != brokerId) notifyBrokerForTopicCreation(id, topic);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to create topic", e);
        }
    }

    private void notifyBrokerForTopicCreation(int brokerId, String topic) {
        BrokerInfo broker = clusterMetadata.get(brokerId);
        if (broker == null) return;
        executor.submit(() -> {
            try (SocketChannel ch = SocketChannel.open()) {
                ch.connect(new InetSocketAddress(broker.getHost(), broker.getPort()));
                ch.write(Protocol.encodeTopicNotification(topic));
                ByteBuffer resp = ByteBuffer.allocate(1);
                ch.read(resp);
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Failed to notify broker " + brokerId, e);
            }
        });
    }

    private void handleTopicNotification(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        String topic = readShortString(buffer);
        try {
            loadTopic(topic);   // 读ZK档案 → 建Partition对象 → 塞进topics表
            ByteBuffer resp = ByteBuffer.allocate(1); resp.put((byte)0); resp.flip();
            clientChannel.write(resp);
        } catch (Exception e) {
            ByteBuffer resp = ByteBuffer.allocate(1); resp.put((byte)1); resp.flip();
            clientChannel.write(resp);
        }
    }

    private void loadTopic(String topic) throws Exception {
        if (topics.containsKey(topic)) return;
        String topicPath = "/topics/" + topic;
        if (!zkClient.exists(topicPath)) throw new Exception("Topic not in ZK: " + topic);

        String topicDir = DATA_DIR + File.separator + brokerId + File.separator + topic;
        new File(topicDir).mkdirs();

        List<String> partitionIds = zkClient.getChildren(topicPath + "/partitions");
        List<Partition> partitions = new ArrayList<>();
        for (String pid : partitionIds) {
            int id = Integer.parseInt(pid);
            String data = zkClient.getData(topicPath + "/partitions/" + pid);
            String[] parts = data.split(";");
            int leader = Integer.parseInt(parts[0]);
            List<Integer> followers = new ArrayList<>();
            if (parts.length > 1 && !parts[1].isEmpty())
                for (String fid : parts[1].split(","))
                    if (!fid.isEmpty()) followers.add(Integer.parseInt(fid));
            String partitionDir = topicDir + File.separator + id;
            new File(partitionDir).mkdirs();
            partitions.add(new Partition(id, leader, followers, partitionDir));
        }
        topics.put(topic, partitions);
    }

    public void loadTopics() {
        try {
            List<String> topicNames = zkClient.getChildren("/topics");
            for (String topic : topicNames) {
                try { loadTopic(topic); }
                catch (Exception e) { LOGGER.log(Level.SEVERE, "Failed to load topic: " + topic, e); }
            }
        } catch (Exception e) { LOGGER.log(Level.SEVERE, "Failed to load topics", e); }
    }

    //==============================辅助方法==============================
    // 读 [2B len][string]，第2课协议的逆操作
    private String readShortString(ByteBuffer buffer) {
        short len = buffer.getShort();
        byte[] bytes = new byte[len];
        buffer.get(bytes);
        return new String(bytes);
    }

    // 按 id 找分区（topics.get(topic) 是 List，下标即分区号）
    private Partition findPartition(String topic, int partitionId) {
        List<Partition> partitions = topics.get(topic);
        if (partitions == null) return null;
        for (Partition p : partitions) if (p.getId() == partitionId) return p;
        return null;
    }

    // ============================== main ==============================
    // 入口：解析 4 个参数 → new Broker → start → 注册 shutdown hook
    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: Broker <brokerId> <host> <port> [zkPort]");
            System.exit(1);
        }
        try {
            int brokerId = Integer.parseInt(args[0]);
            String host = args[1];
            int port = Integer.parseInt(args[2]);
            int zkPort = args.length > 3 ? Integer.parseInt(args[3]) : 2181;

            Broker broker = new Broker(brokerId, host, port, new ZookeeperClient("localhost", zkPort));
            broker.start();
            Runtime.getRuntime().addShutdownHook(new Thread(broker::stop));   // Ctrl+C 优雅停
            System.out.println("Broker " + brokerId + " started. Press Ctrl+C to stop.");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to start broker", e);
        }
    }
}
