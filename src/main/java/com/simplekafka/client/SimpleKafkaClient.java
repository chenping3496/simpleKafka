package com.simplekafka.client;

import com.simplekafka.broker.BrokerInfo;
import com.simplekafka.broker.Partition;
import com.simplekafka.broker.Protocol;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.simplekafka.broker.Protocol.ERROR_RESPONSE;

public class SimpleKafkaClient   {
    private final String bootstrapBroker;//调用方给种子地址
    private final int bootstartPort;

    //缓存leader信息和broker地址
    Map<String, TopicMetadata> topicMetada;
    Map<Integer, BrokerInfo> brokers;

    AtomicInteger correlationId;//请求需要一个递增的 id 做关联（响应和请求配对）。
    int DEFAULT_BUFFER_SIZE = 4096;//读响应 buffer 大小

    public SimpleKafkaClient(int bootstartPort, String bootstrapBroker) {
        this.bootstartPort = bootstartPort;
        this.bootstrapBroker = bootstrapBroker;
        topicMetada = new HashMap<>();
        brokers = new HashMap<>();
        this.correlationId = new AtomicInteger(0);
    }

    // === 方法 ===

    // initialize()：一启动就拉一次元数据
    //   → 调 refreshMetadata()
    public void initialize() throws IOException {
        refreshMetadata();
    }

    // refreshMetadata()：连种子 broker → 发 METADATA 请求 → 读响应 → decode → 更新两个 Map
    //   步骤：SocketChannel.open + connect
    //         channel.write(Protocol.encodeMetadataRequest())
    //         分配 buffer，read，flip
    //         Protocol.decodeMetadataResponse(buffer) → MetadataResult
    //         遍历 result.getBrokers() 填 brokers Map
    //         遍历 result.getTopics() 填 topicMetadata Map（每个 PartitionInfo 记 id/leader/followers）
    //         try-with-resources 关连接
    private void refreshMetadata() throws IOException {
        //连接种子broker
        try(SocketChannel ch = SocketChannel.open()) {
            ch.connect(new InetSocketAddress(bootstrapBroker, bootstartPort));
            //发送METADATA 请求
            ch.write(Protocol.encodeMetadataRequest());
            ByteBuffer buffer = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
            ch.read(buffer);
            buffer.flip();
            //解析响应
            Protocol.MetadataResult result = Protocol.decodeMetadataResponse(buffer);
            for (BrokerInfo broker : result.getBrokers()) {
                brokers.put(broker.getId(), broker);
            }
            for (Protocol.TopicMetadata pTopic : result.getTopics()) {
                List<PartitionInfo> partitions = new ArrayList<>();
                for (Protocol.PartitionMetadata pPart : pTopic.getPartitions()) {
                    partitions.add(new PartitionInfo(
                            pPart.getId(),
                            pPart.getLeader(),
                            pPart.getReplicas()    // Protocol 叫 replicas，客户端叫 followers
                    ));
                }
                topicMetada.put(pTopic.getName(), new TopicMetadata(pTopic.getName(), partitions));
            }
        }

    }



    // createTopic(topic, numPartitions, rf) → boolean：
    //   → brokers 空就先 refreshMetadata
    //   → 取任意一个 broker（brokers.values().iterator().next()）
    //   → 连它 → write(encodeCreateTopicRequest) → 读响应
    //   → 响应第一个字节 == CREATE_TOPIC_RESPONSE？是 → 读 status，0=成功
    //                                       否 → 是 ERROR 就读错误信息，返回 false
    //   → 成功就 refreshMetadata（刷新缓存把新 topic 加进来）

    public boolean createTopic(String topic, int numPartitions, short rf) throws IOException {
        if(brokers.isEmpty()){
            refreshMetadata();
        }
        BrokerInfo broker = brokers.values().iterator().next();
        try(SocketChannel ch = SocketChannel.open()) {
            ch.connect(new InetSocketAddress(broker.getHost(), broker.getPort()));
            ch.write(Protocol.encodeCreateTopicRequest(topic, numPartitions, rf));
            ByteBuffer buffer = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
            ch.read(buffer);
            buffer.flip();
            byte responseType = buffer.get();   // 1B 类型
            if (responseType == Protocol.CREATE_TOPIC_RESPONSE) {
                byte status = buffer.get();      // 1B status（0=成功）
                if (status == 0) {
                    refreshMetadata();
                    return true;
                }
                return false;
            } else if (responseType == Protocol.ERROR_RESPONSE) {
                // 读错误信息：[2B errLen][errMsg]
                short errLen = buffer.getShort();
                byte[] errBytes = new byte[errLen];
                buffer.get(errBytes);
                System.out.println("Error: " + new String(errBytes));
                return false;
            }
        }
        return false;
    }

    // send(topic, partition, message) → long offset：
    //   → topicMetadata 没有 topic？refreshMetadata；还没有 → throw "Topic not found"
    //   → 遍历 topicMetadata.get(topic).getPartitions() 找 id==partition 的
    //   → 拿到 leaderId → brokers.get(leaderId)
    //   → leader == null？refreshMetadata 重试；还 null → throw
    //   → 连 leader → write(encodeProduceRequest) → 读响应 → decodeProduceResponse
    //   → 返回 result.getOffset()
    public Long send(String topic, int partition, byte[] message) throws IOException {
        BrokerInfo leader = getLeader(topic, partition);
        try(SocketChannel ch = SocketChannel.open()) {
            ch.connect(new InetSocketAddress(leader.getHost(), leader.getPort()));
            ch.write(Protocol.encodeProduceRequest(topic, partition, message));
            ByteBuffer buffer = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
            ch.read(buffer);
            buffer.flip();
            Protocol.ProduceResult result = Protocol.decodeProduceResponse(buffer);
            return result.getOffset();
        }
    }

    private BrokerInfo getLeader(String topic, int partition) throws IOException {
        if(topicMetada.get(topic) == null){
            refreshMetadata();
            if(topicMetada.get(topic) == null){
                throw new RuntimeException("Topic not found");
            }
        }
        PartitionInfo partitionInfo = null;
        for (PartitionInfo p : topicMetada.get(topic).getPartitions()) {
            if (p.getId() == partition) { partitionInfo = p; break; }
        }
        if (partitionInfo == null) throw new RuntimeException("Partition not found: " + partition);


        BrokerInfo leader = brokers.get(partitionInfo.getLeader());
        if(leader == null){
            refreshMetadata();
            leader = brokers.get(partitionInfo.getLeader());
            if(leader == null){
                throw new RuntimeException("Leader not found");
            }
        }
        return leader;
    }

    // fetch(topic, partition, offset, maxBytes) → List<byte[]>：
    //   → 同样的"找 leader"逻辑（可以抽成私有方法 findLeader(topic, partition) → BrokerInfo）
    //   → 连 leader → write(encodeFetchRequest) → 读响应 → decodeFetchResponse
    //   → 把 byte[][] 转成 List<byte[]> 返回

    public List<byte[]> fetch(String topic, int partition, long offset, int maxBytes) throws IOException {
        BrokerInfo leader = getLeader(topic, partition);
        try(SocketChannel ch = SocketChannel.open()) {
            ch.connect(new InetSocketAddress(leader.getHost(), leader.getPort()));
            ch.write(Protocol.encodeFetchRequest(topic, partition, offset, maxBytes));
            ByteBuffer buffer = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
            ch.read(buffer);
            buffer.flip();
            Protocol.FetchResult result = Protocol.decodeFetchResponse(buffer);
            return Arrays.asList(result.getMessages());
        }
    }

    // getTopicMetadata(topic) → TopicMetadata：直接从缓存取
    public TopicMetadata getTopicMetadata(String topic) {
        return topicMetada.get(topic);
    }

    // === 内部类 ===
    // TopicMetadata：name (String, final) + List<PartitionInfo> partitions
    //   构造器 + getName() + getPartitions()（返回副本防篡改）

    public static class TopicMetadata {
        private final String name;
        private final List<PartitionInfo> partitions;

        public TopicMetadata(String name, List<PartitionInfo> partitions) {
            this.name = name;
            this.partitions = partitions;
        }

        public String getName() {
            return name;
        }

        public List<PartitionInfo> getPartitions() {
            return partitions;
        }
    }
    // PartitionInfo：id (int, final) + leader (int, final) + followers (List<Integer>, final)
    //   构造器 + getId() + getLeader() + getFollowers()（返回副本）
    public static class PartitionInfo {
        private final int id;
        private final int leader;
        private final List<Integer> followers;

        public PartitionInfo(int id, int leader, List<Integer> followers) {
            this.id = id;
            this.leader = leader;
            this.followers = followers;
        }

        public int getId() {
            return id;
        }

        public int getLeader() {
            return leader;
        }

        public List<Integer> getFollowers() {
            return followers;
        }
    }
}
