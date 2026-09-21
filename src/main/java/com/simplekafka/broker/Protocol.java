package com.simplekafka.broker;

import java.nio.channels.SocketChannel;
import java.io.IOException;                      // 顺手补上，见必修 2

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class Protocol {
    //REQUEST
    public static final byte PRODUCE = 0x01;
    public static final byte FETCH = 0x02;
    public static final byte METADATA = 0x03;
    public static final byte CREATE_TOPIC = 0x04;
    public static final byte REPLICATE = 0x21;
    public static final byte TOPIC_NOTIFICATION = 0x23;

    //RESPONCE
    public static final byte PRODUCE_RESPONSE = 0x11;
    public static final byte FETCH_RESPONSE = 0x12;
    public static final byte METADATA_RESPONSE  = 0x13;
    public static final byte CREATE_TOPIC_RESPONSE  = 0x14;
    public static final byte ERROR_RESPONSE  = 0x1F;
    public static final byte REPLICATE_ACK  = 0x22;

    /** 样板：把 produce 请求打包成字节（注意 allocate 的算术和最后的 flip） */
    public static ByteBuffer encodeProduceRequest(String topic, int partition, byte[] message) {
        ByteBuffer buffer = ByteBuffer.allocate(11 + topic.length() + message.length);
        buffer.put(PRODUCE);                                  // 1B
        buffer.putShort((short) topic.length());              // 2B
        buffer.put(topic.getBytes());                         // topic 字节
        buffer.putInt(partition);                             // 4B
        buffer.putInt(message.length);                        // 4B
        buffer.put(message);                                  // 消息体
        buffer.flip();                                        // 写转读，发送前必做
        return buffer;
    }

    /** 样板：解析 produce 响应。所有 decode 都是同一个套路：先读类型字节并校验 */
    public static ProduceResult decodeProduceResponse(ByteBuffer buffer) {
        byte responseType = buffer.get();                     // 消费 1B
        if (responseType != PRODUCE_RESPONSE) {
            if (responseType == ERROR_RESPONSE) {
                short errorLength = buffer.getShort();
                byte[] errorBytes = new byte[errorLength];
                buffer.get(errorBytes);
                return new ProduceResult(-1, new String(errorBytes));
            }
            return new ProduceResult(-1, "Invalid response type");
        }
        long offset = buffer.getLong();                       // 8B
        byte status = buffer.get();                           // 1B
        return new ProduceResult(offset, status == 0 ? null : "Produce failed");
    }

    // TODO 1: sendErrorResponse —— [1B ERROR_RESPONSE][2B errLen][errMsg]，flip 后 channel.write
    public static void sendErrorResponse(SocketChannel channel, String errorMessage)throws IOException{
        ByteBuffer buffer = ByteBuffer.allocate(3 + errorMessage.length());
        buffer.put(ERROR_RESPONSE);
        buffer.putShort((short) errorMessage.length());
        buffer.put(errorMessage.getBytes());
        buffer.flip();
        channel.write(buffer);
    }

    // TODO 2: encodeFetchRequest / encodeMetadataRequest / encodeCreateTopicRequest /
    //         encodeReplicateRequest / encodeTopicNotification —— 照总表和样板写，算好 allocate
    public static ByteBuffer encodeFetchRequest(String topic, int partition, long offset, int maxBytes){
        ByteBuffer buffer = ByteBuffer.allocate(19 + topic.length());
        buffer.put(FETCH);                                  // 1B
        buffer.putShort((short) topic.length());              // 2B
        buffer.put(topic.getBytes());                         // topic 字节
        buffer.putInt(partition);                             // 4B partition
        buffer.putLong(offset);                        // 8B
        buffer.putInt(maxBytes);                                  // 4B maxBytes
        buffer.flip();                                        // 写转读，发送前必做
        return buffer;
    }

    public static ByteBuffer encodeMetadataRequest(){
        ByteBuffer buffer = ByteBuffer.allocate(1);
        buffer.put(METADATA);
        buffer.flip();
        return buffer;
    }

    public static ByteBuffer encodeCreateTopicRequest(String topic, int numPartition, short replicationFactor){
        ByteBuffer buffer = ByteBuffer.allocate(9 + topic.length());
        buffer.put(CREATE_TOPIC);                                  // 1B
        buffer.putShort((short) topic.length());              // 2B
        buffer.put(topic.getBytes());                         // topic 字节
        buffer.putInt(numPartition);                             // 4B partition
        buffer.putShort(replicationFactor);                             // 2B replicationFactor
        buffer.flip();                                        // 写转读，发送前必做
        return buffer;
    }

    public static ByteBuffer encodeReplicateRequest(String topic, int partition, long offset, byte[] msg){
        ByteBuffer buffer = ByteBuffer.allocate(19 + topic.length() + msg.length);
        buffer.put(REPLICATE);                                  // 1B
        buffer.putShort((short) topic.length());              // 2B
        buffer.put(topic.getBytes());                         // topic 字节
        buffer.putInt(partition);                             // 4B partition
        buffer.putLong(offset);                             // 8B offset
        buffer.putInt(msg.length);                             // 4B msgLen
        buffer.put(msg);                             // 4B msg length
        buffer.flip();
        return buffer;
    }

    public static ByteBuffer encodeTopicNotification(String topic) {
        ByteBuffer buffer = ByteBuffer.allocate(3 + topic.length());
        buffer.put(TOPIC_NOTIFICATION);                                  // 1B
        buffer.putShort((short) topic.length());              // 2B
        buffer.put(topic.getBytes());                         // topic 字节
        buffer.flip();
        return buffer;
    }
    // TODO 3: decodeFetchResponse / decodeMetadataResponse —— 注意"循环读 N 个元素"的写法
    public static FetchResult decodeFetchResponse(ByteBuffer buffer) {
        byte responseType = buffer.get();                     // 消费 1B
        if (responseType != FETCH_RESPONSE) {
            if (responseType == ERROR_RESPONSE) {
                short errorLength = buffer.getShort();
                byte[] errorBytes = new byte[errorLength];
                buffer.get(errorBytes);
                return new FetchResult(new byte[0][], new String(errorBytes));
            }
            return new FetchResult(new byte[0][], "Invalid response type");
        }
        int count = buffer.getInt(); // 4B
        byte[][] messages = new byte[count][];
        for(int i = 0 ;i < count ;i++) {
            long offset = buffer.getLong();                           // 8B
            int len = buffer.getInt();                           // 4B
            messages[i] = new byte[len];
            buffer.get(messages[i]);
        }

        return new FetchResult(messages,null);
    }

    public static MetadataResult decodeMetadataResponse(ByteBuffer buffer){
        byte responseType = buffer.get();                     // 消费 1B
        if (responseType != METADATA_RESPONSE) {
            if(responseType == ERROR_RESPONSE) {
            short errorLength = buffer.getShort();
                byte[] errorBytes = new byte[errorLength];
                buffer.get(errorBytes);
                return new MetadataResult(new ArrayList<>(),new ArrayList<>(),new String (errorBytes));
            }
        }
        int brokerCount = buffer.getInt();
        List<BrokerInfo> brokers = new ArrayList<>();
        for(int i = 0 ;i < brokerCount ;i++) {
            int id = buffer.getInt();
            short hostlen = buffer.getShort();
            byte[] hostBytes = new byte[hostlen];
            buffer.get(hostBytes);
            int port = buffer.getInt();
            BrokerInfo broker = new BrokerInfo(id,new String(hostBytes),port);
            brokers.add(broker);
        }
        int topicCount = buffer.getInt();
        List<TopicMetadata> topics = new ArrayList<>();
        for(int j = 0 ;j < topicCount ;j++) {
            short topiclen = buffer.getShort();
            byte[] topicName = new byte[topiclen];
            buffer.get(topicName);
            int partitionCount = buffer.getInt();
            List<PartitionMetadata> partitions = new ArrayList<>();
            for(int k = 0 ;k < partitionCount ;k++) {
                int partitionId = buffer.getInt();
                int leader = buffer.getInt();
                int followerCount = buffer.getInt();
                List<Integer> followers = new ArrayList<>();
                for(int l = 0 ;l < followerCount ;l++) {
                    int follower = buffer.getInt();
                    followers.add(follower);
                }
                partitions.add(new PartitionMetadata(partitionId,leader,followers));
            }
            topics.add(new TopicMetadata(new String(topicName),partitions));
        }
        return new MetadataResult(brokers,topics,null);
    }

    // TODO 4: 五个静态内部类 ProduceResult / FetchResult / MetadataResult /
    //         TopicMetadata / PartitionMetadata —— 全是 final 字段 + 构造器 + getter + isSuccess()
    public static class ProduceResult{
        private final long offset;
        private final String error;

        public ProduceResult(long offset, String error) {
            this.offset = offset;
            this.error = error;
        }

        public String getError() {
            return error;
        }

        public long getOffset() {
            return offset;
        }

        public boolean isSuccess(){
            return error == null;
        }
    }

    public static class FetchResult{
        private final byte[][] messages;
        private final String error;
        public FetchResult(byte[][] messages, String error) { this.messages = messages; this.error = error; }
        public byte[][] getMessages() { return messages; }
        public int getMessageCount() { return messages.length; }
        public String getError() { return error; }
        public boolean isSuccess() { return error == null; }
    }

    public static class MetadataResult{
        private final List<BrokerInfo> brokers;
        private final List<TopicMetadata> topics;
        private final String error;

        public MetadataResult(List<BrokerInfo> brokers, List<TopicMetadata> topics, String error) {
            this.brokers = brokers;
            this.topics = topics;
            this.error = error;
        }

        public List<BrokerInfo> getBrokers() {
            return brokers;
        }

        public List<TopicMetadata> getTopics() {
            return topics;
        }

        public String getError() {
            return error;
        }

        public boolean isSuccess() { return error == null; }
    }

    public static class TopicMetadata{
        private final String name;
        private final List<PartitionMetadata> partitions;

        public TopicMetadata(String name, List<PartitionMetadata> partitions) {
            this.name = name;
            this.partitions = partitions;
        }

        public String getName() {
            return name;
        }

        public List<PartitionMetadata> getPartitions() {
            return partitions;
        }
    }

    public static class PartitionMetadata{
        private final int id;
        private final int leader;
        private final List<Integer> replicas;

        public PartitionMetadata(int id, int leader, List<Integer> replicas) {
            this.id = id;
            this.leader = leader;
            this.replicas = replicas;
        }

        public int getId() {
            return id;
        }

        public int getLeader() {
            return leader;
        }

        public List<Integer> getReplicas() {
            return replicas;
        }
    }

    public static void main(String[] args) {
        // 1) fetch 响应回环：手工构造一个含 2 条消息的假响应，再 decode 回来
        ByteBuffer fr = ByteBuffer.allocate(1 + 4 + (8 + 4 + 5) * 2);
        fr.put(FETCH_RESPONSE); fr.putInt(2);
        fr.putLong(0L);  fr.putInt(5); fr.put("hello".getBytes());
        fr.putLong(1L);  fr.putInt(5); fr.put("world".getBytes());
        fr.flip();
        FetchResult fres = decodeFetchResponse(fr);
        System.out.println(fres.isSuccess());                    // true
        System.out.println(new String(fres.getMessages()[1]));   // world

        // 2) metadata 响应回环：1 个 broker + 1 个 topic（1 分区，2 副本）
        ByteBuffer mr = ByteBuffer.allocate(64);
        mr.put(METADATA_RESPONSE);
        mr.putInt(1); mr.putInt(1); mr.putShort((short)9); mr.put("localhost".getBytes()); mr.putInt(9091);
        mr.putInt(1); mr.putShort((short)5); mr.put("order".getBytes());
        mr.putInt(1); mr.putInt(0); mr.putInt(1); mr.putInt(1); mr.putInt(2);
        mr.flip();
        MetadataResult mres = decodeMetadataResponse(mr);
        System.out.println(mres.isSuccess());                                    // true
        System.out.println(mres.getBrokers().get(0));                            // BrokerInfo{id=1, ...}
        System.out.println(mres.getTopics().get(0).getPartitions().get(0).getLeader());  // 1
    }

}
