package com.simplekafka.client;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

public class SimpleKafkaProducer {
    SimpleKafkaClient client;
    private final String topic;        //该produce绑定的topic
    Random random;         //无分区时随机选择分区
    boolean createTopicIfNotExist;    //发消息前自动建 topic
    int DEFAULT_PARTITIONS = 3;     //自动建 topic 时的默认分区数
    short DEFAULT_REPLICATION = 2;  //自动建 topic 时的默认副本数

    // === 构造器 ===
    // (broker, port, topic) → this(broker, port, topic, true)  // 默认自动建
    // (broker, port, topic, createIfNotExists) → new SimpleKafkaClient + new Random
    public SimpleKafkaProducer(String broker, int port, String topic) {
        this(broker, port, topic, true);   // 委托给下面的四参数版
    }
    public SimpleKafkaProducer(String broker, int port, String topic, boolean createIfNotExists) {
        client = new SimpleKafkaClient(port,broker);
        this.topic = topic;
        random = new Random();
        createTopicIfNotExist = createIfNotExists;
    }

    // === 方法 ===

    // initialize()：
    //   → client.initialize()
    //   → client.getTopicMetadata(topic) == null && createTopicIfNotExists？
    //       → client.createTopic(topic, DEFAULT_PARTITIONS, DEFAULT_REPLICATION)
    //       → 失败就 throw "Failed to create topic"
    public void initialize() throws IOException {
        client.initialize();
        if (client.getTopicMetadata(topic) == null && createTopicIfNotExist) {
            if (!client.createTopic(topic, DEFAULT_PARTITIONS, DEFAULT_REPLICATION)) {
                throw new RuntimeException("Failed to create topic");
            }
        }
    }

    // send(message) → long：无分区版
    //   → 查 metadata.getPartitions().size() → random.nextInt(分区数)
    //   → 调 send(message, 随机分区)
    public long send(String message) throws IOException {
        return send(message,random.nextInt(client.getTopicMetadata(topic).getPartitions().size()));
    }

    // send(message, partition) → long：
    //   → message.getBytes(UTF_8)
    //   → client.send(topic, partition, bytes) → 返回 offset
    public long send(String message, int partition) throws IOException {
        return client.send(topic, partition, message.getBytes("UTF-8"));
    }

    // close()：教育版无资源要关，空方法
    public void close() {
    }

    // main(args)：演示
    //   → 解析 broker/port/topic 三个参数
    //   → new Producer + initialize
    //   → 循环发 10 条 "Message i - timestamp"
    //   → 每条 sleep 1 秒
    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("Usage: java SimpleKafkaProducer <broker> <port> <topic>");
            System.exit(1);
        }
        String broker = args[0];
        int port = Integer.parseInt(args[1]);
        String topic = args[2];

        try {
            SimpleKafkaProducer producer = new SimpleKafkaProducer(broker, port, topic);
            producer.initialize();
            for (int i = 0; i < 10; i++) {
                producer.send("Message " + i + " - " + System.currentTimeMillis());
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
