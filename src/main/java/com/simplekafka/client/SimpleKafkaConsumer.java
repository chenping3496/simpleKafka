package com.simplekafka.client;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.List;

public class SimpleKafkaConsumer {
    SimpleKafkaClient client;
    private final String topic;  //绑定的 topic
    private final int partition;  //绑定的分区
    long currentOffset;     //自己记消费进度（服务端不记）
    AtomicBoolean running;  //轮询循环开关
    Thread consumerThread;     //轮询线程引用，用来 stop
    int MAX_BYTES = 1024*1024;
    int POLL_INTERVAL_MS= 100;

    // === 构造器 ===
    // (broker, port, topic, partition) → this(..., 0)  // 默认从 offset 0 开始
    // (broker, port, topic, partition, startOffset) → new Client + 存配置 + currentOffset = startOffset
    public SimpleKafkaConsumer(String broker, int port, String topic, int partition, long startOffset) {
        this.client = new SimpleKafkaClient(port,broker);
        this.topic = topic;
        this.partition = partition;
        this.currentOffset = startOffset;
        this.running = new AtomicBoolean(false);
    }
    public SimpleKafkaConsumer(String broker, int port, String topic, int partition) {
        this.client = new SimpleKafkaClient(port,broker);
        this.topic = topic;
        this.partition = partition;
        this.currentOffset = 0;
        this.running = new AtomicBoolean(false);
    }

    // === 方法 ===

    // initialize()：
    //   → client.initialize()
    //   → client.getTopicMetadata(topic) == null？throw "Topic does not exist"
    public void initialize() throws Exception {
        client.initialize();
        if (client.getTopicMetadata(topic) == null) {
            throw new Exception("Topic does not exist");
        }
    }

    // seek(offset)：重置消费进度
    //   → currentOffset = offset
    public void seek(long offset) {
        this.currentOffset = offset;
    }

    // poll() → List<byte[]>：单次拉取
    //   → client.fetch(topic, partition, currentOffset, MAX_BYTES)
    //   → messages 非空 → currentOffset += messages.size()   // ★ 自己推进进度
    //   → 返回 messages
    public List<byte[]> poll() throws IOException {
        List<byte[]> messages = client.fetch(topic, partition, currentOffset, MAX_BYTES);
        if (messages != null) {
            currentOffset += messages.size();
        }
        return messages;

    }

    // startConsuming(handler)：启动轮询线程
    //   → running.compareAndSet(false, true)
    //   → new Thread(() -> {
    //       while (running.get()) {
    //         messages = poll()
    //         for each message: handler.handle(message, offset)
    //         messages 空？sleep(POLL_INTERVAL_MS)
    //       }
    //     })
    //   → setDaemon(true) + start()
    public void startConsuming(MessageHandler handler) {
        running.compareAndSet(false, true);
        consumerThread = new Thread(() -> {
            while (running.get()) {
                try {
                    List<byte[]> messages = poll();
                    if (messages != null && !messages.isEmpty()) {
                        long offset = currentOffset - messages.size();
                        for (byte[] message : messages) {
                            handler.handle(message, offset);
                            offset++;
                        }
                    } else {
                        Thread.sleep(POLL_INTERVAL_MS);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        consumerThread.setDaemon(true);
        consumerThread.start();
    }

    // stopConsuming()：
    //   → running.compareAndSet(true, false)
    //   → consumerThread.interrupt() + join(1000)
    public void stopConsuming() {
        running.compareAndSet(true, false);
        try {
            consumerThread.interrupt();
            consumerThread.join(1000);
        } catch (InterruptedException e){
            throw new RuntimeException(e);
        }

    }

    // getCurrentOffset() → long
    public long getCurrentOffset() {
        return currentOffset;
    }
    // isRunning() → boolean
    public boolean isRunning() {
        return running.get();
    }
    // close()：调 stopConsuming
    public void close() {
        stopConsuming();
    }
    // === 接口 ===
    // MessageHandler { void handle(byte[] message, long offset) }
    public interface MessageHandler {
        void handle(byte[] message, long offset);
    }

    // main(args)：演示
    //   → 解析 broker/port/topic/partition
    //   → new Consumer + initialize
    //   → startConsuming((msg, offset) → 打印消息)
    //   → System.in.read() 等回车 → close
    public static void main(String[] args) {
        try {
            String broker = args[0];
            int port = Integer.parseInt(args[1]);
            String topic = args[2];
            int partition = Integer.parseInt(args[3]);
            SimpleKafkaConsumer consumer = new SimpleKafkaConsumer(broker, port, topic, partition);
            consumer.startConsuming((msg, offset) -> System.out.println("offset: " + offset + ", message: " + new String(msg)));
            System.in.read();
            consumer.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
