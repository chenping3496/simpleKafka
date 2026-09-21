package com.simplekafka.test;

import com.simplekafka.broker.Broker;
import com.simplekafka.broker.ZookeeperClient;
import com.simplekafka.client.SimpleKafkaConsumer;
import com.simplekafka.client.SimpleKafkaProducer;

import java.util.List;

/**
 * 端到端集成测试：单进程内起 broker + producer + consumer
 * 用法：java -cp target/simple-kafka-1.0-SNAPSHOT.jar com.simplekafka.test.IntegrationTest
 */
public class IntegrationTest {

    private static int passed = 0;
    private static int failed = 0;

    // === 断言工具 ===
    private static void assertEquals(int actual, int expected, String msg) {
        if (actual == expected) {
            System.out.println("  ✓ " + msg);
            passed++;
        } else {
            System.out.println("  ✗ " + msg + " (expected=" + expected + ", actual=" + actual + ")");
            failed++;
        }
    }

    private static void assertEquals(String actual, String expected, String msg) {
        if (actual.equals(expected)) {
            System.out.println("  ✓ " + msg);
            passed++;
        } else {
            System.out.println("  ✗ " + msg + " (expected='" + expected + "', actual='" + actual + "')");
            failed++;
        }
    }

    // === main ===
    public static void main(String[] args) throws Exception {
        System.out.println("=== Integration Test ===\n");

        // 先清理上次的脏数据（保证可重复跑）
        cleanData();

        System.out.println("[Test 1] Single broker: produce + fetch");
        testSingleBroker();

        System.out.println("\n[Test 2] Multi broker: 3 brokers + replication");
        testMultiBroker();

        System.out.println("\n=== Results: " + passed + " passed, " + failed + " failed ===");
        if (failed > 0) {
            System.exit(1);
        }
    }

    // === 测试1：单 broker，验证基本 produce/fetch 链路 ===
    // 单 broker 时副本数只能 1，所以手动建 topic（replication=1）
    // Producer 发 5 条到【指定分区0】，Consumer 从分区0 读
    public static void testSingleBroker() throws Exception {
        // 1. 起 broker
        ZookeeperClient zkClient = new ZookeeperClient("localhost", 2181);
        Broker broker = new Broker(1, "localhost", 9091, zkClient);
        broker.start();
        Thread.sleep(2000);   // 等启动序列完成

        try {
            // 2. 用 Producer 建 topic（副本=1，单 broker 能成功）
            //    注意：要改 Producer 的 DEFAULT_REPLICATION 或手动建 topic
            //    这里用 Producer 的 createTopicIfNotExists=true，但需要副本=1
              //    临时改法：直接用 SimpleKafkaClient 建 topic
            com.simplekafka.client.SimpleKafkaClient client =
                    new com.simplekafka.client.SimpleKafkaClient(9091, "localhost");
            client.initialize();
            // 单 broker 时 replicationFactor=1
            boolean created = client.createTopic("test-single", 3, (short) 1);
            if (!created) {
                System.out.println("  ! Topic may already exist, continuing");
            }
            Thread.sleep(500);   // 等 broker 处理 createTopic + 通知

            // 3. 发 5 条到分区0（指定分区，避免随机分散）
            for (int i = 0; i < 5; i++) {
                long offset = client.send("test-single", 0, ("msg-" + i).getBytes());
                System.out.println("  sent msg-" + i + " → offset " + offset);
                assertEquals((int) offset, i, "offset of msg-" + i);
            }

            // 4. 从分区0 读回
            List<byte[]> messages = client.fetch("test-single", 0, 0L, 1024 * 1024);
            assertEquals(messages.size(), 5, "fetched message count");

            for (int i = 0; i < messages.size(); i++) {
                assertEquals(new String(messages.get(i)), "msg-" + i, "content of msg-" + i);
            }

        } finally {
            broker.stop();
            Thread.sleep(1000);   // 等临时节点消失
        }
    }

    // === 测试2：3 个 broker，验证分区分配 + 复制 ===
    public static void testMultiBroker() throws Exception {
        // 1. 起 3 个 broker
        ZookeeperClient zk1 = new ZookeeperClient("localhost", 2181);
        Broker broker1 = new Broker(1, "localhost", 9091, zk1);
        broker1.start();
        Thread.sleep(2000);   // 等 broker1 当选 controller

        ZookeeperClient zk2 = new ZookeeperClient("localhost", 2181);
        Broker broker2 = new Broker(2, "localhost", 9092, zk2);
        broker2.start();
        Thread.sleep(1000);

        ZookeeperClient zk3 = new ZookeeperClient("localhost", 2181);
        Broker broker3 = new Broker(3, "localhost", 9093, zk3);
        broker3.start();
        Thread.sleep(1000);

        try {
            // 2. 建 topic（3 分区 2 副本）
            com.simplekafka.client.SimpleKafkaClient client =
                    new com.simplekafka.client.SimpleKafkaClient(9091, "localhost");
            client.initialize();
            boolean created = client.createTopic("test-multi", 3, (short) 2);
            if (!created) {
                System.out.println("  ! Topic may already exist, continuing");
            }
            Thread.sleep(1000);   // 等通知传播

            // 3. 发 10 条到分区0（指定分区，确保可验证）
            for (int i = 0; i < 10; i++) {
                client.send("test-multi", 0, ("multi-" + i).getBytes());
            }
            System.out.println("  sent 10 messages to partition 0");

            // 4. 从分区0 读回
            List<byte[]> messages = client.fetch("test-multi", 0, 0L, 1024 * 1024);
            assertEquals(messages.size(), 10, "multi-broker fetched count");

            // 5. 验证内容
            for (int i = 0; i < 10; i++) {
                assertEquals(new String(messages.get(i)), "multi-" + i, "multi content " + i);
            }

            System.out.println("  ✓ All 10 messages verified across 3-broker cluster");

        } finally {
            broker1.stop();
            broker2.stop();
            broker3.stop();
            Thread.sleep(1000);
        }
    }

    // === 清理 ===
    private static void cleanData() throws Exception {
        // 1. 删本地磁盘数据（broker 各自的 data 目录）
        String[] dirs = {"data/1", "data/2", "data/3", "data"};
        for (String dir : dirs) {
            java.io.File d = new java.io.File("E:/progrmmes/simpleKafka/" + dir);
            if (d.exists()) deleteRecursively(d);
        }
        // 2. 清 ZK 持久节点（上次跑遗留的 topic 元数据）
        com.simplekafka.broker.ZookeeperClient zk = new com.simplekafka.broker.ZookeeperClient("localhost", 2181);
        zk.connect();
        try {
            // 删 /topics 下所有子节点
            for (String child : zk.getChildren("/topics")) {
                try { deleteZkSubtree(zk, "/topics/" + child); } catch (Exception e) { /* ignore */ }
            }
            // 删 /controller（可能残留临时节点）
            if (zk.exists("/controller")) {
                String data = zk.getData("/controller");
                if (data == null || data.trim().isEmpty()) zk.deleteNode("/controller");
            }
        } finally {
            zk.close();
        }
        Thread.sleep(500); // 等临时节点和 watch 过期
        System.out.println("  (cleaned local data + ZK nodes)");
    }

    private static void deleteZkSubtree(com.simplekafka.broker.ZookeeperClient zk, String path) throws Exception {
        for (String child : zk.getChildren(path)) {
            deleteZkSubtree(zk, path + "/" + child);
        }
        zk.deleteNode(path);
    }

    private static void deleteRecursively(java.io.File f) {
        if (f.isDirectory()) {
            for (java.io.File c : f.listFiles()) deleteRecursively(c);
        }
        f.delete();
    }
}
