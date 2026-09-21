package com.simplekafka.broker;

public class ZkDemo {
    public static void main(String[] args) throws Exception {
        ZookeeperClient zk = new ZookeeperClient("localhost", 2181);
        zk.connect();                                       // 现象A：连接成功、创建骨架路径的日志

        zk.createEphemeralNode("/brokers/1", "localhost:9091");   // 我活着
        zk.watchChildren("/brokers", list ->
                System.out.println(">>> 在线broker: " + list));   // 现象B：立即打印 [1]

        zk.watchNode("/controller", () ->
                System.out.println(">>> controller 变了！"));     // 顺便验证你的 watchNode

        System.out.println(">>> 挂机 2 分钟，去另一个窗口操作 zkCli 吧");
        Thread.sleep(120_000);
        zk.close();
    }
}