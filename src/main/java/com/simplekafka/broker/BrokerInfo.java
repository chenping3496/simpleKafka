package com.simplekafka.broker;

import java.util.Objects;

public class BrokerInfo {
    // 成员变量，final：实例创建后不能修改，不可变对象
    private final int id;
    private final String host;
    private final int port;

    public BrokerInfo(int id, String host, int port) {
        this.id = id;
        this.host = host;
        this.port = port;
    }

    // Getter方法，因为变量private，外部只能读，不能写
    public int getId() {
        return id;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return "BrokerInfo{id=" + id + ", host='" + host + "', port=" + port + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BrokerInfo that = (BrokerInfo) o;
        return id == that.id; //id唯一，根据id比较
    }

    // 重写hashCode：和equals配套，equals相等，hashCode必须相等
    @Override
    public int hashCode() {
        return Integer.hashCode(id);// 只用id计算哈希
    }

    public static void main(String[] args) {
        BrokerInfo a = new BrokerInfo(1, "localhost", 9091);
        BrokerInfo b = new BrokerInfo(1, "192.168.1.5", 9092);
        System.out.println(a);        // 应打印 BrokerInfo{id=1, host='localhost', port=9091}
        System.out.println(a.equals(b));  // true

        java.util.Map<BrokerInfo, String> m = new java.util.HashMap<>();
        m.put(a, "leader");
        System.out.println(m.get(b));     // 应打印 leader
    }
}
