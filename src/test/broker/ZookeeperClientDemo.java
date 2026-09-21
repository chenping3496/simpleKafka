
import org.apache.zookeeper.KeeperException;
import java.util.List;

public class ZookeeperClientDemo {
    public static void main(String[] args) throws Exception, KeeperException {
        // 1. 创建ZK客户端并连接ZK
        ZookeeperClient zkClient = new ZookeeperClient("localhost:2181");
        zkClient.connect();

        // 2. Broker注册：创建临时节点 /brokers/ids/1
        String brokerData = "{\"host\":\"localhost\",\"port\":9092,\"endpoints\":[]}";
        zkClient.createEphemeralNode("/brokers/ids/1", brokerData);

        // 3. 创建topic持久节点
        String topicConfig = "{\"partitions\":3,\"replication_factor\":2}";
        zkClient.createPersistentNode("/topics/my-topic", topicConfig);

        // 4. 监听所有broker（/brokers/ids）子节点变化，感知broker上下线
        zkClient.watchChildren("/brokers/ids", (List<String> children) -> {
            System.out.println("Current online broker ids: " + children);
        });

        // 5. 监听controller节点变化
        zkClient.watchNode("/controller", () -> {
            System.out.println("Controller node changed!");
        });

        Thread.sleep(60000);
        zkClient.close();
    }
}