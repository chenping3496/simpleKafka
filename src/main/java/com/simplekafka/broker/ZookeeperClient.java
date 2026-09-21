package com.simplekafka.broker;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ZookeeperClient implements Watcher {
    private static final Logger logger = Logger.getLogger(ZookeeperClient.class.getName());
    private static final int SESSION_TIMEOUT = 30000;

    private final String host;
    private final int port;
    private ZooKeeper zooKeeper;
    private CountDownLatch connectedSignal = new CountDownLatch(1); //等待就绪



    public ZookeeperClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void connect() throws IOException,InterruptedException {
        zooKeeper = new ZooKeeper(getConnectString(), SESSION_TIMEOUT, this);
        connectedSignal.await();

        createPath("/brokers");
        createPath("/topics");
        createPath("/controller");
    }

    public String getConnectString() { return host + ":" + port; }

    public void close() throws InterruptedException { /* zooKeeper != null 则 close */
        if(zooKeeper != null) {
            zooKeeper.close();
        }
    }


    private void createPath(String path) {
        try {
            // 1. 边界条件处理：如果路径是根节点 "/"，则直接返回，因为根节点必然存在，无需创建。
            if (path.equals("/")) return;

            // 2. 找到当前路径中最后一个斜杠 "/" 的索引。
            // 例如：path = "/a/b/c"，则 lastSlash = 4（即 'b' 和 'c' 之间的 '/' 的位置）
            int lastSlash = path.lastIndexOf('/');

            // 3. 递归创建父路径。
            // lastSlash > 0 保证了父路径不是根目录 "/"（如果 path="/a"，lastSlash=0，就不需要再递归找父级了）
            if (lastSlash > 0) {
                // 截取从 0 到 lastSlash 的子串作为父路径。
                // 例如：path="/a/b/c" -> 截取出 "/a/b"，然后递归调用 createPath("/a/b")
                createPath(path.substring(0, lastSlash));
            }
            // 4. 检查当前路径是否已经存在。
            // zooKeeper.exists(path, false)：
            // 第一个参数是要检查的路径；
            // 第二个参数 false 表示不需要在这个节点上设置 Watcher（监听器）。
            // 如果返回 null，说明该节点不存在。
            if (zooKeeper.exists(path, false) == null) {

                // 5. 节点不存在，则创建该节点。
                // zooKeeper.create(path, data, acl, mode) 四个参数含义：
                // path: 要创建的节点路径
                // new byte[0]: 节点存储的数据，这里是空字节数组（即不存具体数据，仅作为目录使用）
                // ZooDefs.Ids.OPEN_ACL_UNSAFE: ACL 权限控制，表示完全开放，任何客户端都可以对该节点进行任何操作
                // CreateMode.PERSISTENT: 节点类型，表示持久化节点（不会因为客户端断开连接而被自动删除）
                zooKeeper.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                // 6. 记录创建成功的日志
                logger.info("Created ZooKeeper path: " + path);
            }
            // 如果 exists 发现节点已经存在，则什么都不做，直接跳过（保证了幂等性）

        } catch (Exception e) {
            // 7. 异常处理：捕获所有可能发生的异常（如连接丢失、节点已存在并发冲突等），记录警告日志，避免程序因异常中断。
            logger.log(Level.WARNING, "Failed to create path: " + path, e);
        }
    }


    //建立临时节点  监听各个broker的存活情况，是否需要重新竞争controller
    public boolean createEphemeralNode(String path, String data) throws KeeperException, InterruptedException {
        if (zooKeeper.exists(path, false) == null) {
            zooKeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            logger.info("Created ephemeral node: " + path);
            return true;          // 我抢到了
        }
        logger.info("Ephemeral node already exists: " + path);
        return false;             // 别人已经占了 → 我落选，去 watch 它
    }

    //建立持久节点，用于断电后恢复
    public void createPersistentNode(String path, String data) throws KeeperException, InterruptedException {
        // 逻辑：exists 为 null → create(PERSISTENT)；存在 → setData(path, data.getBytes(), -1)
        if( zooKeeper.exists(path, false) == null) {
            zooKeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            logger.info("Created persistent node: " + path);
        } else {
            zooKeeper.setData(path, data.getBytes(), -1);//   -1 表示"不管版本号，直接覆盖"
            logger.info("Updated persistent node: " + path);
        }
    }

    public boolean exists(String path) throws KeeperException, InterruptedException {
        // 逻辑：zooKeeper.exists(path, false) != null
        return zooKeeper.exists(path, false) != null;
    }

    public String getData(String path) throws KeeperException, InterruptedException {
        // 逻辑：byte[] data = zooKeeper.getData(path, false, null); return new String(data);
        byte[] data = zooKeeper.getData(path, false, null);
        //   注意 new String —— 第 2 课的教训，byte[] 永远不能 toString()
        return new String(data);
    }

    public void setData(String path, String data) throws KeeperException, InterruptedException {
        // 逻辑：zooKeeper.setData(path, data.getBytes(), -1)
        zooKeeper.setData(path, data.getBytes(), -1);
    }

    public List<String> getChildren(String path) throws KeeperException, InterruptedException {
        // 逻辑：try { return zooKeeper.getChildren(path, false); }
        try {
            return zooKeeper.getChildren(path, false);
        } catch (KeeperException.NoNodeException e) {
            return new ArrayList<>();
        }
        //      catch (KeeperException.NoNodeException e) { return new ArrayList<>(); }
    }

    public void deleteNode(String path) throws KeeperException, InterruptedException {
        // 逻辑：exists 才 delete(path, -1)
        if (zooKeeper.exists(path, false) != null) {
            zooKeeper.delete(path, -1);
        }
    }

    public void watchChildren(String path, ChildrenCallback callback) {
        try {
            List<String> children = zooKeeper.getChildren(path, event -> {
                // ↑ 第二个参数就是 watch：子节点变化时 ZK 调这个 lambda
                if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                    try {
                        List<String> newChildren = zooKeeper.getChildren(path, event2 -> {
                            if (event2.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                                watchChildren(path, callback);   // ★ 递归续期
                            }
                        });
                        callback.onChildrenChanged(newChildren); // 通知业务层
                    } catch (Exception e) {
                        logger.log(Level.SEVERE, "Failed to watch children", e);
                    }
                }
            });
            callback.onChildrenChanged(children);  // 首次调用：立刻回一份当前列表
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void watchNode(String path, NodeCallback callback) throws InterruptedException, KeeperException {
        // 逻辑：zooKeeper.exists(path, event -> {
        //          if (event.getType() 是 NodeDeleted / NodeDataChanged / NodeCreated 之一)
        //              callback.onNodeChanged();
        //      });
        //      用 exists 设 watch（存在性 watch），controller 选举靠它

        zooKeeper.exists(path, event -> {
            if (event.getType() == Watcher.Event.EventType.NodeDeleted
                    || event.getType() == Watcher.Event.EventType.NodeDataChanged
                    || event.getType() == Watcher.Event.EventType.NodeCreated) {
                try {
                    zooKeeper.exists(path, event2 -> {
                        if (event2.getType() == Watcher.Event.EventType.NodeDeleted
                                || event2.getType() == Watcher.Event.EventType.NodeDataChanged
                                || event2.getType() == Watcher.Event.EventType.NodeCreated) {
                            try {
                                watchNode(path, callback);   // ★ 递归续期
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            } catch (KeeperException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    });
                    callback.onNodeChanged();
                } catch (KeeperException e) {
                    throw new RuntimeException(e);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        );
    }

    @Override
    public void process(WatchedEvent event) {
        // 逻辑：SyncConnected → connectedSignal.countDown()
        //      Disconnected → 打日志
        //      Expired → 关旧的、new 一个新 latch、重连、await（照 connect() 的样子）
        if( event.getState() == Watcher.Event.KeeperState.SyncConnected) {
            connectedSignal.countDown();
        } else if (event.getState() == Watcher.Event.KeeperState.Disconnected) {
            logger.warning("Disconnected from Zookeeper");
        } else if (event.getState() == Watcher.Event.KeeperState.Expired) {
            logger.warning("SessionExpired");
            try {
                if(zooKeeper != null)zooKeeper.close();
                connectedSignal = new CountDownLatch(1);
                connect();
            } catch (Exception e) {
                logger.log(Level.SEVERE,"Unable to reconnect to Zookeeper", e);
            }
        }
    }

    public interface ChildrenCallback { void onChildrenChanged(List<String> children); }
    public interface NodeCallback { void onNodeChanged(); }


}
