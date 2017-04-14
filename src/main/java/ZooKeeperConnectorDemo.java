import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

/**
 * Created by neyao on 2017/4/13.
 */
public class ZooKeeperConnectorDemo {

    ZooKeeper zookeeper;
    java.util.concurrent.CountDownLatch connectedSignal = new java.util.concurrent.CountDownLatch(1);


    public void connect(String host) throws IOException, InterruptedException {
        zookeeper = new ZooKeeper(host, 5000,
                new Watcher() {
                    public void process(WatchedEvent event) {
                        if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                            connectedSignal.countDown();
                        }
                    }
                });
        connectedSignal.await();
    }

    public void close() throws InterruptedException {
        zookeeper.close();
    }

    public ZooKeeper getZooKeeper() {
        if (zookeeper == null || !zookeeper.getState().equals(ZooKeeper.States.CONNECTED)) {
            throw new IllegalStateException("ZooKeeper is not connected.");
        }
        return zookeeper;
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {

        ZooKeeperConnectorDemo connector = new ZooKeeperConnectorDemo();
        connector.connect("192.168.0.200:30011");
        ZooKeeper zk = connector.getZooKeeper();


        byte[] bytes = zk.getData("/mytest/data1", false, null);
        String s = new String(bytes);
        System.out.println("data for /mytest/data1:" + s);
        System.out.println();

//        zk.create("/mytest/data2", "data2_values".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//        bytes = zk.getData("/mytest/data2", false, null);
//        s = new String(bytes);
//        System.out.println("data for /mytest/data2:" + s);

        Stat stat = zk.setData("/mytest/data2", "testAAAA".getBytes(), -1);
        bytes = zk.getData("/mytest/data2", false, null);
        System.out.println("set data done, stat: "+ stat.getVersion());
        s = new String(bytes);
        System.out.println("data for /mytest/data2:" + s);
        System.out.println();


    }


}
