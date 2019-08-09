import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

import org.apache.thrift.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;
import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.utils.*;

import com.google.common.util.concurrent.Striped;

public class KeyValueHandler implements KeyValueService.Iface {
    private static final Striped<ReadWriteLock> stripedReadWriteLocks = Striped.readWriteLock(64);
    private static final ReentrantLock reentrantLock = new ReentrantLock(true);
    private Map<String, String> myMap;
    private CuratorFramework curClient;
    private String zkNode;
    private PathChildrenCache cache;
    private ClientManager cm;
    private String host;
    private int port;
    private String serverAddress;
    private volatile boolean isPrimary = false;
    private volatile boolean isBackupAvailable = false;

    public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode, PathChildrenCache cache) {
        this.myMap = new ConcurrentHashMap<String, String>();
        this.curClient = curClient;
        this.zkNode = zkNode;
        this.cache = cache;
        this.cm = new ClientManager();
        this.host = host;
        this.port = port;
        this.serverAddress = this.host + ":" + this.port;

        this.addListener(this.cache);
    }

    public String get(String key) throws org.apache.thrift.TException {
        if (!this.isPrimary) {
            throw new org.apache.thrift.TException("This is backup server!");
        }

        Lock readLock = stripedReadWriteLocks.get(key).readLock();
        readLock.lock();

        try {
            String ret = this.myMap.get(key);

            if (ret == null)
                return "";
            else
                return ret;
        } finally {
            readLock.unlock();
        }
    }

    public void put(String key, String value) throws org.apache.thrift.TException {
        if (!this.isPrimary) {
            throw new org.apache.thrift.TException("This is backup server!");
        }

        Lock writeLock = stripedReadWriteLocks.get(key).writeLock();
        writeLock.lock();

        try {
            while (reentrantLock.isLocked()) {
            }

            this.myMap.put(key, value);

            if (this.isBackupAvailable) {
                KeyValueService.Client client = this.cm.poll();

                try {
                    client.putOnBackupServer(key, value);
                } catch (Exception e) {
                    this.isBackupAvailable = false;
                }

                this.cm.offer(client);
            }
        } finally {
            writeLock.unlock();
        }
    }

    public void putOnBackupServer(String key, String value) throws org.apache.thrift.TException {
        Lock writeLock = stripedReadWriteLocks.get(key).writeLock();
        writeLock.lock();

        try {
            this.myMap.put(key, value);
        } finally {
            writeLock.unlock();
        }
    }

    public void syncToBackupServer(Map<String, String> primaryMap) throws org.apache.thrift.TException {
        this.myMap = new ConcurrentHashMap<>(primaryMap);
    }

    private void addListener(PathChildrenCache cache) {
        PathChildrenCacheListener listener = new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                switch (event.getType()) {
                case CHILD_ADDED:
                    syncServers();

                    break;
                case CHILD_REMOVED:
                    syncServers();

                    break;
                case CONNECTION_RECONNECTED:
                    cache.rebuild();

                    break;
                default:
                    break;
                }
            }
        };

        cache.getListenable().addListener(listener);
    }

    private void syncServers() {
        try {
            this.curClient.sync();
            List<String> children = this.curClient.getChildren().forPath(this.zkNode);
            Collections.sort(children);

            if (children.size() == 1) {
                this.isPrimary = true;
            } else {
                byte[] backupData = this.curClient.getData()
                        .forPath(this.zkNode + "/" + children.get(children.size() - 1));

                String backupServerAddress = new String(backupData);

                this.isPrimary = !backupServerAddress.equals(this.serverAddress);

                if (this.isPrimary) {
                    reentrantLock.lock();

                    if (!this.isBackupAvailable) {
                        String[] backupServerInfo = backupServerAddress.split(":");
                        String backupServerHost = backupServerInfo[0];
                        int backupServerPort = Integer.parseInt(backupServerInfo[1]);

                        this.cm.setServerAddress(backupServerHost, backupServerPort);

                        KeyValueService.Client client = null;

                        while (client == null) {
                            client = this.cm.createThriftClient();
                        }

                        try {
                            client.syncToBackupServer(this.myMap);
                            this.cm.initialize(client);
                            isBackupAvailable = true;
                        } catch (Exception e) {
                            isBackupAvailable = false;
                        }
                    }

                    reentrantLock.unlock();
                }
            }
        } catch (Exception e) {
            System.out.println("Unable to sync servers!");
        }
    }
}
