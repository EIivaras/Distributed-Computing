import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.thrift.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import org.apache.zookeeper.*;
import org.apache.curator.framework.*;
import org.apache.curator.framework.api.*;

// TODO: After going back and forth crashing the primary we get a "connection reset" exception
// This happens on the BACKUP after the PRIMARY is killed
// --> This sounds like the backup trying to ACK the primary's message but failing to b/c the connection is reset -- there is NO WAY to catch this though!

public class KeyValueHandler implements KeyValueService.Iface, CuratorWatcher {
    private ReentrantLock lock = new ReentrantLock(true);
    private Map<String, String> myMap;
    private ConcurrentHashMap<String, ReentrantLock> lockMap;
    private CuratorFramework curClient;
    private String zkNode;
    private String host;
    private int port;
    private boolean amPrimary = false;
    private List<Client> backupClientList = null;
    private int maxThreads = 8;
    private boolean connectedBackup = false;

    private class Client {
        public KeyValueService.Client backupClient;
        public boolean busy = false;

        public Client(KeyValueService.Client client) {
            this.backupClient = client;
        }
    }

    public void process (WatchedEvent event) {
        System.out.println("ZooKeeper event " + event);
        this.amPrimary = this.amIPrimary();
    }

    public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode) {
        this.host = host;
        this.port = port;
        this.curClient = curClient;
        this.zkNode = zkNode;
        // // ConcurrentHashMap arguments: initial size, load factor (leave at 0.75),
        // //   concurrency level (determines how many writes can happen in parallel, could set to half number of A3Client threads, since there is 50/50 chance of each thread reading or writing)
        // myMap = new ConcurrentHashMap<String, String>(500,(float)0.75,4);
        this.myMap = new HashMap<String, String>();	
        this.lockMap = new ConcurrentHashMap<String, ReentrantLock>();

        this.amPrimary = this.amIPrimary();
    }

    public void connect(String host, int port) {
        System.out.println(String.format("Received connection from %s:%d", host, port));
        while(!this.lock.tryLock()) { }
        this.connectedBackup = true;
        this.backupClientList = new ArrayList<Client>();
        try {
            for (int i = 0; i < maxThreads; i++) {
                TSocket sock = new TSocket(host, port);
                TTransport transport = new TFramedTransport(sock);
                transport.open();
                TProtocol protocol = new TBinaryProtocol(transport);
                KeyValueService.Client backupClient = new KeyValueService.Client(protocol);
                this.backupClientList.add(new Client(backupClient));
    
                if (i == 0) {
                    backupClient.replicateData(this.myMap);
                }
            }
        } catch (Exception e) {
            System.out.println("ERROR: Failed to set up client to talk to other storage node.");
            System.out.println(e.getMessage());
        } finally {
            this.lock.unlock();
        }
    }

    public void replicateData(Map<String, String> dataMap) {
        System.out.println("Received data replication request from primary.");
        while(!this.lock.tryLock()) { }
        Map<String, String> tempMap = new HashMap<String, String>(this.myMap);
        this.myMap = dataMap;

        // Covers for issues where a put request gets to the backup before the replication request does
        if (tempMap.size() > 0) {
            System.out.println("Put request received before replication!");
            for(Map.Entry<String, String> entry : tempMap.entrySet()) {
                this.myMap.put(entry.getKey(), entry.getValue());
            }
        }

        this.lock.unlock();
        System.out.println("Data replication complete");
    }

    public void replicatePut(String key, String value) {
        while(!this.lock.tryLock()) { }
        this.myMap.put(key, value);
        this.lock.unlock();
    }

    private boolean amIPrimary() {
        try {
            curClient.sync();
            List<String> children = this.curClient.getChildren().usingWatcher(this).forPath(this.zkNode);

            if (children.size() == 0) {
                // No other znodes created yet, am primary.
                return true;
            }

            Collections.sort(children);
            byte[] data = this.curClient.getData().forPath(this.zkNode + "/" + children.get(0));
            String strData = new String(data);

            if (strData.equals(String.format("%s:%s", this.host, this.port))) {
                System.out.println("Am primary!");
                return true;                    
            }
            
            System.out.println("Not the primary");
            return false;
        } catch (KeeperException.NoNodeException e) {
            // The primary we are trying to reference doesn't exist (it was removed)
            // Therefore, I (this node) am the only running node, so I must be the primary
            System.out.println("Primary down, I am now the primary!");
            return true;
        } catch (Exception e) {
            System.out.println("Exception caught:");
            System.out.println(e.getMessage());
            return true;
        }
    }

    public String get(String key) throws org.apache.thrift.TException
    {	
        // TODO: Do we /NEED/ locking here?
        // Found that it doesn't cause linearizability violation (could make piazza post)
        try {
            if (this.amPrimary) {
                while (!this.lock.tryLock()) {}
                String ret = this.myMap.get(key);
                this.lock.unlock();
                if (ret == null)
                    return "";
                else
                    return ret;
            } else {
                System.out.println("Throwing not the primary exception!");
                throw new TException("ERROR: Not the primary!");
            }
        } catch (Exception e) {
            System.out.println("Exception!");
            System.out.println(e.getMessage());
        }
        return "";
    }

    public void put(String key, String value) throws org.apache.thrift.TException
    {
        // TODO: Sending data after every request (and locking) is causing TP to tank
        // --> Maybe we don't do it after every new entry?
        try {
            if (this.amPrimary) {
                // TODO: Look into putting these if statements WITHIN the lock instead of outside of it, which would completely prevent the concurrency isseu
                // It's technically possible that it could still happen, although the chance is extremely slim!

                while(!this.lock.tryLock()) { } // Wait until I get a lock
                this.myMap.put(key, value);
                
                if (this.connectedBackup) {
                    Client client = null;
                    for (Client backupClient : backupClientList) {
                        if (!backupClient.busy) {
                            client = backupClient;
                            client.busy = true;
                            break;
                        }
                    } 

                    if (!this.lockMap.containsKey(key)) {
                        this.lockMap.put(key, new ReentrantLock(true));
                    }

                    this.lock.unlock();

                    while (!this.lockMap.get(key).tryLock()) { }
                    try {
                        client.backupClient.replicatePut(key, value);
                    } catch (Exception e) {
                        // Backup has died
                        System.out.println("Exception! Backup dead when trying to send data:");
                        System.out.println(e.getMessage());
                        this.connectedBackup = false;
                    } finally {
                        this.lockMap.get(key).unlock();
                        client.busy = false;
                    }
                } else {
                    this.lock.unlock();
                }
            } else {
                System.out.println("Throwing not the primary exception!");
                throw new TException("ERROR: Not the primary!");
            }
        } catch (Exception e) {
            System.out.println("Exception!");
            System.out.println(e.getMessage());
        }
    }
}
