import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.thrift.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import org.apache.zookeeper.*;
import org.apache.curator.framework.*;

// TODO: Upon killing a primary or backup, client may see a get/put exception
// --> Are these the expected ones?

// TODO: TP Hovering around 21/22000 -- need to bump this up higher

// TODO: Need to look a bit further into our primary determination and MAKE SURE it's correct

// TODO: After going back and forth crashing the primary we get a "connection reset" exception

public class KeyValueHandler implements KeyValueService.Iface {
    private ReentrantLock lock = new ReentrantLock();
    private Map<String, String> myMap;
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

    public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode) {
        this.host = host;
        this.port = port;
        this.curClient = curClient;
        this.zkNode = zkNode;
        // // ConcurrentHashMap arguments: initial size, load factor (leave at 0.75),
        // //   concurrency level (determines how many writes can happen in parallel, could set to half number of A3Client threads, since there is 50/50 chance of each thread reading or writing)
        // myMap = new ConcurrentHashMap<String, String>(500,(float)0.75,4);
        myMap = new HashMap<String, String>();	
    }

    public void connect(String host, int port) {
        System.out.println(String.format("Received connection from %s:%d", host, port));
        while(!this.lock.tryLock()) { }
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
            this.connectedBackup = true;
        } catch (Exception e) {
            System.out.println("ERROR: Failed to set up client to talk to other storage node.");
        }
        this.lock.unlock();
    }

    public void replicateData(Map<String, String> dataMap) {
        //System.out.println("Received data replication request from primary.");
        while(!this.lock.tryLock()) { }
        this.myMap = dataMap;
        this.lock.unlock();
    }

    public void replicatePut(String key, String value) {
        while(!this.lock.tryLock()) { }
        this.myMap.put(key, value);
        this.lock.unlock();
    }

    private boolean amIPrimary() throws Exception {
        if (this.amPrimary) {
            return true;
        }

        curClient.sync(); // Sync the ZK cluster to make sure we get the newest data (is that needed for this?)
		List<String> children = this.curClient.getChildren().forPath(this.zkNode);

		if (children.size() == 0) {
			// Somehow there isn't a child, even though we just created one
            throw new Exception("ERROR: No children of parent znode found.");
		} else {
			Collections.sort(children);
			try {
				byte[] data = this.curClient.getData().forPath(this.zkNode + "/" + children.get(0));
				String strData = new String(data);
	
				if (strData.equals(String.format("%s:%s", this.host, this.port))) {
                    System.out.println("Am primary!");
                    this.amPrimary = true;
                    return true;                    
                }
                
                this.amPrimary = false;
                return false;
			} catch (KeeperException.NoNodeException e) {
                // The primary we are trying to reference doesn't exist (it was removed)
                // Therefore, I (this node) am the only running node, so I must be the primary
                this.amPrimary = true;
                return true;
			}
		}
    }

    public String get(String key) throws org.apache.thrift.TException
    {	
        // TODO: Do we /NEED/ locking here?
        // Found that it doesn't cause linearizability violation (could make piazza post)
        try {
            if (amIPrimary()) {
                //while (!this.lock.tryLock()) {}
                String ret = myMap.get(key);
                //this.lock.unlock();
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
            if (amIPrimary()) {
                if (this.connectedBackup) {
                    while(!this.lock.tryLock()) { } // Wait until I get a lock
                    myMap.put(key, value);
                    
                    Client client = null;
                    for (Client backupClient : backupClientList) {
                        if (!backupClient.busy) {
                            client = backupClient;
                            client.busy = true;
                            break;
                        }
                    } 
                    this.lock.unlock();
                    try {
                        client.backupClient.replicatePut(key, value);
                    } catch (Exception e) {
                        // Backup has died
                        this.connectedBackup = false;
                    }
                    client.busy = false;
                } else {
                    while(!this.lock.tryLock()) { } // Wait until I get a lock
                    myMap.put(key, value);
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
