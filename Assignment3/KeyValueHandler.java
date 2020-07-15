import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.thrift.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;
import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;

/* NOTES:
- The reason we need concurrency control is because each storage node is implemented as a thread pool server
- This means that any incoming request will be serviced in its own thread -- we could have tons of threads all trying to get and put at the same time!
- Additionally, each thread needs to make sure after it puts data inside it's local map that it's also sending this data to the backup node!
--> Spinlocks are needed!

Things to consider:
- Do we send data to the backup after EACH put request?
*/


public class KeyValueHandler implements KeyValueService.Iface {
    private ReentrantLock lock = new ReentrantLock();
    private Map<String, String> myMap;
    private CuratorFramework curClient;
    private String zkNode;
    private String host;
    private int port;
    private KeyValueService.Client backupClient = null;
    private boolean amPrimary = false;

    public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode) {
        this.host = host;
        this.port = port;
        this.curClient = curClient;
        this.zkNode = zkNode;
        myMap = new ConcurrentHashMap<String, String>();	
    }

    public void connect(String host, int port) {
        System.out.println(String.format("Received connection from %s:%d", host, port));
        try {
            TSocket sock = new TSocket(host, port);
            TTransport transport = new TFramedTransport(sock);
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            this.backupClient = new KeyValueService.Client(protocol);

            this.backupClient.replicateData(this.myMap);

        } catch (Exception e) {
            System.out.println("ERROR: Failed to set up client to talk to other storage node.");
        }
    }

    public void replicateData(Map<String, String> dataMap) {
        //System.out.println("Received data replication request from primary.");
        this.myMap = dataMap;
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
        // TODO: Do we need any locking in here?
        try {
            if (amIPrimary()) {
                String ret = myMap.get(key);
                if (ret == null)
                    return "";
                else
                    return ret;
            } else {
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
                while(!this.lock.tryLock()) { } // Wait until I get a lock
                myMap.put(key, value);
                if (this.backupClient != null) {
                    this.backupClient.replicateData(this.myMap);
                } 
                this.lock.unlock();
            } else {
                throw new TException("ERROR: Not the primary!");
            }
        } catch (Exception e) {
            System.out.println("Exception!");
            System.out.println(e.getMessage());
        }
    }
}
