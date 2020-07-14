import java.util.*;
import java.util.concurrent.*;

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
- To implement replication, it's important that each storage node knows if it is the primary or the backup
    - Can do this in a way similar to how the client figures out the primary
--> I do not believe this can be done at the node startup, as new could be added/taken away at different times
--> To this end, do we do it on getting a get/put request?
--> It should be noted that unlike in A1, where the backend nodes would contact the frontend, the backend nodes are being directly talked to by the clients
--> However, it is only the primary that is being contacted AFAIK
*/


public class KeyValueHandler implements KeyValueService.Iface {
    private Map<String, String> myMap;
    private CuratorFramework curClient;
    private String zkNode;
    private String host;
    private int port;

    public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode) {
        this.host = host;
        this.port = port;
        this.curClient = curClient;
        this.zkNode = zkNode;
        myMap = new ConcurrentHashMap<String, String>();	
    }

    public void connect(String host, int port) {
        System.out.println(String.format("Received connection from %s:%d", host, port));
    }

    private boolean amIPrimary() throws Exception {
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
                    return true;                    
                }
                
                return false;
			} catch (KeeperException.NoNodeException e) {
                // The primary we are trying to reference doesn't exist (it was removed)
                // Therefore, I (this node) am the only running node, so I must be the primary
                return true;
			}
		}
    }

    public String get(String key) throws org.apache.thrift.TException
    {	
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
        try {
            if (amIPrimary()) {
                myMap.put(key, value);
            } else {
                throw new TException("ERROR: Not the primary!");
            }
        } catch (Exception e) {
            System.out.println("Exception!");
            System.out.println(e.getMessage());
        }
    }
}
