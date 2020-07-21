import java.io.*;
import java.util.*;

import org.apache.thrift.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;
import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;
import org.apache.curator.utils.*;

import org.apache.log4j.*;

public class StorageNode {
    static Logger log;

    public static void main(String [] args) throws Exception {
		BasicConfigurator.configure();
		log = Logger.getLogger(StorageNode.class.getName());

		if (args.length != 4) {
			System.err.println("Usage: java StorageNode host port zkconnectstring zknode");
			System.exit(-1);
		}

		CuratorFramework curClient =
			CuratorFrameworkFactory.builder()
			.connectString(args[2])
			.retryPolicy(new RetryNTimes(10, 1000))
			.connectionTimeoutMs(1000)
			.sessionTimeoutMs(500)
			.build();

		curClient.start();
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				curClient.close();
			}
			});

		KeyValueService.Processor<KeyValueService.Iface> processor = new KeyValueService.Processor<>(new KeyValueHandler(args[0], Integer.parseInt(args[1]), curClient, args[3]));
		TServerSocket socket = new TServerSocket(Integer.parseInt(args[1]));
		TThreadPoolServer.Args sargs = new TThreadPoolServer.Args(socket);
		sargs.protocolFactory(new TBinaryProtocol.Factory());
		sargs.transportFactory(new TFramedTransport.Factory());
		sargs.processorFactory(new TProcessorFactory(processor));
		sargs.maxWorkerThreads(64);
		TServer server = new TThreadPoolServer(sargs);
		log.info("Launching server");

		new Thread(new Runnable() {
			public void run() {
				server.serve();
			}
		}).start();

		// Create an ephemeral node in ZooKeeper
		// Child znode must store, as its data payload, a host:port string denoting the address of the server process
		String payload = String.format("%s:%s", args[0], args[1]);
		byte[] payloadInBytes = payload.getBytes();
		// args[3] = /zwalford
		// NOTE: When a child node is created, it's reference is stored in string form as "child000000000[someNumber]"
		String thisZNodePath = curClient.create()
								.withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
								.forPath(args[3] + "/child", payloadInBytes);
		// System.out.println("thisZnodePath: " + thisZNodePath);

		curClient.sync();
		List<String> children = curClient.getChildren().forPath(args[3]);
		Collections.sort(children);
		
		// Find duplicate znodes and remove them (expired znodes, queued to be deleted)
		for (String child : children) {
			byte[] data = curClient.getData().forPath(args[3] + "/" + child);
			String strData = new String(data);
			String childZNodePath = args[3] + "/" + child;

			if (strData.equals(payload) && !childZNodePath.equals(thisZNodePath)) {
				// System.out.println("duplicateZNodePath: " + childZNodePath);
				curClient.delete().forPath(childZNodePath);
			} 
		}

		curClient.sync();
		children = curClient.getChildren().forPath(args[3]);
		Collections.sort(children);

		TTransport transport = null;

		// Form connection between primary and backup StorageNodes
		for (String child : children) {
			try {
				byte[] data = curClient.getData().forPath(args[3] + "/" + child);
				String strData = new String(data);

				// all child nodes after me (in lexicographic order) cannot be the primary 
				if (strData.equals(payload)) break;
				// otherwise, I am the backup and child is the primary (if it hasn't crashed)
				String[] otherSplitData = strData.split(":");
				String otherHostName = otherSplitData[0];
				Integer otherPort = Integer.parseInt(otherSplitData[1]);

				TSocket sock = new TSocket(otherHostName, otherPort);
				transport = new TFramedTransport(sock);
				transport.open();
				TProtocol protocol = new TBinaryProtocol(transport);
				KeyValueService.Client client = new KeyValueService.Client(protocol);


				// connect the primary to me (new backup with host name args[0] and port # args[1])
				client.connect(args[0], Integer.parseInt(args[1]));

				transport.close();
			} catch (Exception e) {
				// The child node is still in the znode list but has crashed, so the connection fails
				if (!transport.equals(null)) {
					transport.close();
				}
			}
		}
	}
}
