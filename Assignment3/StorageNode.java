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
			.sessionTimeoutMs(10000)
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

		// TODO: create an ephemeral node in ZooKeeper
		// Child znode must store, as its data payload, a host:port string denoting the address of the server process
		
		String payload = String.format("%s:%s", args[0], args[1]);
		byte[] payloadInBytes = payload.getBytes();

		// args[3] = /zwalford
		// NOTE: When a child node is created, it's reference is stored in string form as "child000000000[someNumber]"
		curClient.create()
				.withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
				.forPath(args[3] + "/child", payloadInBytes);

		// On startup, should the one that isn't the primary talk to the one who is?
		// They'll probably need to send messages to each other
		curClient.sync();
		List<String> children = curClient.getChildren().forPath(args[3]);

		for (String child : children) {
			byte[] data = curClient.getData().forPath(args[3] + "/" + child);
			String strData = new String(data);

			// If the child of the parent I just got is NOT equal to me
			if (!strData.equals(payload)) {
				String[] otherSplitData = strData.split(":");
				String otherHostName = otherSplitData[0];
				Integer otherPort = Integer.parseInt(otherSplitData[1]);

				TSocket sock = new TSocket(otherHostName, otherPort);
				TTransport transport = new TFramedTransport(sock);
				transport.open();
				TProtocol protocol = new TBinaryProtocol(transport);
				KeyValueService.Client client = new KeyValueService.Client(protocol);

				client.connect(args[0], Integer.parseInt(args[1]));
				transport.close();
			}
		}
	}
}
