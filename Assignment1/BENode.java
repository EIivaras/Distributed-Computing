import java.net.InetAddress;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportFactory;


public class BENode {
    static Logger log;

    public static void main(String [] args) throws Exception {
	if (args.length != 3) {
	    System.err.println("Usage: java BENode FE_host FE_port BE_port");
	    System.exit(-1);
	}

	// initialize log4j
	BasicConfigurator.configure();
	log = Logger.getLogger(BENode.class.getName());

	String hostFE = args[0];
	int portFE = Integer.parseInt(args[1]);
	int portBE = Integer.parseInt(args[2]);
	log.info("Launching BE node on port " + portBE + " at host " + getHostName());

	// Setup so that we can talk to front end
	TSocket sock = new TSocket(hostFE, portFE);
	TTransport transport = new TFramedTransport(sock);
	TProtocol protocol = new TBinaryProtocol(transport);
	BcryptService.Client client = new BcryptService.Client(protocol);
	transport.open();

	// Talk to front end

	client.initializeBackend(hostFE, portBE);
	transport.close();

	// launch Thrift server
	BcryptService.Processor processor = new BcryptService.Processor<BcryptService.Iface>(new BcryptServiceHandler());
	TServerSocket socket = new TServerSocket(portBE);
	TSimpleServer.Args sargs = new TSimpleServer.Args(socket);
	sargs.protocolFactory(new TBinaryProtocol.Factory());
	sargs.transportFactory(new TFramedTransport.Factory());
	sargs.processorFactory(new TProcessorFactory(processor));
	//sargs.maxWorkerThreads(64);
	TSimpleServer server = new TSimpleServer(sargs);
	server.serve();

	// TODO: Contact FE Node on it's socket and tell it that we are here and ready to act
    }

    static String getHostName()
    {
	try {
	    return InetAddress.getLocalHost().getHostName();
	} catch (Exception e) {
	    return "localhost";
	}
    }
}
