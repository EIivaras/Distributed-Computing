import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TFramedTransport;


// NOTE: No matter what the backend does, the client should not see any exceptions whatsoever
// Also, pay attention to what should be throwing exceptions in the slide deck

// To "win", going to have to implement a load balancer and deal with clients offering passwords in large batches

// The system must support "on-the-fly" addition of backend nodes
// --> The FE node should perform ALL COMPUTATIONS LOCALLY if there are NO BACKEND NODES
// --> If I then add four backend nodes, TP should increase dramatically

// The maximum TP with twenty client threads, one FE and four BEs should be close to
// W/D cryptographic operations per seconds, especially for larger R (such as R = 10)
// --> W = # of worker threads
// --> D = time required to perform one cryptograph operation on one core using logRounds = R
// --> R = logRounds parameter
// The range of values that we're looking at is somewhere between 75% and 90% of optimal

// The latency at the client should be close to D when clients issue requests for one password at a time (i.e. input list has size = one)
// However, they will only measure our TP so don't worry too much about latency
// That said, TP and latency are positively correlated

// NOTES ON WORKLOADS:
/*
	The grading script will mostly test our solution against 3 workloads:
	a) 1 client thread, 16 pswds per requests, logRounds from 8 to 12
	b) 16 client threads, 1 pswd per request, logRounds from 8 to 12
	c) 4 client threads, 4 pswds per request, logRounds from 8 to 12

	Each will probably require a different execution strategy.
	For a), divide and conquer is the best. Take the batch of 16, divide into pieces, and feed these pieces to diff server processes, combine results and send back to client.
		--> This will be the hardest one, expect towards the lower end of the "optimal" range (75%)
	For b), no divide and conquer because batches are side 1. Really important to have multi-threaded servers that can process requests in parallel.
		--> Should be towards the upper end of the optimal range (90%)
	c) is a compromise between the two.
*/

// GETTING STARTED: Check the slides for a step by step of how to get started.

public class FENode {
    static Logger log;

    public static void main(String [] args) throws Exception {
		if (args.length != 1) {
			System.err.println("Usage: java FENode FE_port");
			System.exit(-1);
		}

		// initialize log4j
		BasicConfigurator.configure();
		log = Logger.getLogger(FENode.class.getName());

		int portFE = Integer.parseInt(args[0]);
		log.info("Launching FE node on port " + portFE);

		// launch Thrift server
		// Create a processor to handler RPC calls
		BcryptService.Processor processor = new BcryptService.Processor<BcryptService.Iface>(new BcryptServiceHandler());
		// Create a socket
		TServerSocket socket = new TServerSocket(portFE);
		// Attach socket to server so that server accepts requests incoming on this socket
		TSimpleServer.Args sargs = new TSimpleServer.Args(socket);
		// Set server arguments
		sargs.protocolFactory(new TBinaryProtocol.Factory());
		sargs.transportFactory(new TFramedTransport.Factory());
		sargs.processorFactory(new TProcessorFactory(processor));
		// Run the server, it can now accept RPC calls
		TSimpleServer server = new TSimpleServer(sargs);
		server.serve();
    }
}
