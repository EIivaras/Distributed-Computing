import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.mindrot.jbcrypt.BCrypt;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;

/*
	In these methods, we should use similar methods as the client calling the FENode in Client.java for the FENode to call the backend nodes
*/

public class BcryptServiceHandler implements BcryptService.Iface {
	// TODO: Ensure this latch won't be overwritten if we're handling a bunch of requests at once?
	private static CountDownLatch latch = null; /* for synchronization between FENode and async requests on BE node  */
	private List<BackendNode> backendNodes = new ArrayList<BackendNode>(); /* backend nodes that are up & running */

	private class HashPassCallback implements AsyncMethodCallback<List<String>> {

		public List<String> resultList;
		public boolean hadError = false;

		public void onComplete(List<String> response) {
			this.resultList = response;
			this.hadError = false;
			
			latch.countDown();
		}
		public void onError(Exception e) {
			System.out.println("Callback onError method called. Exception:");
			System.out.println(e.getMessage());
			this.hadError = true;
			latch.countDown();
		}
	}

	private class CheckPassCallback implements AsyncMethodCallback<List<Boolean>> {

		public List<Boolean> resultList;
		public boolean hadError = false;

		public void onComplete (List<Boolean> response) {
			resultList = response;
			this.hadError = false;

			latch.countDown();
		}

		public void onError(Exception e) {
			System.out.println("Callback onError method called. Exception:");
			System.out.println(e.getMessage());
			this.hadError = true;
			latch.countDown();
		}
	}

	private class BackendNode {
		public Boolean bcryptClientActive;
		public Boolean isWorking;
		public String host;
		public int port;
		public BcryptService.AsyncClient bcryptClient;
		public TTransport transport;
		public HashPassCallback hashPassCallback;
		public CheckPassCallback checkPassCallback;

		public void setHostAndPort(String host, int port) {
			this.host = host;
			this.port = port;
			this.bcryptClientActive = false;
			this.isWorking = false;
		}

		public void setClientAndTransport(BcryptService.AsyncClient bcryptClient, TTransport transport) {
			try {
				this.bcryptClient = bcryptClient;
				this.transport = transport;
				// TODO: Ensure we don't need to open for async connections
				// this.transport.open(); 
				this.bcryptClientActive = true;
				this.hashPassCallback = new HashPassCallback();
				this.checkPassCallback = new CheckPassCallback();
			} catch (Exception e) {
				System.out.println("Failed to open transport for BackendNode.");
				this.bcryptClientActive = false;
			}
		}

		public void hashPassword(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException {
			try {
				System.out.println("Starting hashPassword at backend.");
				this.isWorking = true;
				this.bcryptClient.hashPasswordBE(password, logRounds, this.hashPassCallback);
				this.isWorking = false;
				System.out.println("Completed hashPassword at backend.");
			} catch (Exception e) {
				// TODO: hashPasswordBE throws an IllegalArgument like this, so can I do the same here?
				System.out.println("Failed to hash password at backend.");
				this.isWorking = false;
				throw new IllegalArgument(e.getMessage());
			}
		}

		public void checkPassword(List<String> password, List<String> hash) throws IllegalArgument, org.apache.thrift.TException {
			try {
				System.out.println("Starting checkPassword at backend.");
				this.isWorking = true;
				this.bcryptClient.checkPasswordBE(password, hash, this.checkPassCallback);
				this.isWorking = false;
				System.out.println("Completed checkPassword at backend.");
			} catch (Exception e) {
				System.out.println("Failed to check password at backend.");
				this.isWorking = false;
				throw new IllegalArgument(e.getMessage());
			}
		}

		public void closeTransport() {
			this.transport.close();
		}

		public List<String> getHashPassResults() {
			return this.hashPassCallback.resultList;
		}

		public List<Boolean> getCheckPassResults() {
			return this.checkPassCallback.resultList;
		}

		public Boolean checkHashPassErrors() {
			return this.hashPassCallback.hadError;
		}

		public Boolean checkCheckPassErrors() {
			return this.checkPassCallback.hadError;
		}
	}
	
	private BackendNode createBcryptClient(BackendNode backendNode) {

		// Make sure we're only initializing everything if the bcryptClient has not already been set up
		if (backendNode.bcryptClientActive) {
			return backendNode;
		}
		
		try {
			TNonblockingTransport transport = new TNonblockingSocket(backendNode.host, backendNode.port);
			TProtocolFactory pf = new TBinaryProtocol.Factory();
			TAsyncClientManager cm = new TAsyncClientManager();
			BcryptService.AsyncClient bcryptClient = new BcryptService.AsyncClient(pf, cm, transport);	

			backendNode.setClientAndTransport(bcryptClient, transport);	
		} catch (Exception e) {
			//TODO: handle exception
			System.out.println("Failed to setup async bcryptClient.");
		}

		return backendNode;
	}

	public void initializeBackend(String host, int port) throws IllegalArgument, org.apache.thrift.TException {
		try {
			BackendNode backendNode = new BackendNode();
			backendNode.setHostAndPort(host, port);
			
			backendNodes.add(backendNode);
		}
		catch (Exception e) {
			throw new IllegalArgument(e.getMessage());
		}
	}

    public List<String> hashPassword(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException
    {
		try {
			try {
				BackendNode backendNode = null;
				List<Integer> freeBEIndices = new ArrayList<>();
				List<BackendNode> usedBENodes = new ArrayList<>();
				List<String> result = new ArrayList<>();

				// Find indices of free (idle) backend nodes
				for (int i = 0; i < backendNodes.size(); i++) {
					if (!backendNodes.get(i).isWorking) {
						freeBEIndices.add(i);
					}
				}
				
				// if found one or more free backend nodes, split work evenly between them
				if (freeBEIndices.size() > 0) {
					// # of items to be processed by current node 
					int jobSize = password.size() / freeBEIndices.size();
					if (jobSize < 1) {
						// assign entire job (whole password list) to only one free BE node: latch initialized to 1 for "1 async RPC"
						// TODO: could change behavior below to assign job to less BE nodes, instead of only one
						latch = new CountDownLatch(1);

						usedBENodes.add(backendNodes.get(freeBEIndices.get(0)));
						backendNode = createBcryptClient(backendNodes.get(freeBEIndices.get(0)));
						backendNode.hashPassword(password, logRounds);
					} else {
						// split jobs (password list chunks) evenly between all free BE nodes (jobSize >= 1)
						// set latch (# of async RPCs) to # of free BE nodes 
						latch = new CountDownLatch(freeBEIndices.size());
						// # of items (passwords/hashes) being processed + # of items finished processing (ASSUMES ASYNC RPC) 
						int itemsProcessed = 0;
						
						int nodeNum = 1;
						for (int i : freeBEIndices){
							if (nodeNum == freeBEIndices.size()){
								// handle all remaining items in last job (executed by last freeBE available)
								jobSize = password.size() - itemsProcessed;
							}

							usedBENodes.add(backendNodes.get(i));
							// createBcryptClient() will create the bcryptClient of the node so it can handle requests if it is not created already
							backendNode = createBcryptClient(backendNodes.get(i));
							backendNode.hashPassword(password.subList(itemsProcessed, (itemsProcessed + jobSize)), logRounds);

							itemsProcessed += jobSize;
							nodeNum++;
						}

						latch.await();

						// TODO: If there was a callback error (can check via node.checkHashPassErrors()),
						// We need to change how insertions into this list work
						// --> Might need to do this process in a loop until we get all results correctly
						for (BackendNode node : usedBENodes) {
							result.addAll(node.getHashPassResults());
						}

						return result;
					} 
				}
			} catch (Exception e) {
				/* TODO: must be able to handle BE failures independently, callback onError() function should take care of this
					--> If 3 BE nodes are running and one crashes, the 2 other BE nodes must continue adding to result,
						 but does the FENode process the rest of the batch?
					     	Maybe the "result" array should be outside of this try-catch block
 				*/
				System.out.println(e.getMessage());
			}

			System.out.println("Starting hashPassword at frontend.");

			List<String> ret = new ArrayList<>();

			String passwordString = "";
			String hashString = "";

			for (int i = 0; i < password.size(); i++) {
				passwordString = password.get(i);
				hashString = BCrypt.hashpw(passwordString, BCrypt.gensalt(logRounds));
				ret.add(hashString);
			}

			System.out.println("Completed hashPassword at frontend.");
			
			return ret;
		} catch (Exception e) {
			System.out.println("Failed to hash password at frontend.");
			throw new IllegalArgument(e.getMessage());
		}
    }

    public List<Boolean> checkPassword(List<String> password, List<String> hash) throws IllegalArgument, org.apache.thrift.TException
    {
		try {
			try {
				BackendNode backendNode = null;
				List<Integer> freeBEIndices = new ArrayList<>();
				List<BackendNode> usedBENodes = new ArrayList<>();
				List<Boolean> result = new ArrayList<>();

				for (int i = 0; i < backendNodes.size(); i++) {
					if (!backendNodes.get(i).isWorking) {
						freeBEIndices.add(i);
					}
				}
				
				if (freeBEIndices.size() > 0) {
					int jobSize = password.size() / freeBEIndices.size();
					if (jobSize < 1) {

						latch = new CountDownLatch(1);

						usedBENodes.add(backendNodes.get(freeBEIndices.get(0)));
						backendNode = createBcryptClient(backendNodes.get(freeBEIndices.get(0)));
						backendNode.checkPassword(password, hash);
					} else {
						latch = new CountDownLatch(freeBEIndices.size());
						int itemsProcessed = 0;
						
						int nodeNum = 1;
						for (int i : freeBEIndices){
							if (nodeNum == freeBEIndices.size()){
								jobSize = password.size() - itemsProcessed;
							}

							usedBENodes.add(backendNodes.get(i));
							backendNode = createBcryptClient(backendNodes.get(i));
							backendNode.checkPassword(password.subList(itemsProcessed, (itemsProcessed + jobSize)), hash.subList(itemsProcessed, (itemsProcessed + jobSize)));

							itemsProcessed += jobSize;
							nodeNum++;
						}
					} 

					latch.await();

					// TODO: If there was a callback error (can check via node.checkCheckPassErrors()),
					// We need to change how insertions into this list work
					// --> Might need to do this process in a loop until we get all results correctly
					for (BackendNode node : usedBENodes) {
						result.addAll(node.getCheckPassResults());
					}

					return result;
				}
			} catch (Exception e) {
				/* TODO: must be able to handle BE failures independently, callback onError() function should take care of this
					--> If 3 BE nodes are running and one crashes, the 2 other BE nodes must continue adding to result,
						 but does the FENode process the rest of the batch?
					     	Maybe the "result" array should be outside of this try-catch block
 				*/
				System.out.println(e.getMessage());
			}

			System.out.println("Starting checkPassword at frontend.");

			List<Boolean> ret = new ArrayList<>();

			String passwordString = "";
			String hashString = "";

			for (int i = 0; i < password.size(); i++) {
				passwordString = password.get(i);
				hashString = hash.get(i);

				ret.add(BCrypt.checkpw(passwordString, hashString));
			}

			System.out.println("Completed checkPassword at frontend.");
			return ret;
		} catch (Exception e) {
			System.out.println("Failed to check password at frontend.");
			throw new IllegalArgument(e.getMessage());
		}
	}
	
	public List<String> hashPasswordBE(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException
    {
		try {
			List<String> ret = new ArrayList<>();

			String passwordString = "";
			String hashString = "";

			for (int i = 0; i < password.size(); i++) {
				passwordString = password.get(i);
				hashString = BCrypt.hashpw(passwordString, BCrypt.gensalt(logRounds));
				ret.add(hashString);
			}
			
			return ret;
		} catch (Exception e) {
			System.out.println("Error in hashPasswordBE: ");
			System.out.println(e.getMessage());
			throw new IllegalArgument(e.getMessage());
		}
    }

    public List<Boolean> checkPasswordBE(List<String> password, List<String> hash) throws IllegalArgument, org.apache.thrift.TException
    {
		try {
			List<Boolean> ret = new ArrayList<>();

			String passwordString = "";
			String hashString = "";

			for (int i = 0; i < password.size(); i++) {
				passwordString = password.get(i);
				hashString = hash.get(i);

				ret.add(BCrypt.checkpw(passwordString, hashString));
			}

			return ret;
		} catch (Exception e) {
			System.out.println("Error in checkPasswordBE: ");
			System.out.println(e.getMessage());
			throw new IllegalArgument(e.getMessage());
		}
    }
}
