import java.net.ConnectException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.mindrot.jbcrypt.BCrypt;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TTransport;

public class BcryptServiceHandler implements BcryptService.Iface {

	// TODO: This needs to be a synchronized list for concurrency, and we also need to wrap every instance of interation over this list in a synchronized block
	private List<BackendNode> backendNodes = new ArrayList<BackendNode>(); /* backend nodes that are up & running */

	private class HashPassCallback implements AsyncMethodCallback<List<String>> {

		public CountDownLatch countDownLatch = null;
		public List<String> resultList;
		public boolean hadError = false;
		public boolean isWorking = false;

		public void onComplete(List<String> response) {
			System.out.println("Completed hashPassword at backend.");
			this.resultList = response;
			this.hadError = false;
			this.isWorking = false;
			
			countDownLatch.countDown();
		}
		public void onError(Exception e) {
			System.out.println("Callback onError method called for HashPassCallback. Exception:");
			System.out.println(e.getMessage());
			this.hadError = true;
			this.isWorking = false;
			countDownLatch.countDown();
		}
	}

	private class CheckPassCallback implements AsyncMethodCallback<List<Boolean>> {

		public CountDownLatch countDownLatch = null;
		public List<Boolean> resultList;
		public boolean hadError = false;
		public boolean isWorking = false;

		public void onComplete (List<Boolean> response) {
			System.out.println("Completed checkPassword at backend.");
			resultList = response;
			this.hadError = false;
			this.isWorking = false;

			countDownLatch.countDown();
		}

		public void onError(Exception e) {
			System.out.println("Callback onError method called for CheckPassCallback. Exception:");
			System.out.println(e.getMessage());
			this.hadError = true;
			this.isWorking = false;
			countDownLatch.countDown();
		}
	}

	private class BackendNode {
		public String host;
		public int port;

		public BcryptService.AsyncClient bcryptClient;
		public Boolean bcryptClientActive;
		public Boolean inError;
		public TTransport transport;
		public HashPassCallback hashPassCallback;
		public CheckPassCallback checkPassCallback;

		public int jobStartIndex; /* inclusive start index of input chunk (job) assigned to self */
		public int jobEndIndex; /* exclusive end index of input chunk (job) assigned to self */

		// Constructor: 
		// sets front-end host (hostFE) to communicate with, port used by self (portBE), and callbacks
		public BackendNode(String hostFE, int portBE) {
			this.host = hostFE;
			this.port = portBE;
			this.bcryptClientActive = false;
			this.inError = false;
			this.hashPassCallback = new HashPassCallback();
			this.checkPassCallback = new CheckPassCallback();
			this.jobStartIndex = 0;
			this.jobEndIndex = 0;
		}

		public void setClientAndTransport(BcryptService.AsyncClient bcryptClient, TTransport transport) {
			this.bcryptClient = bcryptClient;
			this.transport = transport;
			this.bcryptClientActive = true;
		}

		public void hashPassword(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException {
			System.out.println("Starting hashPassword at backend.");
			this.hashPassCallback.isWorking = true;
			this.bcryptClient.hashPasswordBE(password, logRounds, this.hashPassCallback);
		}

		public void checkPassword(List<String> password, List<String> hash) throws IllegalArgument, org.apache.thrift.TException {
			System.out.println("Starting checkPassword at backend.");
			this.checkPassCallback.isWorking = true;
			this.bcryptClient.checkPasswordBE(password, hash, this.checkPassCallback);
		}

		public boolean isBcryptClientActive() {
			return this.bcryptClientActive;
		}

		public boolean isBcryptClientInError() {
			return this.inError;
		}
 
		public void bcryptClientInError() {
			this.inError = true;
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

		public void setHashPassLatch(CountDownLatch countDownLatch) {
			this.hashPassCallback.countDownLatch = countDownLatch;
		}

		public void setCheckPassLatch(CountDownLatch countDownLatch) {
			this.checkPassCallback.countDownLatch = countDownLatch;
		}

		public boolean isWorking() {
			return (this.hashPassCallback.isWorking || this.checkPassCallback.isWorking);
		}
	}
	
	private void initializeBcryptClient(BackendNode backendNode) {

		// Make sure we're only initializing everything if the bcryptClient has not already been set up
		if (backendNode.isBcryptClientActive()) {
			return;
		}

		try {
			TNonblockingTransport transport = new TNonblockingSocket(backendNode.host, backendNode.port);
			TProtocolFactory pf = new TBinaryProtocol.Factory();
			TAsyncClientManager cm = new TAsyncClientManager();
			BcryptService.AsyncClient bcryptClient = new BcryptService.AsyncClient(pf, cm, transport);	

			backendNode.setClientAndTransport(bcryptClient, transport);	

			return;
		}  catch (Exception e) {
			System.out.println("Failed to setup async bcryptClient:");
			System.out.println(e.getMessage());
			backendNode.bcryptClientInError();
			return;
		}
	}

	public void initializeBackend(String hostFE, int portBE) throws IllegalArgument, org.apache.thrift.TException {
		try {
			BackendNode backendNode = new BackendNode(hostFE, portBE);
			
			System.out.println("Backend node initialized.");
			backendNodes.add(backendNode);
		}
		catch (Exception e) {
			System.out.println("Error initializing backend.");
			throw new IllegalArgument(e.getMessage());
		}
	}

    public List<String> hashPassword(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException
    {
		// TODO: Make sure appropriate exceptions are thrown for invalid inputs
		// logRounds < 4, password empty --> throw invalid arg exception
		// Check what we have to do for running into a password that is ""
		
		List<String> result = new ArrayList<>();

		try {
			List<BackendNode> nodesForUse = new ArrayList<>();
			List<BackendNode> usedBENodes = new ArrayList<>();

			// Initialize Backend Nodes if not already initialized, and remove any who are disconnected or fail to initialize

			List<BackendNode> removalList = new ArrayList<BackendNode>();
			for (int i = 0; i < backendNodes.size(); i++) {
				initializeBcryptClient(backendNodes.get(i));
				if (backendNodes.get(i).isBcryptClientInError()) {
					removalList.add(backendNodes.get(i));
				}
			}

			backendNodes.removeAll(removalList);

			// Find indices of free (idle) backend nodes
			for (int i = 0; i < backendNodes.size(); i++) {
				if (!backendNodes.get(i).isWorking()) {
					nodesForUse.add(backendNodes.get(i));
				}
			}

			// if found one or more free backend nodes, split work evenly between them
			if (nodesForUse.size() > 0) {
				BackendNode backendNode = null;
				CountDownLatch countDownLatch = null;

				// # of items to be processed by current node 
				int jobSize = password.size() / nodesForUse.size();
		
				if (jobSize < 1) {
					// assign entire job (whole password list) to only one free BE node: latch initialized to 1 for "1 async RPC"
					usedBENodes.add(nodesForUse.get(0));
					backendNode = usedBENodes.get(0);
					countDownLatch = new CountDownLatch(1);
					backendNode.setHashPassLatch(countDownLatch);

					try {
						backendNode.hashPassword(password, logRounds);
					} catch (Exception e) {
						System.out.println("Error as backend node tried to hash password.");
						System.out.println(e.getMessage());
					}
					
					backendNode.jobStartIndex = 0;
					backendNode.jobEndIndex = jobSize;
				} else {
					// split jobs (password list chunks) evenly between all free BE nodes (jobSize >= 1)
					// set latch (# of async RPCs) to # of free BE nodes 
					countDownLatch = new CountDownLatch(nodesForUse.size());
					// # of items (passwords/hashes) being processed + # of items finished processing (ASSUMES ASYNC RPC) 
					int itemsProcessed = 0;
					
					int nodeNum = 1;
					for (BackendNode node : nodesForUse){
						if (nodeNum == nodesForUse.size()){
							// handle all remaining items in last job (executed by last freeBE available)
							jobSize = password.size() - itemsProcessed;
						}

						usedBENodes.add(node);
						// createBcryptClient() will create the bcryptClient of the node so it can handle requests if it is not created already
						backendNode = node;
						backendNode.setHashPassLatch(countDownLatch);
						try {
							backendNode.hashPassword(password.subList(itemsProcessed, (itemsProcessed + jobSize)), logRounds);
						} catch (Exception e) {
							System.out.println("Error as backend node tried to hash password.");
							System.out.println(e.getMessage());
						}
						backendNode.jobStartIndex = itemsProcessed;
						backendNode.jobEndIndex = itemsProcessed + jobSize;

						itemsProcessed += jobSize;
						nodeNum++;
					}
				} 

				countDownLatch.await();

				List<List<String>> resultLists = new ArrayList<List<String>>(usedBENodes.size());
				List<Integer> resultIndexesInError = new ArrayList<Integer>();
				List<Integer> jobStartIndexes = new ArrayList<Integer>();
				List<Integer> jobEndIndexes = new ArrayList<Integer>();

				int index = 0;
				for (BackendNode node : usedBENodes) {
					if (node.checkHashPassErrors().equals(true)) {
						node.bcryptClientInError();
						resultIndexesInError.add(index);
						jobStartIndexes.add(node.jobStartIndex);
						jobEndIndexes.add(node.jobEndIndex);

					} else {
						resultLists.add(index, node.getHashPassResults());
					}
					index++;
				}

				for (int i = 0; i < resultIndexesInError.size(); i++) {
					List<String> partialResult = hashPassword(password.subList(jobStartIndexes.get(i), jobEndIndexes.get(i)), logRounds);
					resultLists.add(resultIndexesInError.get(i), partialResult);	
				}

				for (List<String> list : resultLists) {
					result.addAll(list);
				}
			} else {
				// If no free BEs, do the work yourself

				System.out.println("No free BEs, doing hashPassword work at frontend.");

				for (int i = 0; i < password.size(); i++) {
					result.add(BCrypt.hashpw(password.get(i), BCrypt.gensalt(logRounds)));
				}

				System.out.println("Finished hashPassword work at frontend.");

			}
		} catch (Exception e) {
			System.out.println("Error in frontend hashPassword method:");
			System.out.println(e.getMessage());
		}

		return result;
    }

    public List<Boolean> checkPassword(List<String> password, List<String> hash) throws IllegalArgument, org.apache.thrift.TException
    {
		// TODO: Make sure appropriate exceptions are thrown for invalid inputs
		// password empty, hash empty --> throw invalid arg exception
		// Check what we have to do for running into a password or hash that is ""

		List<Boolean> result = new ArrayList<>();

		try {
			List<BackendNode> nodesForUse = new ArrayList<>();
			List<BackendNode> usedBENodes = new ArrayList<>();

			// Initialize Backend Nodes if not already initialized, and remove any who are disconnected or fail to initialize

			List<BackendNode> removalList = new ArrayList<BackendNode>();
			for (int i = 0; i < backendNodes.size(); i++) {
				initializeBcryptClient(backendNodes.get(i));
				if (backendNodes.get(i).isBcryptClientInError()) {
					removalList.add(backendNodes.get(i));
				}
			}

			backendNodes.removeAll(removalList);

			// Find indices of free (idle) backend nodes
			for (int i = 0; i < backendNodes.size(); i++) {
				if (!backendNodes.get(i).isWorking()) {
					nodesForUse.add(backendNodes.get(i));
				}
			}
			
			// if found one or more free backend nodes, split work evenly between them
			if (nodesForUse.size() > 0) {
				BackendNode backendNode = null;
				CountDownLatch countDownLatch = null;

				// # of items to be processed by current node 
				int jobSize = password.size() / nodesForUse.size();
		
				if (jobSize < 1) {
					// assign entire job (whole password list) to only one free BE node: latch initialized to 1 for "1 async RPC"
					usedBENodes.add(nodesForUse.get(0));
					backendNode = usedBENodes.get(0);
					countDownLatch = new CountDownLatch(1);
					backendNode.setCheckPassLatch(countDownLatch);

					try {
						backendNode.checkPassword(password, hash);
					} catch (Exception e) {
						System.out.println("Error as backend node tried to check password.");
						System.out.println(e.getMessage());
					}
					
					backendNode.jobStartIndex = 0;
					backendNode.jobEndIndex = jobSize;
				} else {
					countDownLatch = new CountDownLatch(nodesForUse.size());
					int itemsProcessed = 0;
					
					int nodeNum = 1;
					for (BackendNode node : nodesForUse){
						if (nodeNum == nodesForUse.size()){
							jobSize = password.size() - itemsProcessed;
						}

						usedBENodes.add(node);
						backendNode = node;
						node.setCheckPassLatch(countDownLatch);
						try {
							backendNode.checkPassword(password.subList(itemsProcessed, (itemsProcessed + jobSize)), hash.subList(itemsProcessed, (itemsProcessed + jobSize)));
						} catch (Exception e) {
							System.out.println("Error as backend node tried to check password.");
							System.out.println(e.getMessage());
						}
						backendNode.jobStartIndex = itemsProcessed;
						backendNode.jobEndIndex = itemsProcessed + jobSize;

						itemsProcessed += jobSize;
						nodeNum++;
					}
				} 

				countDownLatch.await();

				List<List<Boolean>> resultLists = new ArrayList<List<Boolean>>(usedBENodes.size());
				List<Integer> resultIndexesInError = new ArrayList<Integer>();
				List<Integer> jobStartIndexes = new ArrayList<Integer>();
				List<Integer> jobEndIndexes = new ArrayList<Integer>();

				int index = 0;
				for (BackendNode node : usedBENodes) {
					if (node.checkCheckPassErrors().equals(true)) {
						node.bcryptClientInError();
						resultIndexesInError.add(index);
						jobStartIndexes.add(node.jobStartIndex);
						jobEndIndexes.add(node.jobEndIndex);

					} else {
						resultLists.add(index, node.getCheckPassResults());
					}
					index++;
				}

				for (int i = 0; i < resultIndexesInError.size(); i++) {
					List<Boolean> partialResult = checkPassword(password.subList(jobStartIndexes.get(i), jobEndIndexes.get(i)), hash.subList(jobStartIndexes.get(i), jobEndIndexes.get(i)));
					resultLists.add(resultIndexesInError.get(i), partialResult);	
				}

				for (List<Boolean> list : resultLists) {
					result.addAll(list);
				}
			} else {
				// If no free BEs, do the work yourself

				System.out.println("No free BEs, doing checkPassword work at frontend.");
				for (int i = 0; i < password.size(); i++) {
					String passwordString = password.get(i);
					String hashString = hash.get(i);
	
					if (hashString.charAt(0) != '$' && hashString.charAt(1) != '2') {
						result.add(false);
						continue;
					}
	
					result.add(BCrypt.checkpw(passwordString, hashString));
				}
				System.out.println("Frinished checkPassword work at frontend.");
			}
		} catch (Exception e) {
			System.out.println("Error in frontend checkPassword method:");
			System.out.println(e.getMessage());
		}

		return result;
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

				// We don't want to be throwing an exception, only returning false, for an invalid salt version
				// checkpw will throw an exception is there is an invalid salt version, so we have to bypass that
				if (hashString.charAt(0) != '$' && hashString.charAt(1) != '2') {
					ret.add(false);
					continue;
				}

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
