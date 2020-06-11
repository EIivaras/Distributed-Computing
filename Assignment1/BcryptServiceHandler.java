import java.util.ArrayList;
import java.util.List;

import org.mindrot.jbcrypt.BCrypt;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;

/*
	In these methods, we should use similar methods as the client calling the FENode in Client.java for the FENode to call the backend nodes
*/

public class BcryptServiceHandler implements BcryptService.Iface {

	private class BackendNode {
		public Boolean bcryptClientActive;
		public Boolean isWorking;
		public String host;
		public int port;
		public BcryptService.Client bcryptClient;
		public TTransport transport;

		public void setHostAndPort(String host, int port) {
			this.host = host;
			this.port = port;
			this.bcryptClientActive = false;
			this.isWorking = false;
		}

		public void setClientAndTransport(BcryptService.Client bcryptClient, TTransport transport) {
			try {
				this.bcryptClient = bcryptClient;
				this.transport = transport;
				this.transport.open();
				this.bcryptClientActive = true;
			} catch (Exception e) {
				System.out.println("Failed to open transport for BackendNode.");
				this.bcryptClientActive = false;
			}
		}

		public List<String> hashPassword(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException {
			try {
				System.out.println("Starting hashPassword at backend.");
				this.isWorking = true;
				List<String> results = this.bcryptClient.hashPasswordBE(password, logRounds);
				this.isWorking = false;
				System.out.println("Completed hashPassword at backend.");
				return results;
			} catch (Exception e) {
				// TODO: hashPasswordBE throws an IllegalArgument like this, so can I do the same here?
				System.out.println("Failed to hash password at backend.");
				this.isWorking = false;
				throw new IllegalArgument(e.getMessage());
			}
		}

		public List<Boolean> checkPassword(List<String> password, List<String> hash) throws IllegalArgument, org.apache.thrift.TException {
			try {
				System.out.println("Starting checkPassword at backend.");
				this.isWorking = true;
				List<Boolean> results = this.bcryptClient.checkPasswordBE(password, hash);
				this.isWorking = false;
				System.out.println("Completed checkPassword at backend.");
				return results;
			} catch (Exception e) {
				System.out.println("Failed to check password at backend.");
				this.isWorking = false;
				throw new IllegalArgument(e.getMessage());
			}
		}

		public void closeTransport() {
			this.transport.close();
		}
	}
	
	private List<BackendNode> backendNodes = new ArrayList<BackendNode>();

	private BackendNode createBcryptClient(BackendNode backendNode) {

		// Make sure we're only initializing everything if the bcryptClient has not already been set up
		if (backendNode.bcryptClientActive) {
			return backendNode;
		}

		TSocket sock = new TSocket(backendNode.host, backendNode.port);
		TTransport transport = new TFramedTransport(sock);
		TProtocol protocol = new TBinaryProtocol(transport);
		BcryptService.Client bcryptClient = new BcryptService.Client(protocol);			
		backendNode.setClientAndTransport(bcryptClient, transport);

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

				/* TODO: Create a load balancer to choose which backend nodes get chosen for a certain task
					--> Need to make sure that we account for the # of hashes that need to be computed as well.
				*/
				BackendNode backendNode = null;

				// Find a backend node that is not working, and if it's not working, call createBcryptClient
				// CreateClient will create the bcryptClient of the node so it can handle requests if it is not created already
				for (int i = 0; i < backendNodes.size(); i++) {
					if (!backendNodes.get(i).isWorking) {
						backendNode = createBcryptClient(backendNodes.get(i));
						break;
					}
				}

				// If we found a backend node that is ready to work, put it to work!
				if (backendNode != null) {
					List<String> result = backendNode.hashPassword(password, logRounds);
					
					return result;
				}

			} catch (Exception e) {
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

				// Find a backend node that is not working, and if it's not working, call createBcryptClient
				// CreateClient will create the bcryptClient of the node so it can handle requests if it is not created already
				for (int i = 0; i < backendNodes.size(); i++) {
					if (!backendNodes.get(i).isWorking) {
						backendNode = createBcryptClient(backendNodes.get(i));
						break;
					}
				}

				// If we found a backend node that is ready to work, put it to work!
				if (backendNode != null) {
					List<Boolean> result = backendNode.checkPassword(password, hash);
					
					return result;
				}
			} catch (Exception e) {
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
			throw new IllegalArgument(e.getMessage());
		}
    }
}
