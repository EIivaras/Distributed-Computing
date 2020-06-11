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
		public Boolean clientActive;
		public String host;
		public int port;
		public BcryptService.Client client;
		public TTransport transport;

		public void setHostAndPort(String host, int port) {
			this.host = host;
			this.port = port;
			this.clientActive = false;
		}

		public void setClientAndTransport(BcryptService.Client client, TTransport transport) {
			try {
				this.client = client;
				this.transport = transport;
				this.transport.open();
				this.clientActive = true;
			} catch (Exception e) {
				System.out.println("Failed to open transport for BackendNode.");
				this.clientActive = false;
			}
		}

		public void closeTransport() {
			this.transport.close();
		}
	}
	
	private List<BackendNode> backendNodes = new ArrayList<BackendNode>();

	private BackendNode createClient(BackendNode backendNode) {

		// Make sure we're only initializing everything if the client has not already been set up
		for (int i = 0; i < backendNodes.size(); i++) {
			if (backendNode.clientActive) {
				return backendNode;
			}
		}

		TSocket sock = new TSocket(backendNode.host, backendNode.port);
		TTransport transport = new TFramedTransport(sock);
		TProtocol protocol = new TBinaryProtocol(transport);
		BcryptService.Client client = new BcryptService.Client(protocol);			
		backendNode.setClientAndTransport(client, transport);

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

				BackendNode backendNode = createClient(backendNodes.get(0));

				System.out.println("Starting hashPassword at backend.");
				List<String> result = backendNode.client.hashPasswordBE(password, logRounds);
				System.out.println("Completed hashPassword at backend.");
				
				return result;
			} catch (Exception e) {
				System.out.println("Failed trying to call hashPassword on backend node.");
				System.out.println(e.getMessage());
			}


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

    public List<Boolean> checkPassword(List<String> password, List<String> hash) throws IllegalArgument, org.apache.thrift.TException
    {
		try {
			try {
				BackendNode backendNode = createClient(backendNodes.get(0));

				System.out.println("Starting checkPassword at backend.");
				List<Boolean> result = backendNode.client.checkPasswordBE(password, hash);
				System.out.println("Completed checkPassword at backend.");
				
				return result;
			} catch (Exception e) {
				System.out.println("Failed trying to call checkPassword on backend node.");
				System.out.println(e.getMessage());
			}


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
