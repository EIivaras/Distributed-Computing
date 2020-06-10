import java.util.ArrayList;
import java.util.List;

import org.mindrot.jbcrypt.BCrypt;

/*
	In these methods, we should use similar methods as the client calling the FENode in Client.java for the FENode to call the backend nodes
*/

public class BcryptServiceHandler implements BcryptService.Iface {

	private List<String> hostList = new ArrayList<String>();
	private List<Integer> portList = new ArrayList<Integer>();
	// TODO: Verify that we can even declare a list of this, or if thrift complains?
	// private List<BcryptService.Client> clientList = new ArrayList<BcryptService.Client>();

	public void initializeBackend(String host, int port) throws IllegalArgument, org.apache.thrift.TException {
		try {
			this.hostList.add(host);
			this.portList.add(port);
	
			System.out.println("Worked! Port: " + portList.get(0));

			/*
			TODO: The plan is to open a connection to the BE nodes and potentially store the generated "client" in a list
			--> Still not 100% sure if this is the best solution, but it prevents us from having to use multiple hashPassword and checkPassword functions

			TSocket sock = new TSocket(host, port);
			TTransport transport = new TFramedTransport(sock);
			TProtocol protocol = new TBinaryProtocol(transport);
			BcryptService.Client client = new BcryptService.Client(protocol);
			transport.open();

			clientList.add(client);

			// TODO: If we do go with this approach, we will need to close the transport after a BE node exits or crashes!
			*/
		}
		catch (Exception e) {
			throw new IllegalArgument(e.getMessage());
		}
	}

    public List<String> hashPassword(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException
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

    public List<Boolean> checkPassword(List<String> password, List<String> hash) throws IllegalArgument, org.apache.thrift.TException
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
