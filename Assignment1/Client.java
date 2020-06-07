import java.util.List;
import java.util.ArrayList;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransportFactory;

// NOTE: MAY NEED TO EDIT CLIENT TO COVER MORE TEST CASES

public class Client {
    public static void main(String [] args) {
	if (args.length != 3) {
	    System.err.println("Usage: java Client FE_host FE_port password");
	    System.exit(-1);
	}

	try {
	    TSocket sock = new TSocket(args[0], Integer.parseInt(args[1]));
	    TTransport transport = new TFramedTransport(sock);
	    TProtocol protocol = new TBinaryProtocol(transport);
	    BcryptService.Client client = new BcryptService.Client(protocol);
	    transport.open();

	    List<String> password = new ArrayList<>();
	    password.add(args[2]);
	    List<String> hash = client.hashPassword(password, (short)10);
	    System.out.println("Password: " + password.get(0));
	    System.out.println("Hash: " + hash.get(0));
	    System.out.println("Positive check: " + client.checkPassword(password, hash));
	    hash.set(0, "$2a$14$reBHJvwbb0UWqJHLyPTVF.6Ld5sFRirZx/bXMeMmeurJledKYdZmG");
		System.out.println("Negative check: " + client.checkPassword(password, hash));
		
		// NOTE: We /want/ the exception to be thrown here
	    try {
			hash.set(0, "too short");
			List<Boolean> rets = client.checkPassword(password, hash);
			System.out.println("Exception check: no exception thrown");
	    } catch (Exception e) {
			System.out.println("Exception check: exception thrown");
		}
		
		// ~~~~~~~~~~~~ Added Test Cases ~~~~~~~~~~~~ //

		// Test Case 1: Multiple hashes

		List<String> passwords = new ArrayList<>();
		passwords.add("Hype");
		passwords.add("Hype2");
		passwords.add("Hype3");
		List<String> hashes = client.hashPassword(passwords, (short)10);

		try {
			client.checkPassword(passwords, hashes);
			System.out.println("Test case 1 success!");
		} catch (Exception e) {
			System.out.println("Test case 1 failure.");
			System.out.println(e.getMessage());
		}

	    transport.close();
	} catch (TException x) {
	    x.printStackTrace();
	} 
    }
}
