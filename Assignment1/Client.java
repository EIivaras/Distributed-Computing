import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

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
		// Create a socket
		TSocket sock = new TSocket(args[0], Integer.parseInt(args[1]));
		// Create the transport object and protocol
	    TTransport transport = new TFramedTransport(sock);
		TProtocol protocol = new TBinaryProtocol(transport);
		// Implement client stub, used to invoke RPC
		BcryptService.Client client = new BcryptService.Client(protocol);
		// Open the connection to the server on the socket defined above
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

		List<String> passwords = new ArrayList<String>();
		List<String> hashes = new ArrayList<String>();

		// /*

		System.out.println("\n\n~~~~~~~~~~~~~~~~~ Test Cases to test Correctness ~~~~~~~~~~~~~~~~~\n\n");

		// Test Case 1: Multiple Passwords
		System.out.println("\nTest case 1: Multiple Passwords");

		passwords = new ArrayList<String>();
		passwords.add("Hype");
		passwords.add("Hype2");
		passwords.add("Hype3");
		hashes = client.hashPassword(passwords, (short)10);

		try {
			List<Boolean> result = client.checkPassword(passwords, hashes);

			for (int i = 0; i < result.size(); i++) {
				if (!result.get(i)) {
					System.out.println("Test case 1 failure because of wrong password and hash combination!\n");
				}
			}
			System.out.println("Test case 1 success!\n");
		} catch (Exception e) {
			System.out.println("Test case 1 failure because of exception:");
			System.out.println(e.getMessage());
			System.out.println("\n");
		}

		
		// Test Case 2: 16 Passwords
		System.out.println("\nTest case 2: 16 Passwords");

		passwords = new ArrayList<String>();
		passwords.add("Hype");
		passwords.add("Hype2");
		passwords.add("Hype3");
		passwords.add("Hype4");
		passwords.add("Hype5");
		passwords.add("Hype6");
		passwords.add("Hype7");
		passwords.add("Hype8");
		passwords.add("Hype9");
		passwords.add("Hype10");
		passwords.add("Hype11");
		passwords.add("Hype12");
		passwords.add("Hype13");
		passwords.add("Hype14");
		passwords.add("Hype15");
		passwords.add("Hype16");
		hashes = client.hashPassword(passwords, (short)10);

		try {
			List<Boolean> result = client.checkPassword(passwords, hashes);

			for (int i = 0; i < result.size(); i++) {
				if (!result.get(i)) {
					System.out.println("Test case 2 failure because of wrong password and hash combination!\n");
				}
			}
			System.out.println("Test case 2 success!\n");
		} catch (Exception e) {
			System.out.println("Test case 2 failure because of exception:");
			System.out.println(e.getMessage());
			System.out.println("\n");
		}

		// Test Case 3: The password passed in to hashPassword and checkPassword is an empty string.
		System.out.println("\nTest case 3: The password passed in to hashPassword and checkPassword is an empty string.");

		passwords = new ArrayList<String>();
		passwords.add("");

		try {
			hashes = client.hashPassword(passwords, (short)10);
			List<Boolean> result = client.checkPassword(passwords, hashes);

			for (int i = 0; i < result.size(); i++) {
				if (!result.get(i)) {
					System.out.println("Test case 3 failure because of wrong password and hash combination!\n");
				}
			}
			System.out.println("Test case 3 success!\n");
		} catch (Exception e) {
			System.out.println("Test case 3 failure because of exception:");
			System.out.println(e.getMessage());
			System.out.println("\n");
		}

		System.out.println("\n\n~~~~~~~~~~~~~~~~~ Test Cases to test Exception Handling ~~~~~~~~~~~~~~~~~\n\n");

		// Test Case 1: Empty `password` argument to hashPassword
		System.out.println("\nTest case 1: Empty `password` argument to hashPassword");

		passwords = new ArrayList<String>();
		
		try {
			client.hashPassword(passwords, (short) 10);
			System.out.println("Test case 1 failure.\n");
		} catch (Exception e) {
			System.out.println("Test case 1 success IF illegal argument exception thrown below:");
			System.out.println(e.getMessage());
			System.out.println("\n");
		}

		// Test Case 3: Empty `password` argument to checkPassword
		System.out.println("\nTest case 3: Empty `password` argument to checkPassword");

		passwords = new ArrayList<String>();

		hashes = new ArrayList<String>();
		hashes.add("Hype");
		hashes.add("Hype2");
		hashes.add("Hype3");
		
		try {
			client.checkPassword(passwords, hashes);
			System.out.println("Test case 3 failure.\n");
		} catch (Exception e) {
			System.out.println("Test case 3 success IF illegal argument expection thrown below:");
			System.out.println(e.getMessage());
			System.out.println("\n");
		}

		// Test Case 4 Empty `hash` argument to checkPassword
		System.out.println("\nTest case 4: Empty `hash` argument to checkPassword");

		passwords = new ArrayList<String>();
		passwords.add("Hype");
		passwords.add("Hype2");
		passwords.add("Hype3");

		hashes = new ArrayList<String>();
		
		try {
			client.checkPassword(passwords, hashes);
			System.out.println("Test case 4 failure.\n");
		} catch (Exception e) {
			System.out.println("Test case 4 success IF illegal argument expection thrown below:");
			System.out.println(e.getMessage());
			System.out.println("\n");
		}

		// Test Case 6: Both arguments empty to checkPassword
		System.out.println("\nTest case 5: Both arguments empty to checkPassword");

		passwords = new ArrayList<String>();
		hashes = new ArrayList<String>();
		
		try {
			client.checkPassword(passwords, hashes);
			System.out.println("Test case 5 failure.\n");
		} catch (Exception e) {
			System.out.println("Test case 5 success IF illegal argument expection thrown below:");
			System.out.println(e.getMessage());
			System.out.println("\n");
		}

		// Test Case 6: logRounds value < 4 to hashPassword
		System.out.println("\nTest case 6: logRounds value < 4 to hashPassword");

		passwords = new ArrayList<String>();
		passwords.add("Hype");
		
		try {
			client.hashPassword(passwords, (short) 3);
			System.out.println("Test case 6 failure.\n");
		} catch (Exception e) {
			System.out.println("Test case 6 success IF illegal argument expection thrown below:");
			System.out.println(e.getMessage());
			System.out.println("\n");
		}

		
		// Test Case 7: The `password` and `hash` arguments to `checkPassword` are of unequal length. 
		System.out.println("\nTest case 7: The `password` and `hahs` arguments to `checkPassword` are of unequal length");

		passwords = new ArrayList<String>();
		passwords.add("Hype");
		passwords.add("Hype2");
		hashes = new ArrayList<String>();
		hashes.add("$2efaefee");
		
		try {
			client.checkPassword(passwords, hashes);
			System.out.println("Test case 7 failure.\n");
		} catch (Exception e) {
			System.out.println("Test case 7 success IF illegal argument expection thrown below:");
			System.out.println(e.getMessage());
			System.out.println("\n");
		}
		
		transport.close();

		// */
		
		System.out.println("\n\n~~~~~~~~~~~~~~~~~ Throughput Test Cases ~~~~~~~~~~~~~~~~~\n\n");

		long startTime;
		long endTime;

		List<MultithreadedClient> multithreadedClients = new ArrayList<MultithreadedClient>();

		/*
		System.out.println("\n Test case 0: 2 Clients with 128 (1024 characters long) Passwords each");

		passwords = new ArrayList<String>();
		for (int i = 0; i < 128; ++i) {
			String pattern = "abc" + i;
			List<String> repeated = Collections.nCopies(256, pattern);
			passwords.add(String.join("", repeated));
		}

		for (int i = 0; i < 2; i++) {
			multithreadedClients.add(new MultithreadedClient(args[0], Integer.parseInt(args[1]), i, passwords, (short) 10));
		}

		startTime = System.currentTimeMillis();
		for (int i = 0; i < 2; i++) {
			multithreadedClients.get(i).start();
		}

		for (int i = 0; i < 2; i++) {
			try {
				multithreadedClients.get(i).join();
			} catch (Exception e) {
				System.out.println("Thread interrupted during execution. Exception:");
				System.out.println(e.getMessage());
			}
		}

		endTime = System.currentTimeMillis();

		// 512 because 512 total cryptographic operations in this time
		System.out.println("2 Clients, 128 Passwords: Throughput for logRounds=" + 10 + ": " + 512 * 1000f/(endTime-startTime));
		*/
		System.out.println("\n Test case 1: 4 Clients with 4 Passwords each");

		passwords = new ArrayList<String>();
		passwords.add("Hype");
		passwords.add("Hype2");
		passwords.add("Hype3");
		passwords.add("Hype4");

		multithreadedClients = new ArrayList<MultithreadedClient>();

		for (int i = 0; i < 4; i++) {
			multithreadedClients.add(new MultithreadedClient(args[0], Integer.parseInt(args[1]), i, passwords, (short) 10));
		}

		startTime = System.currentTimeMillis();
		for (int i = 0; i < 4; i++) {
			multithreadedClients.get(i).start();
		}

		for (int i = 0; i < 4; i++) {
			try {
				multithreadedClients.get(i).join();
			} catch (Exception e) {
				System.out.println("Thread interrupted during execution. Exception:");
				System.out.println(e.getMessage());
			}
		}

		endTime = System.currentTimeMillis();

		// 32 because 32 total cryptographic operations in this time
		System.out.println("4 Clients, 4 Passwords: Throughput for logRounds=" + 10 + ": " + 32 * 1000f/(endTime-startTime));

		System.out.println("\n Test case 2: 16 Clients with 1 Password per request");

		passwords = new ArrayList<String>();
		passwords.add("Hype");

		multithreadedClients = new ArrayList<MultithreadedClient>();
		for (int i = 0; i < 16; i++) {
			multithreadedClients.add(new MultithreadedClient(args[0], Integer.parseInt(args[1]), i, passwords, (short) 10));
		}

		startTime = System.currentTimeMillis();
		for (int i = 0; i < 16; i++) {
			multithreadedClients.get(i).start();
		}

		for (int i = 0; i < 16; i++) {
			try {
				multithreadedClients.get(i).join();
			} catch (Exception e) {
				System.out.println("Thread interrupted during execution. Exception:");
				System.out.println(e.getMessage());
			}
		}

		endTime = System.currentTimeMillis();

		// 32 because 32 total cryptographic operations in this time
		System.out.println("16 Clients, 1 Password: Throughput for logRounds=" + 10 + ": " + 32 * 1000f/(endTime-startTime));

		System.out.println("\n Test case 3: 1 Clients with 16 Passwords per request");

		passwords = new ArrayList<String>();
		passwords.add("Hype");
		passwords.add("Hype2");
		passwords.add("Hype3");
		passwords.add("Hype4");
		passwords.add("Hype5");
		passwords.add("Hype6");
		passwords.add("Hype7");
		passwords.add("Hype8");
		passwords.add("Hype9");
		passwords.add("Hype10");
		passwords.add("Hype11");
		passwords.add("Hype12");
		passwords.add("Hype13");
		passwords.add("Hype14");
		passwords.add("Hype15");
		passwords.add("Hype16");

		multithreadedClients = new ArrayList<MultithreadedClient>();
		for (int i = 0; i < 1; i++) {
			multithreadedClients.add(new MultithreadedClient(args[0], Integer.parseInt(args[1]), i, passwords, (short) 10));
		}

		startTime = System.currentTimeMillis();
		for (int i = 0; i < 1; i++) {
			multithreadedClients.get(i).start();
		}

		for (int i = 0; i < 1; i++) {
			try {
				multithreadedClients.get(i).join();
			} catch (Exception e) {
				System.out.println("Thread interrupted during execution. Exception:");
				System.out.println(e.getMessage());
			}
		}

		endTime = System.currentTimeMillis();
		
		// 32 because 32 total cryptographic operations in this time
		System.out.println("1 Clients, 16 Passwords: Throughput for logRounds=" + 10 + ": " + 32 * 1000f/(endTime-startTime));

	} catch (TException x) {
	    x.printStackTrace();
	} 
    }
}
