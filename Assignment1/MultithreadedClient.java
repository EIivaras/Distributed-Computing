import java.util.List;
import java.util.ArrayList;

import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TFramedTransport;

public class MultithreadedClient extends Thread {
    String host;
    int port;
    int threadNumber;

    public MultithreadedClient(String host, int port, int threadNumber) {
        this.host = host;
        this.port = port;
        this.threadNumber = threadNumber;
    }

    public void run() {
        try {
            System.out.println("\nThread " + this.threadNumber + " starting.");

            TSocket sock = new TSocket(this.host, this.port);
            TTransport transport = new TFramedTransport(sock);
            TProtocol protocol = new TBinaryProtocol(transport);
            BcryptService.Client client = new BcryptService.Client(protocol);
            transport.open();


            List<String> passwords = new ArrayList<String>();
            passwords.add("Hype");
            passwords.add("Hype2");
            passwords.add("Hype3");
            passwords.add("Hype4");
            List<String> hashes = client.hashPassword(passwords, (short)10);
    
            try {
                List<Boolean> result = client.checkPassword(passwords, hashes);
    
                for (int i = 0; i < result.size(); i++) {
                    if (!result.get(i)) {
                        System.out.println("Thread " + this.threadNumber + " failure because of password and hash mismatch.");
                    }
                }
                System.out.println("Thread " + this.threadNumber + " success!\n");
            } catch (Exception e) {
                System.out.println("Thread " + this.threadNumber + " failure because of exception:");
                System.out.println(e.getMessage());
                System.out.println("\n");
            }

            transport.close();

        } catch (Exception e) {
            System.out.println("Exception in thread " + this.threadNumber + ":");
            System.out.println(e.getMessage());
        }
    }
    
}