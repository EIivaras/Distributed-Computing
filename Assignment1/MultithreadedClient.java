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
    List<String> passwords;
    short logRounds;

    public MultithreadedClient(String host, int port, int threadNumber, List<String> passwords, short logRounds) {
        this.host = host;
        this.port = port;
        this.threadNumber = threadNumber;
        this.passwords = passwords;
        this.logRounds = logRounds;
    }

    public void run() {
        try {
            long startTime;
            long endTime;

            System.out.println("\nThread " + this.threadNumber + " starting.");

            TSocket sock = new TSocket(this.host, this.port);
            TTransport transport = new TFramedTransport(sock);
            TProtocol protocol = new TBinaryProtocol(transport);
            BcryptService.Client client = new BcryptService.Client(protocol);
            transport.open();
    
            try {
                startTime = System.currentTimeMillis();
                List<String> hashes = client.hashPassword(this.passwords, this.logRounds);
                endTime = System.currentTimeMillis();
    
                System.out.println("Latency for logRounds=" + this.logRounds + " on thread " + this.threadNumber + " for hashPassword: " + (endTime-startTime)/this.passwords.size());

                startTime = System.currentTimeMillis();
                List<Boolean> result = client.checkPassword(passwords, hashes);
                endTime = System.currentTimeMillis();

                System.out.println("Latency for logRounds=" + this.logRounds + " on thread " + this.threadNumber + " for checkPassword: " + (endTime-startTime)/this.passwords.size());

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