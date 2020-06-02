import java.util.List;
import java.util.ArrayList;
import java.util.Random;

import org.mindrot.jbcrypt.BCrypt;


public class Calibrator {
    public static void main(String [] args) throws Exception {
	int numRounds = 256;
	for (short s = 8; s <= 12; s++) {
	    byte bytes[] = new byte[1024];
	    Random rand = new Random();
	    rand.nextBytes(bytes);
	    String password = new String(bytes);
	    long startTime = System.currentTimeMillis();
	    int n = 4096/numRounds;
	    for (int i = 0; i < n; i++) {
		String hashed = BCrypt.hashpw(password, BCrypt.gensalt(s));
	    }
	    long endTime = System.currentTimeMillis();
	    System.out.println("Throughput for logRounds=" + s + ": " + n * 1000f/(endTime-startTime));
	    System.out.println("Latency for logRounds=" + s + ": " + (endTime-startTime)/n);
	    numRounds *= 2;
	}
    }
}
