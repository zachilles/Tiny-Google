import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.util.Base64;
import java.util.Hashtable;
import java.util.Random;

public class Util {
	public static boolean stat = false;
	public static long mapTime = 0;
	public static long reduceTime = 0;
	public static boolean TCP = true;
	public static Hashtable<Integer, Integer> portTable = new Hashtable<Integer, Integer>();
	/** Read the object from Base64 string. */
	public static Object fromString(String s) throws IOException, ClassNotFoundException {
		byte[] data = Base64.getDecoder().decode(s);
		ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data));
		Object o = ois.readObject();
		ois.close();
		return o;
	}

	/** Write the object to a Base64 string. */
	public static String toString(Object object) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.writeObject(object);
		oos.close();
		return Base64.getEncoder().encodeToString(baos.toByteArray());
	}
	
	public static boolean isPortAvailable(int port) {
	    try {
	        ServerSocket server = new ServerSocket(port);
	        server.close();
	        return true;
	    } catch (IOException e) {
	    }
	    return false;
	}
	
	public static int availablePort() {
        int max=50000;
        int min=16000;
        Random random = new Random();
        int port = 0;
        do {
        	port = random.nextInt(max)%(max-min+1) + min;
        } while (!isPortAvailable(port));
        portTable.put(port, 0);
        return port;
        
    }
}
