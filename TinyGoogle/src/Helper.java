import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Helper {

	private int numOfThread = 1;
	private String filename = "";
	private int port = 0;
	private ServerSocket mServerSocket;
	// thread pool
	private ExecutorService mExecutorService;

	public Helper(String filename, int port) {
		this.filename = filename;
		this.port = port;
	}
	
	public Helper(String filename, int port, int numOfThread) {
		this.filename = filename;
		this.port = port;
		this.numOfThread = numOfThread;
	}

	public void start() {
		System.out.println("Helper: Created!");
		register();

		try {
			// Create the server
			mServerSocket = new ServerSocket(port);
			// create a thread pool
			mExecutorService = Executors.newCachedThreadPool();
			System.out.println("Helper: Start helper!");

			// Start listening for connections. The program waits until some
			// client connects to the socket.
			System.out.println("Helper: Start listening on port " + port + ".");

			while (true) {
				// Wait for incoming connections
				Socket socket = mServerSocket.accept();
				System.out.println("Helper: New master is comming in.");
				// open a client thread
				mExecutorService.execute(new HelperThread(socket));
			}
		} catch (IOException ioe) {
			ioe.printStackTrace();
		} finally {
			if (mServerSocket != null) {
				try {
					// Close the server
					mServerSocket.close();
					System.out.println("Port Mapper: Closed!");
				} catch (IOException ioe) {
					ioe.printStackTrace();
				}
			}
		}
	}
	
	private void register() {
		try {
			// This will reference one line at a time
			String line = null;
			String server = "";
			int port = 0;
			try {
				// FileReader reads text files in the default encoding.
				FileReader fileReader = new FileReader(filename);

				// Always wrap FileReader in BufferedReader.
				BufferedReader bufferedReader = new BufferedReader(fileReader);

				while ((line = bufferedReader.readLine()) != null) {
					line = line.trim();
					String[] serverInfo = line.split(":");
					server = serverInfo[0];
					port = Integer.parseInt(serverInfo[1]);
					break;
				}
				// Always close files.
				bufferedReader.close();
			} catch (FileNotFoundException ex) {
				System.out.println("Unable to open file '" + filename + "'");
			} catch (IOException ex) {
				System.out.println("Error reading file '" + filename + "'");
				// Or we could just do this:
				// ex.printStackTrace();
			}

			System.out.println("Helper: Server is " + server + ":" + port);
			// connect to server
			Socket mSocket = new Socket(server, port);
			System.out.println("Helper: Register to Server");
			// open input & output stream
			PrintWriter mPrintWriter = new PrintWriter(mSocket.getOutputStream(), true);

			InetAddress addr = InetAddress.getLocalHost();

			mPrintWriter.flush();
			mPrintWriter.println("h," + numOfThread + "," + addr.getHostAddress() + ":" + this.port);
			System.out.println("Helper: Send message to Server: " + "h," + numOfThread + "," + addr.getHostAddress() + ":" + this.port);
			System.out.println("Helper: Bind to Server");
			mSocket.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	
	public static void main(String[] args) {
		String path = "/afs/cs.pitt.edu/usr0/colinzhang/public/Prj2HaoranZhang/socket_based/";
		// String path = "";
		String filename = path + "server.txt";
		int port = Util.availablePort();
		if (args.length == 1) {
			Helper h = new Helper(filename, port, Integer.parseInt(args[0]));
			h.start();
		} else {
			Helper h = new Helper(filename, port);
			h.start();
		}
		
	}

}
