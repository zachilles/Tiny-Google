import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Scanner;

public class Client {

	private String filename = "";
	private String mStrMSG = "";

	public Client(String filename) {
		this.filename = filename;
	}

	@SuppressWarnings("resource")
	public void start() {
		System.out.println("Welcome to the Tiny-Google System");
		
		while(true){
			System.out.println("Please select an operation:");
			System.out.println("1. Create index for a folder");
			System.out.println("2. Search keyword");
			Scanner scan = new Scanner(System.in);
			String input = scan.nextLine();
			if (input.equals("1")) {

				System.out.println(
						"Please type in the folder name, e.g./afs/cs.pitt.edu/usr0/colinzhang/public/Prj1HaoranZhang/");
				String folder = scan.nextLine();
				System.out.println("Do you want to do it recursively? 0 or 1");
				String recursion = scan.nextLine();
				sendRequest(1, recursion + "," + folder);
			} else if (input.equals("2")) {
				System.out.println("Please type in the keywords, e.g. this, is, colin");
				String keywords = scan.nextLine();
				//keywords = "\"" + keywords + "\"";
				sendRequest(2, keywords);
			} else {
				System.out.println("Unknow command, please try again!");
			}
			System.out.println("Type in anything to continue...");
			input = scan.next();
			System.out.println("******************************************************");
		}
		

	}

	private void sendRequest(int type, String request) {
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

			Socket mSocket = new Socket(server, port);

			PrintWriter mPrintWriter = new PrintWriter(mSocket.getOutputStream(), true);
			BufferedReader mBufferedReader = new BufferedReader(new InputStreamReader(mSocket.getInputStream()));
			System.out.println("Sending request: " + "c," + type + "," + request);
			mPrintWriter.flush();
			mPrintWriter.println("c," + type + "," + request);
			
			System.out.println("Wait for response...");
			while (((mStrMSG = mBufferedReader.readLine()) != null)) {
				mStrMSG = mStrMSG.trim();
				if (type == 1) {
					if (mStrMSG.equals("1")) {
						System.out.println("Create index for " + request + ": Success");
					} else {
						System.out.println("Create index for " + request + ": Fail");
					}
				} else if (type == 2) {
					String[] tmp = mStrMSG.split(",");
					if (tmp[0].equals("1")) {
						InvertedIndex returned = (InvertedIndex) Util.fromString(mStrMSG.substring(2, mStrMSG.length()));
						try {
							System.out.println(returned.showResult());
						} catch (Exception e) {
							System.out.println("No matched results");
						}
						
					} else {
						System.out.println("Searching request for " + request + ": Fail");
					}
				}

				break;
			}
			mSocket.close();
			
			if (Util.stat){
				System.out.println("Server: Mapper Time: " + Util.mapTime + ", Reducer Time: " + Util.reduceTime);
				
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String path = "/afs/cs.pitt.edu/usr0/colinzhang/public/Prj2HaoranZhang/socket_based/";
		// String path = "";
		String filename = path + "server.txt";
		Client c = new Client(filename);
		//c.sendRequest(1, "0,/Users/colin/Documents/Study/AOS/Project_2/shake");
		//c.sendRequest(1, "1,/Users/colin/Documents/book/input");
		//c.sendRequest(1, "1,/Users/colin/Documents/PPAP");
		//c.sendRequest(1, "1,/Users/colin/Documents/PPAP2");
		//c.sendRequest(2, "I, apple, colin");
		c.start();

	}

}
