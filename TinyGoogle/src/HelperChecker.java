import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Hashtable;

public class HelperChecker implements Runnable{
	private Hashtable<String, Integer> helperList = null;
	
	public HelperChecker (Hashtable<String, Integer> helperList) {
		this.helperList = helperList;
	}
	
	@Override
	public void run() {
		try {
			while (true){
				ArrayList<String> removeList = new ArrayList<String>();
				BufferedReader br = null;
				PrintWriter pw = null;
				//ArrayList<String> removeList = new ArrayList<String>();
				for (String key : helperList.keySet()) {
					System.out.println("Server: Checking helper availability: " + key);
					String[] tmp = key.split(":");
					int port = Integer.parseInt(tmp[1]);
					String server = tmp[0];
					Socket mSocket = null;
					try {
						mSocket = new Socket(server, port);
						br = new BufferedReader(new InputStreamReader(mSocket.getInputStream()));
						pw = new PrintWriter(mSocket.getOutputStream(), true);
						pw.println("0");
						String msg = "";
						while (((msg = br.readLine()) != null)) {
							msg = msg.trim();
							System.out.println("Server: Receive mssage from helper: " + msg);
							if (!msg.equals("1")) {
								System.out.println("Server: This helper has problem, remove it from helper list");
								
								synchronized (helperList) {
									helperList.remove(key);
									BufferedWriter out = null;
									try {
										out = new BufferedWriter(new FileWriter("helperlist.model"));
										out.write(Util.toString(helperList));
									} catch (IOException e) {
										e.printStackTrace();
									} finally {
										try {
											out.close();
										} catch (IOException e) {
											e.printStackTrace();
										}
									}
								}
								
								
							}
						}
					} catch (IOException e) {
						//e.printStackTrace();
						System.out.println("Server: This helper has problem, remove it from helper list");
						
						removeList.add(key);
					}
				}
				
				for (int i = 0 ; i < removeList.size(); i++) {
					helperList.remove(removeList.get(i));
				}
				Thread.sleep(60000);
			}
			
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
