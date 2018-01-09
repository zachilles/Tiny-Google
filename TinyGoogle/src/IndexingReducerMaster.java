import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Queue;

public class IndexingReducerMaster {
	Hashtable<String, InvertedIndex> iiList;
	ArrayList<String> fileList;
	HelperToken ht;
	String mStrMSG = "";
	Queue<HelperToken> helperQueue;
	public IndexingReducerMaster(Hashtable<String, InvertedIndex> iiList, ArrayList<String> fileList, HelperToken ht, Queue<HelperToken> helperQueue) {
		this.iiList = iiList;
		this.fileList = fileList;
		this.ht = ht;
		this.helperQueue = helperQueue;
	}
	
	public InvertedIndex merge () {
		InvertedIndex localII = new InvertedIndex();
		
		Socket mSocket = null;
		
		int tries = 0;
		int maxRetry = 3;
		String mStrMSG;
		while (tries < maxRetry) {
			tries++;
			localII = new InvertedIndex();
			try {
				
				mSocket = new Socket(ht.getServer(), ht.getPort());
				PrintWriter mPrintWriter = new PrintWriter(mSocket.getOutputStream(), true);
				BufferedReader mBufferedReader = new BufferedReader(new InputStreamReader(mSocket.getInputStream()));
				mPrintWriter.flush();
				
				Hashtable<String, Object> parameters = new Hashtable<String, Object>();
				parameters.put("iiList", iiList);
				parameters.put("fileList", fileList);
				
				if (Util.TCP) {
					mStrMSG = "3,"+Util.toString(parameters);
					mPrintWriter.println(mStrMSG);
				} else {
					int length = UDPUtil.LENGTH;
					int port = Util.availablePort();
					String send = Util.toString(parameters);
					//System.out.println(send.length());
					int bulks = send.length() / length;
					int remain = send.length() % length;
					if (remain > 0) {
						bulks = bulks + 1;
					}
					//System.out.println(bulks);
					mPrintWriter.println(3 + "," + port);
					String remotePort = mBufferedReader.readLine();
					int iPort = Integer.parseInt(remotePort);
					for (int i = 0; i < bulks; i++) {
						//System.out.println(i);
						if (length * i + length < send.length()) {
							UDPUtil.send(send.substring(length * i, length * i + length), port);
							//mPrintWriter.println(result + "," + port);
						} else {
							UDPUtil.send(send.substring(length * i, send.length()), port);
							//mPrintWriter.println(result + "," + port);
						}
						
						mBufferedReader.readLine();
					}
					UDPUtil.send("This is the end", port);
					mBufferedReader.readLine();
					Util.portTable.remove(port);
				}
				
				
				
				
				
				while (((mStrMSG = mBufferedReader.readLine()) != null)) {
					// mStrMSG = mStrMSG.trim();
					
					//System.out.println(mStrMSG);
					String[] msgs = mStrMSG.split(",");
					if (msgs[0].equals("0")) {
						throw new Exception();
					} else {
						mStrMSG = mStrMSG.substring(2, mStrMSG.length());
						if (Util.TCP) {

						} else {
							//System.out.println("Indexing Master Thread: Generating Port Number...");

							int port = Util.availablePort();
							//System.out.println("Indexing Master Thread: The Port Number is " + port);
							mPrintWriter.println(port + "");
							int remotePort = Integer.parseInt(mStrMSG);
							// UdpRece ur = new UdpRece(port, remotePort,
							// ht.getServer());
							// ur.receAll();
							mStrMSG = "";
							String tmp = "";
							do {
								mStrMSG = mStrMSG + tmp;
								tmp = UDPUtil.receive(port, remotePort, ht.getServer());
								// mBufferedReader.readLine();
								mPrintWriter.println("1");
							} while (!tmp.equals("This is the end"));
							Util.portTable.remove(port);
						}

					}

					localII = (InvertedIndex) Util.fromString(mStrMSG);

					break;
				}
				
				mSocket.close();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				if (mSocket != null) {
					try {
						mSocket.close();
					} catch (IOException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}
			}
		}
		
		
		
		
		
		
		
		
		synchronized (helperQueue) {
			// System.out.println("Indexing Master Thread: Put the helper
			// back to the queue");
			helperQueue.add(ht);
		}
		
		
		
		return localII;
	}
	
	
}
