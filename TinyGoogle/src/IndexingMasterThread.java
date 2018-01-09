import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Hashtable;
import java.util.Queue;

public class IndexingMasterThread implements Runnable {
	private String file = "";
	private Hashtable<String, InvertedIndex> iiList;
	private HelperToken ht;
	private InvertedIndex ii = new InvertedIndex();
	private String mStrMSG = "";
	private Queue<HelperToken> helperQueue;
	private int totalLine;

	public IndexingMasterThread(String file, Hashtable<String, InvertedIndex> iiList, HelperToken ht,
			Queue<HelperToken> helperQueue) throws IOException {
		this.file = file;
		this.iiList = iiList;
		this.ht = ht;
		this.helperQueue = helperQueue;

		LineNumberReader lnr = new LineNumberReader(new FileReader(new File(file)));
		lnr.skip(Long.MAX_VALUE);

		this.totalLine = lnr.getLineNumber() + 1;
	}

	@Override
	public void run() {
		Socket mSocket = null;
		System.out.println("Indexing Master Thread: Connect to helper " + ht.getServer() + ":" + ht.getPort()
		+ " to handle " + file);
		int tries = 0;
		int maxRetry = 3;
		while (tries < maxRetry) {
			tries++;
			try {
				for (int i = 0; i < totalLine; i = i + 1000) {
					mStrMSG = "1," + i + "," + file;
					//System.out.println(mStrMSG);
					// Create the server
					
					mSocket = new Socket(ht.getServer(), ht.getPort());

					PrintWriter mPrintWriter = new PrintWriter(mSocket.getOutputStream(), true);
					BufferedReader mBufferedReader = new BufferedReader(new InputStreamReader(mSocket.getInputStream()));
					mPrintWriter.flush();
					mPrintWriter.println(mStrMSG);

					while (((mStrMSG = mBufferedReader.readLine()) != null)) {
						// mStrMSG = mStrMSG.trim();
						

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

						ii.merge((InvertedIndex) Util.fromString(mStrMSG));

						break;
					}
				}
				System.out.println("Indexing Master Thread: Receive result for " + file + " from helper "
						+ ht.getServer() + ":" + ht.getPort());
				// System.out.println("Indexing Master Thread: Ready to enter
				// critical section");
				synchronized (iiList) {
					// System.out.println("Indexing Master Thread: Update indexed
					// file list");
					iiList.put(file, ii);
				}
				synchronized (helperQueue) {
					// System.out.println("Indexing Master Thread: Put the helper
					// back to the queue");
					helperQueue.add(ht);
				}
				mSocket.close();
				System.out.println("Indexing Master Thread: Finish creating index for " + file);
				break;
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
				System.out.println("Indexing Master Thread: Problem occur when handling " + file);
			}
		}
		

	}

}
