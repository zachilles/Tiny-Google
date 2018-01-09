import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Queue;

public class ServerThread implements Runnable {

	private Socket mSocket;
	private BufferedReader mBufferedReader;
	private PrintWriter mPrintWriter;
	private String mStrMSG;
	private Hashtable<String, Integer> helperList;
	private Queue<HelperToken> helperQueue;
	private Queue<SearchTask> workQueue;
	private InvertedIndex mainII;
	private Hashtable<String, ArrayList<String>> indexedFolders;

	public ServerThread(Socket socket, Hashtable<String, Integer> helperList, Queue<HelperToken> helperQueue,
			Queue<SearchTask> workQueue, InvertedIndex mainII, Hashtable<String, ArrayList<String>> indexedFolders)
			throws IOException {
		this.mSocket = socket;
		mBufferedReader = new BufferedReader(new InputStreamReader(mSocket.getInputStream()));
		this.helperList = helperList;
		this.helperQueue = helperQueue;
		this.workQueue = workQueue;
		this.mainII = mainII;
		this.indexedFolders = indexedFolders;
	}

	@Override
	public void run() {
		try {
			while (((mStrMSG = mBufferedReader.readLine()) != null)) {
				mStrMSG = mStrMSG.trim();
				String[] msgs = mStrMSG.split(",");
				System.out.println("Server: Start handling the request");
				if (msgs[0].equals("c")) {
					mStrMSG = mStrMSG.substring(4, mStrMSG.length());
					if (msgs[1].equals("1")) {
						mStrMSG = mStrMSG.substring(2, mStrMSG.length());
						System.out.println("Server: This is an indexing request from Client");
						mPrintWriter = new PrintWriter(mSocket.getOutputStream(), true);

						int result = 0;
						int maxRetry = 2;
						for (int i = 0; i < maxRetry && result == 0; i++){
							IndexingMaster im = new IndexingMaster(helperList, helperQueue, mainII, indexedFolders, mStrMSG,
									Integer.parseInt(msgs[2]), (i+1), maxRetry);
							result = im.execute();
						}
						
						mPrintWriter.println(result);
						mPrintWriter.close();
						mSocket.close();
						
						
					} else if (msgs[1].equals("2")) {
						System.out.println("Server: This is a searching request from Client");
						SearchTask task = new SearchTask(mSocket, mStrMSG);
						synchronized (workQueue){
							workQueue.add(task);
						}
						
					} else {
						// bad request
						mPrintWriter = new PrintWriter(mSocket.getOutputStream(), true);
						mPrintWriter.println("0");
						System.out.println("Server: Bad request, return 0");
						mPrintWriter.close();
						mSocket.close();
					}

				} else if (msgs[0].equals("h")) {

					System.out.println("Server: This is a register request from Helper");
					int numOfThread = Integer.parseInt(msgs[1]);
					boolean flag = false;
					synchronized (helperList) {
						flag = helperList.containsKey(msgs[2]);
						if (!flag) {
							helperList.put(msgs[2], numOfThread);
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

					for (int i = 0; i < numOfThread; i++) {
						String[] tmp = msgs[2].split(":");
						int port = Integer.parseInt(tmp[1]);
						String server = tmp[0];
						synchronized (helperQueue) {
							if (!flag) {
								helperQueue.add(new HelperToken(server, port, i));
							}
						}
						
					}

					System.out.println("Server: Register Helper: " + msgs[2] + ", Number of Thread: " + numOfThread);
					mSocket.close();
				} else {
					// bad request
					mPrintWriter = new PrintWriter(mSocket.getOutputStream(), true);
					mPrintWriter.println("0");
					System.out.println("Server: Bad request, return 0");
					mPrintWriter.close();
					mSocket.close();
				}
				break;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
