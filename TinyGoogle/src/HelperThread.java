import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.LinkedList;

public class HelperThread implements Runnable {
	private Socket mSocket;
	private BufferedReader mBufferedReader;
	private PrintWriter mPrintWriter;
	private String mStrMSG = "";

	public HelperThread(Socket socket) throws IOException {
		this.mSocket = socket;
		mBufferedReader = new BufferedReader(new InputStreamReader(mSocket.getInputStream()));
	}

	@SuppressWarnings("unchecked")
	@Override
	public void run() {
		try {
			while (((mStrMSG = mBufferedReader.readLine()) != null)) {
				int result = 1;
				mStrMSG = mStrMSG.trim();
				long startTime=0;
				long endTime=0;
				String[] msgs = mStrMSG.split(",");
				System.out.println("Helper: Start handling the request");
				if (msgs[0].equals("0")) {
					System.out.println("Helper: Heartbeat request, return 1");
					mPrintWriter = new PrintWriter(mSocket.getOutputStream(), true);
					mPrintWriter.println(result);
					mPrintWriter.close();
					mSocket.close();

				} else if (msgs[0].equals("1")) {
					startTime = System.currentTimeMillis();
					int lineNum = Integer.parseInt(msgs[1]);
					System.out.println("Helper: Indexing request, start working");
					mPrintWriter = new PrintWriter(mSocket.getOutputStream(), true);

					mStrMSG = mStrMSG.substring(3+msgs[1].length(), mStrMSG.length());
					InvertedIndex ii = new InvertedIndex();

					String line = "";
					String word = "";
					try {
						Hashtable<String, Integer> wordCount = new Hashtable<String, Integer>();

						// FileReader reads text files in the default encoding.
						FileReader fileReader = new FileReader(mStrMSG);

						// Always wrap FileReader in BufferedReader.
						BufferedReader bufferedReader = new BufferedReader(fileReader);
						int count = 0;
						while ((line = bufferedReader.readLine()) != null) {
							if(count < lineNum) {
								
							} else if (count == lineNum + 1000){
								break;
							} else {
								line = line.trim().toLowerCase();
								StringBuffer sb = new StringBuffer();
								for (char c : line.toCharArray()) {
									if (c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9') {
										sb.append(c);
									} else {
										word = sb.toString();
										if (word.length() > 0) {
											if (wordCount.containsKey(word)) {
												wordCount.put(word, wordCount.get(word) + 1);
											} else {
												wordCount.put(word, 1);
											}
										}
										sb = new StringBuffer();
									}

								}
								word = sb.toString();
								if (word.length() > 0) {
									if (wordCount.containsKey(word)) {
										wordCount.put(word, wordCount.get(word) + 1);
									} else {
										wordCount.put(word, 1);
									}
								}
							}
							count++;
							
						}

						for (String key : wordCount.keySet()) {
							ii.put(key, new IIItem(mStrMSG, wordCount.get(key)));
						}
						// Always close files.
						bufferedReader.close();
					} catch (FileNotFoundException ex) {
						result = 0;
						System.out.println("Helper: Unable to open file '" + mStrMSG + "'");
					} catch (IOException ex) {
						result = 0;
						System.out.println("Helper: Error reading file '" + mStrMSG + "'");
						// Or we could just do this:
						// ex.printStackTrace();
					}
					if (result == 1) {
						System.out.println("Helper: Job done. Return result");
						endTime = System.currentTimeMillis();
						Util.mapTime = Util.mapTime + (endTime - startTime);
						// System.out.println(ii.toString());
						if (Util.TCP) {
							mPrintWriter.println(result + "," + Util.toString(ii));
						} else {
							int length = UDPUtil.LENGTH;
							int port = Util.availablePort();
							String send = Util.toString(ii);
							//System.out.println(send.length());
							int bulks = send.length() / length;
							int remain = send.length() % length;
							if (remain > 0) {
								bulks = bulks + 1;
							}
							mPrintWriter.println(result + "," + port);
							String remotePort = mBufferedReader.readLine();
							int iPort = Integer.parseInt(remotePort);
							for (int i = 0; i < bulks; i++) {
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
							Util.portTable.remove(port);
						}
						
						
						
					} else {
						mPrintWriter.println(result);
					}

					mPrintWriter.close();
					mSocket.close();
				} else if (msgs[0].equals("2")) {
					startTime = System.currentTimeMillis();
					System.out.println("Helper: Searching request, start working");
					mPrintWriter = new PrintWriter(mSocket.getOutputStream(), true);

					mStrMSG = mStrMSG.substring(2, mStrMSG.length());

					InvertedIndex ii = new InvertedIndex();

					String folder = "";
					try {
						Hashtable<String, Object> parameters = (Hashtable<String, Object>) Util.fromString(mStrMSG);
						folder = (String) parameters.get("folder");
						ArrayList<String> wordList = (ArrayList<String>) parameters.get("wordList");
						ArrayList<String> fileList = (ArrayList<String>) parameters.get("fileList");
						InvertedIndex searchingII = (InvertedIndex) parameters.get("ii");

						Hashtable<String, Integer> hashedFileList = new Hashtable<String, Integer>();
						for (int i = 0; i < fileList.size(); i++) {
							hashedFileList.put(fileList.get(i), 1);
						}

						Hashtable<String, Integer> countTable = new Hashtable<String, Integer>();
						InvertedIndex tmpII = new InvertedIndex();

						for (int i = 0; i < wordList.size(); i++) {
							String word = wordList.get(i);
							if (searchingII.containsKey(word)) {
								LinkedList<IIItem> list = searchingII.get(word);
								for (int j = 0; j < list.size(); j++) {
									if (hashedFileList.containsKey(list.get(j).getID())) {
										tmpII.put("result", list.get(j));
										if (countTable.containsKey(list.get(j).getID())) {
											countTable.put(list.get(j).getID(),
													countTable.get(list.get(j).getID()) + 1);
										} else {
											countTable.put(list.get(j).getID(), 1);
										}
									}
								}
							}
						}
						
						LinkedList<IIItem> list = tmpII.get("result");
						for (int i = 0; i < list.size(); i++) {
							ii.put(countTable.get(list.get(i).getID())+"", list.get(i));
						}
						

					} catch (ClassNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						result = 0;
						System.out.println("Helper: Error searching '" + folder + "'");
					}

					if (result == 1) {
						System.out.println("Helper: Job done. Return result");
						endTime = System.currentTimeMillis();
						Util.mapTime = Util.mapTime + (endTime - startTime);
						// System.out.println(ii.toString());
						if (Util.TCP) {
							mPrintWriter.println(result + "," + Util.toString(ii));
						} else {
							int length = UDPUtil.LENGTH;
							int port = Util.availablePort();
							String send = Util.toString(ii);
							//System.out.println(send.length());
							int bulks = send.length() / length;
							int remain = send.length() % length;
							if (remain > 0) {
								bulks = bulks + 1;
							}
							mPrintWriter.println(result + "," + port);
							String remotePort = mBufferedReader.readLine();
							int iPort = Integer.parseInt(remotePort);
							for (int i = 0; i < bulks; i++) {
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
							Util.portTable.remove(port);
						}
					} else {
						mPrintWriter.println(result);
					}

					mPrintWriter.close();
					mSocket.close();
				} else if (msgs[0].equals("3")) {
					
					
					
					mPrintWriter = new PrintWriter(mSocket.getOutputStream(), true);
					mStrMSG = mStrMSG.substring(2, mStrMSG.length());
					if (Util.TCP){
						mPrintWriter = new PrintWriter(mSocket.getOutputStream(), true);

						
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
							try {
								String ip = mSocket.getInetAddress().toString();
								ip = ip.substring(1, ip.length());
								//System.out.println(mSocket.getInetAddress().toString());
								tmp = UDPUtil.receive(port, remotePort, ip);
							} catch (Exception e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							// mBufferedReader.readLine();
							mPrintWriter.println("1");
						} while (!tmp.equals("This is the end"));
						Util.portTable.remove(port);
					}
					
					
					
					
					
					
					startTime = System.currentTimeMillis();
					Hashtable<String, Object> parameters = null;
					try {
						parameters = (Hashtable<String, Object>) Util.fromString(mStrMSG);
					} catch (ClassNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					Hashtable<String, InvertedIndex> iiList = (Hashtable<String, InvertedIndex>) parameters.get("iiList");
					ArrayList<String> fileList = (ArrayList<String>) parameters.get("fileList");
					InvertedIndex localII = new InvertedIndex();
					for (int i = 0; i < fileList.size(); i++) {
						localII.merge(iiList.get(fileList.get(i)));
					}
					System.out.println("Helper: Job done. Return result");
					endTime = System.currentTimeMillis();
					Util.reduceTime = Util.reduceTime + (endTime - startTime);
					
					
					
					if (Util.TCP) {
						mPrintWriter.println("1," + Util.toString(localII));
					} else {
						int length = UDPUtil.LENGTH;
						int port = Util.availablePort();
						String send = Util.toString(localII);
						//System.out.println(send.length());
						int bulks = send.length() / length;
						int remain = send.length() % length;
						if (remain > 0) {
							bulks = bulks + 1;
						}
						mPrintWriter.println("1," + port);
						String remotePort = mBufferedReader.readLine();
						int iPort = Integer.parseInt(remotePort);
						for (int i = 0; i < bulks; i++) {
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
						Util.portTable.remove(port);
					}
					
					mPrintWriter.close();
					mSocket.close();
				} else {
					// bad request
					mPrintWriter = new PrintWriter(mSocket.getOutputStream(), true);
					mPrintWriter.println("0");
					System.out.println("Helper: Bad request, return 0");
					mPrintWriter.close();
					mSocket.close();
				}
				break;
			}
			if (Util.stat){
				System.out.println("Server: Mapper Time: " + Util.mapTime + ", Reducer Time: " + Util.reduceTime);
				
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
