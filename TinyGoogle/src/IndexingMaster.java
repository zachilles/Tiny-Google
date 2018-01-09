import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class IndexingMaster {

	private Hashtable<String, Integer> helperList;
	private Queue<HelperToken> helperQueue;
	private InvertedIndex mainII;
	private Hashtable<String, ArrayList<String>> indexedFolders;
	private String path;
	private boolean recursion;
	private ArrayList<String> fileList;
	private LinkedList<String> folderList = new LinkedList<String>();
	private int waitTime = 60;
	private int retry = 1;
	private int maximumRetry = 3;

	public IndexingMaster(Hashtable<String, Integer> helperList, Queue<HelperToken> helperQueue, InvertedIndex mainII,
			Hashtable<String, ArrayList<String>> indexedFolders, String path, int recursion, int retry, int maximumRetry) {
		this.helperList = helperList;
		this.helperQueue = helperQueue;
		this.mainII = mainII;
		this.indexedFolders = indexedFolders;
		this.path = path;
		if (recursion == 0) {
			this.recursion = false;
		} else {
			this.recursion = true;
		}
		this.folderList.add(this.path);
		this.retry = retry;
		this.maximumRetry = maximumRetry;

	}

	private ArrayList<String> getFileList(String path) {
		ArrayList<String> filelist = new ArrayList<String>();
		// int fileNum = 0, folderNum = 0;
		File file = new File(path);
		String fileName = "";
		if (file.exists()) {
			File[] files = file.listFiles();
			for (File file2 : files) {
				fileName = file2.getName();
				if (file2.isDirectory()) {
					folderList.add(file2.getAbsolutePath());
					// folderNum++;
				} else if (!fileName.startsWith(".")) {
					filelist.add(file2.getAbsolutePath());
					// fileNum++;
				}
			}
		}
		return filelist;
	}

	@SuppressWarnings("unchecked")
	private int updateLists(ArrayList<String> removeList, Hashtable<String, ArrayList<String>> localIndexedFolders,
			InvertedIndex localII) {
		synchronized (mainII) {
			InvertedIndex backUpII = mainII.clone();
			Hashtable<String, ArrayList<String>> backUpIF = (Hashtable<String, ArrayList<String>>) indexedFolders
					.clone();
			try {
				for (int i = 0; i < removeList.size(); i++) {
					mainII.remove(removeList.get(i));
				}

				for (String key : localIndexedFolders.keySet()) {
					indexedFolders.put(key, localIndexedFolders.get(key));
				}

				mainII.merge(localII);
				
				BufferedWriter out = null;
				try {
					out = new BufferedWriter(new FileWriter("mainii.model"));
					out.write(Util.toString(mainII));
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					try {
						out.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				
				try {
					out = new BufferedWriter(new FileWriter("indexedfolders.model"));
					out.write(Util.toString(indexedFolders));
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					try {
						out.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				
			} catch (Exception e) {
				System.out.println("Indexing Master: Error happen when updating lists! Now roll back!");
				mainII = backUpII;
				indexedFolders = backUpIF;
				BufferedWriter out = null;
				try {
					out = new BufferedWriter(new FileWriter("mainii.model"));
					out.write(Util.toString(mainII));
				} catch (IOException e1) {
					e.printStackTrace();
				} finally {
					try {
						out.close();
					} catch (IOException e1) {
						e.printStackTrace();
					}
				}
				
				try {
					out = new BufferedWriter(new FileWriter("indexedfolders.model"));
					out.write(Util.toString(indexedFolders));
				} catch (IOException e1) {
					e.printStackTrace();
				} finally {
					try {
						out.close();
					} catch (IOException e1) {
						e.printStackTrace();
					}
				}
				return 0;
			}
			System.out.println("Indexing Master: Finished! Return result.");
			//System.out.println(mainII.toString());
			return 1;
		}

	}

	public int execute() {
		System.out.println("Indexing Master: Start working on " + path);
		
		int result = 1;
		ArrayList<String> removeList = new ArrayList<String>();
		Hashtable<String, ArrayList<String>> localIndexedFolders = new Hashtable<String, ArrayList<String>>();
		InvertedIndex localII = new InvertedIndex();
		
		try {
			while (!folderList.isEmpty()) {
				System.out.println("Test: " + folderList.size());
				path = folderList.removeFirst();
				System.out.println("Test: " + path);
				fileList = getFileList(path);

				if (indexedFolders.containsKey(path)) {
					ArrayList<String> list = indexedFolders.get(path);
					for (int i = 0; i < list.size(); i++) {
						removeList.add(list.get(i));
					}
				}

				ExecutorService mExecutorService = Executors.newCachedThreadPool();

				// create index for this folder
				// get results from other helpers
				Hashtable<String, InvertedIndex> iiList = new Hashtable<String, InvertedIndex>();
				Hashtable<String, HelperToken> htList = new Hashtable<String, HelperToken>();
				for (int i = 0; i < fileList.size(); i++) {
					
					HelperToken ht;

					do {
						
							while (helperQueue.isEmpty()) {
								System.out.println("Indexing Master: There is no available helper now, wait for 1s.");
								
								Thread.sleep(1000);
							}
						synchronized (helperQueue) {
							if(!helperQueue.isEmpty()){
								ht = helperQueue.remove();
								
							} else {
								ht = null;
							}
							
						}
						//System.out.println(ht.getServer() + ":" + ht.getPort());
						//System.out.println(helperList.containsKey(ht.getServer() + ":" + ht.getPort()));
					} while (!helperList.containsKey(ht.getServer() + ":" + ht.getPort()) || ht == null);
					
					System.out.println("Indexing Master: Assign a helper to work on " + fileList.get(i));
					mExecutorService.execute(new IndexingMasterThread(fileList.get(i), iiList, ht, helperQueue));
					
					htList.put(fileList.get(i), ht);
				}

				System.out.println("Indexing Master: All jobs were assigned, wait for helpers");
				mExecutorService.shutdown();
				if (!mExecutorService.awaitTermination((waitTime * retry), TimeUnit.SECONDS)) {
					if (retry < maximumRetry) {
						System.out.println("Indexing Master: Threads didn't finish in " + (waitTime * retry)
								+ " seconds! Increase waiting time to " + (waitTime * (retry + 1)) + " seconds and retry!");
					} else {
						System.out.println("Indexing Master: Threads didn't finish in " + (waitTime * retry)
								+ " seconds! Report Error!");
					}
					result = 0;
				} else if (iiList.size() != fileList.size()) {
					if (retry < maximumRetry) {
						System.out.println("Indexing Master: At least one thread have problem! Retry!");
					} else {
						System.out.println("Indexing Master: At least one thread have problem! Report Error!");
					}
					result = 0;
				} else {
					System.out.println("Indexing Master: All jobs done, merging results.");
					
					
					HelperToken ht;

					do {
						
							while (helperQueue.isEmpty()) {
								System.out.println("Indexing Master: There is no available helper now, wait for 1s.");
								
								Thread.sleep(1000);
							}
						synchronized (helperQueue) {
							if(!helperQueue.isEmpty()){
								ht = helperQueue.remove();
							} else {
								ht = null;
							}
							
						}
					} while (!helperList.containsKey(ht.getServer() + ":" + ht.getPort()) || ht == null);
					
					IndexingReducerMaster irm = new IndexingReducerMaster(iiList, fileList, ht, helperQueue);
					localII = irm.merge();
					
					
					
//					for (int i = 0; i < fileList.size(); i++) {
//						localII.merge(iiList.get(fileList.get(i)));
//					}
					//System.out.println(localII.toString());
				}
				
				
				for (int i = 0; i < fileList.size(); i++) {
					if (!iiList.containsKey(fileList.get(i))) {
						synchronized (helperQueue) {
							helperQueue.add(htList.get(fileList.get(i)));
						}
					}
				}
				
				if (result == 0) {
					break;
				}
				
				localIndexedFolders.put(path, fileList);

				if (!recursion) {
					break;
				}
			}
		} catch (Exception e) {
			System.out.println("Indexing Master: Error happen when creating index for " + path);
			result = 0;
		}

		if (result == 1) {
			result = updateLists(removeList, localIndexedFolders, localII);
		}
		
		if (Util.stat){
			System.out.println("Server: Mapper Time: " + Util.mapTime + ", Reducer Time: " + Util.reduceTime);
			
		}
		return result;
	}
}
