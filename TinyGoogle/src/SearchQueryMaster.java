import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SearchQueryMaster implements Runnable {

	private SearchTask task;
	private Hashtable<String, Integer> helperList;
	private Queue<HelperToken> helperQueue;
	private InvertedIndex mainII;
	private Hashtable<String, ArrayList<String>> indexedFolders;
	private PrintWriter mPrintWriter;
	private int waitTime = 60;
	private int retry = 1;
	private int maximumRetry = 2;

	public SearchQueryMaster(SearchTask task, Hashtable<String, Integer> helperList, Queue<HelperToken> helperQueue,
			InvertedIndex mainII, Hashtable<String, ArrayList<String>> indexedFolders) throws IOException {
		this.task = task;
		this.helperList = helperList;
		this.helperQueue = helperQueue;
		this.mainII = mainII;
		this.indexedFolders = indexedFolders;
		new BufferedReader(new InputStreamReader(task.getSocket().getInputStream()));
	}

	@Override
	public void run() {
		System.out.println("Search Query Master: Start to work on query \"" + task.getQuery() + "\" from "
				+ task.getSocket().getRemoteSocketAddress());
		ArrayList<Hashtable<String, Object>> paraList = new ArrayList<Hashtable<String, Object>>();

		InvertedIndex searchingII = new InvertedIndex();
		for (int i = 0; i < task.getWordList().size(); i++) {
			if (mainII.containsKey(task.getWordList().get(i))) {
				searchingII.put(task.getWordList().get(i), mainII.get(task.getWordList().get(i)));
			}
		}

		int result = 1;
		InvertedIndex resultII = null;
		if (searchingII.size() > 0) {
			for (String folder : indexedFolders.keySet()) {
				Hashtable<String, Object> parameters = new Hashtable<String, Object>();
				parameters.put("folder", folder);
				parameters.put("wordList", task.getWordList());
				parameters.put("fileList", indexedFolders.get(folder));
				parameters.put("ii", searchingII);
				paraList.add(parameters);
			}

			do {
				try {
					resultII = new InvertedIndex();
					ExecutorService mExecutorService = Executors.newCachedThreadPool();
					Hashtable<String, HelperToken> htList = new Hashtable<String, HelperToken>();
					Hashtable<String, InvertedIndex> iiList = new Hashtable<String, InvertedIndex>();
					for (int i = 0; i < paraList.size(); i++) {

						HelperToken ht;

						do {

							while (helperQueue.isEmpty()) {
								System.out
										.println("Search Query Master: There is no available helper now, wait for 1s.");

								Thread.sleep(1000);
							}
							synchronized (helperQueue) {
								if (!helperQueue.isEmpty()) {
									ht = helperQueue.remove();

								} else {
									ht = null;
								}

							}
						} while (!helperList.containsKey(ht.getServer() + ":" + ht.getPort()) || ht == null);
						System.out.println(
								"Search Query Master: Assign a helper to work on " + paraList.get(i).get("folder"));
						mExecutorService.execute(new SearchQueryMasterThread(paraList.get(i), iiList, ht, helperQueue));
						htList.put((String) paraList.get(i).get("folder"), ht);
					}

					System.out.println("Search Query Master: All jobs were assigned, wait for helpers");
					mExecutorService.shutdown();
					if (!mExecutorService.awaitTermination((waitTime * retry), TimeUnit.SECONDS)) {
						if (retry < maximumRetry) {
							System.out.println("Search Query Master: Threads didn't finish in " + (waitTime * retry)
									+ " seconds! Increase waiting time to " + (waitTime * (retry + 1))
									+ " seconds and retry!");
						} else {
							System.out.println("Search Query Master: Threads didn't finish in " + (waitTime * retry)
									+ " seconds! Report Error!");
						}
						result = 0;
					} else if (iiList.size() != paraList.size()) {
						if (retry < maximumRetry) {
							System.out.println("Search Query Master: At least one thread have problem! Retry!");
						} else {
							System.out.println("Search Query Master: At least one thread have problem! Report Error!");
						}
						result = 0;
					} else {
						System.out.println("Search Query Master: All jobs done, merging results.");
						long startTime = System.currentTimeMillis();
						for (int i = 0; i < paraList.size(); i++) {
							resultII.merge(iiList.get(paraList.get(i).get("folder")));
						}
						long endTime = System.currentTimeMillis();
						Util.reduceTime = Util.reduceTime + (endTime - startTime);
						// System.out.println(localII.toString());
					}

					for (int i = 0; i < paraList.size(); i++) {
						if (!iiList.containsKey(paraList.get(i).get("folder"))) {
							synchronized (helperQueue) {
								helperQueue.add(htList.get(paraList.get(i).get("folder")));
							}
						}
					}

					result = 1;

				} catch (Exception e) {
					System.out.println("Search Query Master: Error happen when searching " + task.getQuery());
					result = 0;
				}

				retry = retry + 1;
			} while (result == 0 || retry < maximumRetry);
		}

		try {
			mPrintWriter = new PrintWriter(task.getSocket().getOutputStream(), true);
			if (result == 0) {
				mPrintWriter.println(result);
			} else {
				if (Util.stat) {
					System.out.println("Server: Mapper Time: " + Util.mapTime + ", Reducer Time: " + Util.reduceTime);

				}
				mPrintWriter.println(result + "," + Util.toString(resultII));
				// System.out.println(resultII.toString());
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
