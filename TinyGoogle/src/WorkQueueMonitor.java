import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class WorkQueueMonitor implements Runnable{
	private Queue<SearchTask> workQueue = null;
	private int numOfSQM = 0;
	private ExecutorService mExecutorService;
	
	private Hashtable<String, Integer> helperList;
	private Queue<HelperToken> helperQueue;
	private InvertedIndex mainII;
	private Hashtable<String, ArrayList<String>> indexedFolders;
	
	public WorkQueueMonitor (Queue<SearchTask> workQueue, int numOfSQM, Hashtable<String, Integer> helperList, Queue<HelperToken> helperQueue, InvertedIndex mainII,
			Hashtable<String, ArrayList<String>> indexedFolders) {
		this.workQueue = workQueue;
		this.numOfSQM = numOfSQM;
		
		this.helperList = helperList;
		this.helperQueue = helperQueue;
		this.mainII = mainII;
		this.indexedFolders = indexedFolders;
	}
	
	@Override
	public void run() {
		mExecutorService = Executors.newFixedThreadPool(numOfSQM);
		while(true){
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (workQueue.size()>0) {
				try {
					SearchTask task;
					synchronized (workQueue) {
						task = workQueue.remove();
					}
				
					mExecutorService.execute(new SearchQueryMaster(task, helperList, helperQueue, mainII, indexedFolders));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
	}

}
