import java.net.Socket;
import java.util.ArrayList;

public class SearchTask {
	private Socket socket = null;
	private ArrayList<String> wordList = new ArrayList<String>();
	private String query = "";
	
	public SearchTask(Socket socket, String query) {
		this.socket = socket;
		query = query.toLowerCase();
		String word = "";
		StringBuffer sb = new StringBuffer();
		for (char c : query.toCharArray()) {
			if (c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9') {
				sb.append(c);
			} else {
				word = sb.toString();
				if (word.length() > 0) {
					wordList.add(word);
				}
				sb = new StringBuffer();
			}

		}
		word = sb.toString();
		if (word.length() > 0) {
			wordList.add(word);
		}
		
		
		
		
		this.query = query;
	}

	public Socket getSocket() {
		return socket;
	}

	public void setSocket(Socket socket) {
		this.socket = socket;
	}

	public ArrayList<String> getWordList() {
		return wordList;
	}

	public void setWordList(ArrayList<String> wordList) {
		this.wordList = wordList;
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}
	
	
	
}
