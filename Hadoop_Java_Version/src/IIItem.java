import java.io.Serializable;

@SuppressWarnings("serial")
public class IIItem implements Serializable{
	private String id = "";
	private int count = 0;
	private int wordCount = 0;
	
	
	public IIItem(String id, int count) {
		this.id = id;
		this.count = count;
	}
	
	public IIItem(String id, int count, int wordCount) {
		this.id = id;
		this.count = count;
		this.wordCount = wordCount;
	}

	public String getID() {
		return id;
	}

	public void setID(String id) {
		this.id = id;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public int getWordCount() {
		return wordCount;
	}

	public void setWordCount(int wordCount) {
		this.wordCount = wordCount;
	}
	
	
	
}
