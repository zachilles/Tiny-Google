import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Hashtable;
import java.util.LinkedList;


@SuppressWarnings("serial")
public class InvertedIndex implements Serializable{
	private Hashtable<String, LinkedList<IIItem>> list = new Hashtable<String, LinkedList<IIItem>>();
	
	public InvertedIndex() {
		
	}
	
	@SuppressWarnings("unchecked")
	private InvertedIndex(Hashtable<String, LinkedList<IIItem>> list) {
		this.list = (Hashtable<String, LinkedList<IIItem>>) list.clone();
	}
	
	
	public boolean containsKey(String key) {
		return list.containsKey(key);
	}
	
	public InvertedIndex clone() {
		return new InvertedIndex(list);
	}
	
	public int size() {
		return list.size();
	}
	
	public LinkedList<IIItem> get (String word) {
		if (list.containsKey(word)){
			return list.get(word);
		} else {
			return new LinkedList<IIItem>();
		}
		
	}
	
	
	public void remove (String id) {
		for (String word : list.keySet()) {
			remove(word, id);
		}
	}
	
	public void remove (String word, String id) {
		LinkedList<IIItem> tmpList = list.get(word);
		IIItem item = null;
		for (int i = 0; i < tmpList.size(); i++) {
			item = tmpList.get(i);
			if (item.getID().equals(id)) {
				tmpList.remove(i);
				break;
			}
		}
	}
	

	
	public void merge(InvertedIndex newII) {
		
		Hashtable<String, LinkedList<IIItem>> newList = newII.list;
		LinkedList<IIItem> tmpList = null;
		for (String word : newList.keySet()) {
			tmpList = newList.get(word);
			if (list.containsKey(word)) {
				for (int i = 0; i < tmpList.size(); i++) {
					put(word, tmpList.get(i));
				}
				
			} else {
				list.put(word, tmpList);
			}
		}
	}
	
	public void put(String word, IIItem item) {
		LinkedList<IIItem> tmpList = null;
		
		if (list.containsKey(word)){
			tmpList = list.get(word);
			boolean flag = true;
			IIItem tmpItem = null;
			
			
			for(int i = 0; i < tmpList.size(); i++) {
				tmpItem = tmpList.get(i);
				if (tmpItem.getID().equals(item.getID())){
					tmpItem.setCount(tmpItem.getCount() + item.getCount());
					flag = false;
					break;
				}
			}
			if (flag) {
				tmpList.add(item);
			}
			
			Collections.sort(tmpList, new ItemComparator());

		} else {
			tmpList = new LinkedList<IIItem>();
			tmpList.add(item);
			list.put(word, tmpList);
		}
	}
	
	@SuppressWarnings("unchecked")
	public void put(String word, LinkedList<IIItem> tmpList) {
		list.put(word, (LinkedList<IIItem>) tmpList.clone());
	}
	
	public String output(String key) {
		String result = "";
		
		LinkedList<IIItem> tmpList = list.get(key);
		// result = result + key +"\t";
		for (int i = 0; i < tmpList.size(); i++) {
			result = result + tmpList.get(i).getID() + ":" + tmpList.get(i).getCount() + ",";
		}
		
		result = result.substring(0, result.length()-1);
		return result;
	}
	
	public String toString() {
		String result = "";
		for (String key : list.keySet()) {
			LinkedList<IIItem> tmpList = list.get(key);
			result = result + key +"\t";
			for (int i = 0; i < tmpList.size(); i++) {
				result = result + tmpList.get(i).getID() + ":" + tmpList.get(i).getCount() + ",";
			}
			result = result.substring(0, result.length()-1) + "\n";
		}
		result = result.substring(0, result.length()-1);
		return result;
	}
	
	public String showResult() {
		int output = 0;
		String result = "";
		int[] counts = new int[list.size()];
		int count = 0;
		for (String index : list.keySet()) {
			counts[count] = Integer.parseInt(index);
			count++;
		}
		Arrays.sort(counts);
		boolean flag = false;
		for (int i = counts.length-1; i >= 0; i--) {
			if (flag) {
				break;
			}
			
			LinkedList<IIItem> tmpList = list.get(counts[i]+"");
			result = result + "Keyword matched: " + counts[i] + "\n";
			for (int j = 0; j < tmpList.size(); j++) {
				result = result + tmpList.get(j).getID() + " WC: " + tmpList.get(j).getCount() + "\n";
				output++;
				if (output == 10) {
					flag = true;
					break;
				}
				
			}
			if (!result.equals("")) {
				result = result.substring(0, result.length()-1);
			}
			result = result + "\n";
			
			
		}
		if (result.equals("")) {
			result = "Sorry there is no document contains any of the keywords in your searching query, please try again...";
		}
		return result;
	}
	
//	public static void main(String[] args) {
//		// TODO Auto-generated method stub
//		IIItem i1 = new IIItem("1", 1);
//		IIItem i2 = new IIItem("2", 2);
//		IIItem i3 = new IIItem("3", 3);
//		IIItem i4 = new IIItem("2", 3);
//		
//		InvertedIndex ii = new InvertedIndex();
//		ii.put("a", i2);
//		ii.put("a", i3);
//		ii.put("a", i1);
//		ii.put("a", i4);
//		
//		IIItem i21 = new IIItem("1", 1);
//		IIItem i22 = new IIItem("2", 2);
//		IIItem i23 = new IIItem("3", 3);
//		IIItem i24 = new IIItem("2", 3);
//		InvertedIndex ii2 = new InvertedIndex();
//		ii2.put("a", i22);
//		ii2.put("a", i23);
//		ii2.put("a", i21);
//		ii2.put("a", i24);
//		
//		ii.merge(ii2);
//		System.out.println(ii.get("a").get(0).getCount());
//		
//	}

}
