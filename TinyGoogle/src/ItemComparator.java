import java.util.Comparator;

public class ItemComparator implements Comparator<IIItem>{

	@Override
	public int compare(IIItem o1, IIItem o2) {
		if (o1.getWordCount() == o2.getWordCount()){
			return o2.getCount() - o1.getCount();
		} else {
			return o2.getWordCount() - o1.getWordCount();
		}
		
	}

}
