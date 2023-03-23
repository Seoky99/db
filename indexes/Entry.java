package indexes;

import java.util.List;

//Represents an Entry with an index key, and its rids corresponding to that key
public class Entry {

	public int indexKey;
	public List<Rid> ridList;

	/**
	 * Constructor for an Entry
	 * 
	 * @param indexKey - index key for the entry
	 * @param ridList  - list of rids for that key
	 */
	public Entry(int indexKey, List<Rid> ridList) {
		this.indexKey = indexKey;
		this.ridList = ridList;
	}

	/**
	 * Outputs string representation of entry with keys and ridlist
	 * 
	 * @return string representation of the entry
	 */
	public String toString() {
		return "Index: " + indexKey + "Rid's: " + ridList + "\n";
	}

}
