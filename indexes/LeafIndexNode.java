package indexes;

import java.util.List;

public class LeafIndexNode implements Node {
	
	public List<Entry> entryList;
	private int address;
	
	public LeafIndexNode(List<Entry> entryList, int address) {
		this.entryList = entryList;
		this.address = address;
	}
	
	public int getKey() {
		return entryList.get(0).indexKey;
	}
	
	public int getAddress() {
		return address;
	}
}
