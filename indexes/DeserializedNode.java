package indexes;

import java.util.List;

//A tangible node that keeps track of the entries if it's an leaf, or the keys and addresses of its children if its an index.
public class DeserializedNode {
	public boolean isIndex;
	public List<Integer> keys;
	public List<Entry> entries;
	public List<Integer> addresses;
	
	/**
	 * Constructor for deserialized leaf node
	 * @param entries - entries of the leaf node
	 */
	public DeserializedNode(List<Entry> entries) {
		this.entries = entries;
		isIndex = false;
	}
	
	/**
	 * Constructor for deserialized index node
	 * @param keys - keys of the index node
	 * @param addresses - addresses of the children of the index node
	 */
	public DeserializedNode(List<Integer> keys, List<Integer> addresses) {
		this.keys = keys;
		this.addresses = addresses;
		isIndex = true;
	}
}