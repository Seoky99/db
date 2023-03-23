package indexes;

import java.util.List;

// Represents an index node with keys, children, and its address 
public class IndexNode implements Node {
	public List<Integer> keys;
	public List<Node> children;
	private int address;

	/**
	 * Constructor for IndexNode
	 * 
	 * @param keys     - list of keys in the node
	 * @param children - list of children nodes in order
	 * @param address  - page where it is located in the file
	 */
	public IndexNode(List<Integer> keys, List<Node> children, int address) {
		this.keys = keys;
		this.children = children;
		this.address = address;
	}

	/**
	 * Gets key of the first children node in the list
	 * 
	 * @return key of the first child in the children node list
	 */
	public int getKey() {
		return children.get(0).getKey();
	}

	/**
	 * Gets address of index node
	 * 
	 * @return address of the index node
	 */
	public int getAddress() {
		return address;
	}
}
