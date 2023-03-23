package indexes;

import java.util.ArrayList;
import java.util.List;

//Inner index node representation
public class InnerIndexNode {

	public List<Integer> keys;
	public List<InnerIndexNode> innerIndexList;
	public List<LeafIndexNode> leafIndexList;
	public boolean childrenInner;

	/**
	 * Constructor for an inner index node 
	 * @param keys: the keys 
	 */
	public InnerIndexNode(ArrayList<Integer> keys) {
		this.keys = keys;
		childrenInner = false;
	}

	/**
	 * Builds the leaf index layer 
	 * @param leafIndexList: the leaf index nodes as a list 
	 */
	public void buildLeaf(ArrayList<LeafIndexNode> leafIndexList) {
		this.leafIndexList = leafIndexList;
		childrenInner = false;
	}

	/**
	 * Builds the inner index layer 
	 * @param innerIndexList: the inner index nodes as a list 
	 */
	public void buildInner(ArrayList<InnerIndexNode> innerIndexList) {
		this.innerIndexList = innerIndexList;
		childrenInner = true;
	}

}
