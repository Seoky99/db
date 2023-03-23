package cs4321;

//Binary tree used to keep track of the query plan
public class Tree {
	public Tree parent;
	public Tree left;
	public Tree right;
	public String table;
	
	/**
	 * Constructor for Tree with children
	 * @param s - The table represented by the tree, null if the tree represents a join
	 * @param left - the left child of the tree
	 * @param right - the right child of the tree
	 * @return new Tree object
	 */
	public Tree(String s, Tree left, Tree right, Tree parent) {
		this.parent = parent;
		table = s;
		this.left = left;
		this.right = right;
	}
	
	/**
	 * Constructor for Tree without children
	 * @param s - name of the table represented by the tree, null if the tree represents a join
	 * @return new Tree object
	 */
	public Tree(String s, Tree parent) {
		this.parent = parent;
		table = s;
	}
	
	/**
	 * Default constructor for Tree
	 * @return new Tree object
	 */
	public Tree(Tree parent) {
		this.parent = parent;
	}

}

