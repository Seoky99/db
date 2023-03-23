package indexes;

// Class that represents Rid, a reference to a tuple's location in the original file
public class Rid {

	public int pageId;
	public int tupleId;

	/**
	 * Constructor for Rid
	 * 
	 * @param pageId  - page where the tuple is located in the original file
	 * @param tupleId - tuple position where it is located in the original file
	 * 
	 */
	public Rid(int pageId, int tupleId) {
		this.pageId = pageId;
		this.tupleId = tupleId;
	}

	/**
	 * Outputs string of Rid's pageId and tupleId
	 * 
	 * @return string representation of the Rid
	 */
	public String toString() {
		return "PageId: " + pageId + " Tuple ID: " + tupleId;
	}

}
