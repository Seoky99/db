package cs4321;

/*
 * A helper class describing for some index on an attribute: its name, if the index is clustered, and the number of leaves it has. 
 */
public class AttInfo {

	private String attName;
	private boolean clustered; 
	private int numLeaves; 
	
	/**
	 * Constructor for the AttInfo class
	 * 
	 * @param attName - the attribute name: baseTable.col 
	 * @param clustered - whether it's clustered
	 * @param numLeaves - number of leaves of index 
	 *                
	 */
	public AttInfo(String attName, boolean clustered, int numLeaves) {
		this.attName = attName; 
		this.clustered = clustered; 
		this.numLeaves = numLeaves;
	}
	
	/**
	 * Returns the attName 
	 * @return - the attName, of format baseTable.col          
	 */
	public String getAttName() {
		return this.attName; 
	}
	
	/**
	 * Returns if index is clustered
	 * @return - if the index is clustered       
	 */
	public boolean getClustered() {
		return this.clustered; 
	}
	
	/**
	 * Returns the number of leaves
	 * @return - the number of leaves      
	 */
	public int getNumLeaves() {
		return this.numLeaves;
	}
	
	/**
	 * Returns a string representation of attInfo
	 * @return - String representation of attInfo [[attName,clustered,numLeaves]]     
	 */
	public String toString() {
		return "[[" + this.attName + "," + this.clustered + "," + this.numLeaves + "]]"; 
	}
	
}
