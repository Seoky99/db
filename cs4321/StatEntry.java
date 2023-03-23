package cs4321;

import java.util.HashMap;

/*
 * A helper class bundling info about a relation 
 */
public class StatEntry {
	
	private int numTuples;
	private HashMap<String, int[]> attributeMap; 
	
	/**
	 * Constructor for a StatEntry
	 * @param numTuples - the number of tuples in the relation 
	 * @param attributeMap - a map from the attributes to the minimum and maximum values they take on
	 */
	public StatEntry(int numTuples, HashMap<String, int[]> attributeMap) {
		this.numTuples = numTuples; 
		this.attributeMap = attributeMap; 
	}
	
	/**
	 * Get the number of tuples in the relation 
	 * @return - the number of tuples in the relation 
	 */
	public int getTuples() {
		return this.numTuples; 
	}
	
	/**
	 * Get the attribute map: a map from the attributes to the minimum and maximum values they take on
	 * @return - the attribute map 
	 */
	public HashMap<String, int[]> getAttributeMap() {
		return this.attributeMap; 
	}
	
	/**
	 * Return the string representation of a StatEntry
	 * 
	 * @return - The string representation of a StatEntry
	 */
	public String toString() {
		return String.valueOf(numTuples) + "," + this.attributeMap; 
	}

}
