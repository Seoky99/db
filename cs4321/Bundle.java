package cs4321;

import java.util.HashSet;


/**
 * A collection of columns that are all mutually constrained
 */
public class Bundle {
	public Integer upperBound;
	public Integer lowerBound;
	public Integer equalTo;
	public HashSet<String> columns;
	
	/**
	 * Constructor with just an attribute
	 */
	public Bundle(String att) {
		columns = new HashSet<String>();
		columns.add(att);
		upperBound = Integer.MAX_VALUE;
		lowerBound = Integer.MIN_VALUE;
		equalTo = null;
	}
	
	/**
	 * Bundle constructor with bounds and columns
	 * @param upperBound - upper bound for all constrained columns, SHOULD NEVER BE NULL
	 * @param lowerBound - lower bound for all constrained columns, SHOULD NEVER BE NULL
	 * @param equalTo - value that all the columns are constrained to be equal to, null if none exists
	 * @param columns - all of the constrained columns
	 */
	public Bundle(Integer upperBound, Integer lowerBound, Integer equalTo, HashSet<String> columns) {
		this.upperBound = upperBound;
		this.lowerBound = lowerBound;
		this.equalTo = equalTo;
		this.columns = columns;
	}
	
	/**
	 * Sets upperBound
	 * @param newUpperBound - new upper bound
	 */
	public void setUpperBound(Integer newUpperBound) {
		this.upperBound = newUpperBound;
	}
	
	/**
	 * Sets lower bound
	 * @param newLowerBound - new lower bound
	 */
	public void setLowerBound(Integer newLowerBound) {
		this.lowerBound = newLowerBound;
	}
	
	/**
	 * Sets equalTo (also sets lower and upper bounds appropriately)
	 * @param newEqualTo - new equalTo
	 */
	public void setEqualTo(Integer newEqualTo) {
		this.equalTo = newEqualTo;
		this.lowerBound = newEqualTo;
		this.upperBound = newEqualTo;
	}
	
	/**
	 * Merges two bundles
	 * @param b - The other bundle to be merged
	 * @return - The result of merging the two bundles
	 */
	public Bundle merge(Bundle b) {
		HashSet<String> newColumns = new HashSet<String>();
		Integer newUpperBound;
		Integer newLowerBound;
		Integer newEqualTo = null;
		for(String s : b.columns) {
			newColumns.add(s);
		}
		for(String s : this.columns) {
			newColumns.add(s);
		}
		
		if(this.equalTo != null) {
			newUpperBound = equalTo;
			newLowerBound = equalTo;
			newEqualTo = equalTo;
		} else {
			if(b.equalTo != null) {
				newUpperBound = b.equalTo;
				newLowerBound = b.equalTo;
				newEqualTo = b.equalTo;
			} else {
				newUpperBound = Math.min(b.upperBound, upperBound);
				newLowerBound = Math.max(b.lowerBound, lowerBound);
			}
		}
		
		return new Bundle(newUpperBound, newLowerBound, newEqualTo, newColumns);
	}
}
