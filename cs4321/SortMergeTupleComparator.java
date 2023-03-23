package cs4321;

import java.util.Comparator;
import java.util.List;

//Comparator for the Tuple class
public class SortMergeTupleComparator implements Comparator<Tuple> {
	private List<String> leftOrderBy;
	private List<String> rightOrderBy;
	
	/**
	 * Constructor for the tuple class
	 * 
	 * @param orderBy - order of columns by which to do the ordering, assumed to be
	 *                exhaustive
	 */
	public SortMergeTupleComparator(List<String> leftOrderBy, List<String> rightOrderBy) {
		this.leftOrderBy = leftOrderBy;
		this.rightOrderBy = rightOrderBy;
	}

	/** Comparator if two tuples are equal. Necessary for checking if tuples are null and not throw an exception.
	 * @param a, b - The tuples 
	 * @return - Whether the two tuples are equal. Any one tuple being null makes this false. 
	 */
	public boolean isEqual(Tuple a, Tuple b) {
		
		if (a==null || b==null) {
			
			return false;  
		}
		
		for (int i = 0; i < leftOrderBy.size(); i++) {

			int firstValue = a.map.get(leftOrderBy.get(i)).intValue();
			int secondValue = b.map.get(rightOrderBy.get(i)).intValue();

			if (firstValue < secondValue) {

				return false; 
			} else if (firstValue > secondValue) {

				return  false;
			}
		}
		return true;
		
	}
	
	/**
	 * Compares two tuples
	 * 
	 * @return the result of the comparison, as per the Comparator interface
	 * @param a - the first tuple to compare
	 * @param b - the second tuple to compare
	 */
	public int compare(Tuple a, Tuple b) {
		
		if(a == null || b == null) {
			return 0;
		}

		for (int i = 0; i < leftOrderBy.size(); i++) {

			int firstValue = a.map.get(leftOrderBy.get(i)).intValue();
			int secondValue = b.map.get(rightOrderBy.get(i)).intValue();

			if (firstValue < secondValue) {

				return -1;
			} else if (firstValue > secondValue) {

				return 1;
			}
		}
		return 0;

	}

}
