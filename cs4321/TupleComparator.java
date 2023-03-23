package cs4321;

import java.util.Comparator;
import java.util.List;

//Comparator for the Tuple class
public class TupleComparator implements Comparator<Tuple> {
	private List<String> orderBy;
	
	/**
	 * Constructor for the tuple class
	 * @param orderBy - order of columns by which to do the ordering, assumed to be exhaustive
	 */
	public TupleComparator(List<String> orderBy) {
		this.orderBy = orderBy;
	}
	
	
	/**
	 * Compares two tuples
	 * @return the result of the comparison, as per the Comparator interface
	 * @param a - the first tuple to compare
	 * @param b - the second tuple to compare
	 */
	public int compare(Tuple a, Tuple b) {
		for(String s : orderBy) {
			
	
			if(a.map.get(s).intValue() != b.map.get(s).intValue()) {
				return a.map.get(s) - b.map.get(s);
			}
		}
		
		return 0;
	}
	
}
