package tools;

import java.io.PrintStream;
import java.util.Comparator;
import java.util.List;

public class TupleFileComparator implements Comparator<String> {
	
	/**
	 * Compares two tuples in the form of Strings: always in direction from first column to last column
	 * @return the result of the comparison, as per the Comparator interface
	 * @param a - the first tuple to compare
	 * @param b - the second tuple to compare
	 */
	public int compare(String a, String b) {
		
		String[] tupleA = a.split(",");
		String[] tupleB = b.split(","); 
		
		for (int i = 0; i < tupleA.length; i++) {
			
			int tupleAVal = Integer.valueOf(tupleA[i]); 
			int tupleBVal = Integer.valueOf(tupleB[i]); 
			
			if(tupleAVal != tupleBVal) {
				return Integer.valueOf(tupleA[i]) - Integer.valueOf(tupleB[i]);
			}
		}
		return 0; 
	}

}
