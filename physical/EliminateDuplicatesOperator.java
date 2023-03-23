package physical;

import cs4321.Tuple;
import cs4321.TupleComparator;

//Used to remove duplicated from an already sorted operator
public class EliminateDuplicatesOperator extends Operator {
	//The SortOperator we are removing duplicates from
	private Operator so;
	//The last tuple we have seen, null if we are seeing our first tuple
	Tuple lastTuple;
	TupleComparator tc;
	
	/**
	 * Constructor for EliminateDuplicatesOperator
	 * @param so - The sort operator that we will remove duplicates from
	 */
	public EliminateDuplicatesOperator(SortOperator so) {
		this.so = so;
		tc = so.tupleComparator;
	}
	
	public EliminateDuplicatesOperator(ExternalSortOperator so) {
		this.so = so;
		tc = so.tupleComparator;
	}
	
	/**
	 * Gets the next unique tuple from the sort operator, null if none exists
	 * @return - The next tuple of this operator
	 */
	public Tuple getNextTuple() {
		Tuple toReturn = so.getNextTuple();
		//If the sortOperator does not have another tuple, return null
		if(toReturn == null) {
			return null;
		}
		
		//If this tuple is the same as the last tuple we've seen, move on to the next one
		if(lastTuple != null && tc.compare(toReturn, lastTuple) == 0) {
			return getNextTuple();
		}
			
		lastTuple = toReturn;
		return toReturn;
	}
	
	/**
	 * Resets the operator
	 */
	public void reset() {
		so.reset();
		lastTuple = null;
	}

	@Override
	public void reset(int index) {
		// TODO Auto-generated method stub
		
	}

}
