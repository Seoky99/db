package physical;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import cs4321.Tuple;
import cs4321.TupleComparator;

//Operator used to sort tuples from a project operator
public class SortOperator extends Operator {
	public ProjectOperator p;
	public Operator o;
	public TupleComparator tupleComparator;
	private List<Tuple> tuples;
	private int index;
	
	/**
	 * Constructor for SortOperator, dumps entire ProjectOperator upon construction
	 * @param p - The ProjectOperator whose tuples we should sort
	 * @param orderBy - The order of columns we need to sort, does not need to be exhaustive
	 */
	public SortOperator(ProjectOperator p, List<String> orderBy) {
		index = -1;
		this.p = p;
		this.tuples = new ArrayList<Tuple>();
		
		List<String> newOrderBy = new LinkedList<String>();
		if(orderBy != null) {
			newOrderBy = orderBy;
		}
		
		for(String s : p.attributes) {
			if(!newOrderBy.contains(s)) {
				newOrderBy.add(s);
			}
		}
		
		tupleComparator = new TupleComparator(newOrderBy);
		
		Tuple toAdd = p.getNextTuple();
		
		while(toAdd != null) {
			tuples.add(toAdd);
			toAdd = p.getNextTuple();
		}
		
		tuples.sort(tupleComparator);
	}
	
	/**
	 * Constructor for SortOperators that are not used for a final ORDER BY, dumps entire Operator upon construction
	 * @param o - Operator whose tuples we should sort
	 * @param orderBy - The order of columns we need to sort, does not need to be exhaustive
	 */
	public SortOperator(Operator o, List<String> orderBy, boolean isHead) throws Exception {
		if(isHead == true) {
			throw new Exception("This constructor sohuld only be used for non head sorts");
		}
		
		index = -1;
		this.o = o;
		this.tuples = new ArrayList<Tuple>();
		
		tupleComparator = new TupleComparator(orderBy);
		
		Tuple toAdd = o.getNextTuple();
		
		while(toAdd != null) {
			tuples.add(toAdd);
			toAdd = o.getNextTuple();
		}
		
		tuples.sort(tupleComparator);
	}
	
	
	
	/** Gets the next sorted tuple, null if none exists
	 * @return The next sorted tuple
	 */
	public Tuple getNextTuple() {
		index++;
		if(index >= tuples.size()) {
			return null;
		}
		return tuples.get(index);
		
	}
	
	public void reset() {
		index = -1;
	}

	@Override
	public void reset(int index) {
		this.index = index - 1;
	}

}
