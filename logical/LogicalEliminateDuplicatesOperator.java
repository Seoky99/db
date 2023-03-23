package logical;

import java.util.ArrayList;

//Logical representation of the EliminateDuplicatesOperator
public class LogicalEliminateDuplicatesOperator implements LogicalOperator {
	public LogicalSortOperator sortOp;
	
	/**
	 * Constructor for LogicalEliminateDuplicatesOperator
	 * @param sortOp - Sort operator on which to eliminate duplicates
	 */
	public LogicalEliminateDuplicatesOperator(LogicalSortOperator sortOp) {
		this.sortOp = sortOp;
	}
	
	/** Causes the plan builder v to visit this object
	 * @param v - The physical plan that will visit this object
	 */
	public void accept(PhysicalPlanBuilder v) {
		v.visit(this);
	}
	
	/**
	 * Return an array of strings representing this operator
	 * @return - An array of strings representing this operator 
	 */
	public ArrayList<String> lines() {
		ArrayList<String> innerLines = sortOp.lines();
		ArrayList<String> toReturn = new ArrayList<String>();
		toReturn.add("DupElim");
		for(String s : innerLines) {
			toReturn.add("-" + s);
		}
		return toReturn;
	}
}