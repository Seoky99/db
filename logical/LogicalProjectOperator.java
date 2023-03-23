package logical;

import java.util.ArrayList;

//Logical representation of the ProjectOperator
public class LogicalProjectOperator implements LogicalOperator {
	public ArrayList<String> attributes;
	public LogicalOperator op;
	
	/**
	 * Constructor for LogicalProjectOperator
	 * @param attributes - Columns ordering for projection
	 * @param op - Underlying operator on which to perform the projection
	 */
	public LogicalProjectOperator(ArrayList<String> attributes, LogicalOperator op) {
		this.attributes = attributes;
		this.op = op;
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
		ArrayList<String> innerLines = op.lines();
		String topLine = "Project[";
		for(String s : attributes) {
			topLine += s + ",";
		}
		topLine = topLine.substring(0, topLine.length() - 1) + "]";
		ArrayList<String> toReturn = new ArrayList<String>();
		toReturn.add(topLine);
		for(String s : innerLines) {
			toReturn.add("-" + s);
		}
		return toReturn;
	}
}