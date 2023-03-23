package logical;

import java.util.ArrayList;

//Logical representation of the scan operator
public class LogicalScanOperator implements LogicalOperator {
	public String baseTable;
	public String name;
	
	/**
	 * Constructor for the LogicalScanOperator
	 * @param name - The name of the table with potential alias
	 * @param baseTable - The name of the base table
	 */
	public LogicalScanOperator(String baseTable, String name) {
		this.name = name;
		this.baseTable = baseTable;
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
		ArrayList<String> toReturn = new ArrayList<String>();
		toReturn.add("Leaf[" + baseTable + "]");
		return toReturn;
	}
}
