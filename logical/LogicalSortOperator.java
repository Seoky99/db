package logical;

import java.util.ArrayList;
import java.util.List;

//Logical representation of the SortOperator
public class LogicalSortOperator implements LogicalOperator {
	public LogicalOperator projectOp;
	public List<String> orderBy;
	public String tempDir;
	
	/**
	 * Constructor for LogicalSortOperator
	 * @param orderBy - List of column names to orderBy, does not need to be exhaustive
	 * @param projectOp - ProjectOperator on which to perform the sort
	 */
	public LogicalSortOperator(String tempDir, List<String> orderBy, LogicalOperator projectOp) {
		this.orderBy = orderBy;
		this.projectOp = projectOp;
		this.tempDir = tempDir;
	}
	
	/** Causes the plan builder v to visit this object
	 * @param v - The physical plan that will visit this object
	 */
	public void accept(PhysicalPlanBuilder v) {
		v.visit(this);
	}
		
	public ArrayList<String> lines() {
		ArrayList<String> innerLines = projectOp.lines();
		String topLine = "Sort[";
		for(String s : orderBy) {
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