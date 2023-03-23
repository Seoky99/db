package logical;

import java.util.ArrayList;

import net.sf.jsqlparser.expression.Expression;

//Logical representation of the select operator
public class LogicalSelectOperator implements LogicalOperator {
	public LogicalOperator op;
	public Expression e;
	
	/**
	 * Constructor for LogicalSelectOperator
	 * @param e - WHERE condition
	 * @param scanOp - ScanOp of underlying table
	 */
	public LogicalSelectOperator(Expression e, LogicalOperator op) {
		this.op = op;
		this.e = e;
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
		String topLine = "Select[" + e + "]";
		ArrayList<String> toReturn = new ArrayList<String>();
		toReturn.add(topLine);
		for(String s : innerLines) {
			toReturn.add("-" + s);
		}
		return toReturn;
	}
	
}
