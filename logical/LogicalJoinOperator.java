package logical;

import java.util.ArrayList;

import net.sf.jsqlparser.expression.Expression;

//Logical representation of the JoinOperator
public class LogicalJoinOperator implements LogicalOperator {
	public LogicalOperator left;
	public LogicalOperator right;
	public Expression e;
	public String rightTable;
	
	/**
	 * Constructor for LogicalJoinOperator
	 * @param left - Left operator of the join
	 * @param right - Right operator of the join
	 * @param e - Join condition
	 * @param rightTable - the table that came from the right side of the join
	 * 
	 */
	public LogicalJoinOperator(LogicalOperator left, LogicalOperator right, Expression e, String rightTable) {
		this.left = left;
		this.right = right;
		this.e = e;
		this.rightTable = rightTable;
	}
	
	/** Causes the plan builder v to visit this object
	 * @param v - The physical plan that will visit this object
	 */
	public void accept(PhysicalPlanBuilder v) {
		v.visit(this);
	}
	
	/**
	 * Return an array of strings representing this operator (NULL SINCE THIS IS DEPRECATED SINCE LOGICALMUTLIJOIN)
	 * @return - An array of strings representing this operator 
	 */
	public ArrayList<String> lines() {
		return null;
	}
}