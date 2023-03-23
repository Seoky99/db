package physical;

import java.util.ArrayList;

import cs4321.Tuple;
import net.sf.jsqlparser.expression.Expression;
import visitors.EvaluateExpressionVisitor;

//Select relational operator
public class SelectOperator extends Operator {
	String query;
	ArrayList<String> Columns;
	Tuple data;
	private Operator scanOp;
	private Expression e;

	/**
	 * Constructor for SelectOperator
	 * @param e - The WHERE expression that conditions the select
	 * @param scanOp - The scan operator that is selected from
	 */
	public SelectOperator(Expression e, Operator scanOp) {		
		this.scanOp = scanOp;
		this.e = e;
	}

	/**
	 * Returns the next tuple that passes the WHERE condition, null if none exists
	 * @return the next tuple
	 */
	public Tuple getNextTuple() {

		// while through all the tuples selected from scanoperator
		// check the condition from where for each row, if it holds it is the next
		// tuple: else keep looking until it is null

		if(e == null) {
			return scanOp.getNextTuple();
		}
		
		Tuple nextTuple = scanOp.getNextTuple();

		while (nextTuple != null) {
			if (checkCondition(nextTuple)) {
				return nextTuple;
			}

			nextTuple = scanOp.getNextTuple();

		}
		return null;
	}

	/** Resets the Select operator 
	 */
	public void reset() {
		scanOp.reset();
	}


	//Not used in sort
	public void reset(int index) {
			
	}
	
	/** Checks if the condition holds using the expression visitor of a tuple 
	 * @param - The tuple
	 */
	public boolean checkCondition(Tuple tuple) {
		EvaluateExpressionVisitor ev = new EvaluateExpressionVisitor(tuple); 
		e.accept(ev);
		return ev.getResult();
	}
}