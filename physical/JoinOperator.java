package physical;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import cs4321.Tuple;
import net.sf.jsqlparser.expression.Expression;
import visitors.EvaluateExpressionVisitor;

//Joins two operators on a given condition
public class JoinOperator extends Operator {
	Operator left;
	Operator right;
	//Current tuple we are reading from the outer operator
	private Tuple outer;
	Expression joinExpression;
	boolean empty;
	
	/**
	 * Constructor for JoinOperator
	 * @param left - the left operator
	 * @param right - the right operator
	 * @param joinExpression - the condition on which to do the join
	 */
	public JoinOperator(Operator left, Operator right, Expression joinExpression) {
		this.left = left;
		this.right = right;
		this.joinExpression = joinExpression;
	}
	
	/**
	 * Resets the join operator
	 */
	public void reset() {
		left.reset();
		right.reset();
		outer = null;
	}
	
	//Not associated with sort
	public void reset(int index) {
		
	}
	
	/**
	 * Gets the next tuple of the join, null if none exists
	 * @return 
	 */
	public Tuple getNextTuple() {
		Tuple toReturn = null;
		while(!empty && toReturn == null) {
			toReturn = getNextTupleInner();
		}
		return toReturn;
	}
	
	/**
	 * Gets the next tuple of the join, null if it fails the join, sets empty to true if we are out of tuples
	 */
	private Tuple getNextTupleInner() {
		if(outer == null) {
			outer = left.getNextTuple(); 
		}
		if(outer == null) {
			empty = true;
			return null;
		}
		
		Tuple inner = right.getNextTuple();
		
		if(inner == null) {
			outer = left.getNextTuple();
			right.reset();
			return null;
		}
		else {
			Tuple glued = glue(outer, inner);
			
			if(joinExpression == null) {
				return glued;
			}
			
			EvaluateExpressionVisitor ev = new EvaluateExpressionVisitor(glued);
			joinExpression.accept(ev);
			
			if(ev.getResult()) {
				return glued;
			}
			else {
				return null;
			}
		}
	}
	
	/**
	 * Takes two tuples are return one appended to the other
	 * @param a - one of the tuples to be combined
	 * @param b - one of the tuples to be combined
	 * @return The two tuple put together
	 */
	private Tuple glue(Tuple a, Tuple b) {
		HashMap<String, Integer> newMap = new HashMap<String, Integer>();
		ArrayList<Integer> contents = new ArrayList<Integer>();
		
		for(Map.Entry<String, Integer> e : a.map.entrySet()) {
			newMap.put(e.getKey(), e.getValue());
			contents.add(e.getValue());
		}
		
		for(Map.Entry<String, Integer> e : b.map.entrySet()) {
			newMap.put(e.getKey(), e.getValue());
			contents.add(e.getValue());
		}
		
		return new Tuple(newMap);
	}
	
}