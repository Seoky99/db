package logical;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Set;

import cs4321.Bundle;
import cs4321.UnionFind;
import net.sf.jsqlparser.expression.Expression;
import visitors.UnionFindVisitor;

/**
 * Logical representation of a join with multiple children
 */
public class LogicalMultiJoin implements LogicalOperator {

	public UnionFindVisitor ufv;
	public UnionFind uf;
	public HashMap<Set<String>, Expression> joinExpressions;
	public LinkedList<LogicalSelectOperator> children;
	
	/**
	 * Constructor
	 * @param joinExpression - The join expressions not in the union find
	 * @param children - all selects or scans that must be joined
	 */
	public LogicalMultiJoin(HashMap<Set<String>, Expression> joinExpressions, 
			LinkedList<LogicalSelectOperator> children, UnionFind uf, UnionFindVisitor ufv) {
		this.joinExpressions = joinExpressions;
		this.children = children;
		this.uf = uf;
		this.ufv = ufv;
	}
	
	@Override
	public void accept(PhysicalPlanBuilder v) {
		v.visit(this);
	}
	
	/**
	 * Return an array of strings representing this operator
	 * @return - An array of strings representing this operator 
	 */
	public ArrayList<String> lines() {
		ArrayList<String> toReturn = new ArrayList<String>();
		String expressionString = "";
		for(Expression e : this.joinExpressions.values()) {
			expressionString += e;
			expressionString += " AND ";
		}
		if(expressionString.length() != 0) {
			expressionString = expressionString.substring(0, expressionString.length() - 5);
		}
		toReturn.add("Join[" + expressionString + "]");
		for(Bundle b : uf.elements) {
			String bundleString = "[[";
			for(String s : b.columns) {
				bundleString += s + ", ";
			}
			bundleString = bundleString.substring(0, bundleString.length() - 2);
			bundleString += "], equals " + b.equalTo + 
					", min " + (b.lowerBound != Integer.MIN_VALUE ? b.lowerBound : "null") + 
					", max " + (b.upperBound != Integer.MAX_VALUE ? b.upperBound : "null") + "]";
			toReturn.add(bundleString);
		}
		
		for(LogicalSelectOperator child : children) {
			ArrayList<String> childLines = child.lines();
			for(String s : childLines) {
				toReturn.add("-" + s);
			}
		}
		
		return toReturn;
	}

}
