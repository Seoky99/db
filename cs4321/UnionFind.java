package cs4321;

import java.util.HashSet;
import java.util.LinkedList;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

/**
 * Implementation of the UnionFind data structure
 */
public class UnionFind {
	public HashSet<Bundle> elements;
	
	/**
	 * Default Constructor
	 */
	public UnionFind() {
		elements = new HashSet<Bundle>();
	}
	
	/**
	 * Finds the bundle for the given attribute, creates one if none exists
	 * @param att - the attribute for which to find the bundle
	 * @return - the bundle corresponding to the given attribute
	 */
	public Bundle find(String att) {
		for(Bundle b : elements) {
			if(b.columns.contains(att)) {
				return b;
			}
		}
		
		Bundle newBundle = new Bundle(att);
		elements.add(newBundle);
		return newBundle;
	}
	
	/**
	 * Merges two bundles in the underlying data structure
	 * @param a - the first bundle to merge
	 * @param b - the second bundle to merge
	 */
	public void merge(Bundle a, Bundle b) {
		elements.remove(a);
		elements.remove(b);
		elements.add(a.merge(b));
	}
	
	/**
	 * Get the expression from the tableName input
	 * @param t : the Table name 
	 * @return: the Expression 
	 */
	public Expression getExpressionForTable(String t) {
		Expression toReturn = null;
		for(Bundle b : elements) {
			for(String s : b.columns) {
				if(t.equals(s.substring(0, s.indexOf('.')))) {
					Expression lowerBound = null;
					Expression upperBound = null;
					Expression bounds = null;
					Column c = new Column();
					c.setColumnName(s.substring(s.indexOf('.') + 1));
					Table table = new Table();
					table.setName(s.substring(0, s.indexOf('.')));
					c.setTable(table);
					

					if(b.lowerBound != Integer.MIN_VALUE) {
						lowerBound = new GreaterThanEquals(c, new LongValue((long) b.lowerBound));
					}
					if(b.upperBound != Integer.MAX_VALUE) {
						upperBound = new MinorThanEquals(c, new LongValue((long) b.upperBound));
					}
					if(lowerBound != null) {
						bounds = lowerBound;
					}
					if(bounds != null && upperBound != null) {
						bounds = new AndExpression(bounds, upperBound);
					} else if(upperBound != null) {
						bounds = upperBound;
					}
					if(b.lowerBound == b.upperBound) {
						bounds = new EqualsTo(c, new LongValue((long) b.upperBound));
					}
					
					if(toReturn == null) {
						toReturn = bounds;
					} else if(bounds != null){
						toReturn = new AndExpression(bounds, toReturn);
					}
				}
			}
		}
		return toReturn;
	}
}
