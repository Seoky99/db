package visitors;

import java.util.LinkedList;

import cs4321.DatabaseCatalog;
import indexes.IndexConditionSplitter;
import logical.*;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;

public class VCalculator {
	
	/**
	 * Gets the V-Value for a base table
	 * @param o - The scan operator on the baseTable
	 * @param att - The attribute for the V-Value
	 * @return - V-Value for the base table
	 */
	public static double getValue(LogicalScanOperator o, String att) {
		return getMax(o.baseTable, att) - getMin(o.baseTable, att) + 1;
	}
	
	/**
	 * Gets the V-Value for a selection on a base table
	 * @param o - The select operator on the baseTable
	 * @param att - The attribute for the V-Value
	 * @return - V-Value for the selection
	 */
	public static double getValue(LogicalSelectOperator o, String att) {
		double toReturn = 0;
		double scanV = getValue((LogicalScanOperator) o.op, att);
		
		double r = getReduction(((LogicalScanOperator) o.op).baseTable, att, o.e);
		toReturn = scanV * r;
		
		double minSize = getSelectSize(o);
		
		toReturn = Math.min(minSize, toReturn);
		
		return toReturn >= 1 ? toReturn : 1 ;
	}
	
	/**
	 * Gets the V-Value for a join
	 * @param o - The join operator
	 * @param att - The attribute for the V-Value
	 * @return - V-Value for the join
	 */
	public static double getValue(LogicalJoinOperator o, String att) {
		LinkedList<EqualsTo> equalities = getAllEqualitiesFromExpression(o.e);
		LinkedList<EqualsTo> equalitiesWithAtt = new LinkedList<EqualsTo>();
		String attTable = tableFromAtt(att);
		double toReturn = 0;
		
		for(EqualsTo eq : equalities) {
			if(((Column) eq.getLeftExpression()).toString().equals(att) 
					||  ((Column) eq.getRightExpression()).toString().equals(att)) {
				equalitiesWithAtt.add(eq);
			}
		}
		
		if(attTable.equals(o.rightTable)) {
			toReturn = getValue((LogicalSelectOperator) o.right, att);
		} else {
			if(o.left instanceof LogicalSelectOperator) {
				toReturn = getValue((LogicalSelectOperator) o.left, att);
			} else {
				toReturn = getValue((LogicalJoinOperator) o.left, att);
			}
		} 
			
		for(EqualsTo eq : equalities) {
			if(((Column) eq.getLeftExpression()).toString().equals(att)) {
				double otherValue = 0;
				String otherAtt = ((Column) eq.getRightExpression()).toString();
				String otherAttTable = tableFromAtt(otherAtt);
				
				if(otherAttTable.equals(o.rightTable)) {
					otherValue = getValue((LogicalSelectOperator) o.right, att);
				} else {
					if(o.left instanceof LogicalSelectOperator) {
						otherValue = getValue((LogicalSelectOperator) o.left, att);
					} else {
						otherValue = getValue((LogicalJoinOperator) o.left, att);
					}
				} 
				
				toReturn = Math.min(toReturn, otherValue);
			}
		}
					
		return toReturn;
	}
	
	/**
	 * Gets the maximum value that a given attribute takes 
	 * @param baseTable - The base table for the attribute
	 * @param att - The attribute
	 * @return - The maximum value the attribute takes
	 */
	public static double getMax(String baseTable, String att) {
		String attCut = att.substring(att.indexOf('.') + 1);
		return DatabaseCatalog.getInstance().getAdvancedStats().get(baseTable).getAttributeMap().get(attCut)[1]; 
	}
	
	/**
	 * Gets the minimum value that a given attribute takes 
	 * @param baseTable - The base table for the attribute
	 * @param att - The attribute
	 * @return - The minimum value the attribute takes
	 */
	public static double getMin(String baseTable, String att) {
		String attCut = att.substring(att.indexOf('.') + 1);
		return DatabaseCatalog.getInstance().getAdvancedStats().get(baseTable).getAttributeMap().get(attCut)[0]; 
	}
	
	/**
	 * Gets the total number of tuples in a table
	 * @param baseTable - the table
	 * @return - The number of tuples in the table
	 */
	public static double getTotalTuples(String baseTable) {
		return DatabaseCatalog.getInstance().getAdvancedStats().get(baseTable).getTuples(); 
	}
	
	/**
	 * Get estimated size of the join
	 * @param j - The join
	 * @return - The size of the join
	 */
	public static double getJoinSize(LogicalJoinOperator j) {
		LinkedList<EqualsTo> equalities = getAllEqualitiesFromExpression(j.e);
		double leftSize;
		double rightSize;
		
		if(j.left instanceof LogicalJoinOperator) {
			leftSize = getJoinSize((LogicalJoinOperator) j.left);
		} else {
			leftSize = getSelectSize((LogicalSelectOperator) j.left);
		}
		rightSize = getSelectSize((LogicalSelectOperator) j.right);
		
		double bottom = 1;
		
		for(EqualsTo eq : equalities) {
			String leftAtt = ((Column) eq.getLeftExpression()).toString();
			String rightAtt = ((Column) eq.getRightExpression()).toString();
			String leftAttTable = tableFromAtt(leftAtt);
			double leftValue = 1;
			double rightValue = 1;
			
			if(j.rightTable.equals(leftAttTable)) {
				leftValue = getValue((LogicalSelectOperator) j.right, leftAtt);
				
				if(j.left instanceof LogicalSelectOperator) {
					rightValue = getValue((LogicalSelectOperator) j.left, rightAtt);
				} else {
					rightValue = getValue((LogicalJoinOperator) j.left, rightAtt);
				}
			} else {
				rightValue = getValue((LogicalSelectOperator) j.right, rightAtt);
				
				if(j.left instanceof LogicalSelectOperator) {
					leftValue = getValue((LogicalSelectOperator) j.left, leftAtt);
				} else {
					rightValue = getValue((LogicalJoinOperator) j.left, leftAtt);
				}
			}
			
			bottom*=Math.max(rightValue, leftValue);
		}
		
		return (leftSize * rightSize) / bottom;
		
	}
	
	/**
	 * Get all of the individual equalities in an expression
	 * @param e - The expression
	 * @return - All of the equalities
	 */
	public static LinkedList<EqualsTo> getAllEqualitiesFromExpression(Expression e) {
		LinkedList<EqualsTo> expressions = new LinkedList<EqualsTo>();
		if(e instanceof EqualsTo) {
			expressions.add((EqualsTo) e);
		} else if(e instanceof AndExpression) {
			expressions.addAll(getAllEqualitiesFromExpression(((AndExpression) e).getLeftExpression()));
			expressions.addAll(getAllEqualitiesFromExpression(((AndExpression) e).getRightExpression()));
		} 
		return expressions;
	}
	
	/**
	 * Get estimated size of the select
	 * @param j - The select
	 * @return - The size of the select
	 */
	public static double getSelectSize(LogicalSelectOperator s) {
		double totalTuples = getTotalTuples(((LogicalScanOperator) s.op).baseTable);
		for(String st : DatabaseCatalog.getInstance().columnNames(((LogicalScanOperator) s.op).baseTable)) {
			String stTotal = ((LogicalScanOperator) s.op).name + "." + st;
			totalTuples = totalTuples * getReduction(((LogicalScanOperator) s.op).baseTable, stTotal, s.e);
		}
		return totalTuples;
	}
	
	/**
	 * Gets the reduction factor for a selection on a given attribute
	 * @param baseTable - The baseTable the selection is on
	 * @param att - The attribute 
	 * @param e - The selection expression
	 * @return - The Reduction factor
	 */
	public static double getReduction(String baseTable, String att, Expression e) {
		if(e == null) {
			return 1;
		}
		
		IndexConditionSplitter ics = new IndexConditionSplitter(att);
		e.accept(ics);
		double max = getMax(baseTable, att);
		double min = getMin(baseTable, att);
		
		if(ics.lowkey == null && ics.highkey == null) {
			return 1;
		} else if(ics.lowkey == null) {
			return (((double) ics.highkey) - min)/(max - min);
		} else if(ics.highkey == null) {
			return (max - ((double)ics.lowkey)/(max - min));
		} else {
			return (((double) ics.highkey) - ((double) ics.lowkey))/(max - min);
		}
	}
	
	/**
	 * Return the table of an attribute
	 * @param att - The attribute
	 * @return - The table
	 */
	public static String tableFromAtt(String att) {
		return att.substring(0, att.indexOf('.'));
	}
}

