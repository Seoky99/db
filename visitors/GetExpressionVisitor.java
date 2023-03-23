package visitors;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;

/**
 * Turns an expression into a HashMap from sets of table references to the select or join expressions
 * that will be added to the levels of the join tree represented by the table reference or pair of table references
 * I.e leaves of the tree have select conditions and nodes that join two tables have join conditions
 */
public class GetExpressionVisitor implements ExpressionVisitor{
	
	private HashMap<Set<String>, Expression> expressions; 
	public boolean pureIntExpressionExists;
	public boolean pureIntExpressionResult;
	
	
	/**
	 * Constructor for GetExpressionVisitor
	 */
	public GetExpressionVisitor() {
		expressions = new HashMap<Set<String>, Expression>();
		pureIntExpressionExists = false;
		pureIntExpressionResult = true;
	}
	
	/**
	 * Gets the map of expressions for use in the join tree
	 * @return The map of expressions for use in the join tree, null if the where contains something that is always false
	 */
	public HashMap<Set<String>, Expression> getExpressions() {
		if(!pureIntExpressionExists || pureIntExpressionResult) {
			return expressions;
		}
		else {
			return null;
		}
	}
	
	/**
	 * Gets the int value of a LongValue expression
	 * @param e LongValue expression
	 * @return The value of the LongValue
	 */
	private int getIntValue(Expression e) {
		if(e instanceof LongValue) {
			return (int) ((LongValue) e).getValue();
		}
		else return -1;
	}
	
	/**
	 * Adds an expression to a pair of/a single table reference(s), creates an AND if an expression is already mapped
	 * @param s The pair of or single table reference(s) for which the expression is to be added
	 * @param e The expression to add
	 */
	private void addToExpressions(Set<String> s, Expression e) {
		if(expressions.get(s) != null) {
			AndExpression ae = new AndExpression(expressions.get(s), e);
			expressions.put(s, ae);
		} else {
			expressions.put(s, e);
		}
	}
	
	/**
	 * Adds an expression to the map of expressions, contingent on one of the sides of the op being a column reference
	 * @param left - Left side of the op
	 * @param right - Right side of the op
	 * @param arg0 - A comparison operator
	 */
	private void atLeastOneColumn(Expression left, Expression right, Expression arg0) {
		Set<String> tableNames = new HashSet<String>();
		if(left instanceof LongValue) {
			tableNames.add(right.toString().substring(0, right.toString().indexOf(".")));
			addToExpressions(tableNames, arg0);
		}
		else if(right instanceof LongValue) {
			tableNames.add(left.toString().substring(0, left.toString().indexOf(".")));
			addToExpressions(tableNames, arg0);
		}
		else {
			tableNames.add(left.toString().substring(0, left.toString().indexOf(".")));
			tableNames.add(right.toString().substring(0, right.toString().indexOf(".")));
			addToExpressions(tableNames, arg0);
		}
	}
	
	@Override
	public void visit(NullValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Function arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(InverseExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(JdbcParameter arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(DoubleValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(LongValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(DateValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(TimeValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(TimestampValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Parenthesis arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(StringValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Addition arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Division arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Multiplication arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Subtraction arg0) {
		// TODO Auto-generated method stub
		
	}

	/**
	 * Adds an expression to the expression map
	 * @param arg0 - The expression to be added
	 */
	@Override
	public void visit(AndExpression arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
	}

	@Override
	public void visit(OrExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Between arg0) {
		// TODO Auto-generated method stub
		
	}

	/**
	 * Adds an expression to the expression map
	 * @param arg0 - The expression to be added
	 */
	@Override
	public void visit(EqualsTo arg0) {
		Expression left = arg0.getLeftExpression();
		Expression right = arg0.getRightExpression();
		
		if(left instanceof LongValue && right instanceof LongValue) {
			pureIntExpressionExists = true;
			pureIntExpressionResult = pureIntExpressionResult && getIntValue(left) == getIntValue(right);
			return;
		}
		
		atLeastOneColumn(left, right, arg0);
	}

	/**
	 * Adds an expression to the expression map
	 * @param arg0 - The expression to be added
	 */
	@Override
	public void visit(GreaterThan arg0) {
		Expression left = arg0.getLeftExpression();
		Expression right = arg0.getRightExpression();
		
		if(left instanceof LongValue && right instanceof LongValue) {
			pureIntExpressionExists = true;
			pureIntExpressionResult = pureIntExpressionResult && getIntValue(left) > getIntValue(right);
			return;
		}
		
		atLeastOneColumn(left, right, arg0);
	}

	/**
	 * Adds an expression to the expression map
	 * @param arg0 - The expression to be added
	 */
	@Override
	public void visit(GreaterThanEquals arg0) {
		Expression left = arg0.getLeftExpression();
		Expression right = arg0.getRightExpression();
		
		if(left instanceof LongValue && right instanceof LongValue) {
			pureIntExpressionExists = true;
			pureIntExpressionResult = pureIntExpressionResult && getIntValue(left) >= getIntValue(right);
			return;
		}
		
		atLeastOneColumn(left, right, arg0);
	}

	@Override
	public void visit(InExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(IsNullExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(LikeExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	/**
	 * Adds an expression to the expression map
	 * @param arg0 - The expression to be added
	 */
	@Override
	public void visit(MinorThan arg0) {
		Expression left = arg0.getLeftExpression();
		Expression right = arg0.getRightExpression();
		
		if(left instanceof LongValue && right instanceof LongValue) {
			pureIntExpressionExists = true;
			pureIntExpressionResult = pureIntExpressionResult && getIntValue(left) < getIntValue(right);
			return;
		}
		
		atLeastOneColumn(left, right, arg0);
	}

	/**
	 * Adds an expression to the expression map
	 * @param arg0 - The expression to be added
	 */
	@Override
	public void visit(MinorThanEquals arg0) {
		Expression left = arg0.getLeftExpression();
		Expression right = arg0.getRightExpression();
		
		if(left instanceof LongValue && right instanceof LongValue) {
			pureIntExpressionExists = true;
			pureIntExpressionResult = pureIntExpressionResult && getIntValue(left) <= getIntValue(right);
			return;
		}
		
		atLeastOneColumn(left, right, arg0);
	}

	/**
	 * Adds an expression to the expression map
	 * @param arg0 - The expression to be added
	 */
	@Override
	public void visit(NotEqualsTo arg0) {
		Expression left = arg0.getLeftExpression();
		Expression right = arg0.getRightExpression();
		
		if(left instanceof LongValue && right instanceof LongValue) {
			pureIntExpressionExists = true;
			pureIntExpressionResult = pureIntExpressionResult && getIntValue(left) != getIntValue(right);
			return;
		}
		
		atLeastOneColumn(left, right, arg0);	
	}

	@Override
	public void visit(Column arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(SubSelect arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(CaseExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(WhenClause arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(ExistsExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AllComparisonExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AnyComparisonExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Concat arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Matches arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseAnd arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseOr arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseXor arg0) {
		// TODO Auto-generated method stub
		
	}

}