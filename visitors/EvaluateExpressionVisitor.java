package visitors;

import java.util.Map.Entry;

import cs4321.Tuple;
import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.BinaryExpression;
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

//Used to evaluate boolean expression to a boolean result
public class EvaluateExpressionVisitor implements ExpressionVisitor {
	
	private boolean result; 
	public Tuple tuple;

	/**
	 * Constructor for EvaluateExpressionVisitor
	 * @param tuple - The tuple the expressions will be evaluated against
	 */
	public EvaluateExpressionVisitor(Tuple tuple) {
		this.tuple = tuple;
	}
	 
	/** Gets the value of a LongValue or Column
	* @param e - Either a Column or LongValue
	* @return The value of e
	*/
	public int getIntValue(Expression e) throws Exception {
		if(e instanceof LongValue) {
			return (int) ((LongValue) e).getValue();
		}
		else if(e instanceof Column) {
			return tuple.map.get(((Column) e).getWholeColumnName());
		}
		else {
			throw new Exception("This is not a int valued expression");
		}
	}
	
	/** Sets result to the value of the expression
	* @param gtExpression - GreaterThan to evaluate
	*/
	public void visit(GreaterThan gtexpression) {
		Expression left = gtexpression.getLeftExpression();
		Expression right = gtexpression.getRightExpression();
		
		int leftValue;
		int rightValue;
		
		try {
			leftValue = getIntValue(left);
			rightValue = getIntValue(right);
		} catch (Exception e) {
			return;
		}
		
		this.result = leftValue > rightValue;			
	}
	
	/** Gets the result of the expression this visitor visited
	* @return Boolean evaluation of the expression that this visitor visited
	*/
	public boolean getResult() {
		return this.result;
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
	
	/** Sets result to the value of the expression
	* @param arg0 - AndExpression to evaluate
	*/
	@Override
	public void visit(AndExpression arg0) {
		// TODO Auto-generated method stub
		
		Expression left = arg0.getLeftExpression();
		Expression right = arg0.getRightExpression();
	
		boolean leftValue;
		boolean rightValue;
		
		try {
			
			EvaluateExpressionVisitor leftVisitor = new EvaluateExpressionVisitor(tuple); 
			EvaluateExpressionVisitor rightVisitor = new EvaluateExpressionVisitor(tuple); 

			left.accept(leftVisitor);
			right.accept(rightVisitor);
			
			leftValue = leftVisitor.getResult();
			rightValue = rightVisitor.getResult();
			
		} catch (Exception e) {
			return;
		}
		
		this.result = leftValue && rightValue;		
		
	}

	@Override
	public void visit(OrExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Between arg0) {
		// TODO Auto-generated method stub
		
	}

	/** Sets result to the value of the expression
	* @param arg0 - EqualsTo to evaluate
	*/
	@Override
	public void visit(EqualsTo arg0) {
		Expression left = arg0.getLeftExpression();
		Expression right = arg0.getRightExpression();

		int leftValue;
		int rightValue;
		
		try {
			leftValue = getIntValue(left);
			rightValue = getIntValue(right);
		} catch (Exception e) {
			return;
		}
		
		this.result = leftValue == rightValue;
	}

	/** Sets result to the value of the expression
	* @param arg0 - GreaterThanEquals to evaluate
	*/
	@Override
	public void visit(GreaterThanEquals arg0) {
		Expression left = arg0.getLeftExpression();
		Expression right = arg0.getRightExpression();

		int leftValue;
		int rightValue;
		
		try {
			leftValue = getIntValue(left);
			rightValue = getIntValue(right);
		} catch (Exception e) {
			return;
		}
		
		this.result = leftValue >= rightValue;
		
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

	/** Sets result to the value of the expression
	* @param arg0 - MinorThan to evaluate
	*/
	@Override
	public void visit(MinorThan arg0) {
		Expression left = arg0.getLeftExpression();
		Expression right = arg0.getRightExpression();

		int leftValue;
		int rightValue;
		
		try {
			leftValue = getIntValue(left);
			rightValue = getIntValue(right);
		} catch (Exception e) {
			return;
		}
		
		this.result = leftValue < rightValue;	
		
	}

	/** Sets result to the value of the expression
	* @param arg0 - MinorThanEquals to evaluate
	*/
	@Override
	public void visit(MinorThanEquals arg0) {
		Expression left = arg0.getLeftExpression();
		Expression right = arg0.getRightExpression();

		int leftValue;
		int rightValue;
		
		try {
			leftValue = getIntValue(left);
			rightValue = getIntValue(right);
		} catch (Exception e) {
			return;
		}
		
		this.result = leftValue <= rightValue;
		
	}

	/** Sets result to the value of the expression
	* @param arg0 - NotEqualsTo to evaluate
	*/
	@Override
	public void visit(NotEqualsTo arg0) {
		Expression left = arg0.getLeftExpression();
		Expression right = arg0.getRightExpression();

		int leftValue;
		int rightValue;
		
		try {
			leftValue = getIntValue(left);
			rightValue = getIntValue(right);
		} catch (Exception e) {
			return;
		}
		
		this.result = leftValue != rightValue;
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