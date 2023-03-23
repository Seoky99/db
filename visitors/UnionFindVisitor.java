package visitors;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.Set;

import cs4321.Bundle;
import cs4321.UnionFind;

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
 * Updates a UnionFind data structure based on the expression visited
 */
public class UnionFindVisitor implements ExpressionVisitor {

	public UnionFind uf;
	public HashMap<Set<String>, Expression> leftoverExpressions; 
	public HashMap<Set<String>, Expression> equalityJoinExpressions;
	
	public UnionFindVisitor(UnionFind uf) {
		leftoverExpressions = new HashMap<Set<String>, Expression>();
		equalityJoinExpressions = new HashMap<Set<String>, Expression>();
		this.uf = uf;
	}
	
	/**
	 * Adds an expression to a pair of/a single table reference(s), creates an AND if an expression is already mapped
	 * @param s The pair of or single table reference(s) for which the expression is to be added
	 * @param e The expression to add
	 */
	private void addToExpressions(Set<String> s, Expression e) {
		if(leftoverExpressions.get(s) != null) {
			AndExpression ae = new AndExpression(leftoverExpressions.get(s), e);
			leftoverExpressions.put(s, ae);
		} else {
			leftoverExpressions.put(s, e);
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

	@Override
	public void visit(AndExpression arg0) {
		UnionFindVisitor left = new UnionFindVisitor(uf);
		UnionFindVisitor right = new UnionFindVisitor(uf);
		
		arg0.getLeftExpression().accept(left);
		arg0.getRightExpression().accept(right);
		
		for(Entry<Set<String>, Expression> e : left.leftoverExpressions.entrySet()) {
			addToExpressions(e.getKey(), e.getValue());
		}
		for(Entry<Set<String>, Expression> e : right.leftoverExpressions.entrySet()) {
			addToExpressions(e.getKey(), e.getValue());
		}
		
		for(Entry<Set<String>, Expression> e : left.equalityJoinExpressions.entrySet()) {
			addToExpressions(e.getKey(), e.getValue());
		}
		for(Entry<Set<String>, Expression> e : right.equalityJoinExpressions.entrySet()) {
			addToExpressions(e.getKey(), e.getValue());
		}
	}

	@Override
	public void visit(OrExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Between arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(EqualsTo arg0) {
		if(arg0.getLeftExpression() instanceof Column && arg0.getRightExpression() instanceof Column) {
			Bundle leftBundle = uf.find(arg0.getLeftExpression().toString());
			Bundle rightBundle = uf.find(arg0.getRightExpression().toString());
			uf.merge(leftBundle, rightBundle);
			
			HashSet<String> tableNames = new HashSet<String>();
			Column left = (Column) arg0.getLeftExpression();
			Column right = (Column) arg0.getRightExpression();
			tableNames.add(left.toString().substring(0, left.toString().indexOf(".")));
			tableNames.add(right.toString().substring(0, right.toString().indexOf(".")));
			
			equalityJoinExpressions.put(tableNames, arg0);
		}
		else if(arg0.getLeftExpression() instanceof Column) {
			Bundle b = uf.find(arg0.getLeftExpression().toString());
			b.setEqualTo((int)((LongValue)arg0.getRightExpression()).getValue());
		} else {
			Bundle b = uf.find(arg0.getRightExpression().toString());
			b.setEqualTo((int)((LongValue)arg0.getLeftExpression()).getValue());
		}
	}

	@Override
	public void visit(GreaterThan arg0) {
		if(arg0.getLeftExpression() instanceof Column && arg0.getRightExpression() instanceof Column) {
			HashSet<String> tableNames = new HashSet<String>();
			Column left = (Column) arg0.getLeftExpression();
			Column right = (Column) arg0.getRightExpression();
			tableNames.add(left.toString().substring(0, left.toString().indexOf(".")));
			tableNames.add(right.toString().substring(0, right.toString().indexOf(".")));
			addToExpressions(tableNames, arg0);
		}
		else if(arg0.getLeftExpression() instanceof Column) {
			Bundle b = uf.find(arg0.getLeftExpression().toString());
			b.setLowerBound((int)((LongValue)arg0.getRightExpression()).getValue() + 1);
		} else {
			Bundle b = uf.find(arg0.getRightExpression().toString());
			b.setLowerBound((int)((LongValue)arg0.getLeftExpression()).getValue() + 1);
		}
	}

	@Override
	public void visit(GreaterThanEquals arg0) {
		if(arg0.getLeftExpression() instanceof Column && arg0.getRightExpression() instanceof Column) {
			HashSet<String> tableNames = new HashSet<String>();
			Column left = (Column) arg0.getLeftExpression();
			Column right = (Column) arg0.getRightExpression();
			tableNames.add(left.toString().substring(0, left.toString().indexOf(".")));
			tableNames.add(right.toString().substring(0, right.toString().indexOf(".")));
			addToExpressions(tableNames, arg0);
		}
		else if(arg0.getLeftExpression() instanceof Column) {
			Bundle b = uf.find(arg0.getLeftExpression().toString());
			b.setLowerBound((int)((LongValue)arg0.getRightExpression()).getValue());
		} else {
			Bundle b = uf.find(arg0.getRightExpression().toString());
			b.setLowerBound((int)((LongValue)arg0.getLeftExpression()).getValue());
		}
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

	@Override
	public void visit(MinorThan arg0) {
		if(arg0.getLeftExpression() instanceof Column && arg0.getRightExpression() instanceof Column) {
			HashSet<String> tableNames = new HashSet<String>();
			Column left = (Column) arg0.getLeftExpression();
			Column right = (Column) arg0.getRightExpression();
			tableNames.add(left.toString().substring(0, left.toString().indexOf(".")));
			tableNames.add(right.toString().substring(0, right.toString().indexOf(".")));
			addToExpressions(tableNames, arg0);
		}
		else if(arg0.getLeftExpression() instanceof Column) {
			Bundle b = uf.find(arg0.getLeftExpression().toString());
			b.setUpperBound((int)((LongValue)arg0.getRightExpression()).getValue() - 1);
		} else {
			Bundle b = uf.find(arg0.getRightExpression().toString());
			b.setUpperBound((int)((LongValue)arg0.getLeftExpression()).getValue() - 1);
		}
	}

	@Override
	public void visit(MinorThanEquals arg0) {
		if(arg0.getLeftExpression() instanceof Column && arg0.getRightExpression() instanceof Column) {
			HashSet<String> tableNames = new HashSet<String>();
			Column left = (Column) arg0.getLeftExpression();
			Column right = (Column) arg0.getRightExpression();
			tableNames.add(left.toString().substring(0, left.toString().indexOf(".")));
			tableNames.add(right.toString().substring(0, right.toString().indexOf(".")));
			addToExpressions(tableNames, arg0);
		}
		else if(arg0.getLeftExpression() instanceof Column) {
			Bundle b = uf.find(arg0.getLeftExpression().toString());
			b.setUpperBound((int)((LongValue)arg0.getRightExpression()).getValue());
		} else {
			Bundle b = uf.find(arg0.getRightExpression().toString());
			b.setUpperBound((int)((LongValue)arg0.getLeftExpression()).getValue());
		}		
	}

	@Override
	public void visit(NotEqualsTo arg0) {
		if(arg0.getLeftExpression() instanceof Column && arg0.getRightExpression() instanceof Column) {
			HashSet<String> tableNames = new HashSet<String>();
			Column left = (Column) arg0.getLeftExpression();
			Column right = (Column) arg0.getRightExpression();
			tableNames.add(left.toString().substring(0, left.toString().indexOf(".")));
			tableNames.add(right.toString().substring(0, right.toString().indexOf(".")));
			addToExpressions(tableNames, arg0);
		} else if(arg0.getLeftExpression() instanceof Column) {
			HashSet<String> tableNames = new HashSet<String>();
			Column left = (Column) arg0.getLeftExpression();
			tableNames.add(left.toString().substring(0, left.toString().indexOf(".")));
			addToExpressions(tableNames, arg0);
		} else {
			HashSet<String> tableNames = new HashSet<String>();
			Column right = (Column) arg0.getRightExpression();
			tableNames.add(right.toString().substring(0, right.toString().indexOf(".")));
			addToExpressions(tableNames, arg0);
		}
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
