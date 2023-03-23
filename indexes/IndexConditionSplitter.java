package indexes;

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

//Splits the expression based on what can be handled with indexes and selections. It generates a low and high key on this condition. 
public class IndexConditionSplitter implements ExpressionVisitor {

	private String attribute;
	public Expression indexExpression;
	public Expression selectExpression;
	public Integer lowkey;
	public Integer highkey;

	/**
	 * The constructor for the splitter 
	 * @param attribute: The attribute for which the index is on 
	 */
	public IndexConditionSplitter(String attribute) {
		this.attribute = attribute;
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
		IndexConditionSplitter left = new IndexConditionSplitter(attribute);
		IndexConditionSplitter right = new IndexConditionSplitter(attribute);

		arg0.getLeftExpression().accept(left);
		arg0.getRightExpression().accept(right);

		if (left.indexExpression == null) {

			if (right.indexExpression != null) {
				indexExpression = right.indexExpression;
				lowkey = right.lowkey;
				highkey = right.highkey;
			}

		} else {
			if (right.indexExpression == null) {
				indexExpression = left.indexExpression;
				lowkey = left.lowkey;
				highkey = left.highkey;
			} else {

				indexExpression = new AndExpression(left.indexExpression, right.indexExpression);

				
				if (left.lowkey == null && right.lowkey == null) {

					lowkey = null;

				} else if (left.lowkey == null || right.lowkey == null) {

					if (left.lowkey == null) {

						lowkey = right.lowkey;

					} else {

						lowkey = left.lowkey;
					}

				} else {
					lowkey = Math.max(left.lowkey, right.lowkey);
				}

				if (left.highkey == null && right.highkey == null) {

					highkey = null;

				} else if (left.highkey == null || right.highkey == null) {

					if (left.highkey == null) {

						highkey = right.highkey;

					} else {

						highkey = left.highkey;
					}
				}

				else {
					highkey = Math.min(left.highkey, right.highkey);
				}

			}
		}

		if (left.selectExpression == null) {
			if (right.selectExpression != null) {
				selectExpression = right.selectExpression;
			}
		} else {
			if (right.selectExpression == null) {
				selectExpression = left.selectExpression;
			} else {
				selectExpression = new AndExpression(left.selectExpression, right.selectExpression);
			}
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
		if (arg0.getLeftExpression() instanceof Column) {
			if (arg0.getRightExpression() instanceof Column
					|| !((Column) arg0.getLeftExpression()).toString().equals(attribute)) {
				selectExpression = arg0;
			} else {
				lowkey = (int) ((LongValue) arg0.getRightExpression()).getValue();
				highkey = lowkey;
				indexExpression = arg0;
			}
		} else {
			if (!((Column) arg0.getRightExpression()).toString().equals(attribute)) {
				selectExpression = arg0;
			} else {
				lowkey = (int) ((LongValue) arg0.getLeftExpression()).getValue();
				highkey = lowkey;
				indexExpression = arg0;
			}
		}
	}

	@Override
	public void visit(GreaterThan arg0) {
		if (arg0.getLeftExpression() instanceof Column) {
			if (arg0.getRightExpression() instanceof Column
					|| !((Column) arg0.getLeftExpression()).toString().equals(attribute)) {
				selectExpression = arg0;
			} else {
				lowkey = (int) ((LongValue) arg0.getRightExpression()).getValue() + 1;
				highkey = null;
				indexExpression = arg0;
			}
		} else {
			if (!((Column) arg0.getRightExpression()).toString().equals(attribute)) {
				selectExpression = arg0;
			} else {
				highkey = (int) ((LongValue) arg0.getLeftExpression()).getValue() - 1;
				lowkey = null;
				indexExpression = arg0;
			}
		}
	}

	@Override
	public void visit(GreaterThanEquals arg0) {
		if (arg0.getLeftExpression() instanceof Column) {
			if (arg0.getRightExpression() instanceof Column
					|| !((Column) arg0.getLeftExpression()).toString().equals(attribute)) {
				selectExpression = arg0;
			} else {
				lowkey = (int) ((LongValue) arg0.getRightExpression()).getValue();
				highkey = null;
				indexExpression = arg0;
			}
		} else {
			if (!((Column) arg0.getRightExpression()).toString().equals(attribute)) {
				selectExpression = arg0;
			} else {
				highkey = (int) ((LongValue) arg0.getLeftExpression()).getValue();
				lowkey = null;
				indexExpression = arg0;
			}
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
		if (arg0.getLeftExpression() instanceof Column) {
			if (arg0.getRightExpression() instanceof Column
					|| !((Column) arg0.getLeftExpression()).toString().equals(attribute)) {
				selectExpression = arg0;
			} else {
				highkey = (int) ((LongValue) arg0.getRightExpression()).getValue() - 1;
				lowkey = null;
				indexExpression = arg0;
			}
		} else {
			if (!((Column) arg0.getRightExpression()).toString().equals(attribute)) {
				selectExpression = arg0;
			} else {
				lowkey = (int) ((LongValue) arg0.getLeftExpression()).getValue() + 1;
				highkey = null;
				indexExpression = arg0;
			}
		}
	}

	@Override
	public void visit(MinorThanEquals arg0) {
		if (arg0.getLeftExpression() instanceof Column) {
			if (arg0.getRightExpression() instanceof Column
					|| !((Column) arg0.getLeftExpression()).toString().equals(attribute)) {
				selectExpression = arg0;
			} else {
				highkey = (int) ((LongValue) arg0.getRightExpression()).getValue();
				lowkey = null;
				indexExpression = arg0;
			}
		} else {
			if (!((Column) arg0.getRightExpression()).toString().equals(attribute)) {
				selectExpression = arg0;
			} else {
				lowkey = (int) ((LongValue) arg0.getLeftExpression()).getValue();
				highkey = null;
				indexExpression = arg0;
			}
		}
	}

	@Override
	public void visit(NotEqualsTo arg0) {
		selectExpression = arg0;
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
