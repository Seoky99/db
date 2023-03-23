package cs4321;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import logical.*;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import physical.Operator;
import visitors.GetExpressionVisitor;
import visitors.UnionFindVisitor;

/*takes in a Statement then produces a query plan
 * The first step in this process is to create a tree representing all the join conditions.
 * 	Leaves of this tree represent tables, and a node with two children represents a join between two tables
 * We then use our GetExpressionVisitor to create a HashMap between sets of table names and expressions from the WHERE condition
 *  We split the WHERE clause along each AND and look at each clause recursively
 *  If we find two clauses that reference exactly the same tables, we put an AND back together for that set of tables 
 * 	If a set contains one table name, it is mapped to the select condition for that table, null if none exists
 * 	If a set contains two table names, it is mapped to the join condition between those two tables, null if none exists
 * Next, we recurse over a tree, constructing SelectOperators and the leaves and JoinOperators for nodes with children
 * 	We use our expressions HashMap to correctly assign the conditions to these operators
 * Finally, we create a ProjectOperator and conditionally a SortOperator and a EliminateDuplicatesOperator
 * 	We return this root operator
 */
public class QueryPlan {

	public HashMap<String,String> aliases;
	public String logicalPlanOutput;
	private Tree t;
	private List<String> tables;
	private GetExpressionVisitor gev;
	private ArrayList<String> attributes;
	private List<String> orderByElements;
	private boolean distinct;
	private HashSet<String> usedTables;
	private String tempDir;
	private UnionFind uf;
	private UnionFindVisitor ufv;

	/**
	 * Constructor for QueryPlan
	 * @return new QueryPlan
	 * @param statement - The statement from which to construct the query plan
	 */
	public QueryPlan(Statement statement, String tempDir) {
		this.tempDir = tempDir;
		usedTables = new HashSet<String>();
		this.aliases = getAliases(statement);
		DatabaseCatalog.getInstance().setAliases(aliases);
		
		PlainSelect pSelectBody = ((PlainSelect) ((Select) statement).getSelectBody());
		Expression whereCondition = pSelectBody.getWhere();
		List<SelectItem> items = pSelectBody.getSelectItems();
		getAttributes(items);
		List<OrderByElement> orderByItems = pSelectBody.getOrderByElements();
		
		if(pSelectBody.getDistinct() != null) {
			distinct = true;
		}
		
		if(orderByItems != null && orderByItems.size() != 0) {
			orderByElements = getOrderByList(orderByItems);
		}
			
		t = new Tree(null);
		Tree currTree = t;
		
		if(tables.size() > 2) {
			for(int i = 0; i < tables.size() - 2; i++) {
				currTree.right = new Tree(tables.get(tables.size() - i - 1), currTree);
				currTree.left = new Tree(currTree);
				currTree=currTree.left;
			}
			
			currTree.right = new Tree(tables.get(1), currTree);
			currTree.left = new Tree(tables.get(0), currTree);
		}
		else if(tables.size() == 2) {
			t.right = new Tree(tables.get(1), t);
			t.left = new Tree(tables.get(0), t);
		} else {
			t = new Tree(tables.get(0), null);
		}
		
		gev = new GetExpressionVisitor();
		this.uf = new UnionFind();
		ufv = new UnionFindVisitor(this.uf);
		if(whereCondition != null) {
			whereCondition.accept(ufv);
			whereCondition.accept(gev);
		}
	}

	/**
	 * Set local attributes field to a list of column names from a list of select items
	 * @param l - the list of select items after the SELECT
	 */
	private void getAttributes(List<SelectItem> l) {
		attributes = getAttList(l);
	}
	
	/**
	 * Returns the list of column names represented by a list of SelectItems
	 * @param l - List of select items
	 * @return - List of column names, including expanding * to a list of columns
	 */
	private ArrayList<String> getAttList(List<SelectItem> l) {
		ArrayList<String> atts = new ArrayList<String>();
		for(SelectItem si : l) {
			if(si instanceof SelectExpressionItem) {
				atts.add(si.toString());
			}
			else {
				atts.addAll(getStar());
			}
		}
		return atts;
	}
	
	/**
	 * Returns list of columns to order by
	 * @param l - List of OrderByElements from ORDER BY statement
	 * @return - List of column names to order by
	 */
	private List<String> getOrderByList(List<OrderByElement> l) {
		List<String> toReturn = new LinkedList<String>();
		
		for(OrderByElement obe : l) {
			toReturn.add(obe.toString());
		}
		
		return toReturn;
	}
	
	/**
	 * Gets the list of column names represented by a * in the current statement
	 * @return The list of column names represented by a *
	 */
	public List<String> getStar() {
		List<String> toReturn = new LinkedList<String>();
		
		for(String s : tables) {
			List<String> preAtts = DatabaseCatalog.getInstance().columnNames(aliases.get(s));
			List<String> postAtts = new LinkedList<String>();
			for(String si : preAtts) {
				postAtts.add(s + "." + si);
			}
			
			toReturn.addAll(postAtts);
		}
		
		return toReturn;
	}

	/**
	 * Constructs logical query plan the root operator of the new query
	 * @return Root operator of the query represented by the statement
	 */
	private LogicalOperator getLogicalRoot() {
		//New
		LogicalOperator o = getLogicalOpTreePostUnionFind();
		//Old
		//LogicalOperator o = getLogicalOpTree(t);
		if(o == null) {
			return null;
		}
		
		LogicalProjectOperator headP = new LogicalProjectOperator(attributes, o);
		
		if(distinct) {
			return new LogicalEliminateDuplicatesOperator(
					new LogicalSortOperator(tempDir, orderByElements, headP));
		}
		
		if(orderByElements != null) {
			return new LogicalSortOperator(tempDir, orderByElements, headP);
		}
		
		return headP;	
	}
	
	/**
	 * Constructs a physical query plan and returns the root operator
	 * @return - The root operator of the physical query plan
	 */
	public Operator getRoot() {
		PhysicalPlanBuilder b = new PhysicalPlanBuilder();
		
		LogicalOperator root = getLogicalRoot();
				
		if(root == null) {
			return null;
		}
		
		String logicalPlanOutput = "";
		for(String s : root.lines()) {
			String toAdd = s;
			if(toAdd.contains("[[")) {
				toAdd = toAdd.substring(toAdd.indexOf('['));
			}
			
			logicalPlanOutput += toAdd + "\n";
		}
		this.logicalPlanOutput = logicalPlanOutput;
		
		root.accept(b);
		
		return b.result;
	}
	
	/**
	 * Get all non-null join conditions for a given table
	 * @param table - the table that you wish to find join conditions for
	 * @return - An AND expression with every single join condition relating to the given table
	 */
	private Expression getJoinExpression(String table) {
		Expression toReturn = null;
		if(gev.getExpressions().entrySet().size() > 0) {
			for(String t : usedTables) {
				HashSet<String> toCheck = new HashSet<String>();
				toCheck.add(t);
				toCheck.add(table);
				if(gev.getExpressions().get(toCheck) != null) {
					if(toReturn == null) {
						toReturn = gev.getExpressions().get(toCheck);
					}
					else {
						toReturn = new AndExpression(toReturn, gev.getExpressions().get(toCheck));
					}
				}
			}
		}
		
		return toReturn;
	}
	
	/**
	 * Return the LogicalSelectOperator represented by a leaf
	 * @param t - Tree, assumed to have no children
	 * @return - LogicalSelectOperator represented by t
	 */
	private LogicalSelectOperator getLogicalLeaf(Tree t) {
		LogicalScanOperator sc = new LogicalScanOperator(aliases.get(t.table), t.table);
		HashSet<String> hs = new HashSet<String>();
		hs.add(t.table);
		return new LogicalSelectOperator(gev.getExpressions().get(hs), sc);
	}
	
	
	/**
	 * Return logical operator represented by the query
	 * @param t - Left deep tree representing query
	 * @return Logical operator representing the query, without the top projection, 
	 * with arbitrarily many selects to a single join
	 */
	private LogicalOperator getLogicalOpTreePostUnionFind() {
		if(tables.size() == 1) {
			return getLogicalLeaf(new Tree(tables.get(0), null));
		} else {
			LinkedList<LogicalSelectOperator> children = new LinkedList<LogicalSelectOperator>();
			for(String t: tables) {
				HashSet<String> tSet = new HashSet<String>();
				tSet.add(t);
				
				Expression selectExpression = null;
				if(ufv.leftoverExpressions != null) {
					selectExpression = ufv.leftoverExpressions.get(tSet);
				}
				if(selectExpression == null) {
					selectExpression = uf.getExpressionForTable(t);
				} else {
					Expression ufSelectExpression = uf.getExpressionForTable(t);
					if(ufSelectExpression != null) {
						selectExpression = new AndExpression(ufSelectExpression, selectExpression);
					}
				}
				
				LogicalScanOperator scan = new LogicalScanOperator(aliases.get(t), t);
				LogicalSelectOperator select = new LogicalSelectOperator(selectExpression, scan);
				children.add(select);
			}
			
			HashMap<Set<String>, Expression> joinExpressions = new HashMap<Set<String>, Expression>();
			for(Entry<Set<String>, Expression> ent : ufv.leftoverExpressions.entrySet()) {
				if(ent.getKey().size() == 2) {
					joinExpressions.put(ent.getKey(), ent.getValue());
				}
			}
			
			return new LogicalMultiJoin(joinExpressions, children, uf, ufv);
			
		}
	}
	
	/**
	 * Returns the logical AND of two expressions
	 * @param a
	 * @param b
	 */
	private Expression addExpression(Expression a, Expression b) {
		if(a == null) {
			return b;
		}
		if(b == null) {
			return a;
		} else {
			return new AndExpression(a, b);
		}
	}
	
	/**
	 * Return logical operator represented by the given tree
	 * @param t -Left deep tree representing query
	 * @return Logical operator representing the query, without the top projection
	 */
	private LogicalOperator getLogicalOpTree(Tree t) {
		if(gev.getExpressions() == null) {
			return null;
		}
		
		if(t.left == null) {
			return getLogicalLeaf(t);
		}
		
		Tree currTree = t;
		
		while(currTree.left != null) {
			currTree = currTree.left;
		}
		
		currTree = currTree.parent;
		usedTables.add(currTree.left.table);
		LogicalJoinOperator currJoin = 
				new LogicalJoinOperator(getLogicalLeaf(currTree.left), 
						getLogicalLeaf(currTree.right), getJoinExpression(currTree.right.table), currTree.right.table);
		usedTables.add(currTree.right.table);
		
		currTree = currTree.parent;
		
		while(currTree != null) {
			currJoin = new LogicalJoinOperator(currJoin, 
					getLogicalLeaf(currTree.right), getJoinExpression(currTree.right.table), currTree.right.table);
			usedTables.add(currTree.right.table);
			currTree = currTree.parent;
		}
		
		return currJoin;
	}
	
	/**
	 * Given the statement, create a HashMap of the aliases
	 * @param statement - The statement to take aliases from
	 * @return - A hash map from alias to base table name
	 */
	private HashMap<String, String> getAliases(Statement statement) {
		HashMap<String, String> toReturn = new HashMap<String, String>();
		
		List<Join> js = ((PlainSelect) ((Select) statement).getSelectBody()).getJoins();
		
		this.tables = new LinkedList<String>();
		LinkedList<String> tableAs = new LinkedList<String>();
		
		tableAs.add(((PlainSelect) ((Select) statement).getSelectBody()).getFromItem().toString());
		
		if(js != null) {
			for(Join j : js) {
				tableAs.add(j.toString());
			}
		}
		
		for(String s : tableAs) {
			if(s.contains(" AS ")) {
				int index = s.indexOf(" AS ");
				String key = s.substring(index + 4);
				String value = s.substring(0, index);
				toReturn.put(key, value);
				toReturn.put(value, value);
				tables.add(key);
			} else {
				tables.add(s);
				toReturn.put(s, s);
			}
		}
		
		return toReturn;
	}

}