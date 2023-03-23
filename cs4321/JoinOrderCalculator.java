package cs4321;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import logical.*;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import visitors.VCalculator;

public class JoinOrderCalculator {
	public Plan p; 
	private LogicalMultiJoin lmj;
	private ArrayList<String> tables;
	private ArrayList<HashMap<HashSet<String>, Plan>> plans = new ArrayList<HashMap<HashSet<String>, Plan>>();
	private HashMap<String, String> aliases = DatabaseCatalog.getInstance().getAliases();
	ArrayList<HashSet<HashSet<String>>> powerSets = new ArrayList<HashSet<HashSet<String>>>();
	
	/**
	 * Constructor
	 * @param lmj - LogicalMultiJoin used to construct join order
	 */
	public JoinOrderCalculator(LogicalMultiJoin lmj) {
		this.lmj = lmj;
		this.tables = new ArrayList<String>();
		for(LogicalSelectOperator c : lmj.children) {
			tables.add(((LogicalScanOperator) c.op).name);
		}
		
		for(int i = 0; i < this.tables.size(); i++) {
			plans.add(new HashMap<HashSet<String>, Plan>());
			powerSets.add(new HashSet<HashSet<String>>());
		}
		
		HashSet<String> tablesSet = new HashSet<String>();
		for(String s : tables) {
			tablesSet.add(s);
		}
		
		for(HashSet<String> hs : powerSet(tablesSet)) {
			if(hs.size() == 0) {
				continue;
			} else {
				powerSets.get(hs.size()-1).add(hs);
			}
		}
		
		for(int i = 0; i < this.tables.size(); i++) {
			for(HashSet<String> set : powerSets.get(i)) {
				Plan bestPlan = bestPlanForSubset(set);
				if(i == this.tables.size() - 1) {
					this.p = bestPlan;
				}
			}
		}
		
	}
	
	/**
	 * Returns the power set of a given set
	 * @param set - The set
	 * @return - The powerset
	 */
	public HashSet<HashSet<String>> powerSet(HashSet<String> set) {
	    if (set.isEmpty()) {
	        HashSet<HashSet<String>> ret = new HashSet<>();
	        ret.add(set);
	        return ret;
	    }

	    String element = set.iterator().next();
	    HashSet<String> subSetWithoutElement = cloneAndRemove(set, element);
	    HashSet<HashSet<String>> powerSetSubSetWithoutElement = powerSet(subSetWithoutElement);
	    HashSet<HashSet<String>> powerSetSubSetWithElement = addElementToAll(powerSetSubSetWithoutElement, element);

	    HashSet<HashSet<String>> powerSet = new HashSet<>();
	    powerSet.addAll(powerSetSubSetWithoutElement);
	    powerSet.addAll(powerSetSubSetWithElement);
	    return powerSet;
	}
	
	/**
	 * Returns the given set of sets with the string added to every set
	 * @param set - The set of sets
	 * @param str - The string
	 * @return - The set of sets with the string added to every set
	 */
	public HashSet<HashSet<String>> addElementToAll(HashSet<HashSet<String>> set, String str) {
		HashSet<HashSet<String>> toReturn = new HashSet<>();
		for(HashSet<String> hs : set) {
			HashSet<String> toAdd = new HashSet<String>();
			for(String s : hs) {
				toAdd.add(s);
			}
			toAdd.add(str);
			toReturn.add(toAdd);
		}

		return toReturn;
	}
	
	/**
	 * Clones the set and removes the specified string
	 * @param set - Set to clone
	 * @param toRemove - String to remove
	 * @return - Cloned set with string removed
	 */
	public HashSet<String> cloneAndRemove(HashSet<String> set, String toRemove) {
		HashSet<String> toReturn = new HashSet<String>();
		for(String s : set) {
			toReturn.add(s);
		}
		toReturn.remove(toRemove);
		return toReturn;
	}
	
	/**
	 * Returns the best plan for the given subset
	 * @param subset - The subset
	 * @return - The best plan for the given subset
	 */
	public Plan bestPlanForSubset(HashSet<String> subset) {
		ArrayList<String> jO = new ArrayList<String>();
		double cost = 0;
		if(subset.size() == 1) {
			for(String s : subset) {
				jO.add(s);
			}
			Plan p = new Plan(jO, cost, 0, null);
			plans.get(0).put(subset, p);
			return p;
		}
		else if(subset.size() == 2) {
			String[] sArr =  new String[2];
			int x = 0;
			for(String s : subset) {
				sArr[x] = s;
				x++;
			}
			
			double[] costs = new double[2];
			LogicalSelectOperator[] selects = new LogicalSelectOperator[2];
			
			for(int i = 0; i < 2; i++) {
				for(LogicalSelectOperator c : lmj.children) {
					if(((LogicalScanOperator) c.op).name.equals(sArr[i])) {
						costs[i] = VCalculator.getSelectSize((LogicalSelectOperator) c);
						selects[i] = c;
					}
				}
			}
			
			LogicalJoinOperator j = null;
			Expression je = lmj.ufv.equalityJoinExpressions.get(subset);
			Expression lje = lmj.ufv.leftoverExpressions.get(subset);
			
			if(lje != null && je != null) {
				je = new AndExpression(je, lje);
			} else if(lje != null) {
				je = lje;
			}
			
			if(costs[0] <= costs[1]) {
				jO.add(sArr[0]);
				jO.add(sArr[1]);
				j = new LogicalJoinOperator(selects[0], selects[1], je, sArr[1]);
			} else {
				jO.add(sArr[1]);
				jO.add(sArr[0]);
				j = new LogicalJoinOperator(selects[1], selects[0], je, sArr[0]);
			}
			
			double topCost = VCalculator.getJoinSize(j);
			Plan p = new Plan(jO, cost, topCost, j);
			plans.get(1).put(subset, p);
			return p;
		}
		else {
			Plan p = null;
			double minCost = Double.MAX_VALUE;
			
			for(String s : subset) {
				HashSet<String> removedSubset = new HashSet<String>();
				removedSubset.addAll(subset);
				removedSubset.remove(s);
				Plan partialPlan = plans.get(subset.size() - 2).get(removedSubset);
				if(partialPlan.upCost < minCost) {
					minCost = partialPlan.upCost;
					
					LogicalSelectOperator select = null;
					for(LogicalSelectOperator c : lmj.children) {
						if(((LogicalScanOperator) c.op).name.equals(s)) {
							select = c;
							break;
						}
					}
					
					Expression totalJoin = null;
					
					for(String rs : removedSubset) {
						HashSet<String> jSet = new HashSet<String>();
						jSet.add(rs);
						jSet.add(s);
						totalJoin = addExpression(totalJoin,lmj.ufv.equalityJoinExpressions.get(jSet));
						totalJoin = addExpression(totalJoin,lmj.ufv.leftoverExpressions.get(jSet));
					}
					
					LogicalJoinOperator j = new LogicalJoinOperator(partialPlan.op, select, totalJoin, s);
					double topCost = VCalculator.getJoinSize(j);
					jO = new ArrayList<String>();
					for(String jOs : partialPlan.joinOrder) {
						jO.add(jOs);
					}
					jO.add(s);
					p = new Plan(jO, minCost, topCost, j);
				}
			}
			plans.get(subset.size() - 1).put(subset, p);
			return p;
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
	 * Abstraction of a partial plan
	 */
	public static class Plan {
		public ArrayList<String> joinOrder;
		public double cost;
		public double upCost;
		public LogicalOperator op;
		
		/**
		 * Constructor
		 * @param joinOrder - Join order
		 * @param cost - Cost
		 * @param upCost - Cost of any join that has this as a left child
		 * @param op - Logical representation of the plan
		 */
		public Plan(ArrayList<String> joinOrder, double cost, double upCost, LogicalOperator op) {
			this.joinOrder = joinOrder;
			this.cost = cost;
			this.op = op;
			this.upCost = upCost;
		}
	}
}
