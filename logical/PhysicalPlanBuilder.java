package logical;

import cs4321.*;
import indexes.IndexConditionSplitter;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import physical.BNLJOperator;
import physical.EliminateDuplicatesOperator;
import physical.ExternalSortOperator;
import physical.IndexScan;
import physical.JoinOperator;
import physical.Operator;
import physical.ProjectOperator;
import physical.ScanOperator;
import physical.SelectOperator;
import physical.SortMergeJoinOperator;
import physical.SortOperator;
import visitors.CountInequalitiesVisitor;
import visitors.ExtractSortMergeOrderVisitor;
import visitors.VCalculator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

//Builds a physical plan based on the type of logical operator passed in 
public class PhysicalPlanBuilder {

	public Operator result;

	/**
	 * Causes the plan builder to create a physical version of the operator
	 * 
	 * @param o - The type of logical operator
	 */
	public void visit(LogicalScanOperator o) {
				
		OutputPlan out = DatabaseCatalog.getInstance().getOutputPlan(); 
		out.printSomething("TableScan[" +  o.baseTable + "]");
		
		result = new ScanOperator(o.baseTable, o.name);
	}

	/**
	 * Causes the plan builder to create a physical version of the operator
	 * 
	 * @param o - The type of logical operator
	 */
	public void visit(LogicalSelectOperator o) {

		OutputPlan out = DatabaseCatalog.getInstance().getOutputPlan(); 
		
		if (o.op instanceof LogicalScanOperator && o.e != null) {
			
			// Scan Cost
			String baseTable = ((LogicalScanOperator) o.op).baseTable;
			HashMap<String, StatEntry> aStats = DatabaseCatalog.getInstance().getAdvancedStats();

			int numTuples = aStats.get(baseTable).getTuples();
			HashMap<String, int[]> attMap = aStats.get(baseTable).getAttributeMap();
			int bytesTuples = numTuples * attMap.size() * 4;
			int scanCost = bytesTuples / 4096;

			// Index cost
			ArrayList<AttInfo> attInfoList = DatabaseCatalog.getInstance().getAllIndexInformation().get(baseTable);

			// cost of each index, if null: that index was not used in the selection
			HashMap<String, Double> indexCosts = new HashMap<>();
			
			if(attInfoList != null) {
				for (AttInfo attInfo : attInfoList) {
	
					String attName = attInfo.getAttName();
					boolean clustered = attInfo.getClustered();
					int numLeaves = attInfo.getNumLeaves();
	
					// extract aliases and column name
					String[] aliasedAttributeSplit = attName.split("\\.");
					aliasedAttributeSplit[0] = ((LogicalScanOperator) o.op).name;
					String aliasedAttribute = aliasedAttributeSplit[0] + "." + aliasedAttributeSplit[1];
					String columnName = aliasedAttributeSplit[1];
	
					IndexConditionSplitter ics = new IndexConditionSplitter(aliasedAttribute);
					o.e.accept(ics);
	
					if (ics.indexExpression == null) {
						// when the attribute is not present in the index expression, treat as if you
						// were doing scan
						indexCosts.put(aliasedAttribute, null);
					} else {
	
						// high and low bound are ranges attributes can take, low and high range are the
						// selection range
						int highBound = DatabaseCatalog.getInstance().getAdvancedStats().get(baseTable).getAttributeMap()
								.get(columnName)[1];
						int lowBound = DatabaseCatalog.getInstance().getAdvancedStats().get(baseTable).getAttributeMap()
								.get(columnName)[0];
						int lowRange = 0;
						int highRange = 0;
	
						if (ics.lowkey == null) {
							lowRange = lowBound;
							highRange = ics.highkey;
						} else if (ics.highkey == null) {
							lowRange = ics.lowkey;
							highRange = highBound;
						} else {
							lowRange = ics.lowkey;
							highRange = ics.highkey;
						}
	
						double reductionFactor = (double) (highRange - lowRange + 1) / (highBound - lowBound + 1);
	
						int p = scanCost;
						int l = numLeaves;
						double r = reductionFactor;
						int t = numTuples;
	
						// costs can be calculated like this for indexes because you will scan all
						// qualifying index pages anyway for other selection criteria
						if (clustered) {
	
							indexCosts.put(aliasedAttribute, 3 + p * r);
	
						} else {
	
							indexCosts.put(aliasedAttribute, 3 + l * r + t * r);
	
						}
					}
				}
			}

			// Find the minimum cost and store information of the tables, aliased, clustered
			double minimum = scanCost;
			String associatedName = "scan";
			String attName = "";
			boolean isClustered = false;
			
			if(attInfoList != null) {
				for (AttInfo attInfo : attInfoList) {
	
					String baseName = attInfo.getAttName();
					String[] aliasedAttributeSplit = baseName.split("\\.");
					aliasedAttributeSplit[0] = ((LogicalScanOperator) o.op).name;
					String aliasedAttribute = aliasedAttributeSplit[0] + "." + aliasedAttributeSplit[1];
	
					if (indexCosts.get(aliasedAttribute) != null) {
	
						if (indexCosts.get(aliasedAttribute) < minimum) {
							minimum = indexCosts.get(aliasedAttribute);
							associatedName = aliasedAttribute;
							isClustered = attInfo.getClustered();
							attName = baseName;
						}
					}
				}
			}

			// create the actual operators
			if (associatedName.equals("scan")) {
				// scan is cheapest
				PhysicalPlanBuilder b = new PhysicalPlanBuilder();
				o.op.accept(b);
				
				out.printSomething("Select[" +  o.e + "]");
				result = new SelectOperator(o.e, b.result);
			} else {

				// an index is cheaper
				IndexConditionSplitter ics = new IndexConditionSplitter(associatedName);
				o.e.accept(ics);

				String table = ((LogicalScanOperator) o.op).name;

				IndexScan ic = new IndexScan(baseTable, table, DatabaseCatalog.getInstance().getIndexLocation(attName),
						DatabaseCatalog.getInstance().getFileLocation(baseTable), isClustered, ics.lowkey, ics.highkey,
						attName);
				
				String[] aliasedAttributeSplit = attName.split("\\.");
				String columnName = aliasedAttributeSplit[1];

				int highBound = DatabaseCatalog.getInstance().getAdvancedStats().get(baseTable).getAttributeMap()
						.get(columnName)[1];
				int lowBound = DatabaseCatalog.getInstance().getAdvancedStats().get(baseTable).getAttributeMap()
						.get(columnName)[0];
				
				Integer lowResult = ics.lowkey; 
				Integer highResult = ics.highkey; 
				
				if (lowResult == null) {
					lowResult = lowBound; 
				} 
				
				if (highResult == null) {
					highResult = highBound;
				}
				
				out.printSomething("IndexScan[" + baseTable + "," + columnName + "," + lowResult + "," + highResult + "]");
				
				if (ics.selectExpression != null) {
					
					//also probably print the names of non index columns as selects
					//out.printSomething("Select[" + baseTable + "," + columnName + "," + lowResult + "," + highResult + "]");
					result = new SelectOperator(ics.selectExpression, ic);
				} else {
					result = ic;
				}
			}

			// when o.e == null or o.op not instance of LogicalOperator
		} else {

			//out.printSomething("Select[" + o.e + "]");
			PhysicalPlanBuilder b = new PhysicalPlanBuilder();
			o.op.accept(b);
			result = new SelectOperator(o.e, b.result);
		}
	}

	/**
	 * Causes the plan builder to create a physical version of the operator
	 * 
	 * @param o - The type of logical operator
	 */
	public void visit(LogicalProjectOperator o) {
		
		OutputPlan out = DatabaseCatalog.getInstance().getOutputPlan(); 
		out.printSomething("Project" +  o.attributes);
		
		PhysicalPlanBuilder b = new PhysicalPlanBuilder();
		o.op.accept(b);

		result = new ProjectOperator(b.result, o.attributes);
	}

	/**
	 * Causes the plan builder to create a physical version of the operator We
	 * separate the sorts based on external or in memory here
	 * 
	 * @param o - The type of logical operator
	 */
	public void visit(LogicalSortOperator o) {
		
		OutputPlan out = DatabaseCatalog.getInstance().getOutputPlan(); 
		out.printSomething("ExternalSort" + o.orderBy);
		
		// We may want to pick a number intelligently
		int buffNumber = 20;

		PhysicalPlanBuilder b = new PhysicalPlanBuilder();
		o.projectOp.accept(b);

		boolean isProject = o.projectOp instanceof LogicalProjectOperator;

		result = new ExternalSortOperator(buffNumber, DatabaseCatalog.getInstance().getTempDir(), b.result, o.orderBy,
				isProject);
	}

	/**
	 * Causes the plan builder to create a physical version of the operator
	 * 
	 * @param o - The type of logical operator
	 */
	public void visit(LogicalEliminateDuplicatesOperator o) {
		
		OutputPlan out = DatabaseCatalog.getInstance().getOutputPlan(); 
		out.printSomething("DupElim");
		
		PhysicalPlanBuilder b = new PhysicalPlanBuilder();
		o.sortOp.accept(b);
		if (b.result instanceof SortOperator) {
			result = new EliminateDuplicatesOperator((SortOperator) b.result);
		} else {
			result = new EliminateDuplicatesOperator((ExternalSortOperator) b.result);
		}
	}

	/**
	 * Causes the plan builder to create a physical version of the operator For the
	 * join, we separate here based on the values of the inputs in the configuration
	 * file
	 * 
	 * @param o - The type of logical operator
	 */
	public void visit(LogicalJoinOperator o) {
		
		OutputPlan out = DatabaseCatalog.getInstance().getOutputPlan(); 

		
		// RATIONALE: Use SMJ when possible. Use BNLJ for small table sizes,
		// non-equality conditions, and pure cross products.

		// Detecting small table sizes
		boolean useSMJ = true;
		double leftValue = 0;
		double rightValue = 0;

		if ((o.left instanceof LogicalSelectOperator || o.left instanceof LogicalJoinOperator)
				&& (o.right instanceof LogicalSelectOperator || o.right instanceof LogicalJoinOperator)) {
			
			if (o.left instanceof LogicalSelectOperator) {
				leftValue = VCalculator.getSelectSize((LogicalSelectOperator) o.left);
			}

			if (o.left instanceof LogicalJoinOperator) {
				leftValue = VCalculator.getJoinSize((LogicalJoinOperator) o.left);
			}

			if (o.right instanceof LogicalSelectOperator) {
				rightValue = VCalculator.getSelectSize((LogicalSelectOperator) o.right);
			}

			if (o.right instanceof LogicalJoinOperator) {
				rightValue = VCalculator.getJoinSize((LogicalJoinOperator) o.right);
			}

			// larger table sizes
			if (leftValue < 100 || rightValue < 100) {
				useSMJ = false;
			}
		}

		// There are no non-equality conditions
		CountInequalitiesVisitor cev = new CountInequalitiesVisitor();

		if (o.e != null) {
			o.e.accept(cev);
			
			if (cev.getResult() > 0) {
				useSMJ = false;
			}
		}

		// Pure cross products
		if (o.e == null) {
			useSMJ = false;
		}

		// BNLJ
		if (!useSMJ) {

			out.printSomething("BNLJ[" +  o.e + "]");
			
			// BNLJ
			PhysicalPlanBuilder left = new PhysicalPlanBuilder();
			o.left.accept(left);
			PhysicalPlanBuilder right = new PhysicalPlanBuilder();
			o.right.accept(right);

			// Arbitrarily use 20
			result = new BNLJOperator(20, left.result, right.result, o.e);

		} else {

			out.printSomething("SMJ[" +  o.e + "]");
			
			// SMJ
			String rightTable = o.rightTable;

			// extract the left orderby and right orderby from the join expression passed in
			Expression joinExpression = o.e;

			ExtractSortMergeOrderVisitor visitor = new ExtractSortMergeOrderVisitor(rightTable);
			joinExpression.accept(visitor);

			ArrayList<String> leftOrderBy = visitor.leftOrderBy;
			ArrayList<String> rightOrderBy = visitor.rightOrderBy;

			// in memory sort
			try {

				PhysicalPlanBuilder leftPlan = new PhysicalPlanBuilder();
				PhysicalPlanBuilder rightPlan = new PhysicalPlanBuilder();

				String tempDirPath = DatabaseCatalog.getInstance().getTempDir();

				LogicalSortOperator newLogicalLeftSort = new LogicalSortOperator(tempDirPath, leftOrderBy, o.left);

				LogicalSortOperator newLogicalRightSort = new LogicalSortOperator(tempDirPath, rightOrderBy, o.right);

				newLogicalLeftSort.accept(leftPlan);
				newLogicalRightSort.accept(rightPlan);

				result = new SortMergeJoinOperator(leftOrderBy, rightOrderBy, leftPlan.result, rightPlan.result);

			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}

	/**
	 * Parses the config file
	 * 
	 * @return The lines of the config file
	 */
	public static ArrayList<String> parseConfigFile() {

		String configPath = DatabaseCatalog.getInstance().getConfigurationLocation();

		ArrayList<String> configLines = new ArrayList<>();

		try {
			File file = new File(configPath);
			BufferedReader reader = new BufferedReader(new FileReader(file));
			String data;

			while ((data = reader.readLine()) != null) {
				configLines.add(data.trim());
			}

			reader.close();

		} catch (Exception e) {
			System.out.println("An error occurred.");
			e.printStackTrace();
		}

		for (String s : configLines) {
			// System.out.println(s);
		}

		return configLines;

	}

	public void visit(LogicalMultiJoin o) {
		JoinOrderCalculator joc = new JoinOrderCalculator(o);
		
		PhysicalPlanBuilder plan = new PhysicalPlanBuilder();
		joc.p.op.accept(plan);
		
		this.result = plan.result;
	}
}
