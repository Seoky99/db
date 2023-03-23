package logical;

import java.util.ArrayList;

//Interface for an logical operator that accepts PhysicalPlanBuilders
public interface LogicalOperator {
	public void accept(PhysicalPlanBuilder v);
	public ArrayList<String> lines();
}
