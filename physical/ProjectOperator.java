package physical;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import cs4321.Tuple;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectItem;

//Project relational operator
public class ProjectOperator extends Operator {
	
	private Operator so;
	public ArrayList<String> attributes;
	
	/**
	 * Constructor for the project operator
	 * @param so - the operator to perform the projection on
	 * @param attributes - the list of columns that will be the result of the project (e.g S.A, S.B, *)
	 */
	public ProjectOperator(Operator so, ArrayList<String> attributes) {
		this.so = so;
		this.attributes = attributes;
	}

	/**
	 * Gets the next tuple of the project operator, null if none exists
	 * @return - The next tuple of the project operator 
	 */
	@Override
	public Tuple getNextTuple() {
		Tuple t = so.getNextTuple();

		//check if next tuple doesn't exist
		if (t == null) {
			return null;
		}
		
		List<Integer> contents = new LinkedList<Integer>();
		
		for(String s : attributes) {
			contents.add(t.map.get(s));
		}
		
		return new Tuple(t.map, contents);
	}

	/**
	 * Resets the project operator
	 */
	@Override
	public void reset() {
		so.reset();
	}

	@Override
	public void reset(int index) {
		// TODO Auto-generated method stub
		
	}

}