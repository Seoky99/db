package physical;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cs4321.SortMergeTupleComparator;
import cs4321.Tuple;
import net.sf.jsqlparser.expression.Expression;

//Sort Merge implementation of the join operator 
public class SortMergeJoinOperator extends Operator {

	private Operator left;
	private Operator right;
	private SortMergeTupleComparator SMcomparator;

	private int Ts_position;
	private int Tr_position;
	private int Gs_position;

	private Tuple Ts;
	private Tuple Tr;
	private Tuple Gs;
	
	private boolean empty = false;

	private boolean inMiddleOfOutput;
	
	public SortMergeJoinOperator(List<String> leftOrdering, List<String> rightOrdering, Operator left, Operator right) {

		this.left = left;
		this.right = right;
		this.SMcomparator = new SortMergeTupleComparator(leftOrdering, rightOrdering);

		this.Ts_position = 0;
		this.Tr_position = 0;
		this.Gs_position = 0;

		this.Tr = left.getNextTuple();
		this.Ts = right.getNextTuple();
		
		if(this.Tr == null || this.Ts == null) {
			empty = true;
		}
		
		this.Gs = this.Ts;

		this.inMiddleOfOutput = false;

	}

	/** Gets the next tuple of the SMJ implementation 
	 * @return - The next tuple 
	 */
	public Tuple getNextTuple() {
		if(empty) {
			return null;
		}
		
		while (Tr != null && Gs != null) {
			
			
			if (!inMiddleOfOutput) {
				while (SMcomparator.compare(Tr, Gs) < 0) {
					Tr = left.getNextTuple();
					Tr_position++;
				}
				
				while (SMcomparator.compare(Tr, Gs) > 0) {
					Gs = right.getNextTuple();
					Gs_position++;
				}

				Ts = Gs;
				Ts_position = Gs_position;
			}

			while (SMcomparator.isEqual(Tr, Gs) || inMiddleOfOutput) {
				
				// only reset S partition when it isn't Middle of the output
				if (!inMiddleOfOutput) {
					Ts = Gs;
					Ts_position = Gs_position;
					right.reset(Gs_position + 1);
				}

				while (SMcomparator.isEqual(Tr, Ts)) {

					
					Tuple output = glue(Tr, Ts);

					Ts = right.getNextTuple();

					Ts_position++;
					inMiddleOfOutput = true;

					return output;
				}

				//System.out.println("SETTING FALSE");
				inMiddleOfOutput = false;
				Tr = left.getNextTuple();
				Tr_position++;
			}

			Gs = Ts;
			Gs_position = Ts_position;

		}

		return null;
	}

	/**
	 * Takes two tuples are return one appended to the other
	 * 
	 * @param a - one of the tuples to be combined
	 * @param b - one of the tuples to be combined
	 * @return The two tuple put together
	 */
	private Tuple glue(Tuple a, Tuple b) {
		HashMap<String, Integer> newMap = new HashMap<String, Integer>();
		ArrayList<Integer> contents = new ArrayList<Integer>();

		for (Map.Entry<String, Integer> e : a.map.entrySet()) {
			newMap.put(e.getKey(), e.getValue());
			contents.add(e.getValue());
		}

		for (Map.Entry<String, Integer> e : b.map.entrySet()) {
			newMap.put(e.getKey(), e.getValue());
			contents.add(e.getValue());
		}

		return new Tuple(newMap);
	}

	/** Resets the SMJ operator 
	 */
	public void reset() {

		this.Ts_position = 0;
		this.Tr_position = 0;
		this.Gs_position = 0;

		left.reset();
		right.reset();

		this.Tr = left.getNextTuple();
		this.Ts = right.getNextTuple();
		this.Gs = this.Ts;

		this.inMiddleOfOutput = false;
	}

	// I think this can be unimplemented
	public void reset(int index) {

	}
}
