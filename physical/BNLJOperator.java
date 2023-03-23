package physical;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import cs4321.Tuple;
import net.sf.jsqlparser.expression.Expression;
import readwrite.OurTupleWriter;
import visitors.EvaluateExpressionVisitor;


//Class representing the block nest loop join operator 
public class BNLJOperator extends Operator {

	private Operator left;
	private Operator right;
	Expression joinExpression;
	private Tuple out;
	private Tuple inn;
	long start = 0;

	// counts number of Tuples in Buffer
	private int bufferTupleCount;

	private int blockSize;
	private int numTuples;

	// keeps track of index as we iterate through the buffer
	private int bufferPos;

	ArrayList<Tuple> buffer;
	private boolean empty;


	/**
	 * Constructor for the BNLJ operator
	 * 
	 * @param numPages       - number of buffer pages
	 * @param outer          - the operator whose tuples are in the outer table
	 * @param inner          - the operator whose tuples are in the inner table
	 * @param joinExpression - the join expression which we check between tuples
	 */

	public BNLJOperator(int numPages, Operator outer, Operator inner, Expression joinExpression) {
		this.left = outer;
		this.right = inner;
		this.joinExpression = joinExpression;
		this.out = outer.getNextTuple();
		bufferPos = 0;
		bufferTupleCount = 0;

		if(out == null) {
			this.empty = true;
			return;
		}
		
		// number of tuples per page
		this.numTuples = (1024 / (out.map.keySet().size()));

		// total number of tuples the buffer can hold
		this.blockSize = numPages * numTuples;

		// create buffer data structure
		this.buffer = new ArrayList<Tuple>(blockSize);

	}

	/**
	 * Gets the next tuple of the join, null if none exists
	 * @return 
	 */
	public Tuple getNextTuple() {		
		Tuple toReturn = null;
		while(!empty && toReturn == null) {
			toReturn = getNextTupleInner();
		}
		return toReturn;
	}
	
	
	/**

	 * Gets the next tuple in the operator, null if it fails the join expression
	 * 
	 * @return The next tuple in the operator, null if it fails the join expression

	 */
	public Tuple getNextTupleInner() {
		// if we have an empty buffer, load the buffer with the block
		// adds first tuple
		if (buffer.size() == 0) {
			if (out != null) {
				buffer.add(out);
				bufferTupleCount += 1;
			}
			// load buffer with tuples from block
			for (int tupleCount = 0; tupleCount < blockSize - 1; tupleCount++) {

				// fill buffer with tuples from block
				out = left.getNextTuple();
				if (out != null) {
					bufferTupleCount += 1;
					buffer.add(out);
				}
			}

			if (buffer.size() == 0) {
				empty = true;
				return null;
			}
		}

		// bufferPos = 0 means we need to move to the next tuple in the inner relation
		if (bufferPos == 0) {

			inn = right.getNextTuple();

		}

		// if the inner tuple is null, we are at the end of the the inner relation
		if (inn == null) {
			buffer.clear();
			right.reset();
			out = left.getNextTuple();
			bufferTupleCount = 0;
			bufferPos = 0;
			return null;
		}

		else {

			// glue outer and inner tuples and check joinExpression

			Tuple glued = glue(buffer.get(bufferPos), inn);
			bufferPos += 1;
			if (bufferPos == bufferTupleCount) {
				bufferPos = 0;

			}

			if (joinExpression == null) {
				return glued;
			}

			EvaluateExpressionVisitor ev = new EvaluateExpressionVisitor(glued);
			joinExpression.accept(ev);

			if (ev.getResult()) {

				return glued;
			}

			else {
				return null;
			}

		}

	}

	/**

	 * Resets the operator to the tuple in index
	 * 
	 * @param index - index in which we want to reset to
	 */
	public void reset(int index) {

	}

	/**

	 * Resets the operator to its first tuple

	 */
	public void reset() {
		left.reset();
		right.reset();
		out = null;
		inn = null;
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
}
