package physical;

import java.io.IOException;
import java.io.PrintStream;

import cs4321.Tuple;
import readwrite.OurTupleWriter;

//A relation operator
public abstract class Operator {

	/**
	 * Get the next tuple from the operator
	 * 
	 * @return The next tuple from the operator
	 */
	public abstract Tuple getNextTuple();

	/**
	 * Resets the operator, such that getNextTuple() will return the operators first
	 * tuple again
	 */
	public abstract void reset();
	
	/**
	 * Resets the operator, such that getNextTuple() will return the tuple at index
	 */
	public abstract void reset(int index); 

	/**
	 * Repeatedly call getNextTuple() until it returns null and dumps contents to a
	 * PrintStream
	 * 
	 * @param tw - the printstream into which each tuple will be output
	 * @throws IOException
	 */
	public void dump(OurTupleWriter tw) throws IOException {
		//long startTime = System.currentTimeMillis();
		//System.out.println("Start dump " + this.getClass() + ": " + startTime);
		Tuple nextTuple = getNextTuple();
		while (nextTuple != null) {
			tw.write(nextTuple);
			nextTuple = getNextTuple();
		}
		//System.out.println("Total dump " + this.getClass() + ": " + (System.currentTimeMillis() - startTime));
	}

}