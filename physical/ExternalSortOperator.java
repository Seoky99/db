package physical;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.UUID;

import cs4321.Tuple;
import cs4321.TupleComparator;
import readwrite.OurTupleReader;
import readwrite.OurTupleWriter;
import readwrite.TupleReader;

//Class representing the physical sort operator with bounded state using the disk
public class ExternalSortOperator extends Operator {

	private Operator o;
	private int numPages;
	private long numTuples;
	private String id;
	private TupleReader sortedReader;
	public TupleComparator tupleComparator;
	private String tempDir;
	private ArrayList<String> attributes;
	private boolean empty = false;
	
	/**
	 * Constructor for ExternalSortOperator
	 * @param numPages - number of pages the sort can use for its buffer
	 * @param tempDir - the directory to write temporary files to disk
	 * @param o - the operator whose tuples we are sorting
	 * @param orderBy - the order we are sorting the columns by
	 * @param isProject - whether or not the operator is a ProjectOperator
	 */
	public ExternalSortOperator(int numPages, String tempDir, Operator o, List<String> orderBy, boolean isProject) {
		
		this.tempDir = tempDir;
		
		if(isProject) {
			attributes = ((ProjectOperator) o).attributes;
		} else {
			if(o.getNextTuple() == null) {
				empty = true;
				return;
			}
			
			o.reset();
			
			Set<String> unsortedAttsSet = o.getNextTuple().map.keySet();
			String[] unsortedAtts = new String[unsortedAttsSet.size()];
			unsortedAttsSet.toArray(unsortedAtts);
			Arrays.sort(unsortedAtts);
			attributes = new ArrayList<String>(Arrays.asList(unsortedAtts));
			o.reset();
		}
		
		List<String> newOrderBy = new LinkedList<String>();
		if(orderBy != null) {
			newOrderBy = orderBy;
		}
		
		if(isProject) {
			for(String s : attributes) {
				if(!newOrderBy.contains(s)) {
					newOrderBy.add(s);
				}
			}
		}
		
		tupleComparator = new TupleComparator(newOrderBy);

		try {
			this.o = o;
			this.numPages = numPages;
			id = UUID.randomUUID().toString();
			Tuple t = o.getNextTuple();
			numTuples = numPages * (1024/(t.map.keySet().size()));
			ArrayList<Tuple> b = new ArrayList<Tuple>();
			
			LinkedList<String> pass0pages = new LinkedList<String>();
			
			while(t != null) {
				String pathForNextPage = tempDir + id + "pass0" + "page" + String.valueOf(pass0pages.size());
				pass0pages.add(pathForNextPage);
				OurTupleWriter otw = new OurTupleWriter(pathForNextPage);
				
				for(int i = 0; i < numTuples; i++) {
					if(t == null) {
						break;
					}
					b.add(t);	
					t = o.getNextTuple();
				}
				
				b.sort(tupleComparator);
				
				for(Tuple tp : b) {
					otw.write(new Tuple(tp.map, attributes));
				}
				
				b.clear();
				otw.close();
			}
			
			sortedReader = passes(pass0pages, 1);
		} 
		catch(IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Do passes pass-n to merge all of the sorted temporary files in the temp directory
	 * @param passNpages - the names of all of the temporary sorted files to be merged
	 * @param pass - the number of the pass we are on
	 * @return - a tuple reader for the final sorted file
	 */
	private TupleReader passes(LinkedList<String> passNpages, int pass) {
		try {

			if(passNpages.size() == 1) {
				return new OurTupleReader(new File(passNpages.get(0)));
			}
			
			LinkedList<String> newPassPages = new LinkedList<String>();
			
			while(passNpages.size() > 0) {
				String pathForNextPage = tempDir + id + "pass" + String.valueOf(pass) 
					+ "page" + String.valueOf(newPassPages.size());
				newPassPages.add(pathForNextPage);
				OurTupleWriter otw = new OurTupleWriter(pathForNextPage);
				PriorityQueue<BufferedTuple> p = 
						new PriorityQueue<BufferedTuple>(numPages - 1, new BufferedTupleComparator(tupleComparator));
				
				for(int i = 0; i < numPages - 1; i++) {
					if(passNpages.size() == 0) {
						break;
					}
					String page = passNpages.remove();
					OurTupleReader tr = new OurTupleReader(new File(page));
					p.add(new BufferedTuple(tr, attributes));
				}
				
				while(p.size() > 0) {
					BufferedTuple bt = p.poll();
					if(!bt.hasTuple) {
						continue;
					}
					otw.write(bt.t);
					bt.getNextTuple();
					p.add(bt);
				}
				
				otw.close();
			}
			return passes(newPassPages, pass + 1);
		} 
		catch(IOException e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
	/**
	 * Gets the next tuple in the operator, null if finished
	 * @return The next tuple in the operator, null if finished
	 */
	@Override
	public Tuple getNextTuple() {
		if(empty == true) {
			return null;
		}
		String line = null;
		try {
			line = sortedReader.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		if(line == null) {
			return null;
		}
		
		return new Tuple(line, attributes);
	}

	/**
	 * Resets the operator to its first tuple
	 */
	@Override
	public void reset() {
		try {
			sortedReader.reset();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Repeatedly call getNextTuple() until it returns null and dumps contents to a
	 * PrintStream, over-ridden to close print stream 
	 * 
	 * @param tw - the print stream into which each tuple will be output
	 * @throws IOException
	 */
	@Override
	public void dump(OurTupleWriter tw) throws IOException {

		Tuple nextTuple = getNextTuple();
		while (nextTuple != null) {
			tw.write(nextTuple);
			nextTuple = getNextTuple();
		}
		
		sortedReader.close();
	}

	//Class representing a tuple from a tuplereader and its underlying reader
	private static class BufferedTuple {
		public Tuple t;
		public TupleReader r;
		public boolean hasTuple = true;
		public ArrayList<String> attributes;
		
		/**
		 * Constructor for BufferedTuple
		 * @param r - the underlying reader we should read from
		 * @param attributes - the fully qualified column names of the tuples in the file
		 */
		public BufferedTuple(TupleReader r, ArrayList<String> attributes) {
			this.r = r;
			this.attributes = attributes;
			t = getNextTuple();
		}
		
		/**
		 * Gets the next tuple in the tuple reader
		 * @return - the next tuple in the tuple reader
		 */
		public Tuple getNextTuple() {
			String line = null;
			try {
				line = r.readLine();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			if(line == null) {
				hasTuple = false;
				return null;
			}
			t = new Tuple(line, attributes);
			return t;
		}
		
	}
	
	//Comparator for buffered tuples
	private static class BufferedTupleComparator implements Comparator<BufferedTuple> {
		public TupleComparator tc;
		
		/**
		 * Constructor for BufferedTupleComparator
		 * @param tc
		 */
		public BufferedTupleComparator(TupleComparator tc) {
			this.tc = tc;
		}
		
		/**
		 * Compares two BufferedTuples
		 * @param a - the first tuple to be compared
		 * @param b - the second tuple to be compared
		 * @return < 0 if a < b, 0 if a==b, > 0 if a > b
		 */
		public int compare(BufferedTuple a, BufferedTuple b) {
			if(!a.hasTuple && !b.hasTuple) {
				return 0;
			}
			if(!a.hasTuple) {
				return -1;
			}
			if(!b.hasTuple) {
				return 1;
			}
			
			return tc.compare(a.t, b.t);
		}
	}

	/**
	 * Resets the operator to a given tuple index
	 * @param index - the index to reset the operator to
	 */
	@Override
	public void reset(int index) {
		try {
			sortedReader.reset(index);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}