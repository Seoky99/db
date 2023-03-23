package physical;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import cs4321.DatabaseCatalog;
import cs4321.Tuple;
import readwrite.OurTupleReader;
import readwrite.OurTupleWriter;
import readwrite.TupleReader;

//Used to read all tuples from a database file
public class ScanOperator extends Operator {

	private String baseTable;
	public String name;
	private TupleReader br;
	private File tableFile;
	/**
	 * Constructor for the ScanOperator
	 * 
	 * @param baseTable - Name of the table to be scanned, no aliasing
	 * @param name      - Name of the table to be scanned with its potential alias
	 */
	public ScanOperator(String baseTable, String name) {
		this.name = name;
		this.baseTable = baseTable;
		String baseTablePath = DatabaseCatalog.getInstance().getFileLocation(baseTable);
		this.tableFile = new File(baseTablePath);

		// open the file given by the catalog
		createNewBufferReader();

	}

	/**
	 * Creates a BuferedReader for the file where the table is located
	 */
	public void createNewBufferReader() {
		try {
			this.br = new OurTupleReader(tableFile);

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Closes the BufferedReader used to read the file where the table is located
	 */
	public void closeBuffer() {
		try {
			this.br.close();
		} catch (IOException e) {
			// e.printStackTrace();
		}
	}

	/**
	 * Gets the next tuple in the table
	 * 
	 * @return - the next tuple in the table
	 */
	public Tuple getNextTuple() {

		Tuple newTuple = null;

		try {
			String nextLine = br.readLine();

			if (nextLine != null) {
				newTuple = new Tuple(nextLine, (ArrayList<String>) DatabaseCatalog.getInstance().columnNames(baseTable), name);
			} else {
				newTuple = null;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		return newTuple;
	}

	/**
	 * Makes the ScanOperator start reading at the top of the table again
	 */
	public void reset() {
		try {
			this.br.reset();
		} catch (IOException e) {
			// TODO Auto-generated catch block
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
		
		this.br.close();
	}
	
	
	@Override
	public void reset(int index) {
		// TODO Auto-generated method stub
		
	}

}