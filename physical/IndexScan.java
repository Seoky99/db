package physical;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;

import cs4321.DatabaseCatalog;
import cs4321.Tuple;
import indexes.DeserializedNode;
import indexes.Entry;
import indexes.IndexNode;
import indexes.Rid;
import logical.LogicalScanOperator;
import readwrite.OurTupleReader;

//An scanning physical operator that employs the indexes constructed. 
public class IndexScan extends Operator {

	private String baseTableName;
	private String tableName;
	private IndexNode root;
	private boolean clustered;
	private Integer lowkey;
	private Integer highkey;
	private boolean noLowBound;
	private boolean noHighBound;
	private int clusteredIndex;
	private DeserializedNode currentLeafNode;
	private int currentLeafNodeAddress;
	private LinkedList<Tuple> tupleList;
	private int listCounter;
	private int numberOfLeaves;
	private boolean endOfLeaves;

	private OurTupleReader clusteredReader;
	private boolean noClusteredData;
	private String attribute;
	private String aliasedAttribute;

	// buffer stuff
	private static int page_size = 4096;
	private FileInputStream fin;
	private ByteBuffer buffer;
	private FileChannel fc;
	private int bufferPosition;
	private PrintStream ps;
	private OurTupleReader rd;

	// entry tracking
	private boolean startOfEntry;
	// one based entryPosition
	private int entryPosition;
	private int ridPosition;
	private int numberOfRid;
	private int numberOfEntries;

	// lowkey and highkey are closed: lowkey <= index <= highkey
	// get rid of attribute once we fix the clustered
	/**
	 * IndexScan constructor
	 * @param baseTableName - Name of the base table
	 * @param tableName - Name of the table, potentially aliased
	 * @param pathToIndex - Path to the index file
	 * @param pathToData - Path to the table
	 * @param clustered - If the index is clustered
	 * @param lowkey - Inclusive lower bound of tuples to return
	 * @param highkey - Inclusive upper bound of tuples to return
	 * @param attribute - Attribute the index is on
	 */
	public IndexScan(String baseTableName, String tableName, String pathToIndex, String pathToData, boolean clustered,
			Integer lowkey, Integer highkey, String attribute) {

		this.baseTableName = baseTableName;
		this.tableName = tableName;
		this.clustered = clustered;

		this.lowkey = lowkey;
		this.highkey = highkey;

		this.clusteredIndex = 0;
		this.attribute = attribute;
		
		String[] aliasedAttributeSplit = attribute.split("\\.");
		aliasedAttributeSplit[0] = tableName;
		aliasedAttribute = aliasedAttributeSplit[0] + "." + aliasedAttributeSplit[1];
		

		File file = new File(pathToData);
		try {
			this.rd = new OurTupleReader(file);
			fin = new FileInputStream(pathToIndex);
			fc = fin.getChannel();
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (lowkey == null) {
			this.noLowBound = true;
		}

		if (highkey == null) {
			this.noHighBound = true;
		}

		DeserializedNode dNode = findRoot();

		// keep traversing until you hit a leaf
		while (dNode.isIndex) {
			dNode = traverseTree(dNode);
		}
		

		// if it is clustered, we just need to find the first rID from the matching
		// entry in dNode
		if (clustered) {
			setUpClustered(dNode);
		} else {

			// once we find the leaf node, determine tuple list and listCounter for the
			// first time
			this.currentLeafNode = dNode;
			this.tupleList = getTuplesFromLeaf(currentLeafNode);
			this.listCounter = 0;

		}
	}

	/**
	 * Read the header page and move the buffer position to the root node
	 * 
	 * @return: A Deserialized Node that represents the root node.
	 */
	public DeserializedNode findRoot() {
		try {

			fc.position(0);
			createBuffer();
			int rootAddress = buffer.getInt(0);

			// find out how many leaf nodes exist
			numberOfLeaves = buffer.getInt(4);

			DeserializedNode rootNode = deserialize(rootAddress);

			// fc.position(rootAddress * page_size);
			createBuffer();

			return rootNode;

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * Traverses from the parentNode to the returned node based on value of keys
	 * 
	 * @param parentNode: The node which you want to traverse from
	 * @return The DeserializedNode that you move to by traversing from the
	 *         parentNode based on keys.
	 */
	public DeserializedNode traverseTree(DeserializedNode parentNode) {

		int childIndex = 0;

		for (int i = 0; i < parentNode.keys.size(); i++) {
			int key = parentNode.keys.get(i);

			if (noLowBound || lowkey < key) {
				break;
			}

			childIndex++;
		}


		int childAddress;

		// if no lower bound go to the leftmost node
		if (noLowBound) {
			childAddress = parentNode.addresses.get(0);
		}

		childAddress = parentNode.addresses.get(childIndex);

		currentLeafNodeAddress = childAddress;

		DeserializedNode child = deserialize(childAddress);
		return child;
	}

	/**
	 * Create a buffer to be able to read/write from binary files for the
	 * TupleReader/TupleWriters.
	 */
	public void createBuffer() {
		try {
			buffer = ByteBuffer.allocate(page_size);
			fc.read(buffer);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * From the root node, find the first matching entry within lowkey and highkey
	 * and find the first rID. Then set up the clusteredReader field to read from
	 * the clustered binary file we contructed. If we cannot find any matching
	 * tuples set noClusteredData to true, signifying later to return nulls for
	 * getNextTuple() calls.
	 * 
	 * @param dNode: The root node
	 */
	public void setUpClustered(DeserializedNode dNode) {

		
		String clusteredLocation = DatabaseCatalog.getInstance().getInputPath() + "db" + File.separator + "data"
				+ File.separator + baseTableName;
		File fileLoc = new File(clusteredLocation);
		try {
			clusteredReader = new OurTupleReader(fileLoc);
		} catch (IOException e2) {
			e2.printStackTrace();
		}
		
		// find the first matching entry
		for (Entry e : dNode.entries) {
			


			if ((noHighBound || e.indexKey <= highkey) && (noLowBound || e.indexKey >= lowkey)) {

				// set bufferPosition to this location
				Rid firstRid = e.ridList.get(0);
			
				try {
					clusteredReader.reset(firstRid.pageId, firstRid.tupleId);
				} catch (IOException e1) {
					e1.printStackTrace();
				}
				return;
			}
					
		}
		
		try {
			Entry lastEntry = dNode.entries.get(dNode.entries.size() - 1);
			Rid lastRid = lastEntry.ridList.get(lastEntry.ridList.size() - 1);
			clusteredReader.reset(lastRid.pageId, lastRid.tupleId);
			clusteredReader.readLine();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} 

		// no matching tuples: return null every time
		//noClusteredData = true;

	}

	/**
	 * @return the next Tuple of the indexScan, meaning the attribute value fits
	 *         within lowkey and highkey.
	 */
	public Tuple getNextTuple() {

		if (clustered) {

			// this occurs when the leaf node had no matching tuples or we have encountered all the tuples that are matching already
			if (noClusteredData) {
				return null;
			}

			String tupleString = "";
			try {
				tupleString = clusteredReader.readLine();

				// end of file
				if (tupleString == null) {
					return null;
				}

			} catch (IOException e) {
				e.printStackTrace();
			}

			Tuple currentTuple = new Tuple(tupleString, DatabaseCatalog.getInstance().columnNames(baseTableName),
					tableName);

			// check if tuple values are within range
			if ((noHighBound || currentTuple.map.get(aliasedAttribute) <= highkey)) {
				return currentTuple;
			}
			noClusteredData = true;
			return null;

		} else {

			// none of tuples are within the range
			if (tupleList.size() == 0 || endOfLeaves) {
				return null;
			}

			// if we are at the end of the node, go to the next page and construct a new
			// TupleList
			if (listCounter == tupleList.size() - 1) {

				Tuple returnTuple = tupleList.get(listCounter);
				
				// check if we are in last index page
				if (currentLeafNodeAddress == numberOfLeaves) {

					endOfLeaves = true;
					
				} else {

					currentLeafNodeAddress++;
					currentLeafNode = deserialize(currentLeafNodeAddress);
					tupleList = getTuplesFromLeaf(currentLeafNode);
					listCounter = 0;
				}

				return returnTuple;
			}

			Tuple returnTuple = tupleList.get(listCounter);
			listCounter++;
			return returnTuple;

		}
	}

	/**
	 * Return all the tuples from the deserialized leaf node.
	 * 
	 * @param n: the leaf deserialized node
	 * @return A linked list of tuples whose attribute value matches within the
	 *         range of lowkey to highkey
	 */
	public LinkedList<Tuple> getTuplesFromLeaf(DeserializedNode n) {
		LinkedList<Tuple> toReturn = new LinkedList<Tuple>();

		for (Entry e : n.entries) {
			if ((noHighBound || e.indexKey <= highkey) && (noLowBound || e.indexKey >= lowkey)) {
				for (Rid r : e.ridList) {
					String line = "";
					try {
						rd.reset(r.pageId, r.tupleId);
						line = rd.readLine();
					} catch (IOException e1) {
						e1.printStackTrace();
					}

					toReturn.add(new Tuple(line, DatabaseCatalog.getInstance().columnNames(baseTableName), tableName));
				}
			}
		}

		return toReturn;
	}

	/**
	 * From an address returns the deserialized node at that address
	 * 
	 * @param address: The page address of the node
	 * @return: A deserialized node (holds entries if it is an leaf, holds keys and
	 *          addresses if it is an index)
	 */
	public DeserializedNode deserialize(int address) {
		long position = page_size * address;

		try {
			if (position > fc.size()) {
				return null;
			}

			fc.position(page_size * address);
			createBuffer();
		} catch (IOException e) {
			e.printStackTrace();
		}

		int isIndex = buffer.getInt(0);

		if (isIndex == 1) {
			ArrayList<Integer> keys = new ArrayList<Integer>();
			ArrayList<Integer> addresses = new ArrayList<Integer>();
			int numIndexKeys = buffer.getInt(4);
			int bufferPosition = 8;

			for (int i = 0; i < numIndexKeys; i++) {
				keys.add(buffer.getInt(bufferPosition));
				bufferPosition += 4;
			}

			for (int i = 0; i < numIndexKeys + 1; i++) {
				addresses.add(buffer.getInt(bufferPosition));
				bufferPosition += 4;
			}

			return new DeserializedNode(keys, addresses);
		} else {
			ArrayList<Entry> entries = new ArrayList<Entry>();
			numberOfEntries = buffer.getInt(4);

			bufferPosition = 8;
			for (int i = 0; i < numberOfEntries; i++) {

				int k = buffer.getInt(bufferPosition);
				ArrayList<Rid> rids = new ArrayList<Rid>();
				bufferPosition += 4;
				numberOfRid = buffer.getInt(bufferPosition);
				bufferPosition += 4;

				for (int j = 0; j < numberOfRid; j++) {
					int pageId = buffer.getInt(bufferPosition);
					bufferPosition += 4;
					int tupleId = buffer.getInt(bufferPosition);
					bufferPosition += 4;

					rids.add(new Rid(pageId, tupleId));
				}
				entries.add(new Entry(k, rids));
			}
			return new DeserializedNode(entries);
		}

	}

	/**
	 * Resets the index scan
	 */
	public void reset() {

	}

	/**
	 * Resets it to an index
	 * 
	 * @param index: the index to reset it to
	 */
	public void reset(int index) {

	}
}
