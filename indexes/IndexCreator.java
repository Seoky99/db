package indexes;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import cs4321.DatabaseCatalog;
import cs4321.Tuple;
import cs4321.TupleComparator;
import readwrite.OurTupleReader;
import readwrite.OurTupleWriter;

import java.io.File;
import java.io.IOException;

//Generates an index 
public class IndexCreator {

	private String baseTableName;
	private String tableName;
	private HashMap<Tuple, Rid> rMap = new HashMap<Tuple, Rid>();
	private String attribute;
	private int order;
	private boolean clustered;
	private List<Tuple> tuples = new ArrayList<Tuple>();
	private List<Entry> entryList;
	private List<Node> leafNodeList;
	private int currentAddress = 1;
	private SerialWriter w;
	private String clusteredLocation;
	public Node root;

	/**
	 * Constructor for IndexCreator
	 * 
	 * @param baseTableName - Base table name
	 * @param tableName     - Table name, potentially aliased
	 * @param attribute     - Attribute to index by
	 * @param order         - Order of index
	 * @param clustered     - Whether or not it is clustered
	 */
	public IndexCreator(String baseTableName, String tableName, String attribute, int order, boolean clustered) {

		this.baseTableName = baseTableName;
		this.tableName = tableName;
		this.attribute = attribute;
		this.order = order;
		this.clustered = clustered;

		try {

			if (this.clustered) {
				prepareClustered();
			}

			w = new SerialWriter(DatabaseCatalog.getInstance().getIndexLocation(attribute));
			createMap();
			sort();
			createEntries();
			createIndexLeafNodes();
		} catch (IOException e) {
			e.printStackTrace();
		}

		List<Node> currLayer = constructIndexLayer(leafNodeList);

		while (currLayer.size() > 1) {
			currLayer = constructIndexLayer(currLayer);
		}

		this.root = currLayer.get(0);

		try {
			w.createHeaderPage((IndexNode) this.root, order, leafNodeList.size());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Sort the underlying file
	 * 
	 * @throws IOException
	 */
	private void prepareClustered() throws IOException {
		File tableFile = new File(DatabaseCatalog.getInstance().getFileLocation(baseTableName));
		OurTupleReader rd = new OurTupleReader(tableFile);

		String readerLine;
		Tuple currentTuple;

		while ((readerLine = rd.readLine()) != null) {
			currentTuple = new Tuple(readerLine, DatabaseCatalog.getInstance().columnNames(baseTableName), tableName);
			tuples.add(currentTuple);
		}

		sort();

		// clusteredLocation = DatabaseCatalog.getInstance().getTempDir() + "index_" +
		// attribute;

		clusteredLocation = DatabaseCatalog.getInstance().getInputPath() + "db" + File.separator + "data"
				+ File.separator + baseTableName;

		OurTupleWriter otw = new OurTupleWriter(clusteredLocation);

		for (Tuple t : tuples) {
			otw.write(t);
		}

		otw.close();

		tuples = new ArrayList<Tuple>();
	}

	/**
	 * Create the map from tuples to Rids
	 * 
	 * @throws IOException
	 */
	public void createMap() throws IOException {
		File tableFile;

		if (!clustered) {
			tableFile = new File(DatabaseCatalog.getInstance().getFileLocation(baseTableName));
		} else {
			tableFile = new File(clusteredLocation);
		}
		OurTupleReader rd = new OurTupleReader(tableFile);

		int tupleCount = 0;
		int pageCount = 0;

		String readerLine = rd.readLine();
		Tuple currentTuple = new Tuple(readerLine, DatabaseCatalog.getInstance().columnNames(baseTableName), tableName);
		Rid rID = new Rid(0, 0);

		rMap.put(currentTuple, rID);
		tuples.add(currentTuple);

		int pageSize = 4096;
		int numAttributes = currentTuple.map.keySet().size();
		int numTuplePerPage = (pageSize - 2 * 4) / (4 * numAttributes);

		while ((readerLine = rd.readLine()) != null) {
			tupleCount++;

			if (numTuplePerPage == tupleCount) {
				tupleCount = 0;
				pageCount++;
			}

			currentTuple = new Tuple(readerLine, DatabaseCatalog.getInstance().columnNames(baseTableName), tableName);

			Rid newrID = new Rid(pageCount, tupleCount);

			rMap.put(currentTuple, newrID);
			tuples.add(currentTuple);
		}
	}

	/**
	 * Sort the tuples
	 * 
	 * @throws IOException
	 */
	public void sort() throws IOException {
		ArrayList<String> attributes = new ArrayList<String>();
		attributes.add(attribute);

		Collections.sort(tuples, new TupleComparator(attributes));
	}

	/*
	 * Creates entries with the same attribute value. Entries have an index search
	 * key and a list of record ids.
	 */
	public void createEntries() {
		int index = 0;

		entryList = new ArrayList<Entry>();

		Tuple currTuple = tuples.get(index);
		index++;

		List<Rid> ridList = new ArrayList<Rid>();
		ridList.add(rMap.get(currTuple));

		int attributeValue = currTuple.map.get(attribute);

		// Idea is to iterate through all the tuples: tuples with the same attribute
		// value are grouped into the Rid list
		for (int i = 1; i < rMap.keySet().size(); i++) {

			if (index == tuples.size()) {
				break;
			}

			currTuple = tuples.get(index);
			index++;

			// If the attribute value is equal to the current tuple, add it to the rid list.
			// If it's the LAST tuple we end the entry there.
			if (currTuple.map.get(attribute) == attributeValue) {
				ridList.add(rMap.get(currTuple));

				if (i == rMap.keySet().size() - 1) {
					Entry entry = new Entry(attributeValue, ridList);
					entryList.add(entry);
				}

				// If the attribute value is not equal, add the entry to the entry list and
				// create a new entry. Clear ridList and update attributeValue.
			} else {

				// Come back to index key value later
				Entry entry = new Entry(attributeValue, ridList);
				entryList.add(entry);

				ridList = new ArrayList<Rid>();
				ridList.add(rMap.get(currTuple));
				attributeValue = currTuple.map.get(attribute);

				if (index == tuples.size()) {
					entry = new Entry(attributeValue, ridList);
					entryList.add(entry);
				}

			}
		}
	}

	/**
	 * Create the leaf layer of the index tree
	 */
	public void createIndexLeafNodes() {

		leafNodeList = new ArrayList<Node>();

		int entryListSize = entryList.size();

		// if the last two nodes needs to be split. Also if it goes in evenly, no need
		// to split
		if ((entryListSize % (2 * order) < order) && (entryListSize % (2 * order) != 0)) {

			int entryListPosition = 0;

			for (int i = 0; i < (entryListSize / (2 * order)) - 1; i++) {

				List<Entry> leafNodeEntries = new ArrayList<Entry>();

				for (int j = 0; j < 2 * order; j++) {

					leafNodeEntries.add(entryList.get(entryListPosition));
					entryListPosition++;
				}

				LeafIndexNode lin = new LeafIndexNode(leafNodeEntries, currentAddress);
				currentAddress++;
				leafNodeList.add(lin);
			}

			// like the k in the document (remaining entries for last two nodes)
			int k = (entryListSize % (2 * order)) + 2 * order;

			List<Entry> penultimateNodeEntries = new ArrayList<Entry>();

			// index node with k/2 entries
			for (int m = 0; m < k / 2; m++) {

				penultimateNodeEntries.add(entryList.get(entryListPosition));
				entryListPosition++;

			}

			LeafIndexNode penultimateIndexNode = new LeafIndexNode(penultimateNodeEntries, currentAddress);
			currentAddress++;
			leafNodeList.add(penultimateIndexNode);

			List<Entry> finalNodeEntries = new ArrayList<Entry>();

			// index node for remaining entries
			for (int n = 0; n < k - k / 2; n++) {

				finalNodeEntries.add(entryList.get(entryListPosition));
				entryListPosition++;

			}

			LeafIndexNode finalIndexNode = new LeafIndexNode(finalNodeEntries, currentAddress);
			currentAddress++;
			leafNodeList.add(finalIndexNode);

			// we can assign 2d to every leaf node and entryListSize % 2d to the last leaf
		} else {

			int entryListPosition = 0;

			// all the leaves of 2d
			for (int i = 0; i < entryListSize / (2 * order); i++) {

				List<Entry> leafNodeEntries = new ArrayList<Entry>();

				for (int j = 0; j < 2 * order; j++) {

					leafNodeEntries.add(entryList.get(entryListPosition));
					entryListPosition++;
				}

				LeafIndexNode lin = new LeafIndexNode(leafNodeEntries, currentAddress);
				currentAddress++;
				leafNodeList.add(lin);
			}

			// if it splits evenly, skip this step
			if (entryListSize % (2 * order) != 0) {

				// the one leafindexnode with entryListSize % (2d) entries
				// add all the remaining entries here
				List<Entry> finalNodeEntries = new ArrayList<Entry>();

				while (entryListPosition != entryListSize) {

					finalNodeEntries.add(entryList.get(entryListPosition));
					entryListPosition++;
				}

				LeafIndexNode lastIndexNode = new LeafIndexNode(finalNodeEntries, currentAddress);
				currentAddress++;
				leafNodeList.add(lastIndexNode);
			}

		}

		for (Node n : leafNodeList) {
			try {
				w.createLeafNodePage(n.getAddress(), (LeafIndexNode) n);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Create a layer of the index tree
	 * 
	 * @param childLayer - The layer below the layer to be created
	 * @return - The new layer of the index tree
	 */
	public List<Node> constructIndexLayer(List<Node> childLayer) {
		int used = 0;

		ArrayList<Node> toReturn = new ArrayList<Node>();

		while (used < childLayer.size()) {
			ArrayList<Node> children = new ArrayList<Node>();

			// Under fill check
			if (childLayer.size() - used < 3 * order + 2 && childLayer.size() - used > 2 * order + 1) {
				int toAdd = (childLayer.size() - used) / 2;
				for (int i = 0; i < toAdd; i++) {
					children.add(childLayer.get(used));
					used++;
				}
			} else {
				for (int i = 0; i < 2 * order + 1; i++) {
					if (used == childLayer.size()) {
						break;
					}
					children.add(childLayer.get(used));
					used++;
				}
			}

			ArrayList<Integer> keys = new ArrayList<Integer>();

			for (int i = 1; i < children.size(); i++) {
				keys.add(children.get(i).getKey());
			}

			toReturn.add(new IndexNode(keys, children, currentAddress));
			currentAddress++;
		}

		for (Node n : toReturn) {
			try {
				w.createIndexNodePage(n.getAddress(), (IndexNode) n);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return toReturn;
	}
	
}
