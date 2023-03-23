package cs4321;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

//A mapping from column names to integers, along with an optional ordered content list
public class Tuple {

	public HashMap<String, Integer> map;
	public List<Integer> contents;

	/**
	 * Constructor for tuple from a line from a file, the table name or alias, and the
	 * schema
	 * 
	 * @param line   - the file line from which the tuple will be constructed
	 * @param schema - the names of the columns, in the order in which they appear
	 *               in the line
	 * @param name   - the name of the table or its alias
	 */
	public Tuple(String line, ArrayList<String> schema, String name) {
		map = new HashMap<String, Integer>();
		String[] dataContents = line.split(",");
		for (int i = 0; i < dataContents.length; i++) {
			map.put(name + "." + schema.get(i), Integer.valueOf(dataContents[i]));
		}
		contents = new ArrayList<Integer>();
		for(String s : schema) {
			contents.add(map.get(name + "." + s));
		}
	}
	
	/**
	 * Constructor for tuple from a line from a file, and the attributes of the tuple 
	 * 
	 * @param line   - the file line from which the tuple will be constructed
	 * @param schema - the attributes of the tuple, in the order in which they appear
	 *               in the line
	 */
	public Tuple(String line, ArrayList<String> attributes) {
		map = new HashMap<String, Integer>();
		contents = new ArrayList<Integer>();
		String[] dataContents = line.split(",");
		for (int i = 0; i < dataContents.length; i++) {
			contents.add(Integer.valueOf(dataContents[i]));
			map.put(attributes.get(i), Integer.valueOf(dataContents[i]));
		}
	}
	
	public Tuple(HashMap<String, Integer> oldMap, ArrayList<String> attributes) {
		map = new HashMap<String, Integer>();
		contents = new ArrayList<Integer>();
		
		for (String att : attributes) {
			contents.add(oldMap.get(att));
			map.put(att, oldMap.get(att));
		}
	}

	/**
	 * Constructor for tuple from a HashMap mapping column names to integer values
	 * 
	 * @param map - Map from column names to integer values
	 */
	public Tuple(HashMap<String, Integer> map) {
		this.map = map;
	}

	/**
	 * Constructor for tuple from a HashMap mapping column names to integer values
	 * and ordered integer list
	 * 
	 * @param map      - Map from column names to integer values
	 * @param contents - Ordered list of column values
	 */
	public Tuple(HashMap<String, Integer> map, List<Integer> contents) {
		this.map = map;
		this.contents = contents;
	}

	public Tuple(List<Integer> contents) {
		this.contents = contents;
	}

	/**
	 * Tuple toString, assumes tuple was constructed with an ordered content list
	 * 
	 * @return string representation of the tuple
	 */
	public String toString() {

		String arrayRepresentation = contents.toString();

		return arrayRepresentation.substring(1, arrayRepresentation.length() - 1).replaceAll("\\s", "");
	}

}
