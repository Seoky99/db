package cs4321;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;
import java.util.*;

//Keeps track of where the tables and schemas for tables are located
public class DatabaseCatalog {

	// Construct singleton instance of the catalog
	private static final DatabaseCatalog instance = new DatabaseCatalog();

	// Where the location of the input directory is
	private String inputPath;

	// Where the location of tempdir is
	private String tempDir;

	private String outputPath; 
	
	private OutputPlan outputplan; 
	
	private HashMap<String, String> relationsToIndex;
	private HashMap<String, Boolean> relationsToClustered;
	private HashMap<String, ArrayList<AttInfo>> allIndexInformation;
	private ArrayList<String> stats;
	private HashMap<String, StatEntry> advancedStats;
	private HashMap<String, String> aliases;
	
	
	/**
	 * Get the singleton instance on the catalog
	 * 
	 * @return The singleton instance of the catalog
	 */
	public static DatabaseCatalog getInstance() {
		return instance;
	}

	/**
	 * Sets the location of the input directory
	 * 
	 * @param inputPath - the path to the input directory
	 */
	public void setInputPath(String inputPath) {
		this.inputPath = inputPath;
	}

	/**
	 * Gets the location of the input directory
	 * 
	 * @return the path to the input directory
	 */
	public String getInputPath() {
		return this.inputPath;
	}

	/**
	 * Sets the location of the temp directory
	 * 
	 * @param inputPath - the path to the temp directory
	 */
	public void setTempDir(String inputPath) {
		this.tempDir = inputPath;
	}

	/**
	 * Gets the path to the temp dir
	 * 
	 * @return the path to the temp dir
	 */
	public String getTempDir() {
		return this.tempDir;
	}

	/**
	 * Returns the path to the config file
	 * 
	 * @return - The path to the configuration file
	 */
	public String getConfigurationLocation() {
		return inputPath + "plan_builder_config.txt";
	}

	/**
	 * Modifies the index information in the catalog to include the index
	 * information passed in
	 * 
	 * @param baseTableName: the base table name of index
	 * @param attributeName: the attribute name index is on
	 * @param order:         order of the index
	 * @param clustered:     whether the index is clustered
	 * 
	 */
	public void addIndexInformation(String baseTableName, String attributeName, int order, boolean clustered, int numLeaves) {

		if (allIndexInformation == null) {
			allIndexInformation = new HashMap<String, ArrayList<AttInfo>>();
		}

		AttInfo entry = new AttInfo(attributeName, clustered, numLeaves);

		if (allIndexInformation.get(baseTableName) == null) {

			ArrayList<AttInfo> aInfo = new ArrayList<AttInfo>();
			aInfo.add(entry);
			allIndexInformation.put(baseTableName, aInfo);
			
		} else {
			this.allIndexInformation.get(baseTableName).add(entry);
		}

	}

	/**
	 * Returns all currently available index information in the catalog
	 * 
	 * @return indexInformation: tells you about baseTableName, attributeName,
	 *         order, clustered, and path to the index
	 */
	public HashMap<String, ArrayList<AttInfo>> getAllIndexInformation() {
		return this.allIndexInformation;
	}

	/**
	 * Returns information in the index_info.txt file
	 * 
	 * @return An ArrayList of arrays that hold four information: relation name,
	 *         attribute name, clustered/unclustered, and order
	 */
	public ArrayList<String[]> getIndexInfo() {
		ArrayList<String[]> lines = new ArrayList<String[]>();

		try (BufferedReader br = new BufferedReader(
				new FileReader(inputPath + "db" + File.separator + "index_info.txt"))) {
			String line = br.readLine();
			while (line != null) {

				String[] indexInfo = line.split(" ");
				lines.add(indexInfo);
				line = br.readLine();

			}
		} catch (Exception e) {
			return null;
		}

		if (relationsToIndex == null) {
			relationsToIndex = new HashMap<String, String>();
			relationsToClustered = new HashMap<String, Boolean>();
			for (String[] a : lines) {
				relationsToIndex.put(a[0], a[0] + "." + a[1]);
				relationsToClustered.put(a[0], a[2].equals("1"));
			}

		}

		return lines;
	}

	/**
	 * Set the field stats to the list containing stats information passed in
	 * 
	 * @param inputStats: an arrayList consisting of entries that look like:
	 *                    [tableName numTuples att1,min,max ... attN,min,max]
	 */
	public void setStats(ArrayList<String> inputStats) {

		advancedStats = new HashMap<>();

		for (String s : inputStats) {

			String[] split = s.split(" ");
			String tableKey = split[0];
			int numTuples = Integer.valueOf(split[1]);

			HashMap<String, int[]> attributeMap = new HashMap<String, int[]>();

			for (int i = 2; i < split.length; i++) {
				String block = split[i];
				String[] blockSplit = block.split(",");

				int[] minMax = { Integer.valueOf(blockSplit[1]), Integer.valueOf(blockSplit[2]) };
				attributeMap.put(blockSplit[0], minMax);

			}

			StatEntry se = new StatEntry(numTuples, attributeMap);

			advancedStats.put(tableKey, se);
		}

		this.stats = inputStats;
	}

	/**
	 * Get the field stats to the list containing stats information stored in the
	 * database catalog
	 * 
	 * @return an arrayList consisting of entries that look like: tableName
	 *         numTuples att1,min,max ... attN,min,max
	 */
	public ArrayList<String> getStats() {
		return this.stats;
	}

	/**
	 * Get the advanced stats
	 * 
	 * @return a map that maps a table name to a string containing the number of
	 *         tuples and attribute ranges
	 */
	public HashMap<String, StatEntry> getAdvancedStats() {
		return this.advancedStats;
	}

	/**
	 * Returns the attribute an index is built for if the relation has an index
	 * 
	 * @param relation - relation to check
	 * @return - The attribute the index is built for, null if none exists
	 */
	public String relationToAttributeIndexed(String relation) {
		if (relationsToIndex == null) {
			return null;
		}
		return relationsToIndex.get(relation);
	}

	/**
	 * Returns whether or not the index for a given relation is clustered
	 * 
	 * @param relation - the relation to check
	 * @return - Whether or not the relation is clustered, null if no index exists
	 */
	public Boolean relationToClustered(String relation) {
		return relationsToClustered.get(relation);
	}

	/**
	 * Returns the path to the index location
	 * 
	 * @return The path to the index location
	 */
	public String getIndexLocation(String attribute) {
		return inputPath + "db" + File.separator + "indexes" + File.separator + attribute;
	}

	/**
	 * Gets the location of a given table
	 * 
	 * @param tableName - the name of the table in question
	 * @return The location of the file associated with the given table
	 */
	public String getFileLocation(String tableName) {
		return inputPath + "db" + File.separator + "data" + File.separator + tableName;
	}

	/**
	 * Gets the location of the statistics file
	 * 
	 * @return The location of the statistics file
	 */
	public String getStatsPathfile() {
		return inputPath + "db" + File.separator + "stats.txt";
	}

	/**
	 * Gets the column names of the table with the given name
	 * 
	 * @param tableName - the name of the table in question
	 * @return The column names of the table in question
	 */
	public ArrayList<String> columnNames(String tableName) {
		try (BufferedReader br = new BufferedReader(new FileReader(inputPath + "db" + File.separator + "schema.txt"))) {
			String line = br.readLine();
			while (line != null) {
				if (line.contains(tableName)) {
					ArrayList<String> ar = new ArrayList<>();
					for (String s : line.substring(line.indexOf(' ') + 1).split(" ")) {
						ar.add(s);
					}
					return ar;
				} else {
					line = br.readLine();
				}
			}
		} catch (Exception e) {
			return null;
		}
		return null;
	}
	
	public void setOutputPath(String output) {
		this.outputPath = output; 
	}
	
	/**
	 * @return the path to the output 
	 */
	public String getOutputPath() {
		return this.outputPath; 
	}
	
	public void setOutputPlan() {
		this.outputplan = new OutputPlan(); 
	}
	
	public OutputPlan getOutputPlan() {
		return this.outputplan; 
	}

	/**
	 * @return The schema path filepath
	 */
	public String getSchemaPath() {
		return this.getInputPath() + "db" + File.separator + "schema.txt";
	}

	/**
	 * Gets the tables names of the schema
	 * 
	 * @return The tables names of the schema
	 */
	public ArrayList<String> tableNamesFromSchema() {

		ArrayList<String> tableNames = new ArrayList<>();

		try (BufferedReader br = new BufferedReader(new FileReader(getSchemaPath()))) {
			String line = br.readLine();
			while (line != null) {
				String[] wholeLine = line.split("\\s");
				tableNames.add(wholeLine[0]);
				line = br.readLine();
			}
		} catch (Exception e) {
			return null;
		}
		return tableNames;
	}
	
	/**
	 * Sets aliases
	 * @param aliases - The aliases
	 */
	public void setAliases(HashMap<String, String> aliases) {
		this.aliases = aliases;
	}
	
	/**
	 * Gets aliases
	 * @return - The aliases
	 */
	public HashMap<String, String> getAliases() {
		return this.aliases;
	}

}