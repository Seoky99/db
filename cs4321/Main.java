package cs4321;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;
import java.util.Scanner;

import indexes.IndexCreator;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import physical.Operator;
import readwrite.BinaryToHumanReadable;
import readwrite.HeaderPageReader;
import readwrite.OurTupleWriter;

//Harness to read queries and write queries with respect to users specified input and output directories
public class Main {

	// Iterate over all queries in inputDir/queries.sql and write the result in
	public static void main(String[] args) {
		try {
			// Parse the configuration file path
			if (args[0].charAt(args[0].length() - 1) != File.separatorChar) {
				args[0] = args[0] + File.separatorChar;
			} 

			String[] configInputs = parseConfigFile(args[0]);
			/*String[] configInputs = new String[3];
			configInputs[0] = "./test_cases/input";
			configInputs[1] = "./test_cases/output";
			configInputs[2] = "./test_cases/tempdir"; */

			for (int i = 0; i < 3; i++) {
				if ((configInputs[i].charAt(configInputs[i].length() - 1) != File.separatorChar)) {
					configInputs[i] = configInputs[i] + File.separatorChar;
				}
			}

			DatabaseCatalog.getInstance().setInputPath(configInputs[0]);
			DatabaseCatalog.getInstance().setOutputPath(configInputs[1]);
			DatabaseCatalog.getInstance().setTempDir(configInputs[2]);
			
			DatabaseCatalog.getInstance().setOutputPlan();

			Statement statement;
			String inputFile = configInputs[0] + "queries.sql";
			CCJSqlParser parser = new CCJSqlParser(new FileReader(inputFile));

			int queries = 1;

			// USE THIS INFORMATION
			//boolean buildIndexes = Integer.valueOf(configInputs[3]) == 1 ? true : false;
			//boolean evaluateQueries = Integer.valueOf(configInputs[4]) == 1 ? true : false;

			
			// Parsing index_info.txt file
			ArrayList<String[]> indexInfo = DatabaseCatalog.getInstance().getIndexInfo();

			for (String[] index : indexInfo) {

				String baseTableName = index[0];
				String aliasName = index[0];
				String attributeName = aliasName + "." + index[1];
				boolean clustered = Integer.valueOf(index[2]) == 1 ? true : false;
				int order = Integer.valueOf(index[3]);

				int numLeaves = 0;
				
				if (true) {
					IndexCreator i = new IndexCreator(baseTableName, aliasName, attributeName, order, clustered);
				}
				
				//Find the number of leaves by reading the index binary file 
				HeaderPageReader hpr = new HeaderPageReader(new File(DatabaseCatalog.getInstance().getIndexLocation(attributeName)));
				numLeaves = hpr.numLeaves();
				hpr.close();
				
				// Save it to the database catalog
				DatabaseCatalog.getInstance().addIndexInformation(baseTableName, attributeName, order, clustered, numLeaves);
			}

			// Create stats file
			StatsWriter sw = new StatsWriter();
			sw.parseSchema();
			
			if (true) {
				
				while ((statement = parser.Statement()) != null) {
					try {
						
						DatabaseCatalog.getInstance().getOutputPlan().outputNewQuery("query" + String.valueOf(queries));
						String outputFile = configInputs[1] + "query" + String.valueOf(queries);

						OurTupleWriter tw = new OurTupleWriter(outputFile);

						QueryPlan q = new QueryPlan(statement, configInputs[2]);


						long start = System.currentTimeMillis();

						Operator r = q.getRoot();
						if (r != null) {
							r.dump(tw);
						}

						tw.close();

						long end = System.currentTimeMillis();
						
						BinaryToHumanReadable.binaryToHumanReadable(outputFile, outputFile + "human_readable");
						
						String logicalPlanOutputFile = outputFile + "_logicalplan";
					    BufferedWriter writer = new BufferedWriter(new FileWriter(logicalPlanOutputFile));
					    writer.write(q.logicalPlanOutput);
					    writer.close();
						
						queries++;

						File directory = new File(configInputs[2]);

						for (File file : Objects.requireNonNull(directory.listFiles())) {
							if (!file.isDirectory()) {
								file.delete();
							}
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
			
			
			DatabaseCatalog.getInstance().getOutputPlan().closePlan();

		} catch (Exception e) {
			// Catch I/O exceptions
			e.printStackTrace();
			System.out.print(e);
		}
		
	}

	/**
	 * 
	 * @param path - the path to the config file 
	 * @return - an array describing the contents of the config file 
	 * @throws FileNotFoundException
	 */
	private static String[] parseConfigFile(String path) throws FileNotFoundException {
		File pathFile = new File(path);
		try (Scanner fileReader = new Scanner(pathFile)) {
			String[] configInputs = new String[5];
			int fileLine = 0;

			while (fileReader.hasNextLine()) {
				String line = fileReader.nextLine();
				configInputs[fileLine] = line;
				fileLine++;
			}

			return configInputs;
		}
	}

}