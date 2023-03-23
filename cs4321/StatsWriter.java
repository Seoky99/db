package cs4321;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

import readwrite.OurTupleReader;

//Parse the schema file and generate statistics about relations' contents 
public class StatsWriter {

	private ArrayList<String> tableInfo;

	/**
	 * From the schema.txt file, parse the names of the relations and count how many tuples they hold, as well as the 
	 * minimum and maximum value they achieve. Then write to stats.txt 
	 */
	public void parseSchema() {

		ArrayList<String> tableInformation = new ArrayList<>();

		for (String tableName : DatabaseCatalog.getInstance().tableNamesFromSchema()) {

			try {
				OurTupleReader or = new OurTupleReader(
						new File(DatabaseCatalog.getInstance().getFileLocation(tableName)));

				ArrayList<String> columnNames = new ArrayList<String>();
				columnNames = DatabaseCatalog.getInstance().columnNames(tableName);

				int tupleCount = 0;
				ArrayList<Integer[]> minMax = new ArrayList<>();

				for (String col : columnNames) {
					Integer[] values = { Integer.MAX_VALUE, Integer.MIN_VALUE };
					minMax.add(values);
				}

				String tuple;

				while ((tuple = or.readLine()) != null) {

					String[] tupleContents = tuple.split(",");
					tupleCount++;

					int attributeCounter = 0;

					for (String attributeValue : tupleContents) {

						int intAttValue = Integer.valueOf(attributeValue);

						if (intAttValue < minMax.get(attributeCounter)[0]) {
							minMax.get(attributeCounter)[0] = intAttValue;
						}

						if (intAttValue > minMax.get(attributeCounter)[1]) {
							minMax.get(attributeCounter)[1] = intAttValue;
						}

						attributeCounter++;
					}

				}

				StringBuilder sb = new StringBuilder();
				int colCounter = 0;

				for (String colName : columnNames) {

					sb.append(colName + "," + minMax.get(colCounter)[0] + "," + minMax.get(colCounter)[1]);

					if (colCounter != columnNames.size() - 1) {
						sb.append(" ");
					}

					colCounter++;
				}

				sb.insert(0, tableName + " " + tupleCount + " ");
				tableInformation.add(sb.toString());

				tuple = or.readLine();

			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		this.tableInfo = tableInformation;
		DatabaseCatalog.getInstance().setStats(this.tableInfo);
		
		writeToFile();

	}

	/**
	 * Using the data generated from parseSchema(), this actually writes the data to stats.txt
	 */
	public void writeToFile() {

		try {
			FileWriter myWriter = new FileWriter(DatabaseCatalog.getInstance().getStatsPathfile());

			int counter = 0;

			for (String row : tableInfo) {
				myWriter.write(row);

				if (counter != tableInfo.size() - 1) {
					myWriter.write("\n");
				}

				counter++;

			}
			myWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
