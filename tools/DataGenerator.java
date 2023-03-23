package tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

import cs4321.DatabaseCatalog;

//Generates random tuples at samples/input/db/data/tableName, the number and upper bound of values determined by the user
public class DataGenerator {

	//Input tableName, number of tuples to generate, and upper bound of values to receive a file of random tuples
	//Note: for the tableName, make sure you have already input the column names at schema.txt and  
	public static void main(String[] args) {

		Scanner scanner = new Scanner(System.in);
		Random rand = new Random();
		String inputPath = "samples" + File.separator + "input" + File.separator;
		int numberCols = -1;
		String tableName = "";

		while (numberCols < 0) {

			System.out.println("Input the name of the table you want to generate random data for:");
			tableName = scanner.nextLine();

			numberCols = countNumColumns(tableName, inputPath);
		}

		System.out.println("Input how many tuples to generate");
		int numTuples = Integer.valueOf(scanner.nextLine());

		System.out.println("Input the upper bound of generated data [0..Input)");
		int upperBound = Integer.valueOf(scanner.nextLine());

		scanner.close();

		try {
			try (FileWriter myWriter = new FileWriter(
					inputPath + "db" + File.separator + "data" + File.separator + tableName)) {

				for (int i = 0; i < numTuples; i++) {

					String tuple = generateTuple(numberCols, rand, upperBound, i != numTuples - 1);

					myWriter.write(tuple);

				}
				System.out.println("Wrote to file");
			}
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("If this triggers, check if you have a folder named 'samples' with input data.");
		}
	}
	
	/**
	 * Counts the number of columns that tableName has: if the tableName is not present in the file, returns -1. 
	 * @param tableName - the name of the table
	 * @param inputPath - where to start the file search from 
	 * @return - the number of columns of tableName. If the tableName is not present in file, returns -1. 
	 */
	public static int countNumColumns(String tableName, String inputPath) {
		try (BufferedReader br = new BufferedReader(new FileReader(inputPath + "db" + File.separator + "schema.txt"))) {
			String line = br.readLine();
			while (line != null) {
				if (line.contains(tableName)) {

					int colCount = 0;

					for (String s : line.substring(line.indexOf(' ') + 1).split(" ")) {
						colCount++;
					}
					return colCount;
				} else {
					line = br.readLine();
				}
			}
		} catch (Exception e) {
			return -1;
		}
		return -1;
	}

	/**
	 * Generates a single random tuple who has numCols entries and values are bound by upperBound
	 * @param numberCols - the number of entries the tuple should have
	 * @param rand - Pass in the random object 
	 * @param upperBound - The upper bound of the value that any of the tuple entries should have 
	 * @param createNewLine - whether or not to append a newline after the tuple entry
	 * @return - the tuple. Tuples are structured like so: N1,N2,N3,\n
	 */
	public static String generateTuple(int numberCols, Random rand, int upperBound, boolean createNewLine) {

		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < numberCols; i++) {

			sb.append(rand.nextInt(upperBound));

			if (i != numberCols - 1) {
				sb.append(",");
			}
		}

		if (createNewLine) {
			sb.append("\n");
		}

		return sb.toString();
	}
}
