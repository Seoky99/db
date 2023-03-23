package tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

public class SortUtility {

	/**
	 * Takes in a query result file and sorts it in order of first tuple entry to last 
	 * @param fileName - the query result file name, ex. query1 
	 */
	public static void sortData(String fileName) {

		//Later, could also pass just the file path if we use this for stuff other than query output
		ArrayList<String> tupleList = new ArrayList<>();

		String inputPath = "samples" + File.separator + "expected_output" + File.separator; 
		
		try (BufferedReader br = new BufferedReader(new FileReader(inputPath + fileName))) {

			String line = br.readLine();

			while (line != null) {
				tupleList.add(line);
				line=br.readLine(); 
			}
						
			TupleFileComparator tupleComparator = new TupleFileComparator();
			tupleList.sort(tupleComparator);
			
			FileWriter fw = new FileWriter(inputPath + fileName + "-sorted");
			
			for (int i = 0; i < tupleList.size(); i++) {
				
				fw.write(tupleList.get(i));
				
				if (i != tupleList.size() - 1) {
					fw.write("\n");
				}
			}

			System.out.println("Wrote to file.");
			fw.close();
			
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Ooopsie woopsie!");
		}
	}
	
	//Runs the sort utility 
	public static void main(String[] args) {
		sortData("query1");
	}

}
