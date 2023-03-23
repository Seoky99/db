package cs4321;

import java.io.File;
import java.io.FileWriter; // Import the FileWriter class
import java.io.IOException; // Import the IOException class to handle errors

public class OutputPlan {
	
	private FileWriter myWriter;
	private String queryNum; 
	
	public OutputPlan() {
		
		String outputPath = DatabaseCatalog.getInstance().getOutputPath(); 
		try {
			this.myWriter = new FileWriter(outputPath + File.separator + "testing");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void printQuery() {
		System.out.println(queryNum);
	}
	
	public void printSomething(String name) {
		
		System.out.println("print" + name);
		printQuery();
			
		try {
			myWriter.write(name + "\n");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void outputNewQuery(String queryName) {
		
		closePlan();
		String outputPath = DatabaseCatalog.getInstance().getOutputPath(); 
		try {
			this.queryNum = queryName;
			this.myWriter = new FileWriter(outputPath + File.separator + queryName + "_physicalplan");
		} catch (IOException e) {
			e.printStackTrace();
		} 
	}

	public void closePlan() {
		try {
			myWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
