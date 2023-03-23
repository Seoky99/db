package tools;

import cs4321.*;
import readwrite.HumanReadableTupleReader;
import readwrite.OurTupleWriter;
import readwrite.TupleReader;
import readwrite.TupleWriter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

// Converts a human readable file and outputs a binary file from that human readable file
public class HumanReadabletoBinary {
	// Input path of input file (inputFile) and path of output file (outputFile)
	public static void main(String[] args) throws IOException {
		try (Scanner inputScanner = new Scanner(System.in)) {
			// System.out.println("Input the path to the file you want to convert to
			// binary");
			// String inputFile = inputScanner.nextLine();
			// System.out.println("Input the path to the file where it will output the
			// binary file");

			// String outputFile = inputScanner.nextLine();
			// System.out.println(inputFile);
			String path = DatabaseCatalog.getInstance().getInputPath();
			String inputFile = "samples" + File.separator + "input" +
					File.separator + "db" + File.separator + "data" + File.separator + "Boats_gen";
			String outputFile = "samples" + File.separator + "input" +
					File.separator + "db" + File.separator + "data" + File.separator + "Boats";
			TupleReader rd = new HumanReadableTupleReader(new File(inputFile));
			TupleWriter wr = new OurTupleWriter(outputFile);

			String line = rd.readLine();
			while (line != null) {
				String[] data = line.split(",");
				Integer[] content = new Integer[data.length];
				for (int i = 0; i < data.length; i++) {
					content[i] = Integer.parseInt(data[i]);
				}
				List<Integer> contents = Arrays.asList(content);
				Tuple tuple = new Tuple(contents);
				wr.write(tuple);
				line = rd.readLine();
			}
			rd.close();
			wr.close();

		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}