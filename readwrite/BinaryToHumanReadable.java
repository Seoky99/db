package readwrite;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import cs4321.Tuple;

// Converts a binary file and outputs a human readable file of that binary file
public class BinaryToHumanReadable {

  /**
   * Given binary file input, output the equivalent human readable file
   * 
   * @param inputFile  - The binary file that is read
   * @param outputFile - The human readble file that tuples are written to
   * @throws IOException
   */
  public static void binaryToHumanReadable(String inputFile, String outputFile) throws IOException {
    OurTupleReader rd = new OurTupleReader(new File(inputFile));
    HumanReadableTupleWriter wr = new HumanReadableTupleWriter(new File(outputFile));
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
  }
  

}
