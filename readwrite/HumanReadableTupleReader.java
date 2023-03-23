package readwrite;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

// Read human readable file 
public class HumanReadableTupleReader implements TupleReader {
  private BufferedReader br;
  private File inputFile;

  /**
   * Constructor for HumanReadableTupleReader
   * 
   * @param inputfile - The input file that HumanReadableTupleReader reads from
   * @throws IOException
   */
  public HumanReadableTupleReader(File inputFile) throws IOException {
    this.br = new BufferedReader(new FileReader(inputFile));
    this.inputFile = inputFile;
  }

  /**
   * Reads the line
   * 
   * @return - the line in the file
   * @throws IOException
   */
  @Override
  public String readLine() throws IOException {
    return br.readLine();
  }

  /**
   * Closes the current buffer and creates a new one
   * 
   * @throws IOException
   */
  @Override
  public void reset() throws IOException {
    closeBuffer();
    br = new BufferedReader(new FileReader(this.inputFile));
  }
  
  /**
   * Resets the current buffer to a specific tuple
   * @param index - the index of the tuple to reset to
   * 
   * @throws IOException
   */
  public void reset(int index) throws IOException {
	  this.reset();
	  
	  for(int i = 0; i < index; i++) {
		  br.readLine();
	  }
  }

  /**
   * Closes the current buffer
   * 
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    br.close();
  }

  /**
   * Closes the current buffer in the instance if it exists
   */
  public void closeBuffer() {
    try {
      this.br.close();
    } catch (IOException e) {
    }
  }
}