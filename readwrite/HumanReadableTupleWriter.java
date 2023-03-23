package readwrite;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

import cs4321.Tuple;

// Write into a human readable file
public class HumanReadableTupleWriter implements TupleWriter {
  private PrintStream ps;

  /**
   * Constructor for HumanReadableTupleWriter
   * 
   * @param outputfile - The file that HumanReadableTupleWriter writes to
   * @throws IOException
   */
  public HumanReadableTupleWriter(File outputfile) throws IOException {
    this.ps = new PrintStream(outputfile);
  }

  /**
   * Writes tuple contents to file
   * 
   * @param tuple - tuple that is written to the binary file
   * @throws IOException
   */
  @Override
  public void write(Tuple tuple) throws IOException {
    this.ps.println(tuple.toString());
  }

  /**
   * Closes print stream
   * 
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    ps.close();
  }

}