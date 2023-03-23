package readwrite;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

// Read binary file 
public class OurTupleReader implements TupleReader {
  private static int page_size = 4096;
  private int numTuples;
  private int numAttributes;
  private FileInputStream fin;
  private ByteBuffer buffer;
  private FileChannel fc;
  private int bufferPosition;
  
  /**
   * Constructor for OurTupleReader
   * 
   * @param inputfile - The input file that OurTupleReader reads from
   * @throws IOException
   */
  public OurTupleReader(File inputFile) throws IOException {
    fin = new FileInputStream(inputFile);
    fc = fin.getChannel();
    buffer = ByteBuffer.allocate(page_size);
    numTuples = 0;
  }

  /**
   * Reads the line, creates a new buffer if needed
   * 
   * @return - the line in the file
   * @throws IOException
   */
  @Override
  public String readLine() throws IOException {
    if (numTuples == 0) {
      int r = createBuffer();
      if (r == -1)
        return null;
    }
    String line = "";
    for (int i = 0; i < numAttributes - 1; i++) {
      int data = buffer.getInt(bufferPosition);
      line = line + data + ",";
      bufferPosition += 4;
    }
    line = line + buffer.getInt(bufferPosition);
    bufferPosition += 4;
    numTuples--;
    return line;
  }

  /**
   * Resets the position of the channel and number of tuples to 0
   * 
   * @throws IOException
   */
  @Override
  public void reset() throws IOException {
    fc.position(0);
    numTuples = 0;
  }
  
  /**
   * Resets the reader to a specific tuple
   * @param index - the index of the tuple to reset to
   * 
   * @throws IOException
   */
  public void reset(int index) throws IOException {
	  reset();
	  
	  buffer = ByteBuffer.allocate(8);
	  fc.read(buffer);
	  numAttributes = buffer.getInt(0);
	  
	  int numTuplePerPage = (page_size - 2*4) / (4 * numAttributes);
	  int page = index / numTuplePerPage;
	  int indexOnPage = index % numTuplePerPage;
	  
	  fc.position(page_size * page);
	  
	  buffer = ByteBuffer.allocate(page_size);
	  fc.read(buffer);
	  numTuples = buffer.getInt(4);
	  numAttributes = buffer.getInt(0);
	  
	  bufferPosition = 8;
	  bufferPosition+=4*numAttributes*indexOnPage;
	  numTuples-=indexOnPage;
  }
  
  public void reset(int page, int offset) throws IOException {
	  reset();
	  	  
	  fc.position(page_size * page);
	  
	  buffer = ByteBuffer.allocate(page_size);
	  fc.read(buffer);
	  numTuples = buffer.getInt(4);
	  numAttributes = buffer.getInt(0);
	  
	  bufferPosition = 8;
	  bufferPosition+=4*numAttributes*offset;
	  numTuples-=offset;
	  
  }


  /**
   * Closes the inputstream
   * 
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    fin.close();
  }

  /**
   * Creates a new buffer to read
   * 
   * @throws IOException
   */
  public int createBuffer() throws IOException {
    buffer = ByteBuffer.allocate(page_size);
    int r = fc.read(buffer);
    numTuples = buffer.getInt(4);
    numAttributes = buffer.getInt(0);
    bufferPosition = 8;
    return r;
  }
    
}
