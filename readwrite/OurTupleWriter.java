package readwrite;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import cs4321.Tuple;

// Write into a binary file
public class OurTupleWriter implements TupleWriter {
  private static int page_size = 4096;
  private int numTuples;
  private int numAttributes;
  private boolean firstBuffer;
  private FileOutputStream fout;
  private FileChannel fc;
  private ByteBuffer buffer;
  private int bufferPosition;
  private int numBytesLeft;

  /**
   * Constructor for OurTupleWriter
   * 
   * @param path - The path that OurTupleWriter writes to
   * @throws IOException
   */
  public OurTupleWriter(String path) throws IOException {
    fout = new FileOutputStream(path);
    fc = fout.getChannel();
    bufferPosition = 0;
    firstBuffer = true;
  }

  /**
   * Writes tuple to binary file, creates new buffer or outputs buffer if
   * necessary
   * 
   * @param tuple - tuple that is written to the binary file
   * @throws IOException
   */
  @Override
  public void write(Tuple tuple) throws IOException {
    int numContents = tuple.contents.size();
    int numBytesInTuple = numContents * 4;
    if (numBytesInTuple > numBytesLeft) {
      if (firstBuffer) {
        this.buffer = createBuffer(tuple);
        firstBuffer = false;
      } else {
        outputBuffer();
        this.buffer = createBuffer(tuple);
      }
    }

    for (int i = 0; i < numContents; i++) {
      this.buffer.putInt(bufferPosition, tuple.contents.get(i));
      bufferPosition += 4;
    }
    numBytesLeft -= numBytesInTuple;
    numTuples++;
  }

  /**
   * Closes file output stream, outputs buffer if necessary
   * 
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    if (firstBuffer) {
      bufferPosition = 0;
      while (bufferPosition < page_size) {
        this.buffer = ByteBuffer.allocate(page_size);
        this.buffer.putInt(bufferPosition, 0);
        bufferPosition += 4;
      }
    } else {
      outputBuffer();
    }
    fout.close();
  }

  /**
   * Create a new buffer of size page_size
   * 
   * @param tuple - tuple that is used to determine number of attributes
   * @return - new byte buffer
   */
  private ByteBuffer createBuffer(Tuple tuple) {
    numTuples = 0;
    numAttributes = tuple.contents.size();
    bufferPosition = 8;
    numBytesLeft = page_size - 8;
    return ByteBuffer.allocate(page_size);
  }

  /**
   * Outputs buffer by writing it to the channel
   * 
   * @throws IOException
   */
  private void outputBuffer() throws IOException {
    this.buffer.putInt(0, numAttributes);
    this.buffer.putInt(4, numTuples);
    if (numBytesLeft != 0) {
      while (bufferPosition < page_size) {
        this.buffer.putInt(bufferPosition, 0);
        bufferPosition += 4;
      }
    }
    fc.write(this.buffer);
  }
}
