package indexes;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

// Create the index node page for the index file 
public class IndexNodeWriter {
  public IndexNode indexNode;
  public int address;
  private FileChannel fc;
  private ByteBuffer buffer;
  private int bufferPosition;
  public final int page_size = 4096;

  /**
   * Constructor for IndexNodeWriter
   * 
   * @param fc        - file channel in which to write the index node page
   * @param address   - the address in which the index node page is located
   * @param indexNode - the indexnode that will be serialized
   * @throws IOException
   */
  public IndexNodeWriter(FileChannel fc, int address, IndexNode indexNode) throws IOException {
    this.address = address;
    this.indexNode = indexNode;
    this.buffer = ByteBuffer.allocate(page_size);
    this.fc = fc;
    bufferPosition = 0;
  }

  /**
   * Creates the index node page with number of keys. keys in the node in order,
   * and address of all the children of the node in order. Fills rest of the page
   * with zeroes
   * 
   * @throws IOException
   */
  public void createIndexNodePage() throws IOException {
    this.buffer.putInt(0, 1);
    this.buffer.putInt(4, indexNode.keys.size());
    bufferPosition = 8;
    for (int i = 0; i < indexNode.keys.size(); i++) {
      int key = indexNode.keys.get(i);
      this.buffer.putInt(bufferPosition, key);
      bufferPosition += 4;
    }
    for (int j = 0; j < indexNode.children.size(); j++) {
      int address = indexNode.children.get(j).getAddress();
      this.buffer.putInt(bufferPosition, address);
      bufferPosition += 4;
    }
    while (bufferPosition < page_size) { // what if bufferPosition = page_size?
      this.buffer.putInt(bufferPosition, 0);
      bufferPosition += 4;
    }
    fc.write(this.buffer);
  }
}
