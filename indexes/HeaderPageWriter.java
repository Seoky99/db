package indexes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

// Creates a header page for the index file 
public class HeaderPageWriter {
  public FileChannel fc;
  public IndexNode node;
  public int order;
  private ByteBuffer buffer;
  private int bufferPosition;
  private int numLeaves;
  public final int page_size = 4096;

  /**
   * Constructor for HeaderPageWriter
   * 
   * @param fc        - file channel in which to write the header page
   * @param node      - the root node to get its address
   * @param order     - the order of the tree
   * @param numLeaves - number of leaves for the tree
   */
  public HeaderPageWriter(FileChannel fc, IndexNode node, int order, int numLeaves) {
    this.fc = fc;
    this.node = node;
    this.order = order;
    this.buffer = ByteBuffer.allocate(page_size);
    this.fc = fc;
    bufferPosition = 0;
    this.numLeaves = numLeaves;
  }

  /**
   * Creates the header page with root address, number of leaves, and order. Fills
   * rest of the page with zeroes
   * 
   * @throws IOException
   */
  public void createHeaderPage() throws IOException {
    this.buffer.putInt(0, node.getAddress());
    this.buffer.putInt(4, numLeaves);
    this.buffer.putInt(8, order);
    bufferPosition = 8;
    while (bufferPosition < page_size) { // what if bufferPosition = page_size?
      this.buffer.putInt(bufferPosition, 0);
      bufferPosition += 4;
    }
    fc.write(this.buffer);
  }

}
