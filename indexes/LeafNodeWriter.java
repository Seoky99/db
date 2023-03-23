package indexes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

//Writer in binary for a leaf node 
public class LeafNodeWriter {
  public LeafIndexNode leafNode;
  private FileChannel fc;
  private ByteBuffer buffer;
  private int bufferPosition;
  public final int page_size = 4096;

  /**
   * Leaf node writer constructor 
   * @param fc: the file channel 
   * @param address: address of the node 
   * @param leafNode: the node itself 
   */
  public LeafNodeWriter(FileChannel fc, int address, LeafIndexNode leafNode) {
    this.leafNode = leafNode;
    this.buffer = ByteBuffer.allocate(page_size);
    this.fc = fc;
    bufferPosition = 0;
  }

  /**
   * Creates the leaf node pages, taking into account the page,tupleid representation 
   * @throws IOException
   */
  public void createLeafNodePage() throws IOException {
    this.buffer.putInt(0, 0);
    this.buffer.putInt(4, leafNode.entryList.size());
    bufferPosition = 8;
    for (int i = 0; i < leafNode.entryList.size(); i++) {
      Entry entry = leafNode.entryList.get(i);
      int k = entry.indexKey;
      this.buffer.putInt(bufferPosition, k);
      bufferPosition += 4;
      int ridSize = entry.ridList.size();
      this.buffer.putInt(bufferPosition, ridSize);
      bufferPosition += 4;
      for (int j = 0; j < ridSize; j++) {
        Rid rID = entry.ridList.get(j);
        this.buffer.putInt(bufferPosition, rID.pageId);
        bufferPosition += 4;
        this.buffer.putInt(bufferPosition, rID.tupleId);
        bufferPosition += 4;
      }
    }
    while (bufferPosition < page_size) { // what if bufferPosition = page_size?
      this.buffer.putInt(bufferPosition, 0);
      bufferPosition += 4;
    }
    fc.write(this.buffer);
  }

}
