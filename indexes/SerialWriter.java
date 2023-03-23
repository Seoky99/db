package indexes;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

//The writer for the serialization of an index representation 
public class SerialWriter {
	public FileChannel fc;
	public FileOutputStream fos;
	private ByteBuffer buffer;
	private int bufferPosition;
	public final int page_size = 4096;

	/**
	 * Constructor of the serial writer 
	 * @param path - the path to the serialization 
	 */
	public SerialWriter(String path) throws IOException {
		this.fos = new FileOutputStream(path);
		this.fc = fos.getChannel();
		this.fc.position(page_size);
		this.buffer = ByteBuffer.allocate(page_size);
	}

	/**
	 * Creates the header page of the serialization 
	 * @param indexNode - the index node  
	 * @param order - order of the index 
	 * @param numLeaves - leaves of the index
	 * @throws IOException - 
	 */
	public void createHeaderPage(IndexNode indexNode, int order, int numLeaves) throws IOException {
		this.fc.position(0);
		
		this.buffer.putInt(0, indexNode.getAddress());
		this.buffer.putInt(4, numLeaves);
		this.buffer.putInt(8, order);
		bufferPosition = 12;
		while (bufferPosition < page_size) { // what if bufferPosition = page_size?
			this.buffer.putInt(bufferPosition, 0);
			bufferPosition += 4;
		}
		fc.write(this.buffer);
		resetBuffer();
	}

	/**
	 * Reset the buffer
	 */
	public void resetBuffer() {
		this.buffer = ByteBuffer.allocate(page_size);
	}

	/**
	 * Creates the node page 
	 * @param address: The address of the node 
	 * @param leafNode: The node
	 * @throws IOException
	 */
	public void createLeafNodePage(int address, LeafIndexNode leafNode) throws IOException {
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
		resetBuffer();
	}

	/**
	 * Creates the index node page
	 * @param address: The address of the node 
	 * @param indexNode: The index node 
	 * @throws IOException
	 */
	public void createIndexNodePage(int address, IndexNode indexNode) throws IOException {
		this.buffer.putInt(0, 1);
		this.buffer.putInt(4, indexNode.keys.size());
		bufferPosition = 8;
		for (int i = 0; i < indexNode.keys.size(); i++) {
			int key = indexNode.keys.get(i);
			this.buffer.putInt(bufferPosition, key);
			bufferPosition += 4;
		}
		for (int j = 0; j < indexNode.children.size(); j++) {
			int childAddress = indexNode.children.get(j).getAddress();
			this.buffer.putInt(bufferPosition, childAddress);
			bufferPosition += 4;
		}
		while (bufferPosition < page_size) { // what if bufferPosition = page_size?
			this.buffer.putInt(bufferPosition, 0);
			bufferPosition += 4;
		}
		fc.write(this.buffer);
		resetBuffer();
	}

	/**
	 * Close the writer
	 * @throws IOException
	 */
	public void close() throws IOException {

		fos.close();
	}

}
