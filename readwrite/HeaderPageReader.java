package readwrite;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

//The reader for the header page 
public class HeaderPageReader {

	private static int page_size = 4096;
	private int numAttributes;
	private FileInputStream fin;
	private ByteBuffer buffer;
	private FileChannel fc;
	private int bufferPosition;

	/**
	 * The constructor for the header page reader
	 * @param inputFile: the path to the header page 
	 * @throws IOException
	 */
	public HeaderPageReader(File inputFile) throws IOException {
		fin = new FileInputStream(inputFile);
		fc = fin.getChannel();
		buffer = ByteBuffer.allocate(page_size);
	}

	/**
	 * Returns the number of leaves read from the header page 
	 * @return: Number of leaves
	 * @throws IOException
	 */
	public int numLeaves() throws IOException {
		buffer = ByteBuffer.allocate(page_size);
		int r = fc.read(buffer);
		bufferPosition = 8;
		return buffer.getInt(4);
	}

	/**
	 * Closes the reader
	 * @throws IOException
	 */
	public void close() throws IOException {
		fin.close();
	}
}
