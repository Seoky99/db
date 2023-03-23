package tools;

import cs4321.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

// Converts a binary file of an index and outputs a human readable file from that binary file
public class BinarytoHumanIndex {
  private static int page_size = 4096;
  private FileInputStream fin;
  private ByteBuffer buffer;
  private FileChannel fc;
  private int bufferPosition;
  private PrintStream ps;

  /**
   * Constructor for BinarytoHumanIndex
   * 
   * @param inputFile  - The path to the binary index file that is read
   * @param outputFile - The path to the human readable file to output
   * @throws IOException
   */
  public BinarytoHumanIndex(String inputFile, String outputFile) throws IOException {
    fin = new FileInputStream(inputFile);
    fc = fin.getChannel();
    buffer = ByteBuffer.allocate(page_size);
    ps = new PrintStream(new File(outputFile));
  }

  /**
   * Prints header page information
   * 
   * @throws IOException
   */
  public void printIndex() throws IOException {
    fc.position(0);
    createBuffer();
    int rootAddress = buffer.getInt(0);
    System.out.println(rootAddress);
    int numLeaves = buffer.getInt(4);
    System.out.println(numLeaves);
    int order = buffer.getInt(8);
    System.out.println(order);
    bufferPosition = 0;

    ps.println("Header Page info: tree has order " + String.valueOf(order) + ", a root at address "
        + String.valueOf(rootAddress) + " and " + String.valueOf(numLeaves) + " leaf nodes ");

    fc.position(rootAddress * page_size); // go to root page
    createBuffer();
    int numRootKeys = buffer.getInt(4);
    int bufferPosition = 8;
    int[] rootKeys = new int[numRootKeys];
    for (int i = 0; i < numRootKeys; i++) {
      rootKeys[i] = buffer.getInt(bufferPosition);
      bufferPosition += 4;
    }

    ArrayList<Integer> childAddresses = new ArrayList<Integer>();
    int childAddress = buffer.getInt(bufferPosition);

    childAddresses.add(childAddress);
    bufferPosition += 4;
    childAddress = buffer.getInt(bufferPosition);

    while (childAddress != 0) {
      childAddresses.add(childAddress);
      bufferPosition += 4;
      childAddress = buffer.getInt(bufferPosition);

    }
    ps.println();

    ps.println("Root node is: IndexNode with keys " + Arrays.toString(rootKeys) + " and child addresses "
        + printArrayList(childAddresses));
    ps.println();
    ps.println("---------Next layer is index nodes---------");

    int numIndexPages = rootAddress - numLeaves;
    for (int i = 1; i < numIndexPages; i++) {
      printIndexNode((numLeaves + i) * page_size);
    }

    ps.println("---------Next layer is leaf nodes---------");
    for (int i = 1; i < numLeaves + 1; i++) {
      printLeafNode(i * page_size);
    }

    fin.close();
    ps.close();
  }

  /**
   * Create a new buffer of size page_size
   * 
   * @return - new byte buffer
   */
  public int createBuffer() throws IOException {
    buffer = ByteBuffer.allocate(page_size);
    int r = fc.read(buffer);
    bufferPosition = 8;
    return r;
  }

  /**
   * Prints an index node at a certain page location
   * 
   * @param pageLoc - page number that index node is located
   * @throws IOException
   */
  public void printIndexNode(int pageLoc) throws IOException {
    fc.position(pageLoc);
    createBuffer();
    int numRootKeys = buffer.getInt(4);
    int bufferPosition = 8;
    int[] rootKeys = new int[numRootKeys];
    for (int i = 0; i < numRootKeys; i++) {
      rootKeys[i] = buffer.getInt(bufferPosition);
      bufferPosition += 4;
    }

    ArrayList<Integer> childAddresses = new ArrayList<Integer>();
    int childAddress = buffer.getInt(bufferPosition);

    childAddresses.add(childAddress);
    bufferPosition += 4;
    childAddress = buffer.getInt(bufferPosition);

    while (childAddress != 0) {
      childAddresses.add(childAddress);
      bufferPosition += 4;
      childAddress = buffer.getInt(bufferPosition);

    }

    ps.println("IndexNode with keys " + Arrays.toString(rootKeys) + " and child addresses "
        + printArrayList(childAddresses));
    ps.println();
  }

  /**
   * Prints a leaf node at a certain page location
   * 
   * @param pageLoc - page number that leaf node is located
   * @throws IOException
   */
  public void printLeafNode(int pageLoc) throws IOException {
    fc.position(pageLoc);
    createBuffer();
    int numDataEntries = buffer.getInt(4);
    bufferPosition = 8;
    ps.println("LeafNode[");
    for (int i = 0; i < numDataEntries; i++) {
      String leafNode = "<[";
      int k = buffer.getInt(bufferPosition);
      leafNode += String.valueOf(k) + ":";
      bufferPosition += 4;
      int numrIDs = buffer.getInt(bufferPosition);
      bufferPosition += 4;
      for (int j = 0; j < numrIDs; j++) {
        String pair = "(";
        int pageNum = buffer.getInt(bufferPosition);
        bufferPosition += 4;
        int tupleNum = buffer.getInt(bufferPosition);
        bufferPosition += 4;
        pair += String.valueOf(pageNum) + "," + String.valueOf(tupleNum) + ")";
        leafNode += pair;
      }
      leafNode += "]>";
      ps.println(leafNode);
    }
    ps.println("]");
    ps.println();

  }

  /**
   * Pretty prints array list
   * 
   * @param list - array list to be printed
   * @throws IOException
   */
  public String printArrayList(ArrayList<Integer> list) {
    String s = "[";
    for (int i = 0; i < list.size(); i++) {
      int el = list.get(i);
      if (i == list.size() - 1) {
        s += String.valueOf(el) + "]";
      } else {
        s += String.valueOf(el) + ", "; // what about last element
      }
    }

    return s;
  }

  // Input path of input file (inputFile) and path of output file (outputFile)
  public static void main(String[] args) throws IOException {
    try (Scanner inputScanner = new Scanner(System.in)) {
      // System.out.println("Input the path to the file you want to convert to
      // binary");
      // String inputFile = inputScanner.nextLine();
      // System.out.println("Input the path to the file where it will output the
      // binary file");

      // String outputFile = inputScanner.nextLine();
      String path = DatabaseCatalog.getInstance().getInputPath();
      String inputFile = "samples3" + File.separator + "expected_indexes" +
          File.separator + "Sailors.A";
      String outputFile = "samples3" + File.separator + "expected_indexes" +
          File.separator + "Sailors_test.txt";

      BinarytoHumanIndex binaryToHumanIndex = new BinarytoHumanIndex(inputFile, outputFile);
      binaryToHumanIndex.printIndex();

    } catch (NumberFormatException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}
