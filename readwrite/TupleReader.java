package readwrite;

import java.io.IOException;

public interface TupleReader {

  public String readLine() throws IOException;

  public void reset() throws IOException;

  public void reset(int index) throws IOException;
  
  public void close() throws IOException;
}