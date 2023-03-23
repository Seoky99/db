package readwrite;

import java.io.IOException;

import cs4321.Tuple;

public interface TupleWriter {
  public void write(Tuple t) throws IOException;

  public void close() throws IOException;
}
