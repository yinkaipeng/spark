package org.apache.spark.sql.hive.thriftserver.rpc;

import com.cloudera.livy.shaded.kryo.kryo.io.Input;
import com.cloudera.livy.shaded.kryo.kryo.io.Output;
import org.apache.hadoop.io.Writable;
import org.apache.thrift.TByteArrayOutputStream;

import java.io.*;

/**
 * Util methods for (de-) serialization
 */
public class RpcUtil {
  // TByteArrayOutputStream ensures we dont need to copy the internal buffer
  // Must ensure it is reset after each get()
  private static final ThreadLocal<TByteArrayOutputStream> serBufferThLocal =
      new ThreadLocal<TByteArrayOutputStream>() {
        @Override
        protected TByteArrayOutputStream initialValue() {
          return new TByteArrayOutputStream(1024);
        }
      };

  // try upto 2 times to drain remaining pending bytes. Ideally there should be
  // no bytes pending - but there might be some footer, etc remaining.
  private static final int NUM_DRAIN_RETRY = 2;

  // Ensure we read only upto specified bytes from the input stream.
  private static class KryoInputAdapter extends InputStream {

    private int remaining;
    private Input input;

    private KryoInputAdapter(Input input, int length) {
      this.input = input;
      this.remaining = length;
    }

    @Override
    public int read() throws IOException {
      if (remaining <= 0) return -1;

      final int retval = input.read();
      remaining --;
      return retval;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      if (remaining <= 0) return -1;

      final int size = Math.min(len, remaining);
      final int actualRead = input.read(b, off, size);
      remaining -= actualRead;
      return actualRead;
    }

    @Override
    public long skip(long n) throws IOException {
      final long toSkip = Math.min(n, remaining);
      final long actualSkip = input.skip(toSkip);
      remaining -= actualSkip;
      return actualSkip;
    }

    @Override
    public int available() throws IOException {
      return remaining;
    }

    @Override
    public void close() throws IOException {
      remaining = 0;
    }

    @Override
    public void mark(int readlimit) {
      throw new UnsupportedOperationException("Mark/reset not supported");
    }

    @Override
    public void reset() throws IOException {
      throw new IOException("Mark/reset not supported");
    }

    @Override
    public boolean markSupported() {
      return false;
    }

    void drain() throws IOException {
      int retry = 0;
      while (remaining > 0) {
        final long skipped = skip(remaining);
        if (skipped <= 0) {
          if (retry > NUM_DRAIN_RETRY) {
            throw new IOException("Unable to drain remaining = " + remaining + " bytes");
          }
          retry ++;
        } else {
          retry = 0;
        }
      }
    }
  }


  public static void serialize(Serializable object, Output output) {
    final TByteArrayOutputStream baos = serBufferThLocal.get();
    try {
      baos.reset();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(object);
      oos.flush(); oos.close();

      final int length = baos.size();
      output.writeInt(length, true);
      output.write(baos.get(), 0, length);
    } catch (IOException ioEx) {
      throw new IllegalStateException("Unable to serialize " + object, ioEx);
    }
  }

  public static Serializable deserialize(Input input) {
    final int length = input.readInt(true);
    try {
      final KryoInputAdapter stream = new KryoInputAdapter(input, length);
      ObjectInputStream ois = new ObjectInputStream(stream);
      Serializable retval = (Serializable) ois.readObject();
      // Ensure we drain the stream and dont leave any bytes around
      stream.drain();
      return retval;
    } catch (ClassNotFoundException | IOException e) {
      throw new IllegalStateException("Unable to deserialize trowSet from " + length + " bytes", e);
    }
  }

  public static byte[] serializeToBytes(Writable object) {
    final TByteArrayOutputStream baos = serBufferThLocal.get();
    try {
      baos.reset();
      DataOutputStream dataOutput = new DataOutputStream(baos);
      object.write(dataOutput);
      dataOutput.flush(); dataOutput.close();

      return baos.toByteArray();
    } catch (IOException ioEx) {
      throw new IllegalStateException("Unable to serialize " + object, ioEx);
    }
  }

  public static byte[] serializeToBytes(Serializable object) {
    final TByteArrayOutputStream baos = serBufferThLocal.get();
    try {
      baos.reset();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(object);
      oos.flush(); oos.close();

      return baos.toByteArray();
    } catch (IOException ioEx) {
      throw new IllegalStateException("Unable to serialize " + object, ioEx);
    }
  }

  public static Serializable deserializeFromBytes(byte[] bytes) {

    try {
      ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
      ObjectInputStream ois = new ObjectInputStream(bis);
      return (Serializable) ois.readObject();
    } catch (IOException|ClassNotFoundException ex) {
      throw new IllegalStateException("Unable to deserialize from input", ex);
    }
  }


  public static void deserializeFromBytes(Writable writable, byte[] bytes) {

    try {
      ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
      DataInputStream dis = new DataInputStream(bis);
      writable.readFields(dis);
    } catch (IOException ex) {
      throw new IllegalStateException("Unable to deserialize from input for " + writable, ex);
    }
  }
}
