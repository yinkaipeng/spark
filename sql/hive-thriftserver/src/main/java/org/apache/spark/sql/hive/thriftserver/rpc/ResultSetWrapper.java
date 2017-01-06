package org.apache.spark.sql.hive.thriftserver.rpc;

import com.cloudera.livy.shaded.kryo.kryo.Kryo;
import com.cloudera.livy.shaded.kryo.kryo.KryoSerializable;
import com.cloudera.livy.shaded.kryo.kryo.io.Input;
import com.cloudera.livy.shaded.kryo.kryo.io.Output;
import org.apache.hive.service.cli.ColumnBasedSet;
import org.apache.hive.service.cli.RowBasedSet;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.thrift.TRowSet;
import org.apache.thrift.TByteArrayOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Wrap a ColumnBasedSet or RowBasedSet
 * The former cannot be deserialized by kryo gracefully and the latter
 * is serialized inefficiently.
 */
public abstract class ResultSetWrapper implements KryoSerializable {

  // For now, hardcoded in code to test. (note, using env, etc might be
  // risky if we cant ensure it is consistently set in a distributed setting).
  // Ideally, we want to use ColumnBasedSet - and fallback to RowBasedSet
  // only if we are unable to make ColumnBasedSet work.
  public static final boolean enableColumnSet = true;

  public abstract RowSet toRowSet();

  public abstract void addRow(Object[] data);

  protected abstract TRowSet getAsTRowSet();

  protected abstract void deserializeFromTRowSet(TRowSet set);

  @SuppressWarnings("ConstantConditions")
  public static ResultSetWrapper create(TableSchema schema) {
    return enableColumnSet ?
        new ColumnBasedResultSetWrapper(schema) :
        new RowBasedResultSetWrapper(schema);
  }

  // Simple implementation to work around kryo issue - perf not (yet) a concern
  @Override
  public void write(Kryo kryo, Output output) {
    TRowSet trowSet = getAsTRowSet();
    RpcUtil.serialize(trowSet, output);
  }

  @Override
  public void read(Kryo kryo, Input input) {
    deserializeFromTRowSet((TRowSet) RpcUtil.deserialize(input));
  }

  private static class ColumnBasedResultSetWrapper extends ResultSetWrapper {

    private ColumnBasedSet delegate;

    ColumnBasedResultSetWrapper(TableSchema schema) {
      this.delegate = new ColumnBasedSet(schema);
    }

    @Override
    public RowSet toRowSet() {
      return delegate;
    }

    @Override
    public void addRow(Object[] data) {
      delegate.addRow(data);
    }

    @Override
    protected TRowSet getAsTRowSet() {
      return delegate.toTRowSet();
    }

    @Override
    protected void deserializeFromTRowSet(TRowSet set) {
      this.delegate = new ColumnBasedSet(set);
    }
  }

  // RowBasedSet seems to be handled fine by kryo
  private static class RowBasedResultSetWrapper extends ResultSetWrapper {

    private RowBasedSet delegate;

    RowBasedResultSetWrapper(TableSchema schema) {
      this.delegate = new RowBasedSet(schema);
    }

    @Override
    public RowSet toRowSet() {
      return delegate;
    }

    @Override
    public void addRow(Object[] data) {
      delegate.addRow(data);
    }

    @Override
    protected TRowSet getAsTRowSet() {
      return delegate.toTRowSet();
    }

    @Override
    protected void deserializeFromTRowSet(TRowSet set) {
      this.delegate = new RowBasedSet(set);
    }
  }
}
