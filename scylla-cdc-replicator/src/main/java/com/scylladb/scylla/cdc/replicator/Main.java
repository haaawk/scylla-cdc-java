package com.scylladb.scylla.cdc.replicator;

import static com.datastax.driver.core.querybuilder.QueryBuilder.addAll;
import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.putAll;
import static com.datastax.driver.core.querybuilder.QueryBuilder.removeAll;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.datastax.driver.core.querybuilder.QueryBuilder.timestamp;
import static com.datastax.driver.core.querybuilder.QueryBuilder.ttl;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.querybuilder.Assignment;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.ListSetIdxTimeUUIDAssignment;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
import com.google.common.io.BaseEncoding;
import com.google.common.reflect.TypeToken;
import com.scylladb.cdc.Change;
import com.scylladb.cdc.ChangeConsumer;
import com.scylladb.cdc.ScyllaCDC;
import com.scylladb.cdc.common.FutureUtils;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import sun.misc.Signal;

public class Main {

  private static final class Consumer implements ChangeConsumer {
    private static final String TIMESTAMP_MARKER_NAME = "using_timestamp_bind_marker";
    private static final String TTL_MARKER_NAME = "using_ttl_bind_marker";

    private static interface Operation {
      Statement getStatement(Change c, ConsistencyLevel cl);
    }

    private static long timeuuidToTimestamp(UUID from) {
      return (from.timestamp() - 0x01b21dd213814000L) / 10;
    }

    private static abstract class PreparedOperation implements Operation {
      protected final TableMetadata table;
      protected final PreparedStatement preparedStmt;

      protected abstract RegularStatement getStatement(TableMetadata t);

      protected PreparedOperation(Session session, TableMetadata t) {
        table = t;
        preparedStmt = session.prepare(getStatement(table));
      }

      public Statement getStatement(Change c, ConsistencyLevel cl) {
        BoundStatement stmt = preparedStmt.bind();
        stmt.setLong(TIMESTAMP_MARKER_NAME, timeuuidToTimestamp(c.getTime()));
        bindInternal(stmt, c);
        stmt.setConsistencyLevel(cl);
        stmt.setIdempotent(true);
        return stmt;
      }

      protected void bindTTL(BoundStatement stmt, Change c) {
        Integer ttl = c.getTTL();
        if (ttl != null) {
          stmt.setInt(TTL_MARKER_NAME, ttl);
        } else {
          stmt.unset(TTL_MARKER_NAME);
        }
      }

      protected void bindAllNonCDCColumns(BoundStatement stmt, Change c) {
        for (Definition d : c.row.getColumnDefinitions()) {
          if (!d.getName().startsWith("cdc$")) {
            if (c.row.isNull(d.getName()) && !c.isDeleted(d.getName())) {
              stmt.unset(d.getName());
            } else {
              stmt.setBytesUnsafe(d.getName(), c.row.getBytesUnsafe(d.getName()));
            }
          }
        }
      }

      protected void bindPrimaryKeyColumns(BoundStatement stmt, Change c) {
        Set<String> primaryColumns = table.getPrimaryKey().stream().map(ColumnMetadata::getName)
            .collect(Collectors.toSet());
        for (Definition d : c.row.getColumnDefinitions()) {
          if (!d.getName().startsWith("cdc$")) {
            if (primaryColumns.contains(d.getName())) {
              stmt.setBytesUnsafe(d.getName(), c.row.getBytesUnsafe(d.getName()));
            }
          }
        }
      }

      protected void bindPartitionKeyColumns(BoundStatement stmt, Change c) {
        Set<String> partitionColumns = table.getPartitionKey().stream().map(ColumnMetadata::getName)
            .collect(Collectors.toSet());
        for (Definition d : c.row.getColumnDefinitions()) {
          if (!d.getName().startsWith("cdc$")) {
            if (partitionColumns.contains(d.getName())) {
              stmt.setBytesUnsafe(d.getName(), c.row.getBytesUnsafe(d.getName()));
            }
          }
        }
      }

      protected abstract void bindInternal(BoundStatement stmt, Change c);
    }

    private static class UnpreparedUpdateOp implements Operation {
      private final TableMetadata table;

      public UnpreparedUpdateOp(TableMetadata t) {
        table = t;
      }

      @Override
      public Statement getStatement(Change change, ConsistencyLevel cl) {
        Update builder = QueryBuilder.update(table);
        Set<ColumnMetadata> primaryColumns = new HashSet<>(table.getPrimaryKey());
        table.getColumns().stream().forEach(c -> {
          TypeCodec<Object> codec = CodecRegistry.DEFAULT_INSTANCE.codecFor(c.getType());
          if (primaryColumns.contains(c)) {
            builder.where(eq(c.getName(), change.row.get(c.getName(), codec)));
          } else {
            Assignment op = null;
            if (c.getType().isCollection() && !c.getType().isFrozen() && !change.isDeleted(c.getName())) {
              DataType innerType = c.getType().getTypeArguments().get(0);
              TypeToken<Object> type = CodecRegistry.DEFAULT_INSTANCE.codecFor(innerType).getJavaType();
              TypeToken<Object> type2 = null;
              if (c.getType().getTypeArguments().size() > 1) {
                type2 = CodecRegistry.DEFAULT_INSTANCE.codecFor(c.getType().getTypeArguments().get(1)).getJavaType();
              }
              String deletedElementsColumnName = "cdc$deleted_elements_" + c.getName();
              if (!change.row.isNull(deletedElementsColumnName)) {
                if (c.getType().getName() == DataType.Name.SET) {
                  op = removeAll(c.getName(), change.row.getSet(deletedElementsColumnName, type));
                } else if (c.getType().getName() == DataType.Name.MAP) {
                  op = removeAll(c.getName(), change.row.getSet(deletedElementsColumnName, type));
                } else if (c.getType().getName() == DataType.Name.LIST) {
                  for (UUID key : change.row.getSet(deletedElementsColumnName, UUID.class)) {
                    builder.with(new ListSetIdxTimeUUIDAssignment(c.getName(), key, null));
                  }
                  return;
                } else {
                  throw new IllegalStateException();
                }
              } else {
                if (c.getType().getName() == DataType.Name.SET) {
                  op = addAll(c.getName(), change.row.getSet(c.getName(), type));
                } else if (c.getType().getName() == DataType.Name.MAP) {
                  op = putAll(c.getName(), change.row.getMap(c.getName(), type, type2));
                } else if (c.getType().getName() == DataType.Name.LIST) {
                  for (Entry<UUID, Object> e : change.row.getMap(c.getName(), TypeToken.of(UUID.class), type)
                      .entrySet()) {
                    builder.with(new ListSetIdxTimeUUIDAssignment(c.getName(), e.getKey(), e.getValue()));
                  }
                  return;
                } else {
                  throw new IllegalStateException();
                }
              }
            }
            if (op == null) {
              op = set(c.getName(), change.row.get(c.getName(), codec));
            }
            builder.with(op);
          }
        });
        Integer ttl = change.getTTL();
        if (ttl != null) {
          builder.using(timestamp(timeuuidToTimestamp(change.getTime()))).and(ttl(ttl));
        } else {
          builder.using(timestamp(timeuuidToTimestamp(change.getTime())));
        }
        return builder;
      }

    }

    private static class UpdateOp extends PreparedOperation {

      protected RegularStatement getStatement(TableMetadata t) {
        Update builder = QueryBuilder.update(t);
        Set<ColumnMetadata> primaryColumns = new HashSet<>(t.getPrimaryKey());
        t.getColumns().stream().forEach(c -> {
          if (primaryColumns.contains(c)) {
            builder.where(eq(c.getName(), bindMarker(c.getName())));
          } else {
            builder.with(set(c.getName(), bindMarker(c.getName())));
          }
        });

        builder.using(timestamp(bindMarker(TIMESTAMP_MARKER_NAME))).and(ttl(bindMarker(TTL_MARKER_NAME)));
        return builder;
      }

      public UpdateOp(Session session, TableMetadata table) {
        super(session, table);
      }

      @Override
      protected void bindInternal(BoundStatement stmt, Change c) {
        bindTTL(stmt, c);
        bindAllNonCDCColumns(stmt, c);
      }

    }

    private static class InsertOp extends PreparedOperation {

      protected RegularStatement getStatement(TableMetadata t) {
        Insert builder = QueryBuilder.insertInto(t);
        t.getColumns().stream().forEach(c -> builder.value(c.getName(), bindMarker(c.getName())));
        builder.using(timestamp(bindMarker(TIMESTAMP_MARKER_NAME))).and(ttl(bindMarker(TTL_MARKER_NAME)));
        return builder;
      }

      public InsertOp(Session session, TableMetadata table) {
        super(session, table);
      }

      @Override
      protected void bindInternal(BoundStatement stmt, Change c) {
        bindTTL(stmt, c);
        bindAllNonCDCColumns(stmt, c);
      }

    }

    private static class RowDeleteOp extends PreparedOperation {

      protected RegularStatement getStatement(TableMetadata t) {
        Delete builder = QueryBuilder.delete().from(t);
        t.getPrimaryKey().stream().forEach(c -> builder.where(eq(c.getName(), bindMarker(c.getName()))));
        builder.using(timestamp(bindMarker(TIMESTAMP_MARKER_NAME)));
        return builder;
      }

      public RowDeleteOp(Session session, TableMetadata table) {
        super(session, table);
      }

      @Override
      protected void bindInternal(BoundStatement stmt, Change c) {
        bindPrimaryKeyColumns(stmt, c);
      }

    }

    private static class PartitionDeleteOp extends PreparedOperation {

      protected RegularStatement getStatement(TableMetadata t) {
        Delete builder = QueryBuilder.delete().from(t);
        t.getPartitionKey().stream().forEach(c -> builder.where(eq(c.getName(), bindMarker(c.getName()))));
        builder.using(timestamp(bindMarker(TIMESTAMP_MARKER_NAME)));
        return builder;
      }

      public PartitionDeleteOp(Session session, TableMetadata table) {
        super(session, table);
      }

      @Override
      protected void bindInternal(BoundStatement stmt, Change c) {
        bindPartitionKeyColumns(stmt, c);
      }

    }

    private static class RangeDeleteStartOp implements Operation {
      private final RangeTombstoneState state;
      private final boolean inclusive;

      public RangeDeleteStartOp(RangeTombstoneState rtState, boolean inclusive) {
        state = rtState;
        this.inclusive = inclusive;
      }

      @Override
      public Statement getStatement(Change c, ConsistencyLevel cl) {
        state.addStart(c, inclusive);
        return null;
      }

    }

    private static class RangeDeleteEndOp implements Operation {
      private final TableMetadata table;
      private final RangeTombstoneState state;
      private final Map<Integer, Map<Boolean, PreparedStatement>> stmts = new HashMap<>();

      private static PreparedStatement prepare(Session s, TableMetadata t, int prefixSize, boolean startInclusive,
          boolean endInclusive) {
        Delete builder = QueryBuilder.delete().from(t);
        List<ColumnMetadata> pk = t.getPrimaryKey();
        pk.subList(0, prefixSize).stream().forEach(c -> builder.where(eq(c.getName(), bindMarker(c.getName()))));
        ColumnMetadata lastCk = pk.get(prefixSize);
        builder.where(startInclusive ? gte(lastCk.getName(), bindMarker(lastCk.getName() + "_start"))
            : gt(lastCk.getName(), bindMarker(lastCk.getName() + "_start")));
        builder.where(endInclusive ? lte(lastCk.getName(), bindMarker(lastCk.getName() + "_end"))
            : lt(lastCk.getName(), bindMarker(lastCk.getName() + "_end")));
        builder.using(timestamp(bindMarker(TIMESTAMP_MARKER_NAME)));
        return s.prepare(builder);
      }

      public RangeDeleteEndOp(Session session, TableMetadata t, RangeTombstoneState state, boolean inclusive) {
        table = t;
        this.state = state;
        for (int i = t.getPartitionKey().size(); i < t.getPrimaryKey().size(); ++i) {
          Map<Boolean, PreparedStatement> map = new HashMap<>();
          map.put(true, prepare(session, t, i, true, inclusive));
          map.put(false, prepare(session, t, i, false, inclusive));
          stmts.put(i + 1, map);
        }
      }

      private static Statement bind(TableMetadata table, PreparedStatement stmt, Change change, PrimaryKeyValue startVal, ConsistencyLevel cl) {
        BoundStatement s = stmt.bind();
        Iterator<ColumnMetadata> keyIt = table.getPrimaryKey().iterator();
        ColumnMetadata prevCol = keyIt.next();
        ByteBuffer end = change.row.getBytesUnsafe(prevCol.getName());
        byte[] start = startVal.values.get(prevCol.getName());
        while (keyIt.hasNext()) {
          ColumnMetadata col = keyIt.next();
          byte[] newStart = startVal.values.get(col.getName());
          if (newStart == null) {
            break;
          }
          s.setBytesUnsafe(prevCol.getName(), end);
          end = change.row.getBytesUnsafe(col.getName());
          prevCol = col;
          start = newStart;
        }
        s.setBytesUnsafe(prevCol.getName() + "_start", ByteBuffer.wrap(start));
        s.setBytesUnsafe(prevCol.getName() + "_end", end);
        s.setLong(TIMESTAMP_MARKER_NAME, timeuuidToTimestamp(change.getTime()));
        s.setConsistencyLevel(cl);
        s.setIdempotent(true);
        return s;
      }

      @Override
      public Statement getStatement(Change c, ConsistencyLevel cl) {
        byte[] streamId = new byte[16];
        c.streamId.duplicate().get(streamId, 0, 16);
        DeletionStart start = state.getStart(streamId);
        if (start == null) {
          throw new IllegalStateException("Got range deletion end but no start in stream " + BaseEncoding.base16().encode(streamId, 0, 16));
        }
        try {
          return bind(table, stmts.get(start.val.values.size()).get(start.inclusive), c, start.val, cl);
        } finally {
          state.clear(streamId);
        }
      }

    }

    private final Session session;
    private final ConsistencyLevel cl;
    private final Map<Byte, Operation> preparedOps = new HashMap<>();


    private static class DeletionStart {
      public final PrimaryKeyValue val;
      public final boolean inclusive;

      public DeletionStart(PrimaryKeyValue v, boolean i) {
        val = v;
        inclusive = i;
      }
    }

    private static class PrimaryKeyValue {
      public final Map<String, byte[]> values = new HashMap<>();

      public PrimaryKeyValue(TableMetadata table, Row row) {
        for (ColumnMetadata col : table.getPrimaryKey()) {
          ByteBuffer buf = row.getBytesUnsafe(col.getName());
          if (buf != null) {
            byte[] bytes = new byte[buf.remaining()];
            buf.get(bytes);
            values.put(col.getName(), bytes);
          }
        }
      }

    }

    private static class RangeTombstoneState {

      private static class Key {
        public final byte[] val;

        public Key(byte[] v) {
          val = v;
        }

        @Override
        public boolean equals(Object o) {
          return o instanceof Key && Arrays.equals(val, ((Key)o).val);
        }

        @Override
        public int hashCode() {
          return Arrays.hashCode(val);
        }
      }

      private final TableMetadata table;
      private final ConcurrentHashMap<Key, DeletionStart> state = new ConcurrentHashMap<>();

      public RangeTombstoneState(TableMetadata table) {
        this.table = table;
      }

      public void addStart(Change c, boolean inclusive) {
        byte[] bytes = new byte[16];
        c.streamId.duplicate().get(bytes, 0, 16);
        state.put(new Key(bytes), new DeletionStart(new PrimaryKeyValue(table, c.row), inclusive));
      }

      public DeletionStart getStart(byte[] streamId) {
        return state.get(new Key(streamId));
      }

      public void clear(byte[] streamId) {
        state.remove(new Key(streamId));
      }
    }

    public Consumer(Cluster c, Session s, String keyspace, String table, ConsistencyLevel cl) {
      session = s;
      this.cl = cl;
      TableMetadata tableMetadata = c.getMetadata().getKeyspaces().stream().filter(k -> k.getName().equals(keyspace))
          .findAny().get().getTable(table);
      preparedOps.put((byte) 1,
          hasCollection(tableMetadata) ? new UnpreparedUpdateOp(tableMetadata) : new UpdateOp(s, tableMetadata));
      preparedOps.put((byte) 2, new InsertOp(s, tableMetadata));
      preparedOps.put((byte) 3, new RowDeleteOp(s, tableMetadata));
      preparedOps.put((byte) 4, new PartitionDeleteOp(s, tableMetadata));
      RangeTombstoneState rtState = new RangeTombstoneState(tableMetadata);
      preparedOps.put((byte) 5, new RangeDeleteStartOp(rtState, true));
      preparedOps.put((byte) 6, new RangeDeleteStartOp(rtState, false));
      preparedOps.put((byte) 7, new RangeDeleteEndOp(s, tableMetadata, rtState, true));
      preparedOps.put((byte) 8, new RangeDeleteEndOp(s, tableMetadata, rtState, false));
    }

    private static boolean hasCollection(TableMetadata t) {
      return t.getColumns().stream().anyMatch(c -> c.getType().isCollection() && !c.getType().isFrozen());
    }

    @Override
    public CompletableFuture<Void> consume(Change change) {
      Operation op = preparedOps.get(change.getOperation());
      if (op == null) {
        System.err.println("Unsupported operation: " + change.getOperation());
        throw new UnsupportedOperationException("" + change.getOperation());
      }
      Statement stmt = op.getStatement(change, cl);
      if (stmt == null) {
        return FutureUtils.completed(null);
      }
      return FutureUtils.convert(session.executeAsync(stmt));
    }

  }

  private static void replicateChanges(String source, String destination, String keyspace, String table,
      ConsistencyLevel cl) throws InterruptedException, ExecutionException {
    Signal.handle(new Signal("INT"), signal -> System.exit(0));
    try (Cluster sCluster = Cluster.builder().addContactPoint(source).build();
        Session sSession = sCluster.connect();
        Cluster dCluster = Cluster.builder().addContactPoint(destination).build();
        Session dSession = dCluster.connect()) {

      ScyllaCDC cdc = new ScyllaCDC(sSession, keyspace, table, new Consumer(dCluster, dSession, keyspace, table, cl));

      cdc.fetchChanges().get();
    }
  }

  private static Namespace parseArguments(String[] args) {
    ArgumentParser parser = ArgumentParsers.newFor("CDCReplicator").build().defaultHelp(true);
    parser.addArgument("-k", "--keyspace").help("Keyspace name");
    parser.addArgument("-t", "--table").help("Table name");
    parser.addArgument("-s", "--source").help("Address of a node in source cluster");
    parser.addArgument("-d", "--destination").help("Address of a node in destination cluster");
    parser.addArgument("-cl", "--consistency-level").setDefault("quorum")
        .help("Consistency level of writes. QUORUM by default");

    try {
      return parser.parseArgs(args);
    } catch (ArgumentParserException e) {
      parser.handleError(e);
      System.exit(-1);
      return null;
    }
  }

  public static void main(String[] args) throws Exception {
    Namespace ns = parseArguments(args);
    replicateChanges(ns.getString("source"), ns.getString("destination"), ns.getString("keyspace"),
        ns.getString("table"), ConsistencyLevel.valueOf(ns.getString("consistency_level").toUpperCase()));
  }

}
