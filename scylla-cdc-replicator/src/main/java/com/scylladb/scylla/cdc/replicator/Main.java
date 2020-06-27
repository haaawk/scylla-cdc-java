package com.scylladb.scylla.cdc.replicator;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.datastax.driver.core.querybuilder.QueryBuilder.timestamp;
import static com.datastax.driver.core.querybuilder.QueryBuilder.ttl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
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

    private static abstract class PreparedOperation {
      protected final TableMetadata table;
      protected final PreparedStatement preparedStmt;

      protected static long timeuuidToTimestamp(UUID from) {
        return (from.timestamp() - 0x01b21dd213814000L) / 10;
      }

      private static TableMetadata getTable(Cluster cluster, String keyspace, String table) {
        return cluster.getMetadata().getKeyspaces().stream()
            .filter(k -> k.getName().equals(keyspace))
            .findAny()
            .get()
            .getTable(table);
      }

      protected abstract PreparedStatement prepare(Session s, TableMetadata t);

      protected PreparedOperation(Cluster cluster, Session session, String keyspaceName, String tableName) {
        table = getTable(cluster, keyspaceName, tableName);
        preparedStmt = prepare(session, table);
      }

      public BoundStatement bind(Change c, ConsistencyLevel cl) {
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
        Set<String> primaryColumns = table.getPrimaryKey().stream()
            .map(ColumnMetadata::getName)
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
        Set<String> partitionColumns = table.getPartitionKey().stream()
            .map(ColumnMetadata::getName)
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

    private static class UpdateOp extends PreparedOperation {

      protected PreparedStatement prepare(Session session, TableMetadata t) {
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
        return session.prepare(builder);
      }

      public UpdateOp(Cluster cluster, Session session, String keyspace, String table) {
        super(cluster, session, keyspace, table);
      }

      @Override
      protected void bindInternal(BoundStatement stmt, Change c) {
        bindTTL(stmt, c);
        bindAllNonCDCColumns(stmt, c);
      }

    }

    private static class InsertOp extends PreparedOperation {

      protected PreparedStatement prepare(Session session, TableMetadata t) {
        Insert builder = QueryBuilder.insertInto(t);
        t.getColumns().stream().forEach(c -> builder.value(c.getName(), bindMarker(c.getName())));
        builder.using(timestamp(bindMarker(TIMESTAMP_MARKER_NAME))).and(ttl(bindMarker(TTL_MARKER_NAME)));
        return session.prepare(builder);
      }

      public InsertOp(Cluster cluster, Session session, String keyspace, String table) {
        super(cluster, session, keyspace, table);
      }

      @Override
      protected void bindInternal(BoundStatement stmt, Change c) {
        bindTTL(stmt, c);
        bindAllNonCDCColumns(stmt, c);
      }

    }

    private static class RowDeleteOp extends PreparedOperation {

      protected PreparedStatement prepare(Session session, TableMetadata t) {
        Delete builder = QueryBuilder.delete().from(t);
        t.getPrimaryKey().stream().forEach(c -> builder.where(eq(c.getName(), bindMarker(c.getName()))));
        builder.using(timestamp(bindMarker(TIMESTAMP_MARKER_NAME)));
        return session.prepare(builder);
      }

      public RowDeleteOp(Cluster cluster, Session session, String keyspace, String table) {
        super(cluster, session, keyspace, table);
      }

      @Override
      protected void bindInternal(BoundStatement stmt, Change c) {
        bindPrimaryKeyColumns(stmt, c);
      }

    }

    private static class PartitionDeleteOp extends PreparedOperation {

      protected PreparedStatement prepare(Session session, TableMetadata t) {
        Delete builder = QueryBuilder.delete().from(t);
        t.getPartitionKey().stream().forEach(c -> builder.where(eq(c.getName(), bindMarker(c.getName()))));
        builder.using(timestamp(bindMarker(TIMESTAMP_MARKER_NAME)));
        return session.prepare(builder);
      }

      public PartitionDeleteOp(Cluster cluster, Session session, String keyspace, String table) {
        super(cluster, session, keyspace, table);
      }

      @Override
      protected void bindInternal(BoundStatement stmt, Change c) {
        bindPartitionKeyColumns(stmt, c);
      }

    }

    private final Session session;
    private final ConsistencyLevel cl;
    private final Map<Byte, PreparedOperation> preparedOps = new HashMap<>();

    public Consumer(Cluster c, Session s, String keyspace, String table, ConsistencyLevel cl) {
      session = s;
      this.cl = cl;
      preparedOps.put((byte)1, new UpdateOp(c, s, keyspace, table));
      preparedOps.put((byte)2, new InsertOp(c, s, keyspace, table));
      preparedOps.put((byte)3, new RowDeleteOp(c, s, keyspace, table));
      preparedOps.put((byte)4, new PartitionDeleteOp(c, s, keyspace, table));
    }

    @Override
    public CompletableFuture<Void> consume(Change change) {
      return FutureUtils.convert(session.executeAsync(preparedOps.get(change.getOperation()).bind(change, cl)));
    }

  }

  private static void replicateChanges(
      String source, String destination, String keyspace, String table, ConsistencyLevel cl)
          throws InterruptedException, ExecutionException {
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
    parser.addArgument("-cl", "--consistency-level")
        .setDefault("quorum")
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
