package com.scylladb.cdc.driver;

import static com.datastax.driver.core.Metadata.quoteIfNecessary;
import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.in;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.scylladb.cdc.Change;
import com.scylladb.cdc.common.FutureUtils;

public class Reader<T> {

  public static interface DeferringConsumer<U, W> {
    public CompletableFuture<Void> consume(U item);

    public W finish();
  }

  public static interface Consumer<U, W> {
    public void consume(U item);

    public W finish();
  }

  private final Session session;
  private final PreparedStatement preparedStmt;
  private final Function<Row, T> translate;

  private Reader(Session s, RegularStatement stmt, Function<Row, T> trans) {
    session = s;
    preparedStmt = session.prepare(stmt);
    translate = trans;
  }

  private ConsistencyLevel computeCL() {
    return session.getCluster().getMetadata().getAllHosts().size() > 1 ? ConsistencyLevel.QUORUM : ConsistencyLevel.ONE;
  }

  private <W> CompletableFuture<W> consumeResults(DeferringConsumer<T, W> c, ResultSet rs) {
    if (rs.getAvailableWithoutFetching() == 0) {
      if (rs.isFullyFetched()) {
        return FutureUtils.completed(c.finish());
      }
      return FutureUtils.transformDeferred(rs.fetchMoreResults(), r -> consumeResults(c, r));
    }

    return c.consume(translate.apply(rs.one())).thenCompose(v -> consumeResults(c, rs));
  }

  public <W> CompletableFuture<W> query(DeferringConsumer<T, W> c, Object... args) {
    return FutureUtils.transformDeferred(session.executeAsync(preparedStmt.bind(args).setConsistencyLevel(computeCL())),
        rs -> consumeResults(c, rs));
  }

  private <W> CompletableFuture<W> consumeResults(Consumer<T, W> c, ResultSet rs) {
    int availCount = rs.getAvailableWithoutFetching();
    if (availCount == 0) {
      if (rs.isFullyFetched()) {
        return FutureUtils.completed(c.finish());
      }
      return FutureUtils.transformDeferred(rs.fetchMoreResults(), r -> consumeResults(c, r));
    }

    Iterator<Row> it = rs.iterator();
    for (int count = 0; count < availCount; ++count) {
      c.consume(translate.apply(it.next()));
    }

    return consumeResults(c, rs);
  }

  public <W> CompletableFuture<W> query(Consumer<T, W> c, Object... args) {
    return FutureUtils.transformDeferred(session.executeAsync(preparedStmt.bind(args).setConsistencyLevel(computeCL())),
        rs -> consumeResults(c, rs));
  }

  public static Reader<Date> createGenerationsTimestampsReader(Session s) {
    return new Reader<Date>(s, select().column("time").from("system_distributed", "cdc_streams_descriptions"),
        r -> r.getTimestamp(0));
  }

  public static Reader<Set<ByteBuffer>> createGenerationStreamsReader(Session s) {
    return new Reader<Set<ByteBuffer>>(s, select().column("streams")
        .from("system_distributed", "cdc_streams_descriptions").where(eq("time", bindMarker())),
        r -> new TreeSet<ByteBuffer>(r.getSet(0, ByteBuffer.class)));
  }

  public static Reader<Change> createStreamsReader(Session s, String keyspaceName, String tableName) {
    return new Reader<Change>(s,
        select().all().from(quoteIfNecessary(keyspaceName), quoteIfNecessary(tableName + "_scylla_cdc_log"))
            .where(in(quoteIfNecessary("cdc$stream_id"), bindMarker()))
            .and(gt(quoteIfNecessary("cdc$time"), bindMarker())).and(lte(quoteIfNecessary("cdc$time"), bindMarker())),
        Change::new);
  }
}
