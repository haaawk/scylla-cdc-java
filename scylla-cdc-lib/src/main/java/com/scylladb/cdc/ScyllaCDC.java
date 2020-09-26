package com.scylladb.cdc;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import com.datastax.driver.core.PartitioningHelper;
import com.datastax.driver.core.Session;
import com.scylladb.cdc.driver.ClusterObserver;
import com.scylladb.cdc.driver.Reader;
import com.scylladb.cdc.master.GenerationsFetcher;
import com.scylladb.cdc.master.Master;
import com.scylladb.cdc.worker.GenerationEndTimestampFetcher;
import com.scylladb.cdc.worker.Worker;

public class ScyllaCDC {

  private final Master master;

  public ScyllaCDC(Session session, String keyspace, String table, ChangeConsumer c) {
    Reader<Date> tReader = Reader.createGenerationsTimestampsReader(session);
    Reader<Set<ByteBuffer>> gsReader = Reader.createGenerationStreamsReader(session);
    GenerationsFetcher fetcher = new GenerationsFetcher(tReader, gsReader);
    GenerationEndTimestampFetcher endTimestampFetcher = new GenerationEndTimestampFetcher(tReader);
    ClusterObserver observer = new ClusterObserver(session.getCluster());
    master = new Master(fetcher, new Worker(c, Reader.createStreamsReader(session, keyspace, table),
        endTimestampFetcher, observer), observer,
        new PartitioningHelper(session.getCluster()));
  }

  public CompletableFuture<Void> fetchChanges() {
    return master.fetchChanges();
  }

  public void finish() {
    master.finish();
  }

}
