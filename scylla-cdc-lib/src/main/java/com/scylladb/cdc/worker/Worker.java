package com.scylladb.cdc.worker;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.datastax.driver.core.utils.UUIDs;
import com.google.common.flogger.FluentLogger;
import com.scylladb.cdc.Change;
import com.scylladb.cdc.ChangeConsumer;
import com.scylladb.cdc.GenerationMetadata;
import com.scylladb.cdc.driver.ClusterObserver;
import com.scylladb.cdc.driver.Reader;

public class Worker {

  private final Executor delayingExecutor = new DelayingExecutor();

  private static final FluentLogger logger = FluentLogger.forEnclosingClass();
  private final static int LATE_WRITES_WINDOW_SECONDS = 10;
  private final ChangeConsumer consumer;
  private final Reader<Change> streamsReader;
  private final GenerationEndTimestampFetcher generationEndTimestampFetcher;
  private final ClusterObserver observer;
  private final AtomicReference<Date> lastTopologyChangeTime = new AtomicReference<>(new Date(0));
  private final AtomicReference<Date> lastNonEmptySelectTime = new AtomicReference<>(new Date(0));

  public Worker(ChangeConsumer c, Reader<Change> sr, GenerationEndTimestampFetcher gr, ClusterObserver o) {
    consumer = c;
    streamsReader = sr;
    generationEndTimestampFetcher = gr;
    observer = o;
    observer.registerOnTopologyChangedListener(() -> {
      Date changeTime = new Date();
      logger.atInfo().log("Updating last topology change time to %s", changeTime);
      Date previousTime;
      do {
        previousTime = lastTopologyChangeTime.get();
      } while (changeTime.after(previousTime) && !lastTopologyChangeTime.compareAndSet(previousTime, changeTime));
    });
  }

  private class Consumer implements Reader.DeferringConsumer<Change> {
    private boolean empty = true;

    @Override
    public CompletableFuture<Void> consume(Change item) {
      empty = false;
      return consumer.consume(item);
    }

    @Override
    public void finish() {
      if (!empty) {
        // There's a race condition here but it's ok - we don't have to store the last time.
        // We won't be off by more than few ms.
        lastNonEmptySelectTime.set(new Date());
      }
    }

  }

  private CompletableFuture<Void> fetchChangesForTask(UpdateableGenerationMetadata g, Set<ByteBuffer> task,
      UUID start) {
    return g.getEndTimestamp(lastTopologyChangeTime.get(), lastNonEmptySelectTime.get()).thenCompose(endTimestamp -> {
      Date now = Date.from(Instant.now().minusSeconds(LATE_WRITES_WINDOW_SECONDS));
      boolean finished = endTimestamp.isPresent() && !now.before(endTimestamp.get());
      UUID end = UUIDs.endOf((finished ? endTimestamp.get() : now).getTime());
      logger.atInfo().atMostEvery(10, TimeUnit.SECONDS).log("Fetching changes from window [%s, %s]", start, end);
      CompletableFuture<Void> fut = streamsReader.query(new Consumer(), new ArrayList<>(task), start, end);
      return finished ? fut : fut.thenComposeAsync(v -> fetchChangesForTask(g, task, end), delayingExecutor);
    });
  }

  public CompletableFuture<Void> fetchChanges(GenerationMetadata g, Queue<Set<ByteBuffer>> tasks) {
    UpdateableGenerationMetadata m = new UpdateableGenerationMetadata(g, generationEndTimestampFetcher);
    return CompletableFuture.allOf(
        tasks.stream().map(t -> fetchChangesForTask(m, t, UUIDs.startOf(0))).toArray(n -> new CompletableFuture[n]));
  }

}
