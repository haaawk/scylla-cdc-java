package com.scylladb.cdc.worker;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.datastax.driver.core.utils.UUIDs;
import com.google.common.flogger.FluentLogger;
import com.scylladb.cdc.Change;
import com.scylladb.cdc.ChangeConsumer;
import com.scylladb.cdc.GenerationMetadata;
import com.scylladb.cdc.Task;
import com.scylladb.cdc.common.FutureUtils;
import com.scylladb.cdc.driver.ClusterObserver;
import com.scylladb.cdc.driver.Reader;

public class Worker {

  private final Executor delayingExecutor1s = new DelayingExecutor(1);
  private final Executor delayingExecutor10s = new DelayingExecutor(10);
  private final Executor delayingExecutor30s = new DelayingExecutor(30);

  private static final FluentLogger logger = FluentLogger.forEnclosingClass();
  private final static int LATE_WRITES_WINDOW_SECONDS = 10;
  private final ChangeConsumer consumer;
  private final Reader<Change> streamsReader;
  private final GenerationEndTimestampFetcher generationEndTimestampFetcher;
  private final ClusterObserver observer;
  private final AtomicReference<Date> lastTopologyChangeTime = new AtomicReference<>(new Date(0));
  private final AtomicReference<Date> lastNonEmptySelectTime = new AtomicReference<>(new Date(0));
  private boolean finished = false;

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
    public boolean empty = true;
    public AtomicInteger count = new AtomicInteger();

    @Override
    public CompletableFuture<Void> consume(Change item) {
      empty = false;
      count.incrementAndGet();
      return consumer.consume(item);
    }

    @Override
    public void finish() {
      if (!empty) {
        // There's a race condition here but it's ok - we don't have to store the last
        // time.
        // We won't be off by more than few ms.
        lastNonEmptySelectTime.set(new Date());
      }
    }

  }

  private CompletableFuture<Void> fetchChangesForTask(UpdateableGenerationMetadata g, Task task, UUID start,
      int retryCount, int generationResultsCount) {
    return g.getEndTimestamp(lastTopologyChangeTime.get(), lastNonEmptySelectTime.get()).thenCompose(endTimestamp -> {
      Date now = Date.from(Instant.now().minusSeconds(LATE_WRITES_WINDOW_SECONDS));
      boolean finished = endTimestamp.isPresent() && !now.before(endTimestamp.get());
      Date startTs = new Date(UUIDs.unixTimestamp(start));
      Date endTs = finished ? endTimestamp.get() : now;
      Duration window = Duration.between(startTs.toInstant(), endTs.toInstant());
      boolean cropped;
      UUID end;
      if (window.compareTo(Duration.ofSeconds(30)) > 0) {
        cropped = true;
        end = UUIDs.endOf(startTs.toInstant().plusSeconds(30).toEpochMilli());
      } else {
        cropped = false;
        end = UUIDs.endOf(endTs.getTime());
      }
      logger.atInfo().atMostEvery(10, TimeUnit.SECONDS).log("Fetching changes in vnode %s and window [%s(%d), %s(%d)] in generation %s",
          task, start, start.timestamp(), end, end.timestamp(), g.getStartTimestamp());
      Consumer c = new Consumer();
      CompletableFuture<UUID> fut = streamsReader.query(c, task.getStreamIds(), start, end)
          .handle((ignored, e) -> {
            if (e != null) {
              logger.atWarning().withCause(e).log(
                  "Exception while fetching changes in vnode %s and window [%s(%d), %s(%d)] in generation %s. Replicator will retry which can cause more than once delivery. This will be %d retry",
                  task, start, start.timestamp(), end, end.timestamp(), g.getStartTimestamp(), retryCount + 1);
              return start;
            } else {
              return end;
            }
          });
      return fut.thenComposeAsync(nextStart -> {
        if (nextStart == end) {
          logger.atInfo().log("Fetching changes in vnode %s and window [%s(%d), %s(%d)] in generation %s finished successfully with %d changes after %d retries",
              task, start, start.timestamp(), end, end.timestamp(), g.getStartTimestamp(), c.count.get(), retryCount);
          if (finished || (Worker.this.finished && c.empty)) {
            logger.atInfo().log("All changes has been fetched in vnode %s in generation %s. Total %d changes", task, g.getStartTimestamp(), generationResultsCount + c.count.get());
            return FutureUtils.completed(null);
          }
          return fetchChangesForTask(g, task, nextStart, 0, generationResultsCount + c.count.get());
        } else {
          return fetchChangesForTask(g, task, nextStart, retryCount + 1, generationResultsCount);
        }
      }, cropped ? delayingExecutor1s : (c.empty ? delayingExecutor30s : delayingExecutor10s));
    });
  }

  public CompletableFuture<Void> fetchChanges(GenerationMetadata g, Queue<Task> tasks) {
    UpdateableGenerationMetadata m = new UpdateableGenerationMetadata(g, generationEndTimestampFetcher);
    return CompletableFuture.allOf(
        tasks.stream().map(t -> fetchChangesForTask(m, t, UUIDs.startOf(0), 0, 0)).toArray(n -> new CompletableFuture[n]));
  }

  public void finish() {
    finished = true;
  }

}
