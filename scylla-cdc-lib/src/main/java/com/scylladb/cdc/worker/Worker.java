package com.scylladb.cdc.worker;

import java.util.Date;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import com.datastax.driver.core.utils.UUIDs;
import com.google.common.flogger.FluentLogger;
import com.scylladb.cdc.Change;
import com.scylladb.cdc.ChangeConsumer;
import com.scylladb.cdc.GenerationMetadata;
import com.scylladb.cdc.Task;
import com.scylladb.cdc.common.FutureUtils;
import com.scylladb.cdc.driver.ClusterObserver;
import com.scylladb.cdc.driver.Reader;
import com.scylladb.cdc.worker.fetchingwindow.FetchingWindow;
import com.scylladb.cdc.worker.fetchingwindow.FetchingWindowFactory;

public class Worker {

  private final Executor delayingExecutor1s = new DelayingExecutor(1);
  private final Executor delayingExecutor10s = new DelayingExecutor(10);
  private final Executor delayingExecutor30s = new DelayingExecutor(30);

  private static final FluentLogger logger = FluentLogger.forEnclosingClass();
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

  private class Consumer implements Reader.DeferringConsumer<Change, Integer> {
    private final AtomicInteger count = new AtomicInteger();

    @Override
    public CompletableFuture<Void> consume(Change item) {
      count.incrementAndGet();
      return consumer.consume(item);
    }

    @Override
    public Integer finish() {
      if (count.get() > 0) {
        // There's a race condition here but it's ok - we don't have to store the last
        // time.
        // We won't be off by more than few ms.
        lastNonEmptySelectTime.set(new Date());
      }
      return count.get();
    }

  }

  private static class Result {
    public final int count;
    public final UUID nextStart;
    public final boolean error;

    public Result(int c, UUID s, boolean e) {
      count = c;
      nextStart = s;
      error = e;
    }
  }

  private CompletableFuture<Void> fetchChangesForTask(UpdateableGenerationMetadata g, Task task, UUID start,
      int retryCount, int generationResultsCount) {
    return g.getEndTimestamp(lastTopologyChangeTime.get(), lastNonEmptySelectTime.get()).thenCompose(endTimestamp -> {
      Optional<FetchingWindow> windowOpt = FetchingWindowFactory.computeFetchingWindow(start, endTimestamp);
      if (!windowOpt.isPresent()) {
        // start is inside late writes window so we need to wait a bit and retry
        return FutureUtils.completed(null).thenComposeAsync(
            ignored -> fetchChangesForTask(g, task, start, retryCount, generationResultsCount), delayingExecutor30s);
      }
      FetchingWindow window = windowOpt.get();
      logger.atInfo().atMostEvery(10, TimeUnit.SECONDS)
          .log("Fetching changes in vnode %s and window %s in generation %s", task, window, g.getStartTimestamp());
      Consumer c = new Consumer();
      CompletableFuture<Result> fut = streamsReader.query(c,
          task.getStreamIds().stream().map(id -> id.bytes).collect(Collectors.toList()), window.start(), window.end())
          .handle((count, e) -> {
            if (e != null) {
              logger.atWarning().withCause(e).log(
                  "Exception while fetching changes in vnode %s and window %s in generation %s. Replicator will retry which can cause more than once delivery. This will be %d retry",
                  task, window, g.getStartTimestamp(), retryCount + 1);
              return new Result(count, start, true);
            } else {
              return new Result(count, window.end(), false);
            }
          });
      return fut.thenCompose(result -> {
        return FutureUtils.completed(null).thenComposeAsync(ignored -> {
          if (!result.error) {
            logger.atInfo().log(
                "Fetching changes in vnode %s and window %s in generation %s finished successfully with %d changes after %d retries",
                task, window, g.getStartTimestamp(), result.count, retryCount);
            if (window.isLast() || (Worker.this.finished && result.count == 0)) {
              logger.atInfo().log("All changes has been fetched in vnode %s in generation %s. Total %d changes", task,
                  g.getStartTimestamp(), generationResultsCount + result.count);
              return FutureUtils.completed(null);
            }
            return fetchChangesForTask(g, task, result.nextStart, 0, generationResultsCount + result.count);
          } else {
            return fetchChangesForTask(g, task, result.nextStart, retryCount + 1, generationResultsCount);
          }
        }, window.wasCropped() ? delayingExecutor1s : (result.count == 0 ? delayingExecutor30s : delayingExecutor10s));
      });
    });
  }

  public CompletableFuture<Void> fetchChanges(GenerationMetadata g, Queue<Task> tasks) {
    UpdateableGenerationMetadata m = new UpdateableGenerationMetadata(g, generationEndTimestampFetcher);
    return CompletableFuture.allOf(tasks.stream().map(t -> fetchChangesForTask(m, t, UUIDs.startOf(0), 0, 0))
        .toArray(n -> new CompletableFuture[n]));
  }

  public void finish() {
    finished = true;
  }

}
