package com.scylladb.cdc.worker;

import java.time.Instant;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.scylladb.cdc.GenerationMetadata;
import com.scylladb.cdc.common.FutureUtils;

public class UpdateableGenerationMetadata {

  private final Object lock = new Object();
  private final GenerationEndTimestampFetcher generationEndTimestampFetcher;
  private volatile GenerationMetadata metadata;
  private CompletableFuture<Optional<Date>> refreshFuture;
  private boolean running = false;

  public UpdateableGenerationMetadata(GenerationMetadata m, GenerationEndTimestampFetcher f) {
    generationEndTimestampFetcher = f;
    metadata = m;
  }

  public CompletableFuture<Optional<Date>> getEndTimestamp(Date lastTopologyChangeTime, Date lastNonEmptySelectTime) {
    synchronized (lock) {
      if (running) {
        return refreshFuture;
      }
      Date nowMinus10s = Date.from(Instant.now().minusSeconds(10));
      if (metadata.endTimestamp.isPresent()
          || !(metadata.fetchTime.before(lastTopologyChangeTime) || lastNonEmptySelectTime.before(nowMinus10s))) {
        return FutureUtils.completed(metadata.endTimestamp);
      }
      Date time = new Date();
      running = true;
      refreshFuture = generationEndTimestampFetcher.fetch(metadata.startTimestamp).handle((endTimestamp, e) -> {
        if (e != null) {
          System.err.println("Exception while fetching generation end timestamp: " + e.getMessage());
          e.printStackTrace(System.err);
          return metadata.endTimestamp;
        }
        synchronized (lock) {
          metadata = new GenerationMetadata(time, metadata.startTimestamp, endTimestamp);
          running = false;
        }
        return endTimestamp;
      });
      return refreshFuture;
    }
  }

  public Date getStartTimestamp() {
    return metadata.startTimestamp;
  }

}
