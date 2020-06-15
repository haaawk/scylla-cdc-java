package com.scylladb.cdc.worker;

import java.util.Date;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.scylladb.cdc.GenerationMetadata;
import com.scylladb.cdc.common.FutureUtils;

public class UpdateableGenerationMetadata {

  private final Object lock = new Object();
  private final GenerationEndTimestampFetcher generationEndTimestampFetcher;
  private GenerationMetadata metadata;
  private CompletableFuture<Optional<Date>> refreshFuture;

  public UpdateableGenerationMetadata(GenerationMetadata m, GenerationEndTimestampFetcher f) {
    generationEndTimestampFetcher = f;
    metadata = m;
  }

  public CompletableFuture<Optional<Date>> getEndTimestamp(Date lastTopologyChangeTime) {
    synchronized(lock) {
      if (refreshFuture != null) {
        return refreshFuture;
      }
      if (metadata.endTimestamp.isPresent() || !metadata.fetchTime.before(lastTopologyChangeTime)) {
        return FutureUtils.completed(metadata.endTimestamp);
      }
      Date time = new Date();
      refreshFuture = generationEndTimestampFetcher.fetch(metadata.startTimestamp).thenApply(endTimestamp -> {
        synchronized(lock) {
          metadata = new GenerationMetadata(time, metadata.startTimestamp, endTimestamp);
          refreshFuture = null;
        }
        return endTimestamp;
      });
      return refreshFuture;
    }
  }

}
