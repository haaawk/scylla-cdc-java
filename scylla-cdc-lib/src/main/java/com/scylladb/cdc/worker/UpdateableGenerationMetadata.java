package com.scylladb.cdc.worker;

import java.time.Instant;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.scylladb.cdc.GenerationMetadata;
import com.scylladb.cdc.common.FutureUtils;

public class UpdateableGenerationMetadata {
  private final GenerationEndTimestampFetcher generationEndTimestampFetcher;
  private final GenerationMetadata metadata;

  private class FetchEndTimestampResult {
    public FetchEndTimestampResult(Optional<Date> endTimestamp, Date fetchTime) {
      this.endTimestamp = endTimestamp;
      this.fetchTime = fetchTime;
    }

    public Optional<Date> endTimestamp;
    public Date fetchTime;
  }

  private volatile CompletableFuture<FetchEndTimestampResult> refreshFuture;

  private CompletableFuture<FetchEndTimestampResult> refresh(Date prevFetchTime) {
    Date newFetchTime = new Date();
    return generationEndTimestampFetcher.fetch(metadata.startTimestamp).handle((t, e) -> {
      if (e == null) {
        return new FetchEndTimestampResult(t, newFetchTime);
      }

      System.err.println("Exception while fetching generation end timestamp: " + e.getMessage());
      e.printStackTrace(System.err);
      return new FetchEndTimestampResult(metadata.endTimestamp, prevFetchTime);
    });
  }

  public UpdateableGenerationMetadata(GenerationMetadata m, GenerationEndTimestampFetcher f) {
    generationEndTimestampFetcher = f;
    metadata = m;
    refreshFuture = FutureUtils.completed(new FetchEndTimestampResult(m.endTimestamp, m.fetchTime));
  }

  public CompletableFuture<Optional<Date>> getEndTimestamp(Date lastTopologyChangeTime, Date lastNonEmptySelectTime) {
    return refreshFuture.thenCompose(refreshResult -> {
      Date nowMinus10s = Date.from(Instant.now().minusSeconds(10)); // FIXME: magic constant
      if (refreshResult.endTimestamp.isPresent()
          || !(refreshResult.fetchTime.before(lastTopologyChangeTime) || lastNonEmptySelectTime.before(nowMinus10s))) {
        return FutureUtils.completed(refreshResult.endTimestamp);
      }

      refreshFuture = refresh(refreshResult.fetchTime);
      return refreshFuture.thenCompose(r -> {
          return FutureUtils.completed(r.endTimestamp);
      });
    });
  }

  public Date getStartTimestamp() {
    return metadata.startTimestamp;
  }
}
