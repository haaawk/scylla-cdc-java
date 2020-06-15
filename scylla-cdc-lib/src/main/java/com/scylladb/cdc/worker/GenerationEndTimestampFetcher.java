package com.scylladb.cdc.worker;

import java.util.Date;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.scylladb.cdc.driver.Reader;

public class GenerationEndTimestampFetcher {

  private final Reader<Date> timestampsReader;

  public GenerationEndTimestampFetcher(Reader<Date> tReader) {
    timestampsReader = tReader;
  }

  private static class Consumer implements Reader.Consumer<Date, Optional<Date>> {
    private final Date lowerBound;
    private Date min = null;

    public Consumer(Date lb) {
      lowerBound = lb;
    }

    @Override
    public void consume(Date item) {
      if (item.after(lowerBound) && (min == null || item.before(min))) {
        min = item;
      }
    }

    @Override
    public Optional<Date> finish() {
      return Optional.ofNullable(min);
    }

  }

  public CompletableFuture<Optional<Date>> fetch(Date startTimestamp) {
    return timestampsReader.query(new Consumer(startTimestamp));
  }

}
