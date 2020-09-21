package com.scylladb.cdc.master;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Date;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import com.scylladb.cdc.Generation;
import com.scylladb.cdc.GenerationMetadata;
import com.scylladb.cdc.common.FutureUtils;
import com.scylladb.cdc.driver.Reader;

public class GenerationsFetcher {

  private final Reader<Date> timestampsReader;
  private final Reader<Set<ByteBuffer>> streamsReader;

  public GenerationsFetcher(Reader<Date> tReader, Reader<Set<ByteBuffer>> sReader) {
    timestampsReader = tReader;
    streamsReader = sReader;
  }

  private class Consumer implements Reader.Consumer<Date, Optional<GenerationMetadata>> {
    private final Date fetchTime;
    private final Date lowerBound;
    private Date nextGenStartTimestamp = null;
    private Date nextGenEndTimestamp = null;

    public Consumer(Date ft, Date lb) {
      fetchTime = ft;
      lowerBound = lb;
    }

    @Override
    public void consume(Date ts) {
      if (!ts.after(lowerBound)) {
        return;
      }
      if (nextGenStartTimestamp == null || ts.before(nextGenStartTimestamp)) {
        nextGenEndTimestamp = nextGenStartTimestamp;
        nextGenStartTimestamp = ts;
      } else if (nextGenEndTimestamp == null || ts.before(nextGenEndTimestamp)) {
        nextGenEndTimestamp = ts;
      }
    }

    @Override
    public Optional<GenerationMetadata> finish() {
      return nextGenStartTimestamp == null ? Optional.empty() :
        Optional.of(new GenerationMetadata(fetchTime, nextGenStartTimestamp, Optional.ofNullable(nextGenEndTimestamp)));
    }

  }

  private class StreamsConsumer implements Reader.Consumer<Set<ByteBuffer>, Set<ByteBuffer>> {
    private Set<ByteBuffer> buffers = null;

    @Override
    public void consume(Set<ByteBuffer> item) {
      assert(buffers == null);
      buffers = item;
    }

    @Override
    public Set<ByteBuffer> finish() {
      return buffers;
    }

  }

  public CompletableFuture<Generation> fetchNext(Date lowerBound, AtomicBoolean finished) {
    Date fetchTime = new Date();
    return timestampsReader.query(new Consumer(fetchTime, lowerBound)).thenCompose(metadata -> {
      if (!metadata.isPresent()) {
        if (finished.get()) {
          return FutureUtils.completed(null);
        } else {
          return fetchNext(lowerBound, finished);
        }
      }
      if (metadata.get().startTimestamp.after(Date.from(Instant.now().minusSeconds(30)))) {
        return fetchNext(lowerBound, finished);
      }
      return streamsReader.query(new StreamsConsumer(), metadata.get().startTimestamp)
          .thenApply(streams -> new Generation(metadata.get(), streams));
    });
  }

}
