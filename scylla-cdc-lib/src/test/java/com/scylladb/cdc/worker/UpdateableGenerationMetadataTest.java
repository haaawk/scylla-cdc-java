package com.scylladb.cdc.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Date;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.scylladb.cdc.GenerationMetadata;

@ExtendWith(MockitoExtension.class)
public class UpdateableGenerationMetadataTest {

  @Test
  public void testGetEndTimestampWhenAlreadyPresent(@Mock GenerationEndTimestampFetcher fetcher)
      throws InterruptedException, ExecutionException {
    Date fetchTime = new Date(1);
    Date startTs = new Date(100);
    Optional<Date> endTs = Optional.of(new Date(1000));

    GenerationMetadata meta = new GenerationMetadata(fetchTime, startTs, endTs);
    UpdateableGenerationMetadata tested = new UpdateableGenerationMetadata(meta, fetcher);

    {
      CompletableFuture<Optional<Date>> fut =
          tested.getEndTimestamp(Date.from(fetchTime.toInstant().minusNanos(1)), new Date());

      verifyNoMoreInteractions(fetcher);

      assertTrue(fut.isDone());
      assertEquals(endTs, fut.get());
    }

    {
      CompletableFuture<Optional<Date>> fut =
          tested.getEndTimestamp(Date.from(fetchTime.toInstant().plusNanos(1)), new Date());

      verifyNoMoreInteractions(fetcher);

      assertTrue(fut.isDone());
      assertEquals(endTs, fut.get());
    }
  }

  @Test
  public void testGetEndTimestampWhenMissingAndNoChangeSinceFetchTime(@Mock GenerationEndTimestampFetcher fetcher)
      throws InterruptedException, ExecutionException {
    Date fetchTime = new Date(1);
    Date startTs = new Date(100);
    Optional<Date> endTs = Optional.empty();

    GenerationMetadata meta = new GenerationMetadata(fetchTime, startTs, endTs);
    UpdateableGenerationMetadata tested = new UpdateableGenerationMetadata(meta, fetcher);

    CompletableFuture<Optional<Date>> fut = tested.getEndTimestamp(fetchTime, new Date());

    verifyNoMoreInteractions(fetcher);

    assertTrue(fut.isDone());
    assertEquals(endTs, fut.get());
  }

  @Test
  public void testGetEndTimestampConcurrentCalls(@Mock GenerationEndTimestampFetcher fetcher)
      throws InterruptedException, ExecutionException {
    Date fetchTime = new Date(1);
    Date newFetchTime = new Date(2);
    Date startTs = new Date(100);
    Optional<Date> endTs = Optional.empty();
    Optional<Date> newEndTs = Optional.of(new Date(1000));

    CompletableFuture<Optional<Date>> fetchFut = new CompletableFuture<>();
    when(fetcher.fetch(startTs)).thenReturn(fetchFut);

    GenerationMetadata meta = new GenerationMetadata(fetchTime, startTs, endTs);
    UpdateableGenerationMetadata tested = new UpdateableGenerationMetadata(meta, fetcher);

    CompletableFuture<Optional<Date>> fut1 = tested.getEndTimestamp(newFetchTime, new Date());

    verify(fetcher).fetch(startTs);
    verifyNoMoreInteractions(fetcher);

    assertFalse(fut1.isDone());

    CompletableFuture<Optional<Date>> fut2 = tested.getEndTimestamp(newFetchTime, new Date());

    verifyNoMoreInteractions(fetcher);

    assertFalse(fut2.isDone());
    assertEquals(fut1, fut2);

    fetchFut.complete(newEndTs);

    assertTrue(fut1.isDone());
    assertEquals(newEndTs, fut1.get());
  }

}
