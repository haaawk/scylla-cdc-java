package com.scylladb.cdc.worker.fetchingwindow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.Date;
import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.datastax.driver.core.utils.UUIDs;
import com.scylladb.cdc.common.TimeUtils;

public class FetchingWindowFactory_CreateFetchingWindowTest {

  // 22 Sept 2015 - Scylla becomes open-source
  private static final Date startTimestamp = new Date(1442872800000L);
  private static final UUID start = UUIDs.startOf(startTimestamp.getTime());
  // Notice that now is more than LATE_WRITES_WINDOW_SECONDS away from start.
  private static final Instant now = new Date(startTimestamp.getTime()).toInstant()
      .plusSeconds(2 * FetchingWindowFactory.LATE_WRITES_WINDOW_SECONDS);

  private static Optional<Date> createEndByAddingSecondsToStart(long secondsToAdd) {
    return Optional.of(Date.from(startTimestamp.toInstant().plusSeconds(secondsToAdd)));
  }

  @Test
  public void testStartTimestampInsideLateWritesWindow() {
    Instant now = new Date(startTimestamp.getTime() + FetchingWindowFactory.LATE_WRITES_WINDOW_SECONDS - 1).toInstant();
    assertFalse(FetchingWindowFactory.createFetchingWindow(start, Optional.empty(), now).isPresent());
    assertFalse(FetchingWindowFactory.createFetchingWindow(start, createEndByAddingSecondsToStart(1), now).isPresent());
    assertFalse(FetchingWindowFactory.createFetchingWindow(start, Optional.of(Date.from(now)), now).isPresent());
  }

  @Test
  public void testEndTimestampMissing() {
    Optional<FetchingWindow> result = FetchingWindowFactory.createFetchingWindow(start, Optional.empty(), now);
    assertTrue(result.isPresent());
    FetchingWindow w = result.get();
    assertEquals(start, w.start());
    assertEquals(now.minusSeconds(FetchingWindowFactory.LATE_WRITES_WINDOW_SECONDS),
        TimeUtils.instantFromTimeUUID(w.end()));
    assertFalse(w.wasCropped());
    assertFalse(w.isLast());
  }

  @Test
  public void testEndTimestampBeforeLateWritesWindow() {
    Optional<Date> end = Optional.of(Date.from(now.minusSeconds(FetchingWindowFactory.LATE_WRITES_WINDOW_SECONDS)));
    Optional<FetchingWindow> result = FetchingWindowFactory.createFetchingWindow(start, end, now);
    assertTrue(result.isPresent());
    FetchingWindow w = result.get();
    assertEquals(start, w.start());
    assertEquals(end.get(), TimeUtils.dateFromTimeUUID(w.end()));
    assertFalse(w.wasCropped());
    assertTrue(w.isLast());
  }

  @Test
  public void testEndTimestampInsideLateWritesWindow() {
    Optional<Date> end = Optional.of(Date.from(now.minusSeconds(FetchingWindowFactory.LATE_WRITES_WINDOW_SECONDS - 1)));
    Optional<FetchingWindow> result = FetchingWindowFactory.createFetchingWindow(start, end, now);
    assertTrue(result.isPresent());
    FetchingWindow w = result.get();
    assertEquals(start, w.start());
    assertEquals(now.minusSeconds(FetchingWindowFactory.LATE_WRITES_WINDOW_SECONDS),
        TimeUtils.instantFromTimeUUID(w.end()));
    assertFalse(w.wasCropped());
    assertFalse(w.isLast());
  }

}
