package com.scylladb.cdc.worker.fetchingwindow;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Date;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.datastax.driver.core.utils.UUIDs;

public class FetchingWindowFactory_ExceedsLimitTest {

  // 22 Sept 2015 - Scylla becomes open-source
  private static final Date startTimestamp = new Date(1442872800000L);
  private static final UUID start = UUIDs.startOf(startTimestamp.getTime());

  private static UUID createEndByAddingSecondsToStart(long secondsToAdd) {
    return UUIDs.endOf(startTimestamp.toInstant().plusSeconds(secondsToAdd).toEpochMilli());
  }

  @Test
  public void testLengthEqualsLimitPlusOne() {
    UUID end = createEndByAddingSecondsToStart(FetchingWindowFactory.WINDOW_LENGTH_LIMIT_SECONDS + 1);
    assertTrue(FetchingWindowFactory.exceedsLimit(new FetchingWindow(start, end, false, false)));
    assertTrue(FetchingWindowFactory.exceedsLimit(new FetchingWindow(start, end, true, false)));
    assertTrue(FetchingWindowFactory.exceedsLimit(new FetchingWindow(start, end, false, true)));
    assertTrue(FetchingWindowFactory.exceedsLimit(new FetchingWindow(start, end, true, true)));
  }

  @Test
  public void testLengthEqualsLimit() {
    UUID end = createEndByAddingSecondsToStart(FetchingWindowFactory.WINDOW_LENGTH_LIMIT_SECONDS);
    assertFalse(FetchingWindowFactory.exceedsLimit(new FetchingWindow(start, end, false, false)));
    assertFalse(FetchingWindowFactory.exceedsLimit(new FetchingWindow(start, end, true, false)));
    assertFalse(FetchingWindowFactory.exceedsLimit(new FetchingWindow(start, end, false, true)));
    assertFalse(FetchingWindowFactory.exceedsLimit(new FetchingWindow(start, end, true, true)));
  }

  @Test
  public void testLengthEqualsLimitMinusOne() {
    UUID end = createEndByAddingSecondsToStart(FetchingWindowFactory.WINDOW_LENGTH_LIMIT_SECONDS - 1);
    assertFalse(FetchingWindowFactory.exceedsLimit(new FetchingWindow(start, end, false, false)));
    assertFalse(FetchingWindowFactory.exceedsLimit(new FetchingWindow(start, end, true, false)));
    assertFalse(FetchingWindowFactory.exceedsLimit(new FetchingWindow(start, end, false, true)));
    assertFalse(FetchingWindowFactory.exceedsLimit(new FetchingWindow(start, end, true, true)));
  }

}
