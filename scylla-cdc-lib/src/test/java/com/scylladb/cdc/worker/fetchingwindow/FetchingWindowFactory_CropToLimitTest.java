package com.scylladb.cdc.worker.fetchingwindow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Date;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.datastax.driver.core.utils.UUIDs;
import com.scylladb.cdc.common.TimeUtils;

public class FetchingWindowFactory_CropToLimitTest {

  // 22 Sept 2015 - Scylla becomes open-source
  private static final Date startTimestamp = new Date(1442872800000L);
  private static final UUID start = UUIDs.startOf(startTimestamp.getTime());

  private static UUID createEndByAddingSecondsToStart(long secondsToAdd) {
    return UUIDs.endOf(startTimestamp.toInstant().plusSeconds(secondsToAdd).toEpochMilli());
  }

  private static void checkCropped(FetchingWindow tested) {
    assertFalse(tested.wasCropped());
    FetchingWindow result = FetchingWindowFactory.cropToLimit(tested);
    assertNotSame(tested, result);
    assertTrue(result.wasCropped());
    assertEquals(tested.start(), result.start());
    assertTrue(result.end().compareTo(tested.end()) < 0);
    // Check the size of cropped window
    assertEquals(
        TimeUtils.instantFromTimeUUID(result.start()).plusSeconds(FetchingWindowFactory.WINDOW_LENGTH_LIMIT_SECONDS),
        TimeUtils.instantFromTimeUUID(result.end()));
    assertFalse(result.isLast());
  }

  @Test
  public void testShouldCrop() {
    UUID end = createEndByAddingSecondsToStart(FetchingWindowFactory.WINDOW_LENGTH_LIMIT_SECONDS + 1);
    checkCropped(new FetchingWindow(start, end, false, false));
    checkCropped(new FetchingWindow(start, end, false, true));
  }

  private static void checkNotCropped(FetchingWindow tested) {
    assertFalse(tested.wasCropped());
    FetchingWindow result = FetchingWindowFactory.cropToLimit(tested);
    assertSame(tested, result);
    assertFalse(result.wasCropped());
  }

  @Test
  public void testShouldNotCrop() {
    UUID end = createEndByAddingSecondsToStart(FetchingWindowFactory.WINDOW_LENGTH_LIMIT_SECONDS);
    checkNotCropped(new FetchingWindow(start, end, false, false));
    checkNotCropped(new FetchingWindow(start, end, false, true));
  }

  @Test
  public void testShouldNotCropWhenStartingFromTheBeginning() {
    UUID end = UUIDs.endOf(new Date().getTime());
    checkNotCropped(new FetchingWindow(UUIDs.startOf(0), end, false, false));
    checkNotCropped(new FetchingWindow(UUIDs.startOf(0), end, false, true));
  }

}
