package com.scylladb.cdc.worker.fetchingwindow;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Optional;
import java.util.UUID;

import com.datastax.driver.core.utils.UUIDs;
import com.google.common.annotations.VisibleForTesting;
import com.scylladb.cdc.common.TimeUtils;

public final class FetchingWindowFactory {

  @VisibleForTesting
  final static int LATE_WRITES_WINDOW_SECONDS = 10;
  @VisibleForTesting
  final static int WINDOW_LENGTH_LIMIT_SECONDS = 30;

  public static Optional<FetchingWindow> computeFetchingWindow(UUID start, Optional<Date> endTimestamp) {
    return computeFetchingWindow(start, endTimestamp, Instant.now());
  }

  @VisibleForTesting
  static Optional<FetchingWindow> computeFetchingWindow(UUID start, Optional<Date> endTimestamp, Instant now) {
    Optional<FetchingWindow> w = createFetchingWindow(start, endTimestamp, now);
    return w.map(FetchingWindowFactory::cropToLimit);
  }

  @VisibleForTesting
  static Optional<FetchingWindow> createFetchingWindow(UUID start, Optional<Date> endTimestamp, Instant now) {
    Date nowAdjustedToLateWritesWindow = Date.from(now.minusSeconds(LATE_WRITES_WINDOW_SECONDS));
    if (nowAdjustedToLateWritesWindow.before(TimeUtils.dateFromTimeUUID(start))) {
      return Optional.empty();
    }
    if (endTimestamp.isPresent() && !nowAdjustedToLateWritesWindow.before(endTimestamp.get())) {
      return Optional.of(new FetchingWindow(start, UUIDs.endOf(endTimestamp.get().getTime()), false, true));
    } else {
      return Optional.of(new FetchingWindow(start, UUIDs.endOf(nowAdjustedToLateWritesWindow.getTime()), false, false));
    }
  }

  @VisibleForTesting
  static FetchingWindow cropToLimit(FetchingWindow w) {
    if (exceedsLimit(w) && TimeUtils.dateFromTimeUUID(w.start()).getTime() > 0) {
      UUID newEnd = UUIDs
          .endOf(TimeUtils.instantFromTimeUUID(w.start()).plusSeconds(WINDOW_LENGTH_LIMIT_SECONDS).toEpochMilli());
      return new FetchingWindow(w.start(), newEnd, true, false);
    } else {
      return w;
    }
  }

  @VisibleForTesting
  static boolean exceedsLimit(FetchingWindow w) {
    Duration length = Duration.between(TimeUtils.instantFromTimeUUID(w.start()),
        TimeUtils.instantFromTimeUUID(w.end()));
    return length.compareTo(Duration.ofSeconds(WINDOW_LENGTH_LIMIT_SECONDS)) > 0;
  }
}
