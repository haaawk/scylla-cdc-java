package com.scylladb.cdc.worker.fetchingwindow;

import java.util.UUID;

import com.scylladb.cdc.common.TimeUtils;

public final class FetchingWindow {

  private final UUID start;
  private final UUID end;
  private final boolean cropped;
  private final boolean last;

  FetchingWindow(UUID start, UUID end, boolean cropped, boolean last) {
    this.cropped = cropped;
    this.start = start;
    this.end = end;
    this.last = last;
  }

  public UUID start() {
    return start;
  }

  public UUID end() {
    return end;
  }

  public boolean wasCropped() {
    return cropped;
  }

  public boolean isLast() {
    return last;
  }

  @Override
  public String toString() {
    return "[" + start + "(" + TimeUtils.instantFromTimeUUID(start) + "), " + end + "("
        + TimeUtils.instantFromTimeUUID(end) + ")]" + (cropped ? "(cropped)" : "");
  }
}
