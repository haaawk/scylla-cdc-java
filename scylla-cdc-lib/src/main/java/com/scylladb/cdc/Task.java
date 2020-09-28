package com.scylladb.cdc;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;

import com.scylladb.cdc.common.StreamId;

public class Task {
  private final SortedSet<StreamId> streamIds;

  public Task(SortedSet<StreamId> ids) {
    streamIds = ids;
  }

  public List<StreamId> getStreamIds() {
    return new ArrayList<>(streamIds);
  }

  @Override
  public String toString() {
    if (streamIds.isEmpty()) {
      return "empty task";
    }
    return streamIds.iterator().next().toString();
  }
}
