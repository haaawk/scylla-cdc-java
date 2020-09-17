package com.scylladb.cdc;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;

import com.google.common.io.BaseEncoding;

public class Task {
  private final SortedSet<ByteBuffer> streamIds;

  public Task(SortedSet<ByteBuffer> ids) {
    streamIds = ids;
  }

  public static String idToString(ByteBuffer b) {
    byte[] bytes = new byte[16];
    b.duplicate().get(bytes, 0, 16);
    return BaseEncoding.base16().encode(bytes, 0, 16);
  }

  public List<ByteBuffer> getStreamIds() {
    return new ArrayList<>(streamIds);
  }

  @Override
  public String toString() {
    if (streamIds.isEmpty()) {
      return "empty task";
    }
    return idToString(streamIds.iterator().next());
  }
}
