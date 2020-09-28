package com.scylladb.cdc.common;

import java.nio.ByteBuffer;

import com.google.common.io.BaseEncoding;

public final class StreamId implements Comparable<StreamId> {
  public final ByteBuffer bytes;

  public StreamId(ByteBuffer b) {
    bytes = b;
  }

  @Override
  public int hashCode() {
    return bytes.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof StreamId && ((StreamId) o).bytes.equals(bytes);
  }

  @Override
  public String toString() {
    byte[] buf = new byte[16];
    bytes.duplicate().get(buf, 0, 16);
    return BaseEncoding.base16().encode(buf, 0, 16);
  }

  @Override
  public int compareTo(StreamId o) {
    return bytes.compareTo(o.bytes);
  }
}
