package com.scylladb.cdc;

import static com.datastax.driver.core.Metadata.quoteIfNecessary;

import java.nio.ByteBuffer;
import java.util.UUID;

import com.datastax.driver.core.Row;
import com.google.common.io.BaseEncoding;

public class Change {

  public final ByteBuffer streamId;
  public final Row row;

  public Change(Row row) {
    this.streamId = row.getBytes(quoteIfNecessary("cdc$stream_id"));
    this.row = row;
  }

  public byte getOperation() {
    return row.getByte(quoteIfNecessary("cdc$operation"));
  }

  public UUID getTime() {
    return row.getUUID(quoteIfNecessary("cdc$time"));
  }

  public Integer getTTL() {
    return row.isNull(quoteIfNecessary("cdc$ttl")) ? null : (int)row.getLong(quoteIfNecessary("cdc$ttl"));
  }

  public boolean isDeleted(String name) {
    String deletionColumnName = "cdc$deleted_" + name;
    return !row.isNull(deletionColumnName) && row.getBool(deletionColumnName);
  }

  @Override
  public String toString() {
    return BaseEncoding.base16().encode(streamId.array(), streamId.arrayOffset(), streamId.limit()) + " -> " + row;
  }

}
