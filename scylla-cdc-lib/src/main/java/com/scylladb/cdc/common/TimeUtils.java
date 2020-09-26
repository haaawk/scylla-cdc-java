package com.scylladb.cdc.common;

import java.time.Instant;
import java.util.Date;
import java.util.UUID;

import com.datastax.driver.core.utils.UUIDs;

public final class TimeUtils {

  private TimeUtils() {
    throw new UnsupportedOperationException(TimeUtils.class.getName() + " should never be instantiated");
  }

  public static Instant instantFromTimeUUID(UUID uuid) {
    return dateFromTimeUUID(uuid).toInstant();
  }
  
  public static Date dateFromTimeUUID(UUID uuid) {
    return new Date(UUIDs.unixTimestamp(uuid));
  }

}
