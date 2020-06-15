package com.scylladb.cdc;

import java.util.Date;
import java.util.Optional;

public class GenerationMetadata {

  public final Date fetchTime;
  public final Date startTimestamp;
  public final Optional<Date> endTimestamp;

  public GenerationMetadata(Date fetchTime, Date startTimestamp, Optional<Date> endTimestamp) {
    this.fetchTime = fetchTime;
    this.startTimestamp = startTimestamp;
    this.endTimestamp = endTimestamp;
  }

}
