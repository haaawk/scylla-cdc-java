package com.scylladb.cdc;
import java.util.SortedSet;

import com.scylladb.cdc.common.StreamId;

public class Generation {

  public final GenerationMetadata metadata;
  public final SortedSet<StreamId> streamIds;

  public Generation(GenerationMetadata m, SortedSet<StreamId> s) {
    metadata = m;
    streamIds = s;
  }

}
