package com.scylladb.cdc;
import java.nio.ByteBuffer;
import java.util.Set;

public class Generation {

  public final GenerationMetadata metadata;
  public final Set<ByteBuffer> streamIds;

  public Generation(GenerationMetadata m, Set<ByteBuffer> s) {
    metadata = m;
    streamIds = s;
  }

}
