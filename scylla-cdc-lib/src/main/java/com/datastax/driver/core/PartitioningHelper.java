package com.datastax.driver.core;

import java.nio.ByteBuffer;

public class PartitioningHelper {

  private final Cluster cluster;

  public PartitioningHelper(Cluster c) {
    cluster = c;
  }

  public Token getToken(ByteBuffer partitionKey) {
    return cluster.getMetadata().tokenFactory().hash(partitionKey);
  }

}
