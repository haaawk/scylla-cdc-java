package com.scylladb.cdc;

import java.util.concurrent.CompletableFuture;

public interface ChangeConsumer {

  CompletableFuture<Void> consume(Change change);

}
