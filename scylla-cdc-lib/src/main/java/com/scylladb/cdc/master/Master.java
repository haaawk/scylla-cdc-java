package com.scylladb.cdc.master;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.CompletableFuture;

import com.datastax.driver.core.PartitioningHelper;
import com.datastax.driver.core.Token;
import com.google.common.flogger.FluentLogger;
import com.scylladb.cdc.Generation;
import com.scylladb.cdc.driver.ClusterObserver;
import com.scylladb.cdc.worker.Worker;

public class Master {

  private static final FluentLogger logger = FluentLogger.forEnclosingClass();
  private final GenerationsFetcher generationsFetcher;
  private final Worker worker;
  private final ClusterObserver observer;
  private final PartitioningHelper partitioning;

  public Master(GenerationsFetcher f, Worker w, ClusterObserver o, PartitioningHelper p) {
    generationsFetcher = f;
    worker = w;
    observer = o;
    partitioning = p;
  }

  private static final class DecoratedKey implements Comparable<DecoratedKey> {
    public final ByteBuffer key;
    public final Token token;

    public DecoratedKey(ByteBuffer k, Token t) {
      key = k;
      token = t;
    }

    @Override
    public int compareTo(DecoratedKey o) {
      return token.compareTo(o.token);
    }
  }

  private Queue<Set<ByteBuffer>> splitStreams(Set<ByteBuffer> streamIds, SortedSet<Token> tokens) {
    Queue<Set<ByteBuffer>> tasks = new ArrayDeque<>();

    List<DecoratedKey> decorated = new ArrayList<>(streamIds.size());
    for (ByteBuffer b : streamIds) {
      decorated.add(new DecoratedKey(b, partitioning.getToken(b)));
    }

    Collections.sort(decorated);

    Set<ByteBuffer> wraparoundVnode = new HashSet<ByteBuffer>();

    Iterator<DecoratedKey> streamsIt = decorated.iterator();
    DecoratedKey s = streamsIt.next();

    Iterator<Token> tokensIt = tokens.iterator();
    Token t = tokensIt.next();

    while (s != null && s.token.compareTo(t) <= 0) {
      wraparoundVnode.add(s.key);
      s = streamsIt.hasNext() ? streamsIt.next() : null;
    }

    while (s != null && tokensIt.hasNext()) {
      Set<ByteBuffer> vnode = new HashSet<>();
      t = tokensIt.next();
      while (s != null && s.token.compareTo(t) <= 0) {
        vnode.add(s.key);
        s = streamsIt.hasNext() ? streamsIt.next() : null;
      }
      tasks.add(vnode);
    }

    if (s != null) {
      wraparoundVnode.add(s.key);
      while (streamsIt.hasNext()) {
        wraparoundVnode.add(streamsIt.next().key);
      }
    }
    tasks.add(wraparoundVnode);

    return tasks;
  }

  private CompletableFuture<Date> sendTasks(Generation g) {
    Queue<Set<ByteBuffer>> tasks = splitStreams(g.streamIds, observer.getTokens());
    logger.atInfo().log("Sending tasks for generation (%s, %s) to workers - %d streams in %d groups",
        g.metadata.startTimestamp, g.metadata.endTimestamp, g.streamIds.size(), tasks.size());
    logger.atFinest().log("Streams for generation (%s, %s) are %s",
        g.metadata.startTimestamp, g.metadata.endTimestamp, g.streamIds);
    return worker.fetchChanges(g.metadata, tasks).thenApply(v -> g.metadata.startTimestamp);
  }

  private CompletableFuture<Void> fetchChangesFromNextGeneration(Date previousGenerationTimestamp) {
    return generationsFetcher.fetchNext(previousGenerationTimestamp).thenCompose(this::sendTasks)
        .thenCompose(this::fetchChangesFromNextGeneration);
  }

  public CompletableFuture<Void> fetchChanges() {
    return fetchChangesFromNextGeneration(new Date(0));
  }
}
