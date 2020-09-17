package com.scylladb.cdc.master;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.datastax.driver.core.PartitioningHelper;
import com.datastax.driver.core.Token;
import com.google.common.flogger.FluentLogger;
import com.scylladb.cdc.Generation;
import com.scylladb.cdc.Task;
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

  private Queue<Task> splitStreams(Set<ByteBuffer> streamIds, SortedSet<Token> tokens) {
    Queue<Task> tasks = new ArrayDeque<>();

    List<DecoratedKey> decorated = new ArrayList<>(streamIds.size());
    for (ByteBuffer b : streamIds) {
      decorated.add(new DecoratedKey(b, partitioning.getToken(b)));
    }

    Collections.sort(decorated);

    SortedSet<ByteBuffer> wraparoundVnode = new TreeSet<ByteBuffer>();

    Iterator<DecoratedKey> streamsIt = decorated.iterator();
    DecoratedKey s = streamsIt.next();

    Iterator<Token> tokensIt = tokens.iterator();
    Token t = tokensIt.next();

    while (s != null && s.token.compareTo(t) <= 0) {
      wraparoundVnode.add(s.key);
      s = streamsIt.hasNext() ? streamsIt.next() : null;
    }

    while (s != null && tokensIt.hasNext()) {
      SortedSet<ByteBuffer> vnode = new TreeSet<>();
      t = tokensIt.next();
      while (s != null && s.token.compareTo(t) <= 0) {
        vnode.add(s.key);
        s = streamsIt.hasNext() ? streamsIt.next() : null;
      }
      if (!vnode.isEmpty()) {
        tasks.add(new Task(vnode));
      }
    }

    if (s != null) {
      wraparoundVnode.add(s.key);
      while (streamsIt.hasNext()) {
        wraparoundVnode.add(streamsIt.next().key);
      }
    }
    if (!wraparoundVnode.isEmpty()) {
      tasks.add(new Task(wraparoundVnode));
    }

    return tasks;
  }

  private CompletableFuture<Date> sendTasks(Generation g) {
    Queue<Task> tasks = splitStreams(g.streamIds, observer.getTokens());
    logger.atInfo().log("Sending tasks for generation (%s, %s) to workers - %d streams in %d groups",
        g.metadata.startTimestamp, g.metadata.endTimestamp, g.streamIds.size(), tasks.size());
    logger.atInfo().log("Streams for generation (%s, %s) are %s", g.metadata.startTimestamp, g.metadata.endTimestamp,
        g.streamIds);
    StringBuilder sb = new StringBuilder();
    for (Task t : tasks) {
      sb.append("\n").append(t).append(" -> ")
          .append(t.getStreamIds().stream().map(b -> Task.idToString(b)).collect(Collectors.toList()));
    }
    sb.append("\n");
    logger.atInfo().log("Generation starting at %s has following vnodes: %s", g.metadata.startTimestamp, sb.toString());
    return worker.fetchChanges(g.metadata, tasks).thenApply(v -> g.metadata.startTimestamp);
  }

  private CompletableFuture<Generation> fetchNextGenerationUntilSuccess(Date previousGenerationTimestamp) {
    return generationsFetcher.fetchNext(previousGenerationTimestamp).thenApply(CompletableFuture::completedFuture)
        .exceptionally(t -> fetchNextGenerationUntilSuccess(previousGenerationTimestamp))
        .thenCompose(Function.identity());
  }

  private CompletableFuture<Void> fetchChangesFromNextGeneration(Date previousGenerationTimestamp) {
    return fetchNextGenerationUntilSuccess(previousGenerationTimestamp).thenCompose(this::sendTasks)
        .thenCompose(this::fetchChangesFromNextGeneration);
  }

  public CompletableFuture<Void> fetchChanges() {
    return fetchChangesFromNextGeneration(new Date(0));
  }
}
