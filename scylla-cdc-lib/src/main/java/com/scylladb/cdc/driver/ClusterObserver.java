package com.scylladb.cdc.driver;

import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Host.StateListener;
import com.datastax.driver.core.Token;
import com.google.common.flogger.FluentLogger;

public class ClusterObserver {

  private static final FluentLogger logger = FluentLogger.forEnclosingClass();
  private final Cluster cluster;

  public ClusterObserver(Cluster c) {
    cluster = c;
  }

  public SortedSet<Token> getTokens() {
    return cluster.getMetadata().getTokenRanges().stream()
        .map(tr -> tr.getEnd()).collect(Collectors.toCollection(() -> new TreeSet<>()));
  }

  public void registerOnTopologyChangedListener(Runnable listener) {
    cluster.register(new StateListener() {

      @Override
      public void onUp(Host host) {
        logger.atFine().log("onUP %s - ignored", host);
      }

      @Override
      public void onUnregister(Cluster cluster) {
        logger.atFine().log("onUnregister - ignored");
      }

      @Override
      public void onRemove(Host host) {
        logger.atInfo().log("onRemove %s - CDC stream generations might have changed", host);
        listener.run();
      }

      @Override
      public void onRegister(Cluster cluster) {
        logger.atFine().log("onRegister - ignored");
      }

      @Override
      public void onDown(Host host) {
        logger.atFine().log("onDown %s - ignored", host);
      }

      @Override
      public void onAdd(Host host) {
        logger.atInfo().log("onAdd %s - CDC stream generations might have changed", host);
        listener.run();
      }
    });
  }

}
