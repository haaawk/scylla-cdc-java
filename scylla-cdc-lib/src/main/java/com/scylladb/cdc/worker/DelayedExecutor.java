package com.scylladb.cdc.worker;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

final class DelayingExecutor implements Executor {
  private final long delaySeconds;
  private final ScheduledExecutorService internalExecutor =
      Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
          Thread t = new Thread(r);
          t.setName("DelayingExecutorThread");
          t.setDaemon(true);
          return t;
        }
      });

  public DelayingExecutor(long delaySecods) {
    this.delaySeconds = delaySecods;
  }

  public void execute(Runnable r) {
    internalExecutor.schedule(() -> ForkJoinPool.commonPool().execute(r), delaySeconds, TimeUnit.SECONDS);
  }

}
