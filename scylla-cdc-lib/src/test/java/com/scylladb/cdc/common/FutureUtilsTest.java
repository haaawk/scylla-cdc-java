package com.scylladb.cdc.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.Test;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFutureTask;

public class FutureUtilsTest {

  @Test
  public void testCompleted() {
    CompletableFuture<String> tested = FutureUtils.completed("test");

    assertTrue(tested.isDone());
    assertFalse(tested.isCancelled());
    assertFalse(tested.isCompletedExceptionally());
    assertEquals("test", tested.getNow("value other than test"));
  }

  @Test
  public void testConvertImmediateFutureSuccessful() {
    CompletableFuture<Void> tested = FutureUtils.convert(Futures.immediateFuture("test"));

    assertTrue(tested.isDone());
    assertFalse(tested.isCancelled());
    assertFalse(tested.isCompletedExceptionally());
  }

  @Test
  public void testConvertImmediateFutureCancelled() throws InterruptedException, ExecutionException {
    CompletableFuture<Void> tested = FutureUtils.convert(Futures.immediateCancelledFuture());

    assertTrue(tested.isDone());
    assertTrue(tested.isCancelled());
    assertTrue(tested.isCompletedExceptionally());
    assertThrows(CancellationException.class, () -> tested.getNow(null));
  }

  @Test
  public void testConvertImmediateFutureFailed() {
    CompletableFuture<Void> tested = FutureUtils
        .convert(Futures.immediateFailedFuture(new IllegalStateException("test")));

    assertTrue(tested.isDone());
    assertFalse(tested.isCancelled());
    assertTrue(tested.isCompletedExceptionally());
    try {
      tested.getNow(null);
      fail();
    } catch (CompletionException expected) {
      assertEquals(IllegalStateException.class, expected.getCause().getClass());
      assertEquals("test", expected.getCause().getMessage());
    } catch (Exception e) {
      fail(e);
    }
  }

  @Test
  public void testConvertDeferredFutureSuccessful() {
    ListenableFutureTask<String> task = ListenableFutureTask.create(() -> {
    }, "test");
    CompletableFuture<Void> tested = FutureUtils.convert(task);

    task.run();

    assertTrue(tested.isDone());
    assertFalse(tested.isCancelled());
    assertFalse(tested.isCompletedExceptionally());
  }

  @Test
  public void testConvertDeferredFutureCancelled() {
    ListenableFutureTask<String> task = ListenableFutureTask.create(() -> {
    }, "test");
    CompletableFuture<Void> tested = FutureUtils.convert(task);

    task.cancel(false);

    assertTrue(tested.isDone());
    assertTrue(tested.isCancelled());
    assertTrue(tested.isCompletedExceptionally());
    assertThrows(CancellationException.class, () -> tested.getNow(null));
  }

  @Test
  public void testConvertDeferredFutureFailed() {
    ListenableFutureTask<String> task = ListenableFutureTask.create(() -> {
      throw new IllegalStateException("test exception");
    }, "test");
    CompletableFuture<Void> tested = FutureUtils.convert(task);

    task.run();

    assertTrue(tested.isDone());
    assertFalse(tested.isCancelled());
    assertTrue(tested.isCompletedExceptionally());
    try {
      tested.getNow(null);
      fail();
    } catch (CompletionException expected) {
      assertEquals(IllegalStateException.class, expected.getCause().getClass());
      assertEquals("test exception", expected.getCause().getMessage());
    } catch (Exception e) {
      fail(e);
    }
  }

  @Test
  public void testTransformImmediateFutureSuccessful() {
    CompletableFuture<Integer> tested = FutureUtils.transform(Futures.immediateFuture("test"), s -> s.length());

    assertTrue(tested.isDone());
    assertFalse(tested.isCancelled());
    assertFalse(tested.isCompletedExceptionally());
    assertEquals(4, tested.getNow(null));
  }

  @Test
  public void testTransformImmediateFutureCancelled() throws InterruptedException, ExecutionException {
    CompletableFuture<Integer> tested = FutureUtils.transform(Futures.<String>immediateCancelledFuture(),
        s -> fail());

    assertTrue(tested.isDone());
    assertTrue(tested.isCancelled());
    assertTrue(tested.isCompletedExceptionally());
    assertThrows(CancellationException.class, () -> tested.getNow(null));
  }

  @Test
  public void testTransformImmediateFutureFailed() {
    CompletableFuture<Integer> tested = FutureUtils
        .transform(Futures.<String>immediateFailedFuture(new IllegalStateException("test")), s -> fail());

    assertTrue(tested.isDone());
    assertFalse(tested.isCancelled());
    assertTrue(tested.isCompletedExceptionally());
    try {
      tested.getNow(null);
      fail();
    } catch (CompletionException expected) {
      assertEquals(IllegalStateException.class, expected.getCause().getClass());
      assertEquals("test", expected.getCause().getMessage());
    } catch (Exception e) {
      fail(e);
    }
  }

  @Test
  public void testTransformImmediateFutureFailedTransform() {
    CompletableFuture<Integer> tested = FutureUtils.transform(Futures.immediateFuture("test"), s -> {
      throw new IllegalStateException("test exception");
    });

    assertTrue(tested.isDone());
    assertFalse(tested.isCancelled());
    assertTrue(tested.isCompletedExceptionally());
    try {
      tested.getNow(null);
      fail();
    } catch (CompletionException expected) {
      assertEquals(IllegalStateException.class, expected.getCause().getClass());
      assertEquals("test exception", expected.getCause().getMessage());
    } catch (Exception e) {
      fail(e);
    }
  }

  @Test
  public void testTransformDeferredFutureSuccessful() {
    ListenableFutureTask<String> task = ListenableFutureTask.create(() -> {}, "test");
    CompletableFuture<Integer> tested = FutureUtils.transform(task, s -> s.length());

    task.run();

    assertTrue(tested.isDone());
    assertFalse(tested.isCancelled());
    assertFalse(tested.isCompletedExceptionally());
    assertEquals(4, tested.getNow(null));
  }

  @Test
  public void testTransformDeferredFutureCancelled() {
    ListenableFutureTask<String> task = ListenableFutureTask.create(() -> {
    }, "test");
    CompletableFuture<Integer> tested = FutureUtils.transform(task, s -> fail());

    task.cancel(false);

    assertTrue(tested.isDone());
    assertTrue(tested.isCancelled());
    assertTrue(tested.isCompletedExceptionally());
    assertThrows(CancellationException.class, () -> tested.getNow(null));
  }

  @Test
  public void testTransformDeferredFutureFailed() {
    ListenableFutureTask<String> task = ListenableFutureTask.create(() -> {
      throw new IllegalStateException("test exception");
    }, "test");
    CompletableFuture<Integer> tested = FutureUtils.transform(task, s -> fail());

    task.run();

    assertTrue(tested.isDone());
    assertFalse(tested.isCancelled());
    assertTrue(tested.isCompletedExceptionally());
    try {
      tested.getNow(null);
      fail();
    } catch (CompletionException expected) {
      assertEquals(IllegalStateException.class, expected.getCause().getClass());
      assertEquals("test exception", expected.getCause().getMessage());
    } catch (Exception e) {
      fail(e);
    }
  }

  @Test
  public void testTransformDeferredFutureFailedTransform() {
    ListenableFutureTask<String> task = ListenableFutureTask.create(() -> {}, "test");
    CompletableFuture<Integer> tested = FutureUtils.transform(task, s -> {
      throw new IllegalStateException("test exception");
    });

    task.run();

    assertTrue(tested.isDone());
    assertFalse(tested.isCancelled());
    assertTrue(tested.isCompletedExceptionally());
    try {
      tested.getNow(null);
      fail();
    } catch (CompletionException expected) {
      assertEquals(IllegalStateException.class, expected.getCause().getClass());
      assertEquals("test exception", expected.getCause().getMessage());
    } catch (Exception e) {
      fail(e);
    }
  }

}
