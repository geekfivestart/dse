package com.datastax.bdp.plugin;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class AlreadyActiveFuture implements Future<Object> {
   public AlreadyActiveFuture() {
   }

   public boolean cancel(boolean mayInterruptIfRunning) {
      return false;
   }

   public Object get() throws InterruptedException, ExecutionException {
      return null;
   }

   public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      return null;
   }

   public boolean isCancelled() {
      return false;
   }

   public boolean isDone() {
      return true;
   }
}
