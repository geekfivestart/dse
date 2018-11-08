package com.datastax.bdp.db.utils.concurrent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.IntFunction;
import java.util.stream.Stream;

public abstract class CompletableFutures {
   private CompletableFutures() {
   }

   public static <T> CompletableFuture<T> exceptionallyCompletedFuture(Throwable t) {
      CompletableFuture<T> future = new CompletableFuture();
      future.completeExceptionally(t);
      return future;
   }

   public static CompletableFuture<Void> allOf(Collection<CompletableFuture<?>> futures) {
      return futures.isEmpty()?CompletableFuture.completedFuture((Object)null):CompletableFuture.allOf((CompletableFuture[])futures.toArray(new CompletableFuture[0]));
   }

   public static CompletableFuture<Void> allOf(Stream<CompletableFuture<?>> futures) {
      return CompletableFuture.allOf((CompletableFuture[])futures.toArray((x$0) -> {
         return new CompletableFuture[x$0];
      }));
   }

   public static <T> CompletableFuture<List<T>> allAsList(List<CompletableFuture<T>> futures) {
      CompletableFuture<List<T>> result = CompletableFuture.completedFuture(new ArrayList(futures.size()));

      CompletableFuture future;
      for(Iterator var2 = futures.iterator(); var2.hasNext(); result = result.thenCombine(future, (l, t) -> {
         l.add(t);
         return l;
      })) {
         future = (CompletableFuture)var2.next();
      }

      return result;
   }
}
