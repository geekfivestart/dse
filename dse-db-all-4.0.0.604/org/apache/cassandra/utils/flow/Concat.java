package org.apache.cassandra.utils.flow;

import io.reactivex.functions.Function;
import java.util.Arrays;
import java.util.concurrent.Callable;

class Concat {
   Concat() {
   }

   static <T> Flow<T> concat(Flow<Flow<T>> source) {
      return source.flatMap((x) -> {
         return x;
      });
   }

   static <T> Flow<T> concat(Iterable<Flow<T>> sources) {
      return concat(Flow.fromIterable(sources));
   }

   static <O> Flow<O> concat(Flow<O>[] sources) {
      return concat((Iterable)Arrays.asList(sources));
   }

   static <T> Flow<T> concatWith(Flow<T> source, Callable<Flow<T>> supplier) {
      return new Concat.ConcatWithFlow(source, supplier);
   }

   private static class ConcatWithFlow<T> extends FlowTransform<T, T> {
      private final Callable<Flow<T>> supplier;
      boolean completeOnRequest = false;
      private final FlowSubscription requestFirstInLoop = new FlowSubscription() {
         public void requestNext() {
            Concat.ConcatWithFlow<T> us = ConcatWithFlow.this;
            ConcatWithFlow.this.sourceFlow.requestFirst(us, us);
         }

         public void close() throws Exception {
         }
      };

      ConcatWithFlow(Flow<T> source, Callable<Flow<T>> supplier) {
         super(source);
         this.supplier = supplier;
      }

      public void requestNext() {
         if(!this.completeOnRequest) {
            this.source.requestNext();
         } else {
            this.completeOnRequest = false;
            this.onComplete();
         }

      }

      public void close() throws Exception {
         if(this.source != null) {
            this.source.close();
         }

      }

      public String toString() {
         return formatTrace("concat-with", this.supplier, this.sourceFlow);
      }

      public void onNext(T item) {
         this.subscriber.onNext(item);
      }

      public void onFinal(T item) {
         this.completeOnRequest = true;
         this.subscriber.onNext(item);
      }

      public void onComplete() {
         try {
            this.source.close();
         } catch (Throwable var4) {
            this.onError(var4);
            return;
         }

         this.source = null;

         Flow next;
         try {
            next = (Flow)this.supplier.call();
         } catch (Throwable var3) {
            this.subscriber.onError(var3);
            return;
         }

         if(next == null) {
            this.subscriber.onComplete();
         } else {
            this.sourceFlow = next;
            this.requestInLoop(this.requestFirstInLoop);
         }
      }
   }
}
