package org.apache.cassandra.utils.flow;

import io.reactivex.functions.Function;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.cassandra.utils.Reducer;
import org.apache.cassandra.utils.Throwables;

public class Merge {
   public Merge() {
   }

   public static <In, Out> Flow<Out> get(List<? extends Flow<In>> sources, Comparator<? super In> comparator, Reducer<In, Out> reducer) {
      if(sources.size() == 1) {
         if(!reducer.trivialReduceIsTrivial()) {
            return ((Flow)sources.get(0)).map((next) -> {
               reducer.onKeyChange();
               reducer.reduce(0, (In)next);
               return reducer.getReduced();
            });
         } else {
            Flow<Out> converted = (Flow)sources.get(0);
            return converted;
         }
      } else {
         return new Merge.ManyToOne(reducer, sources, comparator);
      }
   }

   protected static final class Candidate<In> implements Comparable<Merge.Candidate<In>>, FlowSubscriber<In>, AutoCloseable {
      private final Merge.ManyToOne<In, ?> merger;
      private final Flow<In> sourceFlow;
      private FlowSubscription source;
      private final Comparator<? super In> comp;
      private final int idx;
      private In item;
      private boolean completeOnNextRequest = false;
      Throwable error = null;
      private final AtomicReference<Merge.Candidate.State> state;
      boolean equalParent;

      public Candidate(Merge.ManyToOne<In, ?> merger, int idx, Flow<In> source, Comparator<? super In> comp) {
         this.state = new AtomicReference(Merge.Candidate.State.NEEDS_REQUEST);
         this.merger = merger;
         this.comp = comp;
         this.idx = idx;
         this.sourceFlow = source;
      }

      void requestFirst() {
         if(this.verifyStateChange(Merge.Candidate.State.NEEDS_REQUEST, Merge.Candidate.State.AWAITING_ADVANCE, true)) {
            this.sourceFlow.requestFirst(this, this);
         }
      }

      public void onSubscribe(FlowSubscription source) {
         this.source = source;
      }

      public boolean needsRequest() {
         return this.state.get() == Merge.Candidate.State.NEEDS_REQUEST;
      }

      private boolean verifyStateChange(Merge.Candidate.State from, Merge.Candidate.State to, boolean itemShouldBeNull) {
         Merge.Candidate.State prev = (Merge.Candidate.State)this.state.getAndSet(to);
         if(prev == from && (!itemShouldBeNull || this.item == null)) {
            return true;
         } else {
            this.onOurError(new AssertionError("Invalid state " + prev + (this.item == null?"/":"/non-") + "null item to transition " + from + (itemShouldBeNull?"/null item":"") + "->" + to));
            return false;
         }
      }

      private AssertionError onOurError(AssertionError e) {
         Throwable t = Throwables.merge(this.error, e);
         this.merger.onError(Flow.wrapException(t, this));
         return e;
      }

      protected void request() {
         if(this.verifyStateChange(Merge.Candidate.State.NEEDS_REQUEST, Merge.Candidate.State.AWAITING_ADVANCE, true)) {
            if(this.completeOnNextRequest) {
               this.onComplete();
            } else {
               this.source.requestNext();
            }

         }
      }

      public void onComplete() {
         this.onAdvance(null);
      }

      public void onError(Throwable error) {
         this.error = Flow.wrapException(error, this);
         this.onAdvance(null);
      }

      public void onNext(In next) {
         if(next != null) {
            this.onAdvance(next);
         } else {
            this.onError(new AssertionError("null item in onNext"));
         }

      }

      public void onFinal(In next) {
         this.completeOnNextRequest = true;
         this.onNext(next);
      }

      private void onAdvance(In next) {
         if(this.verifyStateChange(Merge.Candidate.State.AWAITING_ADVANCE, Merge.Candidate.State.ADVANCED, true)) {
            this.item = next;
            this.merger.onAdvance();
         }
      }

      public boolean justAdvanced() {
         if(this.state.get() == Merge.Candidate.State.PROCESSED) {
            return false;
         } else {
            this.verifyStateChange(Merge.Candidate.State.ADVANCED, Merge.Candidate.State.PROCESSED, false);
            return true;
         }
      }

      public int compareTo(Merge.Candidate<In> that) {
         if(this.state.get() == Merge.Candidate.State.PROCESSED && that.state.get() == Merge.Candidate.State.PROCESSED) {
            if(this.error == null && that.error == null) {
               assert this.item != null && that.item != null;

               int ret = this.comp.compare(this.item, that.item);
               return ret;
            } else {
               return (this.error != null?-1:0) - (that.error != null?-1:0);
            }
         } else {
            Merge.Candidate<In> invalid = this.state.get() != Merge.Candidate.State.PROCESSED?this:that;
            throw this.onOurError(new AssertionError("Comparing unprocessed item " + invalid + " in state " + invalid.state.get()));
         }
      }

      public void consume(Reducer<In, ?> reducer) {
         if(this.verifyStateChange(Merge.Candidate.State.PROCESSED, Merge.Candidate.State.NEEDS_REQUEST, false)) {
            if(this.error != null) {
               reducer.error(this.error);
            } else {
               reducer.reduce(this.idx, this.item);
            }

            this.item = null;
            this.error = null;
         }
      }

      public void close() throws Exception {
         this.source.close();
      }

      public String toString() {
         return Flow.formatTrace("merge child", this.sourceFlow);
      }

      static enum State {
         NEEDS_REQUEST,
         AWAITING_ADVANCE,
         ADVANCED,
         PROCESSED;

         private State() {
         }
      }
   }

   static final class ManyToOne<In, Out> extends Flow.RequestLoopFlow<Out> implements FlowSubscription {
      protected Merge.Candidate<In>[] heap;
      private final Reducer<In, Out> reducer;
      FlowSubscriber<Out> subscriber;
      AtomicInteger advancing = new AtomicInteger();
      int size;
      int needingAdvance;
      static final int SORTED_SECTION_SIZE = 4;

      public ManyToOne(Reducer<In, Out> reducer, List<? extends Flow<In>> sources, Comparator<? super In> comparator) {
         this.reducer = reducer;
         Merge.Candidate<In>[] heap = new Merge.Candidate[sources.size()];
         this.heap = heap;
         this.size = 0;

         for(int i = 0; i < sources.size(); ++i) {
            Merge.Candidate<In> candidate = new Merge.Candidate(this, i, (Flow)sources.get(i), comparator);
            heap[this.size++] = candidate;
         }

         this.needingAdvance = this.size;
      }

      public void requestFirst(FlowSubscriber<Out> subscriber, FlowSubscriptionRecipient subscriptionRecipient) {
         assert this.subscriber == null : "Flow are single-use.";

         this.subscriber = subscriber;
         subscriptionRecipient.onSubscribe(this);
         this.advancing.set(this.size);

         for(int i = 0; i < this.needingAdvance; ++i) {
            this.heap[i].requestFirst();
         }

      }

      public void close() throws Exception {
         Throwable t = Throwables.close((Throwable)null, Arrays.asList(this.heap));
         Throwables.maybeFail(t);
      }

      public String toString() {
         return Flow.formatTrace("merge", (Object)this.reducer);
      }

      public void requestNext() {
         int prev = this.advancing.getAndIncrement();
         if(prev != 0) {
            this.subscriber.onError(new AssertionError("Merge advance called while another has " + prev + " outstanding requests."));
         } else {
            for(int i = this.needingAdvance - 1; i >= 0; --i) {
               Merge.Candidate<In> candidate = this.heap[i];
               if(candidate.needsRequest()) {
                  this.advancing.incrementAndGet();
                  candidate.request();
               }
            }

            this.onAdvance();
         }
      }

      void onAdvance() {
         if(this.advancing.decrementAndGet() <= 0) {
            for(int i = this.needingAdvance - 1; i >= 0; --i) {
               Merge.Candidate<In> candidate = this.heap[i];
               if(candidate.justAdvanced()) {
                  this.replaceAndSink(this.heap[i], i);
               }
            }

            this.needingAdvance = 0;
            this.consume();
         }
      }

      private void consume() {
         if(this.size == 0) {
            this.subscriber.onComplete();
         } else {
            try {
               this.reducer.onKeyChange();
               this.heap[0].consume(this.reducer);
               int size = this.size;
               int sortedSectionSize = Math.min(size, 4);
               int i = 1;

               while(true) {
                  if(i < sortedSectionSize) {
                     if(this.heap[i].equalParent) {
                        this.heap[i].consume(this.reducer);
                        ++i;
                        continue;
                     }
                  } else {
                     i = Math.max(i, this.consumeHeap(i) + 1);
                  }

                  this.needingAdvance = i;
                  break;
               }
            } catch (Throwable var4) {
               this.onError(var4);
               return;
            }

            Throwable error = this.reducer.getErrors();
            if(error != null) {
               this.onError(error);
            } else {
               Out item = this.reducer.getReduced();
               if(item != null) {
                  this.subscriber.onNext(item);
               } else {
                  this.requestInLoop(this);
               }
            }

         }
      }

      void onError(Throwable error) {
         this.subscriber.onError(error);
      }

      private int consumeHeap(int idx) {
         if(idx < this.size && this.heap[idx].equalParent) {
            this.heap[idx].consume(this.reducer);
            int nextIdx = (idx << 1) - 3;
            return Math.max(idx, Math.max(this.consumeHeap(nextIdx), this.consumeHeap(nextIdx + 1)));
         } else {
            return -1;
         }
      }

      private void replaceAndSink(Merge.Candidate<In> candidate, int currIdx) {
         if(candidate.item == null && candidate.error == null) {
            Merge.Candidate<In> toDrop = candidate;
            candidate = this.heap[--this.size];
            this.heap[this.size] = toDrop;
         }

         candidate.equalParent = false;
         int size = this.size;

         int sortedSectionSize;
         int nextIdx;
         int siblingCmp;
         for(sortedSectionSize = Math.min(size - 1, 4); (nextIdx = currIdx + 1) <= sortedSectionSize; currIdx = nextIdx) {
            if(!this.heap[nextIdx].equalParent) {
               siblingCmp = candidate.compareTo(this.heap[nextIdx]);
               if(siblingCmp <= 0) {
                  this.heap[nextIdx].equalParent = siblingCmp == 0;
                  this.heap[currIdx] = candidate;
                  return;
               }
            }

            this.heap[currIdx] = this.heap[nextIdx];
         }

         while((nextIdx = currIdx * 2 - (sortedSectionSize - 1)) + 1 < size) {
            if(!this.heap[nextIdx].equalParent) {
               if(!this.heap[nextIdx + 1].equalParent) {
                  siblingCmp = this.heap[nextIdx + 1].compareTo(this.heap[nextIdx]);
                  if(siblingCmp < 0) {
                     ++nextIdx;
                  }

                  int cmp = candidate.compareTo(this.heap[nextIdx]);
                  if(cmp <= 0) {
                     if(cmp == 0) {
                        this.heap[nextIdx].equalParent = true;
                        if(siblingCmp == 0) {
                           this.heap[nextIdx + 1].equalParent = true;
                        }
                     }

                     this.heap[currIdx] = candidate;
                     return;
                  }

                  if(siblingCmp == 0) {
                     this.heap[nextIdx + 1].equalParent = true;
                  }
               } else {
                  ++nextIdx;
               }
            }

            this.heap[currIdx] = this.heap[nextIdx];
            currIdx = nextIdx;
         }

         if(nextIdx >= size) {
            this.heap[currIdx] = candidate;
         } else {
            if(!this.heap[nextIdx].equalParent) {
               siblingCmp = candidate.compareTo(this.heap[nextIdx]);
               if(siblingCmp <= 0) {
                  this.heap[nextIdx].equalParent = siblingCmp == 0;
                  this.heap[currIdx] = candidate;
                  return;
               }
            }

            this.heap[currIdx] = this.heap[nextIdx];
            this.heap[nextIdx] = candidate;
         }
      }
   }
}
