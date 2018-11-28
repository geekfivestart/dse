package org.apache.cassandra.utils;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public abstract class MergeIterator<In, Out> extends AbstractIterator<Out> implements IMergeIterator<In, Out> {
   protected final Reducer<In, Out> reducer;
   protected final List<? extends Iterator<In>> iterators;

   protected MergeIterator(List<? extends Iterator<In>> iters, Reducer<In, Out> reducer) {
      this.iterators = iters;
      this.reducer = reducer;
   }

   public static <In, Out> MergeIterator<In, Out> get(List<? extends Iterator<In>> sources, Comparator<? super In> comparator, Reducer<In, Out> reducer) {
      return (MergeIterator)(sources.size() == 1?(reducer.trivialReduceIsTrivial()?new MergeIterator.TrivialOneToOne(sources, reducer):new MergeIterator.OneToOne(sources, reducer)):new MergeIterator.ManyToOne(sources, comparator, reducer));
   }

   public Iterable<? extends Iterator<In>> iterators() {
      return this.iterators;
   }

   public void close() {
      int i = 0;

      for(int length = this.iterators.size(); i < length; ++i) {
         Iterator iterator = (Iterator)this.iterators.get(i);

         try {
            if(iterator instanceof AutoCloseable) {
               ((AutoCloseable)iterator).close();
            }
         } catch (Exception var5) {
            throw new RuntimeException(var5);
         }
      }

      this.reducer.close();
   }

   private static class TrivialOneToOne<In, Out> extends MergeIterator<In, Out> {
      private final Iterator<In> source;

      public TrivialOneToOne(List<? extends Iterator<In>> sources, Reducer<In, Out> reducer) {
         super(sources, reducer);
         this.source = (Iterator)sources.get(0);
      }

      protected Out computeNext() {
         return !this.source.hasNext()?this.endOfData():(Out)this.source.next();
      }
   }

   private static class OneToOne<In, Out> extends MergeIterator<In, Out> {
      private final Iterator<In> source;

      public OneToOne(List<? extends Iterator<In>> sources, Reducer<In, Out> reducer) {
         super(sources, reducer);
         this.source = (Iterator)sources.get(0);
      }

      protected Out computeNext() {
         if(!this.source.hasNext()) {
            return this.endOfData();
         } else {
            this.reducer.onKeyChange();
            this.reducer.reduce(0, this.source.next());
            return this.reducer.getReduced();
         }
      }
   }

   protected static final class Candidate<In> implements Comparable<MergeIterator.Candidate<In>> {
      private final Iterator<? extends In> iter;
      private final Comparator<? super In> comp;
      private final int idx;
      private In item;
      boolean equalParent;

      public Candidate(int idx, Iterator<? extends In> iter, Comparator<? super In> comp) {
         this.iter = iter;
         this.comp = comp;
         this.idx = idx;
      }

      protected MergeIterator.Candidate<In> advance() {
         if(!this.iter.hasNext()) {
            return null;
         } else {
            this.item = this.iter.next();
            return this;
         }
      }

      public int compareTo(MergeIterator.Candidate<In> that) {
         assert this.item != null && that.item != null;

         return this.comp.compare(this.item, that.item);
      }

      public void consume(Reducer reducer) {
         reducer.reduce(this.idx, this.item);
         this.item = null;
      }

      public boolean needsAdvance() {
         return this.item == null;
      }
   }

   static final class ManyToOne<In, Out> extends MergeIterator<In, Out> {
      protected final MergeIterator.Candidate<In>[] heap;
      int size;
      int needingAdvance;
      static final int SORTED_SECTION_SIZE = 4;

      public ManyToOne(List<? extends Iterator<In>> iters, Comparator<? super In> comp, Reducer<In, Out> reducer) {
         super(iters, reducer);
         MergeIterator.Candidate<In>[] heap = new MergeIterator.Candidate[iters.size()];
         this.heap = heap;
         this.size = 0;

         for(int i = 0; i < iters.size(); ++i) {
            MergeIterator.Candidate<In> candidate = new MergeIterator.Candidate(i, (Iterator)iters.get(i), comp);
            heap[this.size++] = candidate;
         }

         this.needingAdvance = this.size;
      }

      protected final Out computeNext() {
         this.advance();
         return this.consume();
      }

      private void advance() {
         for(int i = this.needingAdvance - 1; i >= 0; --i) {
            MergeIterator.Candidate<In> candidate = this.heap[i];
            if(candidate.needsAdvance()) {
               this.replaceAndSink(candidate.advance(), i);
            }
         }

      }

      private Out consume() {
         if(this.size == 0) {
            return this.endOfData();
         } else {
            this.reducer.onKeyChange();

            assert !this.heap[0].equalParent;

            this.heap[0].consume(this.reducer);
            int size = this.size;
            int sortedSectionSize = Math.min(size, 4);
            int i = 1;

            while(true) {
               if(i >= sortedSectionSize) {
                  i = Math.max(i, this.consumeHeap(i) + 1);
                  break;
               }

               if(!this.heap[i].equalParent) {
                  break;
               }

               this.heap[i].consume(this.reducer);
               ++i;
            }

            this.needingAdvance = i;
            return this.reducer.getReduced();
         }
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

      private void replaceAndSink(MergeIterator.Candidate<In> candidate, int currIdx) {
         if(candidate == null) {
            candidate = this.heap[--this.size];
            this.heap[this.size] = null;
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
