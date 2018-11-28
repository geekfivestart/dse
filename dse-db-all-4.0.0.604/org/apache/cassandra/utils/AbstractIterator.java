package org.apache.cassandra.utils;

import com.google.common.collect.PeekingIterator;
import java.util.Iterator;
import java.util.NoSuchElementException;

public abstract class AbstractIterator<V> implements Iterator<V>, PeekingIterator<V>, CloseableIterator<V> {
   private AbstractIterator.State state;
   private V next;

   public AbstractIterator() {
      this.state = AbstractIterator.State.MUST_FETCH;
   }

   protected V endOfData() {
      this.state = AbstractIterator.State.DONE;
      return null;
   }

   protected abstract V computeNext();

   public boolean hasNext() {
      switch (this.state) {
         case MUST_FETCH: {
            this.state = State.FAILED;
            this.next = this.computeNext();
         }
         default: {
            if (this.state == State.DONE) {
               return false;
            }
            this.state = State.HAS_NEXT;
            return true;
         }
         case FAILED:
      }
      throw new IllegalStateException();
   }

   public V next() {
      if(this.state != AbstractIterator.State.HAS_NEXT && !this.hasNext()) {
         throw new NoSuchElementException();
      } else {
         this.state = AbstractIterator.State.MUST_FETCH;
         V result = this.next;
         this.next = null;
         return result;
      }
   }

   public V peek() {
      if(!this.hasNext()) {
         throw new NoSuchElementException();
      } else {
         return this.next;
      }
   }

   public void setDefaultState() {
      this.state = AbstractIterator.State.MUST_FETCH;
   }

   public void remove() {
      throw new UnsupportedOperationException();
   }

   public void close() {
   }

   private static enum State {
      MUST_FETCH,
      HAS_NEXT,
      DONE,
      FAILED;

      private State() {
      }
   }
}
