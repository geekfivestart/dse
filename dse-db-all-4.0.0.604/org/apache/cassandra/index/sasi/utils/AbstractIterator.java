package org.apache.cassandra.index.sasi.utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.PeekingIterator;
import java.util.NoSuchElementException;

public abstract class AbstractIterator<T> implements PeekingIterator<T> {
   protected AbstractIterator.State state;
   protected T next;

   protected AbstractIterator() {
      this.state = AbstractIterator.State.NOT_READY;
   }

   protected abstract T computeNext();

   protected final T endOfData() {
      this.state = AbstractIterator.State.DONE;
      return null;
   }

   public final boolean hasNext() {
      Preconditions.checkState((boolean)(this.state != State.FAILED));
      switch (this.state) {
         case DONE: {
            return false;
         }
         case READY: {
            return true;
         }
      }
      return this.tryToComputeNext();
   }

   protected boolean tryToComputeNext() {
      this.state = AbstractIterator.State.FAILED;
      this.next = this.computeNext();
      if(this.state != AbstractIterator.State.DONE) {
         this.state = AbstractIterator.State.READY;
         return true;
      } else {
         return false;
      }
   }

   public final T next() {
      if(!this.hasNext()) {
         throw new NoSuchElementException();
      } else {
         this.state = AbstractIterator.State.NOT_READY;
         return this.next;
      }
   }

   public void remove() {
      throw new UnsupportedOperationException();
   }

   public final T peek() {
      if(!this.hasNext()) {
         throw new NoSuchElementException();
      } else {
         return this.next;
      }
   }

   protected static enum State {
      READY,
      NOT_READY,
      DONE,
      FAILED;

      private State() {
      }
   }
}
