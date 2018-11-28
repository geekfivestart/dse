package org.apache.cassandra.utils.concurrent;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

public class Accumulator<E> implements Iterable<E> {
   private volatile int nextIndex;
   private volatile int presentCount;
   private final Object[] values;
   private static final AtomicIntegerFieldUpdater<Accumulator> nextIndexUpdater = AtomicIntegerFieldUpdater.newUpdater(Accumulator.class, "nextIndex");
   private static final AtomicIntegerFieldUpdater<Accumulator> presentCountUpdater = AtomicIntegerFieldUpdater.newUpdater(Accumulator.class, "presentCount");

   public Accumulator(int size) {
      this.values = new Object[size];
   }

   public void add(E item) {
      int insertPos;
      do {
         insertPos = this.nextIndex;
         if(insertPos >= this.values.length) {
            throw new IllegalStateException();
         }
      } while(!nextIndexUpdater.compareAndSet(this, insertPos, insertPos + 1));

      this.values[insertPos] = item;
      boolean volatileWrite = false;

      while(true) {
         while(true) {
            int cur = this.presentCount;
            if(cur != insertPos && (cur == this.values.length || this.values[cur] == null)) {
               if(volatileWrite || cur >= insertPos || presentCountUpdater.compareAndSet(this, cur, cur)) {
                  return;
               }

               volatileWrite = true;
            } else {
               presentCountUpdater.compareAndSet(this, cur, cur + 1);
               volatileWrite = true;
            }
         }
      }
   }

   public boolean isEmpty() {
      return this.presentCount == 0;
   }

   public int size() {
      return this.presentCount;
   }

   public int capacity() {
      return this.values.length;
   }

   public Iterator<E> iterator() {
      return new Iterator<E>() {
         int p = 0;

         public boolean hasNext() {
            return this.p < Accumulator.this.presentCount;
         }

         public E next() {
            return (E)Accumulator.this.values[this.p++];
         }

         public void remove() {
            throw new UnsupportedOperationException();
         }
      };
   }

   public E get(int i) {
      if(i >= this.presentCount) {
         throw new IndexOutOfBoundsException();
      } else {
         return (E)this.values[i];
      }
   }
}
