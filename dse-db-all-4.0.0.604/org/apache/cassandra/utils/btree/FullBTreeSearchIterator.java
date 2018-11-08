package org.apache.cassandra.utils.btree;

import java.util.Comparator;
import java.util.NoSuchElementException;

public class FullBTreeSearchIterator<K, V> extends TreeCursor<K> implements BTreeSearchIterator<K, V> {
   private final boolean forwards;
   private int index;
   private byte state;
   private final int lowerBound;
   private final int upperBound;
   private static final int MIDDLE = 0;
   private static final int ON_ITEM = 1;
   private static final int BEFORE_FIRST = 2;
   private static final int LAST = 4;
   private static final int END = 5;

   public FullBTreeSearchIterator(Object[] btree, Comparator<? super K> comparator, BTree.Dir dir) {
      this(btree, comparator, dir, 0, BTree.size(btree) - 1);
   }

   FullBTreeSearchIterator(Object[] btree, Comparator<? super K> comparator, BTree.Dir dir, int lowerBound, int upperBound) {
      super(comparator, btree);
      this.forwards = dir == BTree.Dir.ASC;
      this.lowerBound = lowerBound;
      this.upperBound = upperBound;
      this.rewind();
   }

   private int compareToLast(int idx) {
      return this.forwards?idx - this.upperBound:this.lowerBound - idx;
   }

   private int compareToFirst(int idx) {
      return this.forwards?idx - this.lowerBound:this.upperBound - idx;
   }

   public boolean hasNext() {
      return this.state != 5;
   }

   public V next() {
      switch(this.state) {
      case 1:
         if(this.compareToLast(this.index = this.moveOne(this.forwards)) >= 0) {
            this.state = 5;
         }
         break;
      case 2:
         this.seekTo(this.index = this.forwards?this.lowerBound:this.upperBound);
         this.state = (byte)(this.upperBound == this.lowerBound?4:0);
      case 0:
      case 4:
         this.state = (byte)(this.state | 1);
         break;
      case 3:
      default:
         throw new NoSuchElementException();
      }

      return this.current();
   }

   public V next(K target) {
      if(!this.hasNext()) {
         return null;
      } else {
         int state = this.state;
         boolean found = this.seekTo(target, this.forwards, (state & 3) != 0);
         int index = this.cur.globalIndex();
         V next = null;
         if(state == 2 && this.compareToFirst(index) < 0) {
            return null;
         } else {
            int compareToLast = this.compareToLast(index);
            int state;
            if(compareToLast <= 0) {
               state = compareToLast < 0?0:4;
               if(found) {
                  state |= 1;
                  next = this.currentValue();
               }
            } else {
               state = 5;
            }

            this.state = (byte)state;
            this.index = index;
            return next;
         }
      }
   }

   public void rewind() {
      if(this.upperBound < this.lowerBound) {
         this.state = 5;
      } else {
         this.reset(this.forwards);
         this.state = 2;
      }

   }

   private void checkOnItem() {
      if((this.state & 1) != 1) {
         throw new NoSuchElementException();
      }
   }

   public V current() {
      this.checkOnItem();
      return this.currentValue();
   }

   public int indexOfCurrent() {
      this.checkOnItem();
      return this.compareToFirst(this.index);
   }
}
