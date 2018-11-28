package org.apache.cassandra.utils.btree;

import java.util.Arrays;
import java.util.Comparator;
import java.util.NoSuchElementException;

public class LeafBTreeSearchIterator<K, V> implements BTreeSearchIterator<K, V> {
   private final boolean forwards;
   private final K[] keys;
   private final Comparator<? super K> comparator;
   private int nextPos;
   private final int lowerBound;
   private final int upperBound;
   private boolean hasNext;
   private boolean hasCurrent;

   public LeafBTreeSearchIterator(Object[] btree, Comparator<? super K> comparator, BTree.Dir dir) {
      this(btree, comparator, dir, 0, BTree.size(btree) - 1);
   }

   LeafBTreeSearchIterator(Object[] btree, Comparator<? super K> comparator, BTree.Dir dir, int lowerBound, int upperBound) {
      this.keys = (K[])btree;
      this.forwards = dir == BTree.Dir.ASC;
      this.comparator = comparator;
      this.lowerBound = lowerBound;
      this.upperBound = upperBound;
      this.rewind();
   }

   public V next() {
      if(!this.hasNext) {
         throw new NoSuchElementException();
      } else {
         V elem = (V)this.keys[this.nextPos];
         this.nextPos += this.forwards?1:-1;
         this.hasNext = this.nextPos >= this.lowerBound && this.nextPos <= this.upperBound;
         this.hasCurrent = true;
         return elem;
      }
   }

   public boolean hasNext() {
      return this.hasNext;
   }

   private int searchNext(K key) {
      int lb = this.forwards?this.nextPos:this.lowerBound;
      int ub = this.forwards?this.upperBound:this.nextPos;
      return Arrays.binarySearch(this.keys, lb, ub + 1, key, this.comparator);
   }

   public V next(K key) {
      if(!this.hasNext) {
         return null;
      } else {
         V result = null;
         int find = this.searchNext(key);
         if(find >= 0) {
            this.hasCurrent = true;
            result = (V)this.keys[find];
            this.nextPos = find + (this.forwards?1:-1);
         } else {
            this.nextPos = (this.forwards?-1:-2) - find;
            this.hasCurrent = false;
         }

         this.hasNext = this.nextPos >= this.lowerBound && this.nextPos <= this.upperBound;
         return result;
      }
   }

   public V current() {
      if(!this.hasCurrent) {
         throw new NoSuchElementException();
      } else {
         int current = this.forwards?this.nextPos - 1:this.nextPos + 1;
         return (V)this.keys[current];
      }
   }

   public int indexOfCurrent() {
      if(!this.hasCurrent) {
         throw new NoSuchElementException();
      } else {
         int current = this.forwards?this.nextPos - 1:this.nextPos + 1;
         return this.forwards?current - this.lowerBound:this.upperBound - current;
      }
   }

   public void rewind() {
      this.nextPos = this.forwards?this.lowerBound:this.upperBound;
      this.hasNext = this.nextPos >= this.lowerBound && this.nextPos <= this.upperBound;
   }
}
