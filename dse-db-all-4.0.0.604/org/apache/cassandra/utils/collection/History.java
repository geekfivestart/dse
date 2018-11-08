package org.apache.cassandra.utils.collection;

import java.util.AbstractCollection;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class History<E> extends AbstractCollection<E> {
   private final E[] array;
   private int idx;
   private boolean atCapacity;

   public History(int capacity) {
      E[] a = (Object[])(new Object[capacity]);
      this.array = a;
   }

   public List<E> last(int n) {
      return new History.View(n);
   }

   public E last() {
      return this.isEmpty()?null:this.array[(this.idx - 1 + this.array.length) % this.array.length];
   }

   public List<E> listView() {
      return new History.View(this.capacity());
   }

   public Iterator<E> iterator() {
      return this.last(this.capacity()).iterator();
   }

   public int capacity() {
      return this.array.length;
   }

   public int size() {
      return this.atCapacity?this.array.length:this.idx;
   }

   public boolean isAtCapacity() {
      return this.atCapacity;
   }

   public boolean add(E e) {
      this.array[this.idx++] = e;
      if(this.idx == this.array.length) {
         this.atCapacity = true;
         this.idx = 0;
      }

      return true;
   }

   public void clear() {
      Arrays.fill(this.array, (Object)null);
      this.idx = 0;
      this.atCapacity = false;
   }

   private class View extends AbstractList<E> {
      private final int n;

      private View(int n) {
         this.n = n;
      }

      public E get(int i) {
         if(i >= 0 && i < this.size()) {
            return History.this.array[(History.this.idx - 1 - i + History.this.array.length) % History.this.array.length];
         } else {
            throw new IndexOutOfBoundsException();
         }
      }

      public int size() {
         return Math.min(History.this.size(), this.n);
      }
   }
}
