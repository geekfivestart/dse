package org.apache.cassandra.utils;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LockedDynamicList<E> extends DynamicList<E> {
   private final ReadWriteLock lock = new ReentrantReadWriteLock();

   public LockedDynamicList(int maxExpectedSize) {
      super(maxExpectedSize);
   }

   public DynamicList.Node<E> append(E value, int maxSize) {
      this.lock.writeLock().lock();

      DynamicList.Node var3;
      try {
         var3 = super.append(value, maxSize);
      } finally {
         this.lock.writeLock().unlock();
      }

      return var3;
   }

   public void remove(DynamicList.Node<E> node) {
      this.lock.writeLock().lock();

      try {
         super.remove(node);
      } finally {
         this.lock.writeLock().unlock();
      }

   }

   public E get(int index) {
      this.lock.readLock().lock();

      Object var2;
      try {
         var2 = super.get(index);
      } finally {
         this.lock.readLock().unlock();
      }

      return var2;
   }

   public int size() {
      this.lock.readLock().lock();

      int var1;
      try {
         var1 = super.size();
      } finally {
         this.lock.readLock().unlock();
      }

      return var1;
   }
}
