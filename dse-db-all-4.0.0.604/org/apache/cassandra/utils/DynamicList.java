package org.apache.cassandra.utils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;

public class DynamicList<E> {
   private final int maxHeight;
   private final DynamicList.Node<E> head;
   private int size;

   public DynamicList(int maxExpectedSize) {
      this.maxHeight = 3 + Math.max(0, (int)Math.ceil(Math.log((double)maxExpectedSize) / Math.log(2.0D)));
      this.head = new DynamicList.Node(this.maxHeight, (Object)null);
   }

   private int randomLevel() {
      return 1 + Integer.bitCount(ThreadLocalRandom.current().nextInt() & (1 << this.maxHeight - 1) - 1);
   }

   public DynamicList.Node<E> append(E value) {
      return this.append(value, 2147483647);
   }

   public DynamicList.Node<E> append(E value, int maxSize) {
      DynamicList.Node<E> newTail = new DynamicList.Node(this.randomLevel(), value);
      if(this.size >= maxSize) {
         return null;
      } else {
         ++this.size;
         DynamicList.Node<E> tail = this.head;

         int i;
         DynamicList.Node next;
         for(i = this.maxHeight - 1; i >= newTail.height(); --i) {
            while((next = tail.next(i)) != null) {
               tail = next;
            }

            ++tail.size[i];
         }

         for(i = newTail.height() - 1; i >= 0; --i) {
            while((next = tail.next(i)) != null) {
               tail = next;
            }

            tail.setNext(i, newTail);
            newTail.setPrev(i, tail);
         }

         return newTail;
      }
   }

   public void remove(DynamicList.Node<E> node) {
      assert node.value != null;

      node.value = null;
      --this.size;

      int i;
      for(i = 0; i < node.height(); ++i) {
         DynamicList.Node<E> prev = node.prev(i);
         DynamicList.Node<E> next = node.next(i);

         assert prev != null;

         prev.setNext(i, next);
         if(next != null) {
            next.setPrev(i, prev);
         }

         int[] var10000 = prev.size;
         var10000[i] += node.size[i] - 1;
      }

      for(i = node.height(); i < this.maxHeight; ++i) {
         while(i == node.height()) {
            node = node.prev(i - 1);
         }

         --node.size[i];
      }

   }

   public E get(int index) {
      if(index >= this.size) {
         return null;
      } else {
         ++index;
         int c = 0;
         DynamicList.Node<E> finger = this.head;

         for(int i = this.maxHeight - 1; i >= 0; --i) {
            while(c + finger.size[i] <= index) {
               c += finger.size[i];
               finger = finger.next(i);
            }
         }

         assert c == index;

         return finger.value;
      }
   }

   public int size() {
      return this.size;
   }

   private boolean isWellFormed() {
      for(int i = 0; i < this.maxHeight; ++i) {
         int c = 0;

         for(DynamicList.Node node = this.head; node != null; node = node.next(i)) {
            if(node.prev(i) != null && node.prev(i).next(i) != node) {
               return false;
            }

            if(node.next(i) != null && node.next(i).prev(i) != node) {
               return false;
            }

            c += node.size[i];
            if(i + 1 < this.maxHeight && node.parent(i + 1).next(i + 1) == node.next(i)) {
               if(node.parent(i + 1).size[i + 1] != c) {
                  return false;
               }

               c = 0;
            }
         }

         if(i == this.maxHeight - 1 && c != this.size + 1) {
            return false;
         }
      }

      return true;
   }

   public static void main(String[] args) {
      DynamicList<Integer> list = new DynamicList(20);
      TreeSet<Integer> canon = new TreeSet();
      HashMap<Integer, DynamicList.Node> nodes = new HashMap();
      int c = 0;

      for(int i = 0; i < 100000; ++i) {
         nodes.put(Integer.valueOf(c), list.append(Integer.valueOf(c)));
         canon.add(Integer.valueOf(c));
         ++c;
      }

      ThreadLocalRandom rand = ThreadLocalRandom.current();

      assert list.isWellFormed();

      for(int loop = 0; loop < 100; ++loop) {
         System.out.println(loop);

         for(int i = 0; i < 100000; ++i) {
            int index = rand.nextInt(100000);
            Integer seed = (Integer)list.get(index);
            list.remove((DynamicList.Node)nodes.remove(seed));
            canon.remove(seed);
            nodes.put(Integer.valueOf(c), list.append(Integer.valueOf(c)));
            canon.add(Integer.valueOf(c));
            ++c;
         }

         assert list.isWellFormed();
      }

   }

   public static class Node<E> {
      private final int[] size;
      private final DynamicList.Node<E>[] links;
      private E value;

      private Node(int height, E value) {
         this.value = value;
         this.links = new DynamicList.Node[height * 2];
         this.size = new int[height];
         Arrays.fill(this.size, 1);
      }

      private int height() {
         return this.size.length;
      }

      private DynamicList.Node<E> next(int i) {
         return this.links[i * 2];
      }

      private DynamicList.Node<E> prev(int i) {
         return this.links[1 + i * 2];
      }

      private void setNext(int i, DynamicList.Node<E> next) {
         this.links[i * 2] = next;
      }

      private void setPrev(int i, DynamicList.Node<E> prev) {
         this.links[1 + i * 2] = prev;
      }

      private DynamicList.Node parent(int parentHeight) {
         DynamicList.Node prev = this;

         while(true) {
            int height = prev.height();
            if(parentHeight < height) {
               return prev;
            }

            prev = prev.prev(height - 1);
         }
      }
   }
}
