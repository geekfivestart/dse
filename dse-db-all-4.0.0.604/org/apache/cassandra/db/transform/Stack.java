package org.apache.cassandra.db.transform;

import java.util.Arrays;
import java.util.Iterator;
import org.apache.cassandra.utils.AbstractIterator;

public class Stack implements Iterable<Transformation> {
   public static final int INITIAL_TRANSFORMATION_LENGTH = 5;
   public static final Transformation[] EMPTY_TRANSFORMATIONS = new Transformation[0];
   public static final Stack.MoreContentsHolder[] EMPTY_MORE_CONTENTS_HOLDERS = new Stack.MoreContentsHolder[0];
   static final Stack EMPTY = new Stack();
   Transformation[] stack;
   int length;
   Stack.MoreContentsHolder[] moreContents;

   public Iterator<Transformation> iterator() {
      return new AbstractIterator<Transformation>() {
         int i = 0;

         protected Transformation computeNext() {
            return this.i < Stack.this.length?Stack.this.stack[this.i++]:(Transformation)this.endOfData();
         }
      };
   }

   public int size() {
      return this.length;
   }

   public Stack() {
      this.stack = EMPTY_TRANSFORMATIONS;
      this.moreContents = EMPTY_MORE_CONTENTS_HOLDERS;
   }

   Stack(Stack copy) {
      this.stack = copy.stack;
      this.length = copy.length;
      this.moreContents = copy.moreContents;
   }

   public void add(Transformation add) {
      if(this.length == this.stack.length) {
         this.stack = (Transformation[])resize(this.stack);
      }

      this.stack[this.length++] = add;
   }

   void add(MoreContents more) {
      this.moreContents = (Stack.MoreContentsHolder[])Arrays.copyOf(this.moreContents, this.moreContents.length + 1);
      this.moreContents[this.moreContents.length - 1] = new Stack.MoreContentsHolder(more, this.length);
   }

   private static <E> E[] resize(E[] array) {
      int newLen = array.length == 0?5:array.length * 2;
      return Arrays.copyOf(array, newLen);
   }

   void refill(Stack prefix, Stack.MoreContentsHolder holder, int index) {
      this.moreContents = (Stack.MoreContentsHolder[])splice(prefix.moreContents, prefix.moreContents.length, this.moreContents, index, this.moreContents.length);
      this.stack = (Transformation[])splice(prefix.stack, prefix.length, this.stack, holder.length, this.length);
      this.length += prefix.length - holder.length;
      holder.length = prefix.length;
   }

   private static <E> E[] splice(E[] prefix, int prefixCount, E[] keep, int keepFrom, int keepTo) {
      int keepCount = keepTo - keepFrom;
      int newCount = prefixCount + keepCount;
      if(newCount > keep.length) {
         keep = Arrays.copyOf(keep, newCount);
      }

      if(keepFrom != prefixCount) {
         System.arraycopy(keep, keepFrom, keep, prefixCount, keepCount);
      }

      if(prefixCount != 0) {
         System.arraycopy(prefix, 0, keep, 0, prefixCount);
      }

      return keep;
   }

   static class MoreContentsHolder implements AutoCloseable {
      MoreContents moreContents;
      int length;

      private MoreContentsHolder(MoreContents moreContents, int length) {
         this.moreContents = moreContents;
         this.length = length;
      }

      public void close() {
         this.moreContents.close();
      }
   }
}
