package org.apache.cassandra.io.tries;

import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.utils.ByteSource;

public class ReverseValueIterator<Concrete extends ReverseValueIterator<Concrete>> extends Walker<Concrete> {
   final ByteSource limit;
   ReverseValueIterator.IterationPosition stack;
   long next;
   boolean reportingPrefixes;

   protected ReverseValueIterator(Rebufferer source, long root, Rebufferer.ReaderConstraint rc) {
      super(source, root, rc);
      this.stack = new ReverseValueIterator.IterationPosition(root, -1, 256, (ReverseValueIterator.IterationPosition)null);
      this.limit = null;
      this.next = this.advanceNode();
   }

   protected ReverseValueIterator(Rebufferer source, long root, ByteSource start, ByteSource end, boolean admitPrefix, Rebufferer.ReaderConstraint rc) {
      super(source, root, rc);
      this.limit = start;
      ReverseValueIterator.IterationPosition prev = null;
      boolean atLimit = true;
      this.reportingPrefixes = admitPrefix;
      start.reset();
      end.reset();
      this.go(root);

      while(true) {
         int s = end.next();
         int childIndex = this.search(s);
         int limitByte = -1;
         if(atLimit) {
            limitByte = start.next();
            if(s > limitByte) {
               atLimit = false;
            }
         }

         if(childIndex < 0) {
            childIndex = -1 - childIndex;
            this.stack = new ReverseValueIterator.IterationPosition(this.position, childIndex, limitByte, prev);
            this.next = this.advanceNode();
            return;
         }

         prev = new ReverseValueIterator.IterationPosition(this.position, childIndex, limitByte, prev);
         this.go(this.transition(childIndex));
      }
   }

   protected long nextPayloadedNode() {
      long toReturn = this.next;
      if(this.next != -1L) {
         this.next = this.advanceNode();
      }

      return toReturn;
   }

   long advanceNode() {
      if(this.stack == null) {
         return -1L;
      } else {
         this.go(this.stack.node);

         while(true) {
            int childIdx = this.stack.childIndex - 1;
            boolean beyondLimit = true;
            int transitionByte;
            if(childIdx >= 0) {
               transitionByte = this.transitionByte(childIdx);
               beyondLimit = transitionByte < this.stack.limit;
               if(beyondLimit) {
                  assert this.stack.limit >= 0;

                  this.reportingPrefixes = false;
               }
            } else {
               transitionByte = -2147483648;
            }

            if(beyondLimit) {
               ReverseValueIterator.IterationPosition stackTop = this.stack;
               this.stack = this.stack.prev;
               if(this.reportingPrefixes && this.payloadFlags() != 0) {
                  if(stackTop.limit >= 0) {
                     this.reportingPrefixes = false;
                  }

                  return stackTop.node;
               }

               if(this.stack == null) {
                  return -1L;
               }

               this.go(this.stack.node);
            } else {
               long child = this.transition(childIdx);
               if(child != -1L) {
                  this.go(child);
                  this.stack.childIndex = childIdx;
                  int l = -1;
                  if(transitionByte == this.stack.limit) {
                     l = this.limit.next();
                  }

                  this.stack = new ReverseValueIterator.IterationPosition(child, this.transitionRange(), l, this.stack);
               } else {
                  this.stack.childIndex = childIdx;
               }
            }
         }
      }
   }

   static class IterationPosition {
      long node;
      int childIndex;
      int limit;
      ReverseValueIterator.IterationPosition prev;

      public IterationPosition(long node, int childIndex, int limit, ReverseValueIterator.IterationPosition prev) {
         this.node = node;
         this.childIndex = childIndex;
         this.limit = limit;
         this.prev = prev;
      }
   }
}
