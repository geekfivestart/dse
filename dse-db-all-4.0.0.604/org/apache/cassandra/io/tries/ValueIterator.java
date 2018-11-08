package org.apache.cassandra.io.tries;

import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.utils.ByteSource;

public class ValueIterator<Concrete extends ValueIterator<Concrete>> extends Walker<Concrete> {
   private final ByteSource limit;
   private ValueIterator.IterationPosition stack;
   private long next;

   protected ValueIterator(Rebufferer source, long root, Rebufferer.ReaderConstraint rc) {
      super(source, root, rc);
      this.stack = new ValueIterator.IterationPosition(root, -1, 256, (ValueIterator.IterationPosition)null);
      this.limit = null;

      try {
         this.go(root);
         if(this.payloadFlags() != 0) {
            this.next = root;
         } else {
            this.next = this.advanceNode();
         }

      } catch (Throwable var6) {
         super.close();
         throw var6;
      }
   }

   protected ValueIterator(Rebufferer source, long root, ByteSource start, ByteSource end, boolean admitPrefix, Rebufferer.ReaderConstraint rc) {
      super(source, root, rc);
      this.limit = end;
      ValueIterator.IterationPosition prev = null;
      boolean atLimit = true;
      long payloadedNode = -1L;

      try {
         start.reset();
         end.reset();
         this.go(root);

         while(true) {
            int s = start.next();
            int childIndex = this.search(s);
            if(admitPrefix) {
               if(childIndex != 0 && childIndex != -1) {
                  payloadedNode = -1L;
               } else if(this.payloadFlags() != 0) {
                  payloadedNode = this.position;
               }
            }

            int limitByte = 256;
            if(atLimit) {
               limitByte = end.next();
               if(s < limitByte) {
                  atLimit = false;
               }
            }

            if(childIndex < 0) {
               childIndex = -1 - childIndex - 1;
               this.stack = new ValueIterator.IterationPosition(this.position, childIndex, limitByte, prev);
               if(payloadedNode != -1L) {
                  this.next = payloadedNode;
               } else {
                  this.next = this.advanceNode();
               }

               return;
            }

            prev = new ValueIterator.IterationPosition(this.position, childIndex, limitByte, prev);
            this.go(this.transition(childIndex));
         }
      } catch (Throwable var15) {
         super.close();
         throw var15;
      }
   }

   protected long nextPayloadedNode() {
      long toReturn = this.next;
      if(this.next != -1L) {
         this.next = this.advanceNode();
      }

      return toReturn;
   }

   private long advanceNode() {
      this.go(this.stack.node);

      while(true) {
         while(true) {
            int childIndex = this.stack.childIndex + 1;
            int transitionByte = this.transitionByte(childIndex);
            if(transitionByte > this.stack.limit) {
               this.stack = this.stack.prev;
               if(this.stack == null) {
                  return -1L;
               }

               this.go(this.stack.node);
            } else {
               long child = this.transition(childIndex);
               if(child != -1L) {
                  assert child >= 0L : String.format("Expected value >= 0 but got %d - %s", new Object[]{Long.valueOf(child), this});

                  this.go(child);
                  int l = 256;
                  if(transitionByte == this.stack.limit) {
                     l = this.limit.next();
                  }

                  this.stack.childIndex = childIndex;
                  this.stack = new ValueIterator.IterationPosition(child, -1, l, this.stack);
                  if(this.payloadFlags() != 0) {
                     return child;
                  }
               } else {
                  this.stack.childIndex = childIndex;
               }
            }
         }
      }
   }

   static class IterationPosition {
      long node;
      int childIndex;
      int limit;
      ValueIterator.IterationPosition prev;

      IterationPosition(long node, int childIndex, int limit, ValueIterator.IterationPosition prev) {
         this.node = node;
         this.childIndex = childIndex;
         this.limit = limit;
         this.prev = prev;
      }

      public String toString() {
         return String.format("[Node %d, child %d, limit %d]", new Object[]{Long.valueOf(this.node), Integer.valueOf(this.childIndex), Integer.valueOf(this.limit)});
      }
   }
}
