package org.apache.cassandra.utils.concurrent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpOrderThreaded implements OpOrder {
   private static final Logger logger = LoggerFactory.getLogger(OpOrderThreaded.class);
   final OpOrderThreaded.ThreadIdentifier mapper;
   final Object creator;
   private volatile OpOrderSimple.Group[] current;

   public OpOrderThreaded(Object creator, OpOrderThreaded.ThreadIdentifier mapper, int idLimit) {
      this.mapper = mapper;
      this.creator = creator;
      OpOrderSimple.Group[] groups = new OpOrderSimple.Group[idLimit];

      for(int i = 0; i < idLimit; ++i) {
         groups[i] = new OpOrderSimple.Group();
      }

      this.current = groups;
   }

   public OpOrder.Group start() {
      int coreId = this.mapper.idFor(Thread.currentThread());

      OpOrderSimple.Group current;
      do {
         current = this.current[coreId];
      } while(!current.register());

      return current;
   }

   public OpOrderThreaded.Barrier newBarrier() {
      return new OpOrderThreaded.Barrier();
   }

   public String toString() {
      return String.format("OpOrderThreaded {} with creator {}/{}", new Object[]{Integer.valueOf(this.hashCode()), this.creator.getClass().getName(), Integer.valueOf(this.creator.hashCode())});
   }

   public final class Barrier implements OpOrder.Barrier {
      private OpOrderSimple.Group[] orderOnOrBefore = null;

      public Barrier() {
      }

      public boolean isAfter(OpOrder.Group group) {
         return this.orderOnOrBefore == null?true:this.orderOnOrBefore[0].id - ((OpOrderSimple.Group)group).id >= 0L;
      }

      public void issue() {
         if(this.orderOnOrBefore != null) {
            throw new IllegalStateException("Can only call issue() once on each Barrier");
         } else {
            OpOrderThreaded var2 = OpOrderThreaded.this;
            OpOrderSimple.Group[] current;
            int i;
            synchronized(OpOrderThreaded.this) {
               current = OpOrderThreaded.this.current;
               this.orderOnOrBefore = current;
               OpOrderSimple.Group[] groups = new OpOrderSimple.Group[current.length];
               i = 0;

               while(true) {
                  if(i >= current.length) {
                     OpOrderThreaded.this.current = groups;
                     break;
                  }

                  groups[i] = current[i].next = new OpOrderSimple.Group(current[i]);
                  ++i;
               }
            }

            OpOrderSimple.Group[] var7 = current;
            int var8 = current.length;

            for(i = 0; i < var8; ++i) {
               OpOrderSimple.Group g = var7[i];
               g.expire();
            }

         }
      }

      public void markBlocking() {
         OpOrderSimple.Group[] var1 = this.orderOnOrBefore;
         int var2 = var1.length;

         for(int var3 = 0; var3 < var2; ++var3) {
            OpOrderSimple.Group g = var1[var3];
            this.markBlocking(g);
         }

      }

      private void markBlocking(OpOrderSimple.Group current) {
         while(current != null) {
            current.markBlocking();
            current = current.prev;
         }

      }

      public WaitQueue.Signal register() {
         WaitQueue.Signal[] signals = new WaitQueue.Signal[this.orderOnOrBefore.length];

         for(int i = 0; i < this.orderOnOrBefore.length; ++i) {
            signals[i] = this.orderOnOrBefore[i].waiting.register();
         }

         return WaitQueue.any(signals);
      }

      public boolean allPriorOpsAreFinished() {
         OpOrderSimple.Group[] current = this.orderOnOrBefore;
         if(current == null) {
            throw new IllegalStateException("This barrier needs to have issue() called on it before prior operations can complete");
         } else {
            OpOrderSimple.Group[] var2 = current;
            int var3 = current.length;

            for(int var4 = 0; var4 < var3; ++var4) {
               OpOrderSimple.Group g = var2[var4];
               if(g.next.prev != null) {
                  return false;
               }
            }

            return true;
         }
      }

      public void await() {
         assert OpOrderThreaded.this.mapper.barrierPermitted() : "Shouldn't have created a barrier for " + OpOrderThreaded.this.mapper + " on thread " + Thread.currentThread();

         OpOrderSimple.Group[] current = this.orderOnOrBefore;
         if(current == null) {
            throw new IllegalStateException("This barrier needs to have issue() called on it before prior operations can complete");
         } else {
            OpOrderSimple.Group[] var2 = current;
            int var3 = current.length;

            for(int var4 = 0; var4 < var3; ++var4) {
               OpOrderSimple.Group g = var2[var4];
               if(g.next.prev != null) {
                  WaitQueue.Signal signal = g.waiting.register();
                  if(g.next.prev != null) {
                     signal.awaitUninterruptibly();
                  } else {
                     signal.cancel();
                  }
               }
            }

         }
      }
   }

   public interface ThreadIdentifier {
      int idFor(Thread var1);

      boolean barrierPermitted();
   }
}
