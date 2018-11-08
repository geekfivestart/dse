package org.apache.cassandra.utils.concurrent;

import org.apache.cassandra.utils.Throwables;

public interface Transactional extends AutoCloseable {
   Throwable commit(Throwable var1);

   Throwable abort(Throwable var1);

   void prepareToCommit();

   void close();

   public abstract static class AbstractTransactional implements Transactional {
      private boolean permitRedundantTransitions;
      private volatile Transactional.AbstractTransactional.State state;

      public AbstractTransactional() {
         this.state = Transactional.AbstractTransactional.State.IN_PROGRESS;
      }

      protected abstract Throwable doCommit(Throwable var1);

      protected abstract Throwable doAbort(Throwable var1);

      protected Throwable doPreCleanup(Throwable accumulate) {
         return accumulate;
      }

      protected Throwable doPostCleanup(Throwable accumulate) {
         return accumulate;
      }

      protected abstract void doPrepare();

      public final Throwable commit(Throwable accumulate) {
         if(this.permitRedundantTransitions && this.state == Transactional.AbstractTransactional.State.COMMITTED) {
            return accumulate;
         } else if(this.state != Transactional.AbstractTransactional.State.READY_TO_COMMIT) {
            throw new IllegalStateException("Cannot commit unless READY_TO_COMMIT; state is " + this.state);
         } else {
            accumulate = this.doCommit(accumulate);
            accumulate = this.doPostCleanup(accumulate);
            this.state = Transactional.AbstractTransactional.State.COMMITTED;
            return accumulate;
         }
      }

      public final Throwable abort(Throwable accumulate) {
         if(this.state == Transactional.AbstractTransactional.State.ABORTED) {
            return accumulate;
         } else if(this.state == Transactional.AbstractTransactional.State.COMMITTED) {
            try {
               throw new IllegalStateException("Attempted to abort a committed operation");
            } catch (Throwable var3) {
               accumulate = Throwables.merge(accumulate, var3);
               return accumulate;
            }
         } else {
            this.state = Transactional.AbstractTransactional.State.ABORTED;
            accumulate = this.doPreCleanup(accumulate);
            accumulate = this.doAbort(accumulate);
            accumulate = this.doPostCleanup(accumulate);
            return accumulate;
         }
      }

      public final void close() {
         switch(null.$SwitchMap$org$apache$cassandra$utils$concurrent$Transactional$AbstractTransactional$State[this.state.ordinal()]) {
         default:
            this.abort();
         case 1:
         case 2:
         }
      }

      public final void prepareToCommit() {
         if(!this.permitRedundantTransitions || this.state != Transactional.AbstractTransactional.State.READY_TO_COMMIT) {
            if(this.state != Transactional.AbstractTransactional.State.IN_PROGRESS) {
               throw new IllegalStateException("Cannot prepare to commit unless IN_PROGRESS; state is " + this.state);
            } else {
               this.doPrepare();
               Throwables.maybeFail(this.doPreCleanup((Throwable)null));
               this.state = Transactional.AbstractTransactional.State.READY_TO_COMMIT;
            }
         }
      }

      public Object finish() {
         this.prepareToCommit();
         this.commit();
         return this;
      }

      public final void abort() {
         Throwables.maybeFail(this.abort((Throwable)null));
      }

      public final void commit() {
         Throwables.maybeFail(this.commit((Throwable)null));
      }

      public final Transactional.AbstractTransactional.State state() {
         return this.state;
      }

      protected void permitRedundantTransitions() {
         this.permitRedundantTransitions = true;
      }

      public static enum State {
         IN_PROGRESS,
         READY_TO_COMMIT,
         COMMITTED,
         ABORTED;

         private State() {
         }
      }
   }
}
