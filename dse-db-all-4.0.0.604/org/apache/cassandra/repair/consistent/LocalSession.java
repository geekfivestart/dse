package org.apache.cassandra.repair.consistent;

import com.google.common.base.Preconditions;
import org.apache.cassandra.utils.FBUtilities;

public class LocalSession extends ConsistentSession {
   public final int startedAt;
   private volatile int lastUpdate;

   public LocalSession(LocalSession.Builder builder) {
      super(builder);
      this.startedAt = builder.startedAt;
      this.lastUpdate = builder.lastUpdate;
   }

   public boolean isCompleted() {
      ConsistentSession.State s = this.getState();
      return s == ConsistentSession.State.FINALIZED || s == ConsistentSession.State.FAILED;
   }

   public int getStartedAt() {
      return this.startedAt;
   }

   public int getLastUpdate() {
      return this.lastUpdate;
   }

   public void setLastUpdate() {
      this.lastUpdate = FBUtilities.nowInSeconds();
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(o != null && this.getClass() == o.getClass()) {
         if(!super.equals(o)) {
            return false;
         } else {
            LocalSession session = (LocalSession)o;
            return this.startedAt != session.startedAt?false:this.lastUpdate == session.lastUpdate;
         }
      } else {
         return false;
      }
   }

   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + this.startedAt;
      result = 31 * result + this.lastUpdate;
      return result;
   }

   public String toString() {
      return "LocalSession{sessionID=" + this.sessionID + ", state=" + this.getState() + ", coordinator=" + this.coordinator + ", tableIds=" + this.tableIds + ", repairedAt=" + this.repairedAt + ", ranges=" + this.ranges + ", participants=" + this.participants + ", startedAt=" + this.startedAt + ", lastUpdate=" + this.lastUpdate + '}';
   }

   public static LocalSession.Builder builder() {
      return new LocalSession.Builder();
   }

   public static class Builder extends ConsistentSession.AbstractBuilder<LocalSession.Builder> {
      private int startedAt;
      private int lastUpdate;

      public Builder() {
      }

      public LocalSession.Builder withStartedAt(int startedAt) {
         this.startedAt = startedAt;
         return this;
      }

      public LocalSession.Builder withLastUpdate(int lastUpdate) {
         this.lastUpdate = lastUpdate;
         return this;
      }

      void validate() {
         super.validate();
         Preconditions.checkArgument(this.startedAt > 0);
         Preconditions.checkArgument(this.lastUpdate > 0);
      }

      public LocalSession build() {
         this.validate();
         return new LocalSession(this);
      }
   }
}
