package org.apache.cassandra.db.commitlog;

import java.io.IOException;
import org.apache.cassandra.db.Mutation;

public interface CommitLogReadHandler {
   boolean shouldSkipSegmentOnError(CommitLogReadHandler.CommitLogReadException var1) throws IOException;

   void handleUnrecoverableError(CommitLogReadHandler.CommitLogReadException var1);

   void handleMutation(Mutation var1, int var2, int var3, CommitLogDescriptor var4);

   public static class CommitLogReadException extends IOException {
      public final CommitLogReadHandler.CommitLogReadErrorReason reason;
      public final boolean permissible;

      CommitLogReadException(String message, CommitLogReadHandler.CommitLogReadErrorReason reason, boolean permissible) {
         super(message);
         this.reason = reason;
         this.permissible = permissible;
      }
   }

   public static enum CommitLogReadErrorReason {
      RECOVERABLE_DESCRIPTOR_ERROR,
      UNRECOVERABLE_DESCRIPTOR_ERROR,
      MUTATION_ERROR,
      UNRECOVERABLE_UNKNOWN_ERROR,
      EOF;

      private CommitLogReadErrorReason() {
      }
   }
}
