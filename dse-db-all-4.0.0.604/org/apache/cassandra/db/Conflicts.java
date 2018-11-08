package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.utils.FastByteOperations;

public abstract class Conflicts {
   private Conflicts() {
   }

   public static Conflicts.Resolution resolveRegular(long leftTimestamp, boolean leftLive, int leftLocalDeletionTime, ByteBuffer leftValue, long rightTimestamp, boolean rightLive, int rightLocalDeletionTime, ByteBuffer rightValue) {
      if(leftTimestamp != rightTimestamp) {
         return leftTimestamp < rightTimestamp?Conflicts.Resolution.RIGHT_WINS:Conflicts.Resolution.LEFT_WINS;
      } else if(leftLive != rightLive) {
         return leftLive?Conflicts.Resolution.RIGHT_WINS:Conflicts.Resolution.LEFT_WINS;
      } else {
         int c = FastByteOperations.compareUnsigned(leftValue, rightValue);
         return c < 0?Conflicts.Resolution.RIGHT_WINS:(c > 0?Conflicts.Resolution.LEFT_WINS:(leftLocalDeletionTime < rightLocalDeletionTime?Conflicts.Resolution.RIGHT_WINS:Conflicts.Resolution.LEFT_WINS));
      }
   }

   public static Conflicts.Resolution resolveCounter(long leftTimestamp, boolean leftLive, ByteBuffer leftValue, long rightTimestamp, boolean rightLive, ByteBuffer rightValue) {
      return !leftLive?(!rightLive && leftTimestamp <= rightTimestamp?Conflicts.Resolution.RIGHT_WINS:Conflicts.Resolution.LEFT_WINS):(!rightLive?Conflicts.Resolution.RIGHT_WINS:(leftValue.hasRemaining()?(!rightValue.hasRemaining()?Conflicts.Resolution.RIGHT_WINS:Conflicts.Resolution.MERGE):(!rightValue.hasRemaining() && leftTimestamp <= rightTimestamp?Conflicts.Resolution.RIGHT_WINS:Conflicts.Resolution.LEFT_WINS)));
   }

   public static ByteBuffer mergeCounterValues(ByteBuffer left, ByteBuffer right) {
      return CounterContext.instance().merge(left, right);
   }

   public static enum Resolution {
      LEFT_WINS,
      MERGE,
      RIGHT_WINS;

      private Resolution() {
      }
   }
}
