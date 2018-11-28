package org.apache.cassandra.db;

import com.google.common.hash.Hasher;
import java.util.Objects;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.HashingUtils;

public class LivenessInfo {
   public static final long NO_TIMESTAMP = -9223372036854775808L;
   public static final int NO_TTL = 0;
   public static final int EXPIRED_LIVENESS_TTL = 2147483647;
   public static final int NO_EXPIRATION_TIME = 2147483647;
   public static final LivenessInfo EMPTY = new LivenessInfo(-9223372036854775808L);
   protected final long timestamp;

   protected LivenessInfo(long timestamp) {
      this.timestamp = timestamp;
   }

   public static LivenessInfo create(long timestamp, int nowInSec) {
      return new LivenessInfo(timestamp);
   }

   public static LivenessInfo expiring(long timestamp, int ttl, int nowInSec) {
      assert ttl != 2147483647;

      return new LivenessInfo.ExpiringLivenessInfo(timestamp, ttl, ExpirationDateOverflowHandling.computeLocalExpirationTime(nowInSec, ttl));
   }

   public static LivenessInfo create(long timestamp, int ttl, int nowInSec) {
      return ttl == 0?create(timestamp, nowInSec):expiring(timestamp, ttl, nowInSec);
   }

   public static LivenessInfo withExpirationTime(long timestamp, int ttl, int localExpirationTime) {
      return (LivenessInfo)(ttl == 2147483647?new LivenessInfo.ExpiredLivenessInfo(timestamp, ttl, localExpirationTime):(ttl == 0?new LivenessInfo(timestamp):new LivenessInfo.ExpiringLivenessInfo(timestamp, ttl, localExpirationTime)));
   }

   public boolean isEmpty() {
      return this.timestamp == -9223372036854775808L;
   }

   public long timestamp() {
      return this.timestamp;
   }

   public boolean isExpiring() {
      return false;
   }

   public int ttl() {
      return 0;
   }

   public int localExpirationTime() {
      return 2147483647;
   }

   public boolean isLive(int nowInSec) {
      return !this.isEmpty();
   }

   public void digest(Hasher hasher) {
      HashingUtils.updateWithLong(hasher, this.timestamp());
   }

   public void validate() {
   }

   public int dataSize() {
      return TypeSizes.sizeof(this.timestamp());
   }

   public boolean supersedes(LivenessInfo other) {
      return this.timestamp != other.timestamp?this.timestamp > other.timestamp:(this.isExpired() ^ other.isExpired()?this.isExpired():(this.isExpiring() == other.isExpiring()?this.localExpirationTime() > other.localExpirationTime():this.isExpiring()));
   }

   protected boolean isExpired() {
      return false;
   }

   public LivenessInfo withUpdatedTimestamp(long newTimestamp) {
      return new LivenessInfo(newTimestamp);
   }

   public LivenessInfo withUpdatedTimestampAndLocalDeletionTime(long newTimestamp, int newLocalDeletionTime) {
      return create(newTimestamp, this.ttl(), newLocalDeletionTime);
   }

   public String toString() {
      return String.format("[ts=%d]", new Object[]{Long.valueOf(this.timestamp)});
   }

   public boolean equals(Object other) {
      if(!(other instanceof LivenessInfo)) {
         return false;
      } else {
         LivenessInfo that = (LivenessInfo)other;
         return this.timestamp() == that.timestamp() && this.ttl() == that.ttl() && this.localExpirationTime() == that.localExpirationTime();
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{Long.valueOf(this.timestamp()), Integer.valueOf(this.ttl()), Integer.valueOf(this.localExpirationTime())});
   }

   private static class ExpiringLivenessInfo extends LivenessInfo {
      private final int ttl;
      private final int localExpirationTime;

      private ExpiringLivenessInfo(long timestamp, int ttl, int localExpirationTime) {
         super(timestamp);

         assert ttl != 0 && localExpirationTime != 2147483647;

         this.ttl = ttl;
         this.localExpirationTime = localExpirationTime;
      }

      public int ttl() {
         return this.ttl;
      }

      public int localExpirationTime() {
         return this.localExpirationTime;
      }

      public boolean isExpiring() {
         return true;
      }

      public boolean isLive(int nowInSec) {
         return nowInSec < this.localExpirationTime;
      }

      public void digest(Hasher hasher) {
         super.digest(hasher);
         HashingUtils.updateWithInt(hasher, this.localExpirationTime);
         HashingUtils.updateWithInt(hasher, this.ttl);
      }

      public void validate() {
         if(this.ttl < 0) {
            throw new MarshalException("A TTL should not be negative");
         } else if(this.localExpirationTime < 0) {
            throw new MarshalException("A local expiration time should not be negative");
         }
      }

      public int dataSize() {
         return super.dataSize() + TypeSizes.sizeof(this.ttl) + TypeSizes.sizeof(this.localExpirationTime);
      }

      public LivenessInfo withUpdatedTimestamp(long newTimestamp) {
         return new LivenessInfo.ExpiringLivenessInfo(newTimestamp, this.ttl, this.localExpirationTime);
      }

      public String toString() {
         return String.format("[ts=%d ttl=%d, let=%d]", new Object[]{Long.valueOf(this.timestamp), Integer.valueOf(this.ttl), Integer.valueOf(this.localExpirationTime)});
      }
   }

   private static class ExpiredLivenessInfo extends LivenessInfo.ExpiringLivenessInfo {
      private ExpiredLivenessInfo(long timestamp, int ttl, int localExpirationTime) {
         super(timestamp, ttl, localExpirationTime);

         assert ttl == 2147483647;

         assert timestamp != -9223372036854775808L;

      }

      public boolean isExpired() {
         return true;
      }

      public boolean isLive(int nowInSec) {
         return false;
      }

      public LivenessInfo withUpdatedTimestamp(long newTimestamp) {
         return new LivenessInfo.ExpiredLivenessInfo(newTimestamp, this.ttl(), this.localExpirationTime());
      }
   }
}
