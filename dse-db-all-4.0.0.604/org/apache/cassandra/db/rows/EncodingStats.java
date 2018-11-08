package org.apache.cassandra.db.rows;

import java.io.IOException;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Locale;
import java.util.Objects;
import java.util.TimeZone;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.partitions.PartitionStatisticsCollector;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class EncodingStats {
   private static final long TIMESTAMP_EPOCH;
   private static final int DELETION_TIME_EPOCH;
   private static final int TTL_EPOCH = 0;
   public static final EncodingStats NO_STATS;
   public static final EncodingStats.Serializer serializer;
   public final long minTimestamp;
   public final int minLocalDeletionTime;
   public final int minTTL;

   public EncodingStats(long minTimestamp, int minLocalDeletionTime, int minTTL) {
      this.minTimestamp = minTimestamp == -9223372036854775808L?TIMESTAMP_EPOCH:minTimestamp;
      this.minLocalDeletionTime = minLocalDeletionTime == 2147483647?DELETION_TIME_EPOCH:minLocalDeletionTime;
      this.minTTL = minTTL;
   }

   public EncodingStats mergeWith(EncodingStats that) {
      long minTimestamp = this.minTimestamp == TIMESTAMP_EPOCH?that.minTimestamp:(that.minTimestamp == TIMESTAMP_EPOCH?this.minTimestamp:Math.min(this.minTimestamp, that.minTimestamp));
      int minDelTime = this.minLocalDeletionTime == DELETION_TIME_EPOCH?that.minLocalDeletionTime:(that.minLocalDeletionTime == DELETION_TIME_EPOCH?this.minLocalDeletionTime:Math.min(this.minLocalDeletionTime, that.minLocalDeletionTime));
      int minTTL = this.minTTL == 0?that.minTTL:(that.minTTL == 0?this.minTTL:Math.min(this.minTTL, that.minTTL));
      return new EncodingStats(minTimestamp, minDelTime, minTTL);
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(o != null && this.getClass() == o.getClass()) {
         EncodingStats that = (EncodingStats)o;
         return this.minLocalDeletionTime == that.minLocalDeletionTime && this.minTTL == that.minTTL && this.minTimestamp == that.minTimestamp;
      } else {
         return false;
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{Long.valueOf(this.minTimestamp), Integer.valueOf(this.minLocalDeletionTime), Integer.valueOf(this.minTTL)});
   }

   public String toString() {
      return String.format("EncodingStats(ts=%d, ldt=%d, ttl=%d)", new Object[]{Long.valueOf(this.minTimestamp), Integer.valueOf(this.minLocalDeletionTime), Integer.valueOf(this.minTTL)});
   }

   static {
      Calendar c = Calendar.getInstance(TimeZone.getTimeZone("GMT-0"), Locale.US);
      c.set(1, 2015);
      c.set(2, 8);
      c.set(5, 22);
      c.set(11, 0);
      c.set(12, 0);
      c.set(13, 0);
      c.set(14, 0);
      TIMESTAMP_EPOCH = c.getTimeInMillis() * 1000L;
      DELETION_TIME_EPOCH = (int)(c.getTimeInMillis() / 1000L);
      NO_STATS = new EncodingStats(TIMESTAMP_EPOCH, DELETION_TIME_EPOCH, 0);
      serializer = new EncodingStats.Serializer();
   }

   public static class Serializer {
      public Serializer() {
      }

      public void serialize(EncodingStats stats, DataOutputPlus out) throws IOException {
         out.writeUnsignedVInt(stats.minTimestamp - EncodingStats.TIMESTAMP_EPOCH);
         out.writeUnsignedVInt((long)(stats.minLocalDeletionTime - EncodingStats.DELETION_TIME_EPOCH));
         out.writeUnsignedVInt((long)(stats.minTTL - 0));
      }

      public int serializedSize(EncodingStats stats) {
         return TypeSizes.sizeofUnsignedVInt(stats.minTimestamp - EncodingStats.TIMESTAMP_EPOCH) + TypeSizes.sizeofUnsignedVInt((long)(stats.minLocalDeletionTime - EncodingStats.DELETION_TIME_EPOCH)) + TypeSizes.sizeofUnsignedVInt((long)(stats.minTTL - 0));
      }

      public EncodingStats deserialize(DataInputPlus in) throws IOException {
         long minTimestamp = in.readUnsignedVInt() + EncodingStats.TIMESTAMP_EPOCH;
         int minLocalDeletionTime = (int)in.readUnsignedVInt() + EncodingStats.DELETION_TIME_EPOCH;
         int minTTL = (int)in.readUnsignedVInt() + 0;
         return new EncodingStats(minTimestamp, minLocalDeletionTime, minTTL);
      }
   }

   public static class Collector implements PartitionStatisticsCollector {
      private boolean isTimestampSet;
      private long minTimestamp = 9223372036854775807L;
      private boolean isDelTimeSet;
      private int minDeletionTime = 2147483647;
      private boolean isTTLSet;
      private int minTTL = 2147483647;

      public Collector() {
      }

      public void update(LivenessInfo info) {
         if(!info.isEmpty()) {
            this.updateTimestamp(info.timestamp());
            if(info.isExpiring()) {
               this.updateTTL(info.ttl());
               this.updateLocalDeletionTime(info.localExpirationTime());
            }

         }
      }

      public void update(ColumnData columnData) {
      }

      public void update(Cell cell) {
         this.updateTimestamp(cell.timestamp());
         if(cell.isExpiring()) {
            this.updateTTL(cell.ttl());
            this.updateLocalDeletionTime(cell.localDeletionTime());
         } else if(cell.isTombstone()) {
            this.updateLocalDeletionTime(cell.localDeletionTime());
         }

      }

      public void update(DeletionTime deletionTime) {
         if(!deletionTime.isLive()) {
            this.updateTimestamp(deletionTime.markedForDeleteAt());
            this.updateLocalDeletionTime(deletionTime.localDeletionTime());
         }
      }

      public void updateTimestamp(long timestamp) {
         this.isTimestampSet = true;
         this.minTimestamp = Math.min(this.minTimestamp, timestamp);
      }

      public void updateLocalDeletionTime(int deletionTime) {
         this.isDelTimeSet = true;
         this.minDeletionTime = Math.min(this.minDeletionTime, deletionTime);
      }

      public void updateTTL(int ttl) {
         this.isTTLSet = true;
         this.minTTL = Math.min(this.minTTL, ttl);
      }

      public void updateRowStats() {
      }

      public void updateHasLegacyCounterShards(boolean hasLegacyCounterShards) {
      }

      public EncodingStats get() {
         return new EncodingStats(this.isTimestampSet?this.minTimestamp:EncodingStats.TIMESTAMP_EPOCH, this.isDelTimeSet?this.minDeletionTime:EncodingStats.DELETION_TIME_EPOCH, this.isTTLSet?this.minTTL:0);
      }

      public static EncodingStats collect(Row staticRow, Iterator<Row> rows, DeletionInfo deletionInfo) {
         EncodingStats.Collector collector = new EncodingStats.Collector();
         deletionInfo.collectStats(collector);
         if(!staticRow.isEmpty()) {
            Rows.collectStats(staticRow, collector);
         }

         while(rows.hasNext()) {
            Rows.collectStats((Row)rows.next(), collector);
         }

         return collector.get();
      }
   }

   public static class Merger {
      private long minTimestamp;
      private int minLocalDeletionTime;
      private int minTTL;

      public Merger(EncodingStats stats) {
         this.minTimestamp = stats.minTimestamp;
         this.minLocalDeletionTime = stats.minLocalDeletionTime;
         this.minTTL = stats.minTTL;
      }

      public void mergeWith(EncodingStats that) {
         this.minTimestamp = this.minTimestamp == EncodingStats.TIMESTAMP_EPOCH?that.minTimestamp:(that.minTimestamp == EncodingStats.TIMESTAMP_EPOCH?this.minTimestamp:Math.min(this.minTimestamp, that.minTimestamp));
         this.minLocalDeletionTime = this.minLocalDeletionTime == EncodingStats.DELETION_TIME_EPOCH?that.minLocalDeletionTime:(that.minLocalDeletionTime == EncodingStats.DELETION_TIME_EPOCH?this.minLocalDeletionTime:Math.min(this.minLocalDeletionTime, that.minLocalDeletionTime));
         this.minTTL = this.minTTL == 0?that.minTTL:(that.minTTL == 0?this.minTTL:Math.min(this.minTTL, that.minTTL));
      }

      public EncodingStats get() {
         return new EncodingStats(this.minTimestamp, this.minLocalDeletionTime, this.minTTL);
      }
   }
}
