package com.datastax.bdp.db.nodesync;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.metrics.NodeSyncMetrics;
import org.apache.cassandra.repair.SystemDistributedKeyspace;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.units.SizeUnit;
import org.apache.cassandra.utils.units.Units;

public class ValidationMetrics implements Serializable {
   private static final long serialVersionUID = 42L;
   private final long startTime = NodeSyncHelpers.time().currentTimeMillis();
   private final long[] pagesByOutcome = new long[ValidationOutcome.values().length];
   private long objectsValidated;
   private long objectsRepaired;
   private long dataValidated;
   private long dataRepaired;
   private long repairDataSent;
   private long repairObjectsSent;

   ValidationMetrics() {
   }

   void addPageOutcome(ValidationOutcome outcome) {
      ++this.pagesByOutcome[outcome.ordinal()];
   }

   void addRepair(PartitionUpdate update) {
      this.repairDataSent += (long)update.dataSize();
      this.repairObjectsSent += (long)update.operationCount();
   }

   void addDataValidated(int size, boolean isConsistent) {
      this.dataValidated += (long)size;
      if(!isConsistent) {
         this.dataRepaired += (long)size;
      }

   }

   void incrementRowsRead(boolean isConsistent) {
      ++this.objectsValidated;
      if(!isConsistent) {
         ++this.objectsRepaired;
      }

   }

   void incrementRangeTombstoneMarkersRead(boolean isConsistent) {
      ++this.objectsValidated;
      if(!isConsistent) {
         ++this.objectsRepaired;
      }

   }

   long dataValidated() {
      return this.dataValidated;
   }

   long dataRepaired() {
      return this.dataRepaired;
   }

   void addTo(NodeSyncMetrics metrics) {
      ValidationOutcome[] var2 = ValidationOutcome.values();
      int var3 = var2.length;

      for(int var4 = 0; var4 < var3; ++var4) {
         ValidationOutcome outcome = var2[var4];
         metrics.addPageOutcomes(outcome, this.pagesByOutcome[outcome.ordinal()]);
      }

      metrics.incrementObjects(this.objectsValidated, this.objectsRepaired);
      metrics.incrementDataSizes(this.dataValidated, this.dataRepaired);
      metrics.incrementRepairSent(this.repairDataSent, this.repairObjectsSent);
   }

   static ValidationMetrics merge(ValidationMetrics m1, ValidationMetrics m2) {
      ValidationMetrics result = new ValidationMetrics();
      int s = result.pagesByOutcome.length;
      System.arraycopy(m1.pagesByOutcome, 0, result.pagesByOutcome, 0, s);

      for(int i = 0; i < s; ++i) {
         result.pagesByOutcome[i] += m2.pagesByOutcome[i];
      }

      result.objectsValidated = m1.objectsValidated + m2.objectsValidated;
      result.objectsRepaired = m1.objectsRepaired + m2.objectsRepaired;
      result.dataValidated = m1.dataValidated + m2.dataValidated;
      result.dataRepaired = m1.dataRepaired + m2.dataRepaired;
      result.repairDataSent = m1.repairDataSent + m2.repairDataSent;
      result.repairObjectsSent = m1.repairObjectsSent + m2.repairObjectsSent;
      return result;
   }

   public static ValidationMetrics fromBytes(ByteBuffer bytes) {
      UserType type = SystemDistributedKeyspace.NodeSyncMetrics;
      ByteBuffer[] values = type.split(bytes);
      if(values.length < type.size()) {
         throw new IllegalArgumentException(String.format("Invalid number of components for nodesync_metrics, expected %d but got %d", new Object[]{Integer.valueOf(type.size()), Integer.valueOf(values.length)}));
      } else {
         try {
            ValidationMetrics m = new ValidationMetrics();
            m.dataValidated = ((Long)type.composeField(0, values[0])).longValue();
            m.dataRepaired = ((Long)type.composeField(1, values[1])).longValue();
            m.objectsValidated = ((Long)type.composeField(2, values[2])).longValue();
            m.objectsRepaired = ((Long)type.composeField(3, values[3])).longValue();
            m.repairDataSent = ((Long)type.composeField(4, values[4])).longValue();
            m.repairObjectsSent = ((Long)type.composeField(5, values[5])).longValue();
            Map<String, Long> outcomesMap = (Map)type.composeField(6, values[6]);
            ValidationOutcome[] var5 = ValidationOutcome.values();
            int var6 = var5.length;

            for(int var7 = 0; var7 < var6; ++var7) {
               ValidationOutcome outcome = var5[var7];
               m.pagesByOutcome[outcome.ordinal()] = ((Long)outcomesMap.getOrDefault(outcome.toString(), Long.valueOf(0L))).longValue();
            }

            return m;
         } catch (MarshalException var9) {
            throw new IllegalArgumentException("Error deserializing nodesync_metrics from " + ByteBufferUtil.toDebugHexString(bytes), var9);
         }
      }
   }

   public ByteBuffer toBytes() {
      UserType type = SystemDistributedKeyspace.NodeSyncMetrics;
      ByteBuffer[] values = new ByteBuffer[type.size()];
      values[0] = type.decomposeField(0, Long.valueOf(this.dataValidated));
      values[1] = type.decomposeField(1, Long.valueOf(this.dataRepaired));
      values[2] = type.decomposeField(2, Long.valueOf(this.objectsValidated));
      values[3] = type.decomposeField(3, Long.valueOf(this.objectsRepaired));
      values[4] = type.decomposeField(4, Long.valueOf(this.repairDataSent));
      values[5] = type.decomposeField(5, Long.valueOf(this.repairObjectsSent));
      values[6] = type.decomposeField(6, ValidationOutcome.toMap(this.pagesByOutcome));
      return UserType.buildValue(values);
   }

   String toDebugString() {
      StringBuilder sb = new StringBuilder();
      long duration = NodeSyncHelpers.time().currentTimeMillis() - this.startTime;
      sb.append("duration=").append(duration).append("ms");
      if(duration > 1000L) {
         sb.append(duration > 1000000L?" (~":" (").append(Units.toString(duration, TimeUnit.MILLISECONDS)).append(')');
      }

      sb.append(", pages={ ");
      int i = 0;
      ValidationOutcome[] var5 = ValidationOutcome.values();
      int var6 = var5.length;

      for(int var7 = 0; var7 < var6; ++var7) {
         ValidationOutcome outcome = var5[var7];
         if(this.pagesByOutcome[outcome.ordinal()] != 0L) {
            sb.append(i++ == 0?"":", ");
            sb.append(outcome).append(": ").append(this.pagesByOutcome[outcome.ordinal()]);
         }
      }

      sb.append(" }, ");
      sb.append("objects={ validated: ").append(this.objectsValidated).append(", repaired: ").append(this.objectsRepaired).append("}, ");
      sb.append("data={ validated: ").append(Units.toLogString(this.dataValidated, SizeUnit.BYTES)).append(", repaired: ").append(this.dataRepaired).append("}, ");
      sb.append("repair sent={ data: ").append(Units.toLogString(this.repairDataSent, SizeUnit.BYTES)).append(", objects: ").append(this.repairObjectsSent).append('}');
      return sb.toString();
   }

   public String toString() {
      long totalPages = LongStream.of(this.pagesByOutcome).sum();
      long partialPages = this.pagesByOutcome[ValidationOutcome.PARTIAL_IN_SYNC.ordinal()] + this.pagesByOutcome[ValidationOutcome.PARTIAL_REPAIRED.ordinal()];
      long uncompletedPages = this.pagesByOutcome[ValidationOutcome.UNCOMPLETED.ordinal()];
      long failedPages = this.pagesByOutcome[ValidationOutcome.FAILED.ordinal()];
      String partialString = partialPages == 0L?"":String.format("; %d%% only partially validated/repaired", new Object[]{Integer.valueOf(this.percent(partialPages, totalPages))});
      String uncompletedString = uncompletedPages == 0L?"":String.format("; %d%% uncompleted", new Object[]{Integer.valueOf(this.percent(uncompletedPages, totalPages))});
      String failedString = failedPages == 0L?"":String.format("; %d%% failed", new Object[]{Integer.valueOf(this.percent(failedPages, totalPages))});
      String repairString = this.repairDataSent == 0L?"everything was in sync":String.format("%d repaired (%d%%); %s of repair data sent", new Object[]{Long.valueOf(this.objectsRepaired), Integer.valueOf(this.percent(this.objectsRepaired, this.objectsValidated)), Units.toString(this.repairDataSent, SizeUnit.BYTES)});
      return String.format("validated %s - %s%s%s%s", new Object[]{Units.toString(this.dataValidated, SizeUnit.BYTES), repairString, partialString, uncompletedString, failedString});
   }

   private int percent(long value, long total) {
      return total == 0L?0:(int)(value * 100L / total);
   }
}
