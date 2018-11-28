package org.apache.cassandra.io.sstable.metadata;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.IntervalSet;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.UUIDSerializer;
import org.apache.cassandra.utils.streamhist.TombstoneHistogram;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class StatsMetadata extends MetadataComponent {
   public static final IMetadataComponentSerializer serializer = new StatsMetadata.StatsMetadataSerializer();
   public static final ISerializer<IntervalSet<CommitLogPosition>> commitLogPositionSetSerializer;
   public final EstimatedHistogram estimatedPartitionSize;
   public final EstimatedHistogram estimatedColumnCount;
   public final IntervalSet<CommitLogPosition> commitLogIntervals;
   public final long minTimestamp;
   public final long maxTimestamp;
   public final int minLocalDeletionTime;
   public final int maxLocalDeletionTime;
   public final int minTTL;
   public final int maxTTL;
   public final double compressionRatio;
   public final TombstoneHistogram estimatedTombstoneDropTime;
   public final int sstableLevel;
   public final List<ByteBuffer> minClusteringValues;
   public final List<ByteBuffer> maxClusteringValues;
   public final boolean hasLegacyCounterShards;
   public final long repairedAt;
   public final long totalColumnsSet;
   public final long totalRows;
   public final UUID pendingRepair;

   public StatsMetadata(EstimatedHistogram estimatedPartitionSize, EstimatedHistogram estimatedColumnCount, IntervalSet<CommitLogPosition> commitLogIntervals, long minTimestamp, long maxTimestamp, int minLocalDeletionTime, int maxLocalDeletionTime, int minTTL, int maxTTL, double compressionRatio, TombstoneHistogram estimatedTombstoneDropTime, int sstableLevel, List<ByteBuffer> minClusteringValues, List<ByteBuffer> maxClusteringValues, boolean hasLegacyCounterShards, long repairedAt, long totalColumnsSet, long totalRows, UUID pendingRepair) {
      this.estimatedPartitionSize = estimatedPartitionSize;
      this.estimatedColumnCount = estimatedColumnCount;
      this.commitLogIntervals = commitLogIntervals;
      this.minTimestamp = minTimestamp;
      this.maxTimestamp = maxTimestamp;
      this.minLocalDeletionTime = minLocalDeletionTime;
      this.maxLocalDeletionTime = maxLocalDeletionTime;
      this.minTTL = minTTL;
      this.maxTTL = maxTTL;
      this.compressionRatio = compressionRatio;
      this.estimatedTombstoneDropTime = estimatedTombstoneDropTime;
      this.sstableLevel = sstableLevel;
      this.minClusteringValues = minClusteringValues;
      this.maxClusteringValues = maxClusteringValues;
      this.hasLegacyCounterShards = hasLegacyCounterShards;
      this.repairedAt = repairedAt;
      this.totalColumnsSet = totalColumnsSet;
      this.totalRows = totalRows;
      this.pendingRepair = pendingRepair;
   }

   public MetadataType getType() {
      return MetadataType.STATS;
   }

   public double getEstimatedDroppableTombstoneRatio(int gcBefore) {
      long estimatedColumnCount = this.estimatedColumnCount.mean() * this.estimatedColumnCount.count();
      if(estimatedColumnCount > 0L) {
         double droppable = this.getDroppableTombstonesBefore(gcBefore);
         return droppable / (double)estimatedColumnCount;
      } else {
         return 0.0D;
      }
   }

   public double getDroppableTombstonesBefore(int gcBefore) {
      return this.estimatedTombstoneDropTime.sum((double)gcBefore);
   }

   public StatsMetadata mutateLevel(int newLevel) {
      return new StatsMetadata(this.estimatedPartitionSize, this.estimatedColumnCount, this.commitLogIntervals, this.minTimestamp, this.maxTimestamp, this.minLocalDeletionTime, this.maxLocalDeletionTime, this.minTTL, this.maxTTL, this.compressionRatio, this.estimatedTombstoneDropTime, newLevel, this.minClusteringValues, this.maxClusteringValues, this.hasLegacyCounterShards, this.repairedAt, this.totalColumnsSet, this.totalRows, this.pendingRepair);
   }

   public StatsMetadata mutateRepairedAt(long newRepairedAt) {
      return new StatsMetadata(this.estimatedPartitionSize, this.estimatedColumnCount, this.commitLogIntervals, this.minTimestamp, this.maxTimestamp, this.minLocalDeletionTime, this.maxLocalDeletionTime, this.minTTL, this.maxTTL, this.compressionRatio, this.estimatedTombstoneDropTime, this.sstableLevel, this.minClusteringValues, this.maxClusteringValues, this.hasLegacyCounterShards, newRepairedAt, this.totalColumnsSet, this.totalRows, this.pendingRepair);
   }

   public StatsMetadata mutatePendingRepair(UUID newPendingRepair) {
      return new StatsMetadata(this.estimatedPartitionSize, this.estimatedColumnCount, this.commitLogIntervals, this.minTimestamp, this.maxTimestamp, this.minLocalDeletionTime, this.maxLocalDeletionTime, this.minTTL, this.maxTTL, this.compressionRatio, this.estimatedTombstoneDropTime, this.sstableLevel, this.minClusteringValues, this.maxClusteringValues, this.hasLegacyCounterShards, this.repairedAt, this.totalColumnsSet, this.totalRows, newPendingRepair);
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(o != null && this.getClass() == o.getClass()) {
         StatsMetadata that = (StatsMetadata)o;
         return (new EqualsBuilder()).append(this.estimatedPartitionSize, that.estimatedPartitionSize).append(this.estimatedColumnCount, that.estimatedColumnCount).append(this.commitLogIntervals, that.commitLogIntervals).append(this.minTimestamp, that.minTimestamp).append(this.maxTimestamp, that.maxTimestamp).append(this.minLocalDeletionTime, that.minLocalDeletionTime).append(this.maxLocalDeletionTime, that.maxLocalDeletionTime).append(this.minTTL, that.minTTL).append(this.maxTTL, that.maxTTL).append(this.compressionRatio, that.compressionRatio).append(this.estimatedTombstoneDropTime, that.estimatedTombstoneDropTime).append(this.sstableLevel, that.sstableLevel).append(this.repairedAt, that.repairedAt).append(this.maxClusteringValues, that.maxClusteringValues).append(this.minClusteringValues, that.minClusteringValues).append(this.hasLegacyCounterShards, that.hasLegacyCounterShards).append(this.totalColumnsSet, that.totalColumnsSet).append(this.totalRows, that.totalRows).append(this.pendingRepair, that.pendingRepair).build().booleanValue();
      } else {
         return false;
      }
   }

   public int hashCode() {
      return (new HashCodeBuilder()).append(this.estimatedPartitionSize).append(this.estimatedColumnCount).append(this.commitLogIntervals).append(this.minTimestamp).append(this.maxTimestamp).append(this.minLocalDeletionTime).append(this.maxLocalDeletionTime).append(this.minTTL).append(this.maxTTL).append(this.compressionRatio).append(this.estimatedTombstoneDropTime).append(this.sstableLevel).append(this.repairedAt).append(this.maxClusteringValues).append(this.minClusteringValues).append(this.hasLegacyCounterShards).append(this.totalColumnsSet).append(this.totalRows).append(this.pendingRepair).build().intValue();
   }

   static {
      commitLogPositionSetSerializer = IntervalSet.serializer(CommitLogPosition.serializer);
   }

   public static class StatsMetadataSerializer implements IMetadataComponentSerializer<StatsMetadata> {
      public StatsMetadataSerializer() {
      }

      public int serializedSize(Version version, StatsMetadata component) throws IOException {
         int size = 0;
         size = (int)((long)size + EstimatedHistogram.serializer.serializedSize(component.estimatedPartitionSize));
         size = (int)((long)size + EstimatedHistogram.serializer.serializedSize(component.estimatedColumnCount));
         size = (int)((long)size + CommitLogPosition.serializer.serializedSize((CommitLogPosition)component.commitLogIntervals.upperBound().orElse(CommitLogPosition.NONE)));
         size += 48;
         size = (int)((long)size + TombstoneHistogram.serializer.serializedSize(component.estimatedTombstoneDropTime));
         size += TypeSizes.sizeof(component.sstableLevel);
         size += 4;

         Iterator var4;
         ByteBuffer value;
         for(var4 = component.minClusteringValues.iterator(); var4.hasNext(); size += 2 + value.remaining()) {
            value = (ByteBuffer)var4.next();
         }

         size += 4;

         for(var4 = component.maxClusteringValues.iterator(); var4.hasNext(); size += 2 + value.remaining()) {
            value = (ByteBuffer)var4.next();
         }

         size += TypeSizes.sizeof(component.hasLegacyCounterShards);
         size += 16;
         if(version.hasCommitLogLowerBound()) {
            size = (int)((long)size + CommitLogPosition.serializer.serializedSize((CommitLogPosition)component.commitLogIntervals.lowerBound().orElse(CommitLogPosition.NONE)));
         }

         if(version.hasCommitLogIntervals()) {
            size = (int)((long)size + StatsMetadata.commitLogPositionSetSerializer.serializedSize(component.commitLogIntervals));
         }

         if(version.hasPendingRepair()) {
            ++size;
            if(component.pendingRepair != null) {
               size = (int)((long)size + UUIDSerializer.serializer.serializedSize(component.pendingRepair));
            }
         }

         return size;
      }

      public void serialize(Version version, StatsMetadata component, DataOutputPlus out) throws IOException {
         EstimatedHistogram.serializer.serialize(component.estimatedPartitionSize, out);
         EstimatedHistogram.serializer.serialize(component.estimatedColumnCount, out);
         CommitLogPosition.serializer.serialize((CommitLogPosition)component.commitLogIntervals.upperBound().orElse(CommitLogPosition.NONE), out);
         out.writeLong(component.minTimestamp);
         out.writeLong(component.maxTimestamp);
         out.writeInt(component.minLocalDeletionTime);
         out.writeInt(component.maxLocalDeletionTime);
         out.writeInt(component.minTTL);
         out.writeInt(component.maxTTL);
         out.writeDouble(component.compressionRatio);
         TombstoneHistogram.serializer.serialize(component.estimatedTombstoneDropTime, out);
         out.writeInt(component.sstableLevel);
         out.writeLong(component.repairedAt);
         out.writeInt(component.minClusteringValues.size());
         Iterator var4 = component.minClusteringValues.iterator();

         ByteBuffer value;
         while(var4.hasNext()) {
            value = (ByteBuffer)var4.next();
            ByteBufferUtil.writeWithShortLength(value, out);
         }

         out.writeInt(component.maxClusteringValues.size());
         var4 = component.maxClusteringValues.iterator();

         while(var4.hasNext()) {
            value = (ByteBuffer)var4.next();
            ByteBufferUtil.writeWithShortLength(value, out);
         }

         out.writeBoolean(component.hasLegacyCounterShards);
         out.writeLong(component.totalColumnsSet);
         out.writeLong(component.totalRows);
         if(version.hasCommitLogLowerBound()) {
            CommitLogPosition.serializer.serialize((CommitLogPosition)component.commitLogIntervals.lowerBound().orElse(CommitLogPosition.NONE), out);
         }

         if(version.hasCommitLogIntervals()) {
            StatsMetadata.commitLogPositionSetSerializer.serialize(component.commitLogIntervals, out);
         }

         if(version.hasPendingRepair()) {
            if(component.pendingRepair != null) {
               out.writeByte(1);
               UUIDSerializer.serializer.serialize(component.pendingRepair, out);
            } else {
               out.writeByte(0);
            }
         }

      }

      public StatsMetadata deserialize(Version version, DataInputPlus in) throws IOException {
         EstimatedHistogram partitionSizes = EstimatedHistogram.serializer.deserialize(in);
         EstimatedHistogram columnCounts = EstimatedHistogram.serializer.deserialize(in);
         CommitLogPosition commitLogLowerBound = CommitLogPosition.NONE;
         CommitLogPosition commitLogUpperBound = CommitLogPosition.serializer.deserialize(in);
         long minTimestamp = in.readLong();
         long maxTimestamp = in.readLong();
         int minLocalDeletionTime = in.readInt();
         int maxLocalDeletionTime = in.readInt();
         int minTTL = in.readInt();
         int maxTTL = in.readInt();
         double compressionRatio = in.readDouble();
         TombstoneHistogram tombstoneHistogram = TombstoneHistogram.serializer.deserialize(in);
         int sstableLevel = in.readInt();
         long repairedAt = in.readLong();
         int colCount = in.readInt();
         List<ByteBuffer> minClusteringValues = new ArrayList(colCount);

         for(int i = 0; i < colCount; ++i) {
            minClusteringValues.add(ByteBufferUtil.readWithShortLength(in));
         }

         colCount = in.readInt();
         List<ByteBuffer> maxClusteringValues = new ArrayList(colCount);

         for(int i = 0; i < colCount; ++i) {
            maxClusteringValues.add(ByteBufferUtil.readWithShortLength(in));
         }

         boolean hasLegacyCounterShards = in.readBoolean();
         long totalColumnsSet = in.readLong();
         long totalRows = in.readLong();
         if(version.hasCommitLogLowerBound()) {
            commitLogLowerBound = CommitLogPosition.serializer.deserialize(in);
         }

         IntervalSet commitLogIntervals;
         if(version.hasCommitLogIntervals()) {
            commitLogIntervals = (IntervalSet)StatsMetadata.commitLogPositionSetSerializer.deserialize(in);
         } else {
            commitLogIntervals = new IntervalSet(commitLogLowerBound, commitLogUpperBound);
         }

         UUID pendingRepair = null;
         if(version.hasPendingRepair() && in.readByte() != 0) {
            pendingRepair = UUIDSerializer.serializer.deserialize(in);
         }

         return new StatsMetadata(partitionSizes, columnCounts, commitLogIntervals, minTimestamp, maxTimestamp, minLocalDeletionTime, maxLocalDeletionTime, minTTL, maxTTL, compressionRatio, tombstoneHistogram, sstableLevel, minClusteringValues, maxClusteringValues, hasLegacyCounterShards, repairedAt, totalColumnsSet, totalRows, pendingRepair);
      }
   }
}
