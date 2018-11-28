package org.apache.cassandra.db.rows;

import com.google.common.collect.Collections2;
import java.io.IOException;
import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringBoundOrBoundary;
import org.apache.cassandra.db.ClusteringBoundary;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.EncodingVersion;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.SearchIterator;
import org.apache.cassandra.utils.WrappedException;
import org.apache.cassandra.utils.versioning.VersionDependent;
import org.apache.cassandra.utils.versioning.Versioned;

public class UnfilteredSerializer extends VersionDependent<EncodingVersion> {
   public static final Versioned<EncodingVersion, UnfilteredSerializer> serializers = EncodingVersion.versioned(UnfilteredSerializer::new);
   private static final int END_OF_PARTITION = 1;
   private static final int IS_MARKER = 2;
   private static final int HAS_TIMESTAMP = 4;
   private static final int HAS_TTL = 8;
   private static final int HAS_DELETION = 16;
   private static final int HAS_ALL_COLUMNS = 32;
   private static final int HAS_COMPLEX_DELETION = 64;
   private static final int EXTENSION_FLAG = 128;
   private static final int IS_STATIC = 1;
   /** @deprecated */
   @Deprecated
   private static final int HAS_SHADOWABLE_DELETION = 2;

   private UnfilteredSerializer(EncodingVersion version) {
      super(version);
   }

   public void serialize(Unfiltered unfiltered, SerializationHeader header, DataOutputPlus out) throws IOException {
      assert !header.isForSSTable();

      this.serialize(unfiltered, header, out, 0L);
   }

   public void serialize(Unfiltered unfiltered, SerializationHeader header, DataOutputPlus out, long previousUnfilteredSize) throws IOException {
      if(unfiltered.kind() == Unfiltered.Kind.RANGE_TOMBSTONE_MARKER) {
         this.serialize((RangeTombstoneMarker)unfiltered, header, out, previousUnfilteredSize);
      } else {
         this.serialize((Row)unfiltered, header, out, previousUnfilteredSize);
      }

   }

   public void serializeStaticRow(Row row, SerializationHeader header, DataOutputPlus out) throws IOException {
      assert row.isStatic();

      this.serialize(row, header, out, 0L);
   }

   private void serialize(Row row, SerializationHeader header, DataOutputPlus out, long previousUnfilteredSize) throws IOException {
      int flags = 0;
      int extendedFlags = 0;
      boolean isStatic = row.isStatic();
      Columns headerColumns = header.columns(isStatic);
      LivenessInfo pkLiveness = row.primaryKeyLivenessInfo();
      Row.Deletion deletion = row.deletion();
      boolean hasComplexDeletion = row.hasComplexDeletion();
      boolean hasAllColumns = row.size() == headerColumns.size();
      boolean hasExtendedFlags = hasExtendedFlags(row);
      if(isStatic) {
         extendedFlags |= 1;
      }

      if(!pkLiveness.isEmpty()) {
         flags |= 4;
      }

      if(pkLiveness.isExpiring()) {
         flags |= 8;
      }

      if(!deletion.isLive()) {
         flags |= 16;
         if(deletion.isShadowable()) {
            extendedFlags |= 2;
         }
      }

      if(hasComplexDeletion) {
         flags |= 64;
      }

      if(hasAllColumns) {
         flags |= 32;
      }

      if(hasExtendedFlags) {
         flags |= 128;
      }

      out.writeByte((byte)flags);
      if(hasExtendedFlags) {
         out.writeByte((byte)extendedFlags);
      }

      if(!isStatic) {
         Clustering.serializer.serialize(row.clustering(), out, ((EncodingVersion)this.version).clusteringVersion, header.clusteringTypes());
      }

      if(header.isForSSTable()) {
         DataOutputBuffer dob = (DataOutputBuffer)DataOutputBuffer.scratchBuffer.get();
         Throwable var16 = null;

         try {
            this.serializeRowBody(row, flags, header, dob);
            out.writeUnsignedVInt(dob.position() + (long)TypeSizes.sizeofUnsignedVInt(previousUnfilteredSize));
            out.writeUnsignedVInt(previousUnfilteredSize);
            out.write(dob.getData(), 0, dob.getLength());
         } catch (Throwable var25) {
            var16 = var25;
            throw var25;
         } finally {
            if(dob != null) {
               if(var16 != null) {
                  try {
                     dob.close();
                  } catch (Throwable var24) {
                     var16.addSuppressed(var24);
                  }
               } else {
                  dob.close();
               }
            }

         }
      } else {
         this.serializeRowBody(row, flags, header, out);
      }

   }

   private void serializeRowBody(Row row, int flags, SerializationHeader header, DataOutputPlus out) throws IOException {
      boolean isStatic = row.isStatic();
      Columns headerColumns = header.columns(isStatic);
      LivenessInfo pkLiveness = row.primaryKeyLivenessInfo();
      Row.Deletion deletion = row.deletion();
      if((flags & 4) != 0) {
         header.writeTimestamp(pkLiveness.timestamp(), out);
      }

      if((flags & 8) != 0) {
         header.writeTTL(pkLiveness.ttl(), out);
         header.writeLocalDeletionTime(pkLiveness.localExpirationTime(), out);
      }

      if((flags & 16) != 0) {
         header.writeDeletionTime(deletion.time(), out);
      }

      if((flags & 32) == 0) {
         Columns.serializer.serializeSubset(Collections2.transform(row, ColumnData::column), headerColumns, out);
      }

      SearchIterator si = header.columnsIterator(isStatic);

      try {
         row.apply((cd) -> {
            ColumnMetadata column = (ColumnMetadata)si.next(cd.column());

            assert column != null : cd.column.toString();

            try {
               if(cd.column.isSimple()) {
                  Cell.serializer.serialize((Cell)cd, column, out, pkLiveness, header);
               } else {
                  this.writeComplexColumn((ComplexColumnData)cd, column, (flags & 64) != 0, pkLiveness, header, out);
               }

            } catch (IOException var9) {
               throw new WrappedException(var9);
            }
         }, false);
      } catch (WrappedException var11) {
         if(var11.getCause() instanceof IOException) {
            throw (IOException)var11.getCause();
         } else {
            throw var11;
         }
      }
   }

   private void writeComplexColumn(ComplexColumnData data, ColumnMetadata column, boolean hasComplexDeletion, LivenessInfo rowLiveness, SerializationHeader header, DataOutputPlus out) throws IOException {
      if(hasComplexDeletion) {
         header.writeDeletionTime(data.complexDeletion(), out);
      }

      out.writeUnsignedVInt((long)data.cellsCount());
      Iterator var7 = data.iterator();

      while(var7.hasNext()) {
         Cell cell = (Cell)var7.next();
         Cell.serializer.serialize(cell, column, out, rowLiveness, header);
      }

   }

   private void serialize(RangeTombstoneMarker marker, SerializationHeader header, DataOutputPlus out, long previousUnfilteredSize) throws IOException {
      out.writeByte(2);
      ClusteringBoundOrBoundary.serializer.serialize(marker.clustering(), out, ((EncodingVersion)this.version).clusteringVersion, header.clusteringTypes());
      if(header.isForSSTable()) {
         out.writeUnsignedVInt(this.serializedMarkerBodySize(marker, header, previousUnfilteredSize));
         out.writeUnsignedVInt(previousUnfilteredSize);
      }

      if(marker.isBoundary()) {
         RangeTombstoneBoundaryMarker bm = (RangeTombstoneBoundaryMarker)marker;
         header.writeDeletionTime(bm.endDeletionTime(), out);
         header.writeDeletionTime(bm.startDeletionTime(), out);
      } else {
         header.writeDeletionTime(((RangeTombstoneBoundMarker)marker).deletionTime(), out);
      }

   }

   public long serializedSize(Unfiltered unfiltered, SerializationHeader header) {
      assert !header.isForSSTable();

      return this.serializedSize(unfiltered, header, 0L);
   }

   public long serializedSize(Unfiltered unfiltered, SerializationHeader header, long previousUnfilteredSize) {
      return unfiltered.kind() == Unfiltered.Kind.RANGE_TOMBSTONE_MARKER?this.serializedSize((RangeTombstoneMarker)unfiltered, header, previousUnfilteredSize):this.serializedSize((Row)unfiltered, header, previousUnfilteredSize);
   }

   private long serializedSize(Row row, SerializationHeader header, long previousUnfilteredSize) {
      long size = 1L;
      if(hasExtendedFlags(row)) {
         ++size;
      }

      if(!row.isStatic()) {
         size += Clustering.serializer.serializedSize(row.clustering(), ((EncodingVersion)this.version).clusteringVersion, header.clusteringTypes());
      }

      return size + this.serializedRowBodySize(row, header, previousUnfilteredSize);
   }

   private long serializedRowBodySize(Row row, SerializationHeader header, long previousUnfilteredSize) {
      long size = 0L;
      if(header.isForSSTable()) {
         size += (long)TypeSizes.sizeofUnsignedVInt(previousUnfilteredSize);
      }

      boolean isStatic = row.isStatic();
      Columns headerColumns = header.columns(isStatic);
      LivenessInfo pkLiveness = row.primaryKeyLivenessInfo();
      Row.Deletion deletion = row.deletion();
      boolean hasComplexDeletion = row.hasComplexDeletion();
      boolean hasAllColumns = row.size() == headerColumns.size();
      if(!pkLiveness.isEmpty()) {
         size += header.timestampSerializedSize(pkLiveness.timestamp());
      }

      if(pkLiveness.isExpiring()) {
         size += header.ttlSerializedSize(pkLiveness.ttl());
         size += header.localDeletionTimeSerializedSize(pkLiveness.localExpirationTime());
      }

      if(!deletion.isLive()) {
         size += header.deletionTimeSerializedSize(deletion.time());
      }

      if(!hasAllColumns) {
         size += Columns.serializer.serializedSubsetSize(Collections2.transform(row, ColumnData::column), header.columns(isStatic));
      }

      SearchIterator<ColumnMetadata, ColumnMetadata> si = headerColumns.searchIterator();
      Iterator var14 = row.iterator();

      while(var14.hasNext()) {
         ColumnData data = (ColumnData)var14.next();
         ColumnMetadata column = (ColumnMetadata)si.next(data.column());

         assert column != null;

         if(data.column.isSimple()) {
            size += Cell.serializer.serializedSize((Cell)data, column, pkLiveness, header);
         } else {
            size += this.sizeOfComplexColumn((ComplexColumnData)data, column, hasComplexDeletion, pkLiveness, header);
         }
      }

      return size;
   }

   private long sizeOfComplexColumn(ComplexColumnData data, ColumnMetadata column, boolean hasComplexDeletion, LivenessInfo rowLiveness, SerializationHeader header) {
      long size = 0L;
      if(hasComplexDeletion) {
         size += header.deletionTimeSerializedSize(data.complexDeletion());
      }

      size += (long)TypeSizes.sizeofUnsignedVInt((long)data.cellsCount());

      Cell cell;
      for(Iterator var8 = data.iterator(); var8.hasNext(); size += Cell.serializer.serializedSize(cell, column, rowLiveness, header)) {
         cell = (Cell)var8.next();
      }

      return size;
   }

   private long serializedSize(RangeTombstoneMarker marker, SerializationHeader header, long previousUnfilteredSize) {
      assert !header.isForSSTable();

      return 1L + ClusteringBoundOrBoundary.serializer.serializedSize(marker.clustering(), ((EncodingVersion)this.version).clusteringVersion, header.clusteringTypes()) + this.serializedMarkerBodySize(marker, header, previousUnfilteredSize);
   }

   private long serializedMarkerBodySize(RangeTombstoneMarker marker, SerializationHeader header, long previousUnfilteredSize) {
      long size = 0L;
      if(header.isForSSTable()) {
         size += (long)TypeSizes.sizeofUnsignedVInt(previousUnfilteredSize);
      }

      if(marker.isBoundary()) {
         RangeTombstoneBoundaryMarker bm = (RangeTombstoneBoundaryMarker)marker;
         size += header.deletionTimeSerializedSize(bm.endDeletionTime());
         size += header.deletionTimeSerializedSize(bm.startDeletionTime());
      } else {
         size += header.deletionTimeSerializedSize(((RangeTombstoneBoundMarker)marker).deletionTime());
      }

      return size;
   }

   public void writeEndOfPartition(DataOutputPlus out) throws IOException {
      out.writeByte(1);
   }

   public long serializedSizeEndOfPartition() {
      return 1L;
   }

   public Unfiltered deserialize(DataInputPlus in, SerializationHeader header, SerializationHelper helper, Row.Builder builder) throws IOException {
      Unfiltered unfiltered;
      do {
         unfiltered = this.deserializeOne(in, header, helper, builder);
         if(unfiltered == null) {
            return null;
         }
      } while(unfiltered.isEmpty());

      return unfiltered;
   }

   private Unfiltered deserializeOne(DataInputPlus in, SerializationHeader header, SerializationHelper helper, Row.Builder builder) throws IOException {
      assert builder.isSorted();

      int flags = in.readUnsignedByte();
      if(isEndOfPartition(flags)) {
         return null;
      } else {
         int extendedFlags = readExtendedFlags(in, flags);
         if(kind(flags) == Unfiltered.Kind.RANGE_TOMBSTONE_MARKER) {
            ClusteringBoundOrBoundary bound = ClusteringBoundOrBoundary.serializer.deserialize(in, helper.version.clusteringVersion, header.clusteringTypes());
            return this.deserializeMarkerBody(in, header, bound);
         } else if(isStatic(extendedFlags)) {
            throw new IOException("Corrupt flags value for unfiltered partition (isStatic flag set): " + flags);
         } else {
            builder.newRow(Clustering.serializer.deserialize(in, helper.version.clusteringVersion, header.clusteringTypes()));
            return this.deserializeRowBody(in, header, helper, flags, extendedFlags, builder);
         }
      }
   }

   public Unfiltered deserializeTombstonesOnly(FileDataInput in, SerializationHeader header, SerializationHelper helper) throws IOException {
      while(true) {
         int flags = in.readUnsignedByte();
         if(isEndOfPartition(flags)) {
            return null;
         }

         int extendedFlags = readExtendedFlags(in, flags);
         if(kind(flags) == Unfiltered.Kind.RANGE_TOMBSTONE_MARKER) {
            ClusteringBoundOrBoundary bound = ClusteringBoundOrBoundary.serializer.deserialize(in, helper.version.clusteringVersion, header.clusteringTypes());
            return this.deserializeMarkerBody(in, header, bound);
         }

         assert !isStatic(extendedFlags);

         if((flags & 16) != 0) {
            assert header.isForSSTable();

            boolean hasTimestamp = (flags & 4) != 0;
            boolean hasTTL = (flags & 8) != 0;
            boolean deletionIsShadowable = (extendedFlags & 2) != 0;
            Clustering clustering = Clustering.serializer.deserialize((DataInputPlus)in, helper.version.clusteringVersion, header.clusteringTypes());
            long nextPosition = in.readUnsignedVInt() + in.getFilePointer();
            in.readUnsignedVInt();
            if(hasTimestamp) {
               header.readTimestamp(in);
               if(hasTTL) {
                  header.readTTL(in);
                  header.readLocalDeletionTime(in);
               }
            }

            Row.Deletion deletion = new Row.Deletion(header.readDeletionTime(in), deletionIsShadowable);
            in.seek(nextPosition);
            return ArrayBackedRow.emptyDeletedRow(clustering, deletion);
         }

         Clustering.serializer.skip(in, helper.version.clusteringVersion, header.clusteringTypes());
         this.skipRowBody(in);
      }
   }

   public Row deserializeStaticRow(DataInputPlus in, SerializationHeader header, SerializationHelper helper) throws IOException {
      int flags = in.readUnsignedByte();

      assert !isEndOfPartition(flags) && kind(flags) == Unfiltered.Kind.ROW && isExtended(flags) : flags;

      int extendedFlags = in.readUnsignedByte();
      Row.Builder builder = Row.Builder.sorted();
      builder.newRow(Clustering.STATIC_CLUSTERING);
      return this.deserializeRowBody(in, header, helper, flags, extendedFlags, builder);
   }

   public RangeTombstoneMarker deserializeMarkerBody(DataInputPlus in, SerializationHeader header, ClusteringBoundOrBoundary bound) throws IOException {
      if(header.isForSSTable()) {
         in.readUnsignedVInt();
         in.readUnsignedVInt();
      }

      return (RangeTombstoneMarker)(bound.isBoundary()?new RangeTombstoneBoundaryMarker((ClusteringBoundary)bound, header.readDeletionTime(in), header.readDeletionTime(in)):new RangeTombstoneBoundMarker((ClusteringBound)bound, header.readDeletionTime(in)));
   }

   public Row deserializeRowBody(DataInputPlus in, SerializationHeader header, SerializationHelper helper, int flags, int extendedFlags, Row.Builder builder) throws IOException {
      try {
         boolean isStatic = isStatic(extendedFlags);
         boolean hasTimestamp = (flags & 4) != 0;
         boolean hasTTL = (flags & 8) != 0;
         boolean hasDeletion = (flags & 16) != 0;
         boolean deletionIsShadowable = (extendedFlags & 2) != 0;
         boolean hasComplexDeletion = (flags & 64) != 0;
         boolean hasAllColumns = (flags & 32) != 0;
         Columns headerColumns = header.columns(isStatic);
         if(header.isForSSTable()) {
            in.readUnsignedVInt();
            in.readUnsignedVInt();
         }

         LivenessInfo rowLiveness = LivenessInfo.EMPTY;
         if(hasTimestamp) {
            long timestamp = header.readTimestamp(in);
            int ttl = hasTTL?header.readTTL(in):0;
            int localDeletionTime = hasTTL?header.readLocalDeletionTime(in):2147483647;
            rowLiveness = LivenessInfo.withExpirationTime(timestamp, ttl, localDeletionTime);
         }

         builder.addPrimaryKeyLivenessInfo(rowLiveness);
         builder.addRowDeletion(hasDeletion?new Row.Deletion(header.readDeletionTime(in), deletionIsShadowable):Row.Deletion.LIVE);
         Columns columns = hasAllColumns?headerColumns:Columns.serializer.deserializeSubset(headerColumns, in);
         LivenessInfo livenessInfo = rowLiveness;

         try {
            Iterator var26 = columns.iterator();

            while(var26.hasNext()) {
               ColumnMetadata column = (ColumnMetadata)var26.next();

               try {
                  if(column.isSimple()) {
                     this.readSimpleColumn(column, in, header, helper, builder, livenessInfo);
                  } else {
                     this.readComplexColumn(column, in, header, helper, hasComplexDeletion, builder, livenessInfo);
                  }
               } catch (IOException var21) {
                  throw new WrappedException(var21);
               }
            }
         } catch (WrappedException var22) {
            if(var22.getCause() instanceof IOException) {
               throw (IOException)var22.getCause();
            }

            throw var22;
         }

         return builder.build();
      } catch (Rebufferer.NotInCacheException var23) {
         throw var23;
      } catch (AssertionError | RuntimeException var24) {
         throw new IOException("Error building row with data deserialized from " + in, var24);
      }
   }

   private void readSimpleColumn(ColumnMetadata column, DataInputPlus in, SerializationHeader header, SerializationHelper helper, Row.Builder builder, LivenessInfo rowLiveness) throws IOException {
      if(helper.includes(column)) {
         Cell cell = Cell.serializer.deserialize(in, rowLiveness, column, header, helper);
         if(helper.includes(cell, rowLiveness) && !helper.isDropped(cell, false)) {
            builder.addCell(cell);
         }
      } else {
         Cell.serializer.skip(in, column, header);
      }

   }

   private void readComplexColumn(ColumnMetadata column, DataInputPlus in, SerializationHeader header, SerializationHelper helper, boolean hasComplexDeletion, Row.Builder builder, LivenessInfo rowLiveness) throws IOException {
      if(helper.includes(column)) {
         helper.startOfComplexColumn(column);
         if(hasComplexDeletion) {
            DeletionTime complexDeletion = header.readDeletionTime(in);
            if(!helper.isDroppedComplexDeletion(complexDeletion)) {
               builder.addComplexDeletion(column, complexDeletion);
            }
         }

         int count = (int)in.readUnsignedVInt();

         while(true) {
            --count;
            if(count < 0) {
               helper.endOfComplexColumn();
               break;
            }

            Cell cell = Cell.serializer.deserialize(in, rowLiveness, column, header, helper);
            if(helper.includes(cell, rowLiveness) && !helper.isDropped(cell, true)) {
               builder.addCell(cell);
            }
         }
      } else {
         this.skipComplexColumn(in, column, header, hasComplexDeletion);
      }

   }

   public void skipRowBody(DataInputPlus in) throws IOException {
      int rowSize = (int)in.readUnsignedVInt();
      in.skipBytesFully(rowSize);
   }

   public void skipStaticRow(DataInputPlus in, SerializationHeader header, SerializationHelper helper) throws IOException {
      int flags = in.readUnsignedByte();

      assert !isEndOfPartition(flags) && kind(flags) == Unfiltered.Kind.ROW && isExtended(flags) : "Flags is " + flags;

      int extendedFlags = in.readUnsignedByte();

      assert isStatic(extendedFlags);

      this.skipRowBody(in);
   }

   public void skipMarkerBody(DataInputPlus in) throws IOException {
      int markerSize = (int)in.readUnsignedVInt();
      in.skipBytesFully(markerSize);
   }

   private void skipComplexColumn(DataInputPlus in, ColumnMetadata column, SerializationHeader header, boolean hasComplexDeletion) throws IOException {
      if(hasComplexDeletion) {
         header.skipDeletionTime(in);
      }

      int count = (int)in.readUnsignedVInt();

      while(true) {
         --count;
         if(count < 0) {
            return;
         }

         Cell.serializer.skip(in, column, header);
      }
   }

   public static boolean isEndOfPartition(int flags) {
      return (flags & 1) != 0;
   }

   public static Unfiltered.Kind kind(int flags) {
      return (flags & 2) != 0?Unfiltered.Kind.RANGE_TOMBSTONE_MARKER:Unfiltered.Kind.ROW;
   }

   public static boolean isStatic(int extendedFlags) {
      return (extendedFlags & 1) != 0;
   }

   private static boolean isExtended(int flags) {
      return (flags & 128) != 0;
   }

   public static int readExtendedFlags(DataInputPlus in, int flags) throws IOException {
      return isExtended(flags)?in.readUnsignedByte():0;
   }

   public static boolean hasExtendedFlags(Row row) {
      return row.isStatic() || row.deletion().isShadowable();
   }
}
