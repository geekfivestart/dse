package org.apache.cassandra.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Function;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.versioning.VersionDependent;
import org.apache.cassandra.utils.versioning.Versioned;

public class DataRange {
   public static final Versioned<ReadVerbs.ReadVersion, DataRange.Serializer> serializers = ReadVerbs.ReadVersion.versioned((x$0) -> {
      return new DataRange.Serializer(x$0);
   });
   protected final AbstractBounds<PartitionPosition> keyRange;
   protected final ClusteringIndexFilter clusteringIndexFilter;

   public DataRange(AbstractBounds<PartitionPosition> range, ClusteringIndexFilter clusteringIndexFilter) {
      this.keyRange = range;
      this.clusteringIndexFilter = clusteringIndexFilter;
   }

   public static DataRange allData(IPartitioner partitioner) {
      return forTokenRange(new Range(partitioner.getMinimumToken(), partitioner.getMinimumToken()));
   }

   public static DataRange forTokenRange(Range<Token> tokenRange) {
      return forKeyRange(Range.makeRowRange(tokenRange));
   }

   public static DataRange forKeyRange(Range<PartitionPosition> keyRange) {
      return new DataRange(keyRange, new ClusteringIndexSliceFilter(Slices.ALL, false));
   }

   public static DataRange allData(IPartitioner partitioner, ClusteringIndexFilter filter) {
      return new DataRange(Range.makeRowRange(new Range(partitioner.getMinimumToken(), partitioner.getMinimumToken())), filter);
   }

   public AbstractBounds<PartitionPosition> keyRange() {
      return this.keyRange;
   }

   public PartitionPosition startKey() {
      return (PartitionPosition)this.keyRange.left;
   }

   public PartitionPosition stopKey() {
      return (PartitionPosition)this.keyRange.right;
   }

   public boolean isNamesQuery() {
      return this.clusteringIndexFilter instanceof ClusteringIndexNamesFilter;
   }

   public boolean isPaging() {
      return false;
   }

   public boolean isWrapAround() {
      return this.keyRange instanceof Range && ((Range)this.keyRange).isWrapAround();
   }

   public boolean contains(PartitionPosition pos) {
      return this.keyRange.contains(pos);
   }

   public boolean isUnrestricted() {
      return this.startKey().isMinimum() && this.stopKey().isMinimum() && this.clusteringIndexFilter.selectsAllPartition();
   }

   public boolean selectsAllPartition() {
      return this.clusteringIndexFilter.selectsAllPartition();
   }

   public boolean isReversed() {
      return this.clusteringIndexFilter.isReversed();
   }

   public ClusteringIndexFilter clusteringIndexFilter(DecoratedKey key) {
      return this.clusteringIndexFilter;
   }

   public DataRange forPaging(AbstractBounds<PartitionPosition> range, ClusteringComparator comparator, Clustering lastReturned, boolean inclusive) {
      return new DataRange.Paging(range, this.clusteringIndexFilter, comparator, lastReturned, inclusive);
   }

   public DataRange forSubRange(AbstractBounds<PartitionPosition> range) {
      return new DataRange(range, this.clusteringIndexFilter);
   }

   public String toString(TableMetadata metadata) {
      return String.format("range=%s pfilter=%s", new Object[]{this.keyRange.getString(metadata.partitionKeyType), this.clusteringIndexFilter.toString(metadata)});
   }

   public String toCQLString(TableMetadata metadata) {
      if(this.isUnrestricted()) {
         return "UNRESTRICTED";
      } else {
         StringBuilder sb = new StringBuilder();
         boolean needAnd = false;
         if(!this.startKey().isMinimum()) {
            this.appendClause(this.startKey(), sb, metadata, true, this.keyRange.isStartInclusive());
            needAnd = true;
         }

         if(!this.stopKey().isMinimum()) {
            if(needAnd) {
               sb.append(" AND ");
            }

            this.appendClause(this.stopKey(), sb, metadata, false, this.keyRange.isEndInclusive());
            needAnd = true;
         }

         String filterString = this.clusteringIndexFilter.toCQLString(metadata);
         if(!filterString.isEmpty()) {
            sb.append(needAnd?" AND ":"").append(filterString);
         }

         return sb.toString();
      }
   }

   private void appendClause(PartitionPosition pos, StringBuilder sb, TableMetadata metadata, boolean isStart, boolean isInclusive) {
      sb.append("token(");
      sb.append(ColumnMetadata.toCQLString((Iterable)metadata.partitionKeyColumns()));
      sb.append(") ").append(getOperator(isStart, isInclusive)).append(" ");
      if(pos instanceof DecoratedKey) {
         sb.append("token(");
         appendKeyString(sb, metadata.partitionKeyType, ((DecoratedKey)pos).getKey());
         sb.append(")");
      } else {
         sb.append(((Token.KeyBound)pos).getToken());
      }

   }

   private static String getOperator(boolean isStart, boolean isInclusive) {
      return isStart?(isInclusive?">=":">"):(isInclusive?"<=":"<");
   }

   public static void appendKeyString(StringBuilder sb, AbstractType<?> type, ByteBuffer key) {
      if(type instanceof CompositeType) {
         CompositeType ct = (CompositeType)type;
         ByteBuffer[] values = ct.split(key);

         for(int i = 0; i < ct.types.size(); ++i) {
            sb.append(i == 0?"":", ").append(((AbstractType)ct.types.get(i)).getString(values[i]));
         }
      } else {
         sb.append(type.getString(key));
      }

   }

   public boolean equals(Object other) {
      if(other != null && this.getClass().equals(other.getClass())) {
         DataRange that = (DataRange)other;
         return this.keyRange.equals(that.keyRange) && this.clusteringIndexFilter.equals(that.clusteringIndexFilter);
      } else {
         return false;
      }
   }

   public static class Serializer extends VersionDependent<ReadVerbs.ReadVersion> {
      private Serializer(ReadVerbs.ReadVersion version) {
         super(version);
      }

      public void serialize(DataRange range, DataOutputPlus out, TableMetadata metadata) throws IOException {
         AbstractBounds.rowPositionSerializer.serialize(range.keyRange, out, ((ReadVerbs.ReadVersion)this.version).boundsVersion);
         ((ClusteringIndexFilter.Serializer)ClusteringIndexFilter.serializers.get(this.version)).serialize(range.clusteringIndexFilter, out);
         boolean isPaging = range instanceof DataRange.Paging;
         out.writeBoolean(isPaging);
         if(isPaging) {
            Clustering.serializer.serialize(((DataRange.Paging)range).lastReturned, out, ((ReadVerbs.ReadVersion)this.version).encodingVersion.clusteringVersion, metadata.comparator.subtypes());
            out.writeBoolean(((DataRange.Paging)range).inclusive);
         }

      }

      public DataRange deserialize(DataInputPlus in, TableMetadata metadata) throws IOException {
         AbstractBounds<PartitionPosition> range = (AbstractBounds)AbstractBounds.rowPositionSerializer.deserialize(in, metadata.partitioner, ((ReadVerbs.ReadVersion)this.version).boundsVersion);
         ClusteringIndexFilter filter = ((ClusteringIndexFilter.Serializer)ClusteringIndexFilter.serializers.get(this.version)).deserialize(in, metadata);
         if(in.readBoolean()) {
            ClusteringComparator comparator = metadata.comparator;
            Clustering lastReturned = Clustering.serializer.deserialize(in, ((ReadVerbs.ReadVersion)this.version).encodingVersion.clusteringVersion, comparator.subtypes());
            boolean inclusive = in.readBoolean();
            return new DataRange.Paging(range, filter, comparator, lastReturned, inclusive);
         } else {
            return new DataRange(range, filter);
         }
      }

      public long serializedSize(DataRange range, TableMetadata metadata) {
         long size = (long)AbstractBounds.rowPositionSerializer.serializedSize(range.keyRange, ((ReadVerbs.ReadVersion)this.version).boundsVersion) + ((ClusteringIndexFilter.Serializer)ClusteringIndexFilter.serializers.get(this.version)).serializedSize(range.clusteringIndexFilter) + 1L;
         if(range instanceof DataRange.Paging) {
            size += Clustering.serializer.serializedSize(((DataRange.Paging)range).lastReturned, ((ReadVerbs.ReadVersion)this.version).encodingVersion.clusteringVersion, metadata.comparator.subtypes());
            ++size;
         }

         return size;
      }
   }

   public static class Paging extends DataRange {
      private final ClusteringComparator comparator;
      private final Clustering lastReturned;
      private final boolean inclusive;

      private Paging(AbstractBounds<PartitionPosition> range, ClusteringIndexFilter filter, ClusteringComparator comparator, Clustering lastReturned, boolean inclusive) {
         super(range, filter);

         assert !(range instanceof Range) || !((Range)range).isTrulyWrapAround() : range;

         assert lastReturned != null;

         this.comparator = comparator;
         this.lastReturned = lastReturned;
         this.inclusive = inclusive;
      }

      public ClusteringIndexFilter clusteringIndexFilter(DecoratedKey key) {
         return key.equals(this.startKey())?this.clusteringIndexFilter.forPaging(this.comparator, this.lastReturned, this.inclusive):this.clusteringIndexFilter;
      }

      public DataRange forSubRange(AbstractBounds<PartitionPosition> range) {
         return (DataRange)(((PartitionPosition)range.left).equals(this.keyRange().left)?new DataRange.Paging(range, this.clusteringIndexFilter, this.comparator, this.lastReturned, this.inclusive):new DataRange(range, this.clusteringIndexFilter));
      }

      public Clustering getLastReturned() {
         return this.lastReturned;
      }

      public boolean isPaging() {
         return true;
      }

      public boolean isUnrestricted() {
         return false;
      }

      public boolean equals(Object other) {
         if(other != null && this.getClass().equals(other.getClass())) {
            DataRange.Paging that = (DataRange.Paging)other;
            return this.keyRange.equals(that.keyRange) && this.clusteringIndexFilter.equals(that.clusteringIndexFilter) && this.lastReturned.equals(that.lastReturned) && this.inclusive == that.inclusive;
         } else {
            return false;
         }
      }

      public String toString(TableMetadata metadata) {
         return String.format("range=%s (paging) pfilter=%s lastReturned=%s (%s)", new Object[]{this.keyRange.getString(metadata.partitionKeyType), this.clusteringIndexFilter.toString(metadata), this.lastReturned.toString(metadata), this.inclusive?"included":"excluded"});
      }
   }
}
