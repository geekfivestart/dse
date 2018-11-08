package org.apache.cassandra.db.filter;

import com.google.common.collect.UnmodifiableIterator;
import java.io.IOException;
import java.util.function.Function;
import org.apache.cassandra.db.ReadVerbs;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.versioning.VersionDependent;
import org.apache.cassandra.utils.versioning.Versioned;

public abstract class AbstractClusteringIndexFilter implements ClusteringIndexFilter {
   static final Versioned<ReadVerbs.ReadVersion, ClusteringIndexFilter.Serializer> serializers = ReadVerbs.ReadVersion.versioned((x$0) -> {
      return new AbstractClusteringIndexFilter.FilterSerializer(x$0);
   });
   protected final boolean reversed;

   protected AbstractClusteringIndexFilter(boolean reversed) {
      this.reversed = reversed;
   }

   public boolean isReversed() {
      return this.reversed;
   }

   protected abstract void serializeInternal(DataOutputPlus var1, ReadVerbs.ReadVersion var2) throws IOException;

   protected abstract long serializedSizeInternal(ReadVerbs.ReadVersion var1);

   protected void appendOrderByToCQLString(TableMetadata metadata, StringBuilder sb) {
      if(this.reversed) {
         sb.append(" ORDER BY (");
         int i = 0;
         UnmodifiableIterator var4 = metadata.clusteringColumns().iterator();

         while(var4.hasNext()) {
            ColumnMetadata column = (ColumnMetadata)var4.next();
            sb.append(i++ == 0?"":", ").append(column.name).append(column.type instanceof ReversedType?" ASC":" DESC");
         }

         sb.append(')');
      }

   }

   public static class FilterSerializer extends VersionDependent<ReadVerbs.ReadVersion> implements ClusteringIndexFilter.Serializer {
      private FilterSerializer(ReadVerbs.ReadVersion version) {
         super(version);
      }

      public void serialize(ClusteringIndexFilter pfilter, DataOutputPlus out) throws IOException {
         AbstractClusteringIndexFilter filter = (AbstractClusteringIndexFilter)pfilter;
         out.writeByte(filter.kind().ordinal());
         out.writeBoolean(filter.isReversed());
         filter.serializeInternal(out, (ReadVerbs.ReadVersion)this.version);
      }

      public ClusteringIndexFilter deserialize(DataInputPlus in, TableMetadata metadata) throws IOException {
         ClusteringIndexFilter.Kind kind = ClusteringIndexFilter.Kind.values()[in.readUnsignedByte()];
         boolean reversed = in.readBoolean();
         return kind.deserializer.deserialize(in, (ReadVerbs.ReadVersion)this.version, metadata, reversed);
      }

      public long serializedSize(ClusteringIndexFilter pfilter) {
         AbstractClusteringIndexFilter filter = (AbstractClusteringIndexFilter)pfilter;
         return (long)(1 + TypeSizes.sizeof(filter.isReversed())) + filter.serializedSizeInternal((ReadVerbs.ReadVersion)this.version);
      }
   }
}
