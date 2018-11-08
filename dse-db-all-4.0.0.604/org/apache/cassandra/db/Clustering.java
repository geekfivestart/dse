package org.apache.cassandra.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.memory.AbstractAllocator;

public interface Clustering extends ClusteringPrefix {
   long EMPTY_SIZE = ObjectSizes.measure(new BufferClustering(ByteBufferUtil.EMPTY_BUFFER_ARRAY));
   Clustering.Serializer serializer = new Clustering.Serializer();
   Clustering STATIC_CLUSTERING = new BufferClustering(ByteBufferUtil.EMPTY_BUFFER_ARRAY) {
      public ClusteringPrefix.Kind kind() {
         return ClusteringPrefix.Kind.STATIC_CLUSTERING;
      }

      public String toString() {
         return "STATIC";
      }

      public String toString(TableMetadata metadata) {
         return this.toString();
      }
   };
   Clustering EMPTY = new BufferClustering(ByteBufferUtil.EMPTY_BUFFER_ARRAY) {
      public String toString(TableMetadata metadata) {
         return "EMPTY";
      }
   };

   long unsharedHeapSizeExcludingData();

   default Clustering copy(AbstractAllocator allocator) {
      if(this.size() == 0) {
         return this.kind() == ClusteringPrefix.Kind.STATIC_CLUSTERING?this:EMPTY;
      } else {
         ByteBuffer[] newValues = new ByteBuffer[this.size()];

         for(int i = 0; i < this.size(); ++i) {
            ByteBuffer val = this.get(i);
            newValues[i] = val == null?null:allocator.clone(val);
         }

         return new BufferClustering(newValues);
      }
   }

   default String toString(TableMetadata metadata) {
      StringBuilder sb = new StringBuilder();

      for(int i = 0; i < this.size(); ++i) {
         ColumnMetadata c = (ColumnMetadata)metadata.clusteringColumns().get(i);
         sb.append(i == 0?"":", ").append(c.name).append('=').append(this.get(i) == null?"null":c.type.getString(this.get(i)));
      }

      return sb.toString();
   }

   default String toCQLString(TableMetadata metadata) {
      StringBuilder sb = new StringBuilder();

      for(int i = 0; i < this.size(); ++i) {
         ColumnMetadata c = (ColumnMetadata)metadata.clusteringColumns().get(i);
         sb.append(i == 0?"":", ").append(c.type.getString(this.get(i)));
      }

      return sb.toString();
   }

   default String toBinaryString() {
      StringBuilder sb = new StringBuilder();

      for(int i = 0; i < this.size(); ++i) {
         ByteBuffer bb = this.get(i);
         sb.append(bb == null?"null":ByteBufferUtil.bytesToHex(bb));
         sb.append(", ");
      }

      return sb.toString();
   }

   static default Clustering make(ByteBuffer... values) {
      return (Clustering)(values.length == 0?EMPTY:new BufferClustering(values));
   }

   public static class Serializer {
      public Serializer() {
      }

      public void serialize(Clustering clustering, DataOutputPlus out, ClusteringVersion version, List<AbstractType<?>> types) throws IOException {
         assert clustering != Clustering.STATIC_CLUSTERING : "We should never serialize a static clustering";

         assert clustering.size() == types.size() : "Invalid clustering for the table: " + clustering;

         ClusteringPrefix.serializer.serializeValuesWithoutSize(clustering, out, version, types);
      }

      public ByteBuffer serialize(Clustering clustering, ClusteringVersion version, List<AbstractType<?>> types) {
         try {
            DataOutputBuffer buffer = new DataOutputBuffer((int)this.serializedSize(clustering, version, types));
            Throwable var5 = null;

            ByteBuffer var6;
            try {
               this.serialize(clustering, buffer, version, types);
               var6 = buffer.buffer();
            } catch (Throwable var16) {
               var5 = var16;
               throw var16;
            } finally {
               if(buffer != null) {
                  if(var5 != null) {
                     try {
                        buffer.close();
                     } catch (Throwable var15) {
                        var5.addSuppressed(var15);
                     }
                  } else {
                     buffer.close();
                  }
               }

            }

            return var6;
         } catch (IOException var18) {
            throw new RuntimeException("Writing to an in-memory buffer shouldn't trigger an IOException", var18);
         }
      }

      public long serializedSize(Clustering clustering, ClusteringVersion version, List<AbstractType<?>> types) {
         return ClusteringPrefix.serializer.valuesWithoutSizeSerializedSize(clustering, version, types);
      }

      public void skip(DataInputPlus in, ClusteringVersion version, List<AbstractType<?>> types) throws IOException {
         if(!types.isEmpty()) {
            ClusteringPrefix.serializer.skipValuesWithoutSize(in, types.size(), version, types);
         }

      }

      public Clustering deserialize(DataInputPlus in, ClusteringVersion version, List<AbstractType<?>> types) throws IOException {
         if(types.isEmpty()) {
            return Clustering.EMPTY;
         } else {
            ByteBuffer[] values = ClusteringPrefix.serializer.deserializeValuesWithoutSize(in, types.size(), version, types);
            return new BufferClustering(values);
         }
      }

      public Clustering deserialize(ByteBuffer in, ClusteringVersion version, List<AbstractType<?>> types) {
         try {
            DataInputBuffer buffer = new DataInputBuffer(in, true);
            Throwable var5 = null;

            Clustering var6;
            try {
               var6 = this.deserialize((DataInputPlus)buffer, version, types);
            } catch (Throwable var16) {
               var5 = var16;
               throw var16;
            } finally {
               if(buffer != null) {
                  if(var5 != null) {
                     try {
                        buffer.close();
                     } catch (Throwable var15) {
                        var5.addSuppressed(var15);
                     }
                  } else {
                     buffer.close();
                  }
               }

            }

            return var6;
         } catch (IOException var18) {
            throw new RuntimeException("Reading from an in-memory buffer shouldn't trigger an IOException", var18);
         }
      }
   }
}
