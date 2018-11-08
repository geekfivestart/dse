package org.apache.cassandra.db;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Hasher;
import io.reactivex.Single;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Function;
import org.apache.cassandra.db.partitions.ArrayBackedPartition;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.partitions.UnfilteredPartitionsSerializer;
import org.apache.cassandra.db.rows.FlowablePartitions;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.HashingUtils;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.flow.Flow;
import org.apache.cassandra.utils.flow.FlowSource;
import org.apache.cassandra.utils.versioning.Versioned;

public abstract class ReadResponse {
   public static final Versioned<ReadVerbs.ReadVersion, Serializer<ReadResponse>> serializers = ReadVerbs.ReadVersion.versioned((v) -> {
      return new Serializer<ReadResponse>() {
         public void serialize(ReadResponse response, DataOutputPlus out) throws IOException {
            boolean isDigest = response instanceof ReadResponse.DigestResponse;
            ByteBuffer digest = isDigest?((ReadResponse.DigestResponse)response).digest:ByteBufferUtil.EMPTY_BYTE_BUFFER;
            ByteBufferUtil.writeWithVIntLength(digest, out);
            if(!isDigest) {
               ByteBuffer data = ((ReadResponse.DataResponse)response).data;
               ByteBufferUtil.writeWithVIntLength(data, out);
            }

         }

         public ReadResponse deserialize(DataInputPlus in) throws IOException {
            ByteBuffer digest = ByteBufferUtil.readWithVIntLength(in);
            if(digest.hasRemaining()) {
               return new ReadResponse.DigestResponse(digest, null);
            } else {
               ByteBuffer data = ByteBufferUtil.readWithVIntLength(in);
               return new ReadResponse.RemoteDataResponse(data, v.encodingVersion);
            }
         }

         public long serializedSize(ReadResponse response) {
            boolean isDigest = response instanceof ReadResponse.DigestResponse;
            ByteBuffer digest = isDigest?((ReadResponse.DigestResponse)response).digest:ByteBufferUtil.EMPTY_BYTE_BUFFER;
            long size = (long)ByteBufferUtil.serializedSizeWithVIntLength(digest);
            if(!isDigest) {
               ByteBuffer data = ((ReadResponse.DataResponse)response).data;
               size += (long)ByteBufferUtil.serializedSizeWithVIntLength(data);
            }

            return size;
         }
      };
   });

   protected ReadResponse() {
   }

   public static Single<ReadResponse> createDataResponse(Flow<FlowableUnfilteredPartition> partitions, ReadCommand command, boolean forLocalDelivery) {
      return forLocalDelivery?ReadResponse.LocalResponse.build(partitions):ReadResponse.LocalDataResponse.build(partitions, EncodingVersion.last(), command);
   }

   @VisibleForTesting
   public static ReadResponse createRemoteDataResponse(Flow<FlowableUnfilteredPartition> partitions, ReadCommand command) {
      EncodingVersion version = EncodingVersion.last();
      return (ReadResponse)UnfilteredPartitionsSerializer.serializerForIntraNode(version).serialize(partitions, command.columnFilter()).map((buffer) -> {
         return new ReadResponse.RemoteDataResponse(buffer, version);
      }).blockingSingle();
   }

   public static Single<ReadResponse> createDigestResponse(Flow<FlowableUnfilteredPartition> partitions, ReadCommand command) {
      return makeDigest(partitions, command).map((digest) -> {
         return new ReadResponse.DigestResponse(digest, null);
      });
   }

   public abstract Flow<FlowableUnfilteredPartition> data(ReadCommand var1);

   public abstract Single<ByteBuffer> digest(ReadCommand var1);

   public abstract boolean isDigestResponse();

   public String toDebugString(ReadCommand command, DecoratedKey key) {
      if(this.isDigestResponse()) {
         return "Digest:0x" + ByteBufferUtil.bytesToHex((ByteBuffer)this.digest(command).blockingGet());
      } else {
         UnfilteredPartitionIterator iter = FlowablePartitions.toPartitions(this.data(command), command.metadata());
         Throwable var4 = null;

         while(true) {
            Object var7;
            try {
               if(!iter.hasNext()) {
                  return "<key " + key + " not found>";
               }

               UnfilteredRowIterator partition = (UnfilteredRowIterator)iter.next();
               Throwable var6 = null;

               try {
                  if(!partition.partitionKey().equals(key)) {
                     continue;
                  }

                  var7 = this.toDebugString(partition, command.metadata());
               } catch (Throwable var33) {
                  var7 = var33;
                  var6 = var33;
                  throw var33;
               } finally {
                  if(partition != null) {
                     if(var6 != null) {
                        try {
                           partition.close();
                        } catch (Throwable var32) {
                           var6.addSuppressed(var32);
                        }
                     } else {
                        partition.close();
                     }
                  }

               }
            } catch (Throwable var35) {
               var4 = var35;
               throw var35;
            } finally {
               if(iter != null) {
                  if(var4 != null) {
                     try {
                        iter.close();
                     } catch (Throwable var31) {
                        var4.addSuppressed(var31);
                     }
                  } else {
                     iter.close();
                  }
               }

            }

            return (String)var7;
         }
      }
   }

   private String toDebugString(UnfilteredRowIterator partition, TableMetadata metadata) {
      StringBuilder sb = new StringBuilder();
      sb.append(String.format("[%s] key=%s partition_deletion=%s columns=%s", new Object[]{metadata, metadata.partitionKeyType.getString(partition.partitionKey().getKey()), partition.partitionLevelDeletion(), partition.columns()}));
      if(partition.staticRow() != Rows.EMPTY_STATIC_ROW) {
         sb.append("\n    ").append(partition.staticRow().toString(metadata, true));
      }

      while(partition.hasNext()) {
         sb.append("\n    ").append(((Unfiltered)partition.next()).toString(metadata, true));
      }

      return sb.toString();
   }

   protected static Single<ByteBuffer> makeDigest(Flow<FlowableUnfilteredPartition> partitions, ReadCommand command) {
      Hasher hasher = HashingUtils.CURRENT_HASH_FUNCTION.newHasher();
      return UnfilteredPartitionIterators.digest(partitions, hasher, command.digestVersion()).processToRxCompletable().toSingle(() -> {
         return ByteBuffer.wrap(hasher.hash().asBytes());
      });
   }

   abstract static class DataResponse extends ReadResponse {
      private final ByteBuffer data;
      protected final EncodingVersion version;
      private final SerializationHelper.Flag flag;

      protected DataResponse(ByteBuffer data, EncodingVersion version, SerializationHelper.Flag flag) {
         this.data = data;
         this.version = version;
         this.flag = flag;
      }

      public Flow<FlowableUnfilteredPartition> data(ReadCommand command) {
         return UnfilteredPartitionsSerializer.serializerForIntraNode(this.version).deserialize(this.data, command.metadata(), command.columnFilter(), this.flag);
      }

      public Single<ByteBuffer> digest(ReadCommand command) {
         return makeDigest(this.data(command), command);
      }

      public boolean isDigestResponse() {
         return false;
      }
   }

   private static class RemoteDataResponse extends ReadResponse.DataResponse {
      protected RemoteDataResponse(ByteBuffer data, EncodingVersion version) {
         super(data, version, SerializationHelper.Flag.FROM_REMOTE);
      }
   }

   private static class LocalDataResponse extends ReadResponse.DataResponse {
      private LocalDataResponse(ByteBuffer data, EncodingVersion version) {
         super(data, version, SerializationHelper.Flag.LOCAL);
      }

      private static Single<ReadResponse> build(Flow<FlowableUnfilteredPartition> partitions, EncodingVersion version, ReadCommand command) {
         return UnfilteredPartitionsSerializer.serializerForIntraNode(version).serialize(partitions, command.columnFilter()).mapToRxSingle((buffer) -> {
            return new ReadResponse.LocalDataResponse(buffer, version);
         });
      }
   }

   private static class LocalResponse extends ReadResponse {
      private final List<Partition> partitions;

      private LocalResponse(List<Partition> partitions) {
         this.partitions = partitions;
      }

      public static Single<ReadResponse> build(Flow<FlowableUnfilteredPartition> partitions) {
         return ArrayBackedPartition.create(partitions).toList().mapToRxSingle(ReadResponse.LocalResponse::<init>);
      }

      public Flow<FlowableUnfilteredPartition> data(final ReadCommand command) {
         return new FlowSource<FlowableUnfilteredPartition>() {
            private int idx = 0;

            public void requestNext() {
               if(this.idx < LocalResponse.this.partitions.size()) {
                  this.subscriber.onNext(((Partition)LocalResponse.this.partitions.get(this.idx++)).unfilteredPartition(command.columnFilter(), Slices.ALL, command.isReversed()));
               } else {
                  this.subscriber.onComplete();
               }

            }

            public void close() throws Exception {
            }

            public String toString() {
               return Flow.formatTrace("LocalResponse", (Object)this.subscriber);
            }
         };
      }

      public boolean isDigestResponse() {
         return false;
      }

      public Single<ByteBuffer> digest(ReadCommand command) {
         return makeDigest(this.data(command), command);
      }
   }

   private static class DigestResponse extends ReadResponse {
      private final ByteBuffer digest;

      private DigestResponse(ByteBuffer digest) {
         assert digest.hasRemaining();

         this.digest = digest;
      }

      public Flow<FlowableUnfilteredPartition> data(ReadCommand command) {
         throw new UnsupportedOperationException();
      }

      public Single<ByteBuffer> digest(ReadCommand command) {
         return Single.just(this.digest);
      }

      public boolean isDigestResponse() {
         return true;
      }
   }
}
