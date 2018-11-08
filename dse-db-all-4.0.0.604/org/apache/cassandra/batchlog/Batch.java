package org.apache.cassandra.batchlog;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.UUID;
import java.util.function.Function;
import org.apache.cassandra.concurrent.SchedulableMessage;
import org.apache.cassandra.concurrent.StagedScheduler;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.concurrent.TracingAwareExecutor;
import org.apache.cassandra.db.EncodingVersion;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.WriteVerbs;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.UUIDSerializer;
import org.apache.cassandra.utils.versioning.VersionDependent;
import org.apache.cassandra.utils.versioning.Versioned;

public final class Batch implements SchedulableMessage {
   public static final Versioned<WriteVerbs.WriteVersion, Serializer<Batch>> serializers = WriteVerbs.WriteVersion.versioned((x$0) -> {
      return new Batch.BatchSerializer(x$0);
   });
   public final UUID id;
   public final long creationTime;
   final Collection<Mutation> decodedMutations;
   final Collection<ByteBuffer> encodedMutations;

   private Batch(UUID id, long creationTime, Collection<Mutation> decodedMutations, Collection<ByteBuffer> encodedMutations) {
      this.id = id;
      this.creationTime = creationTime;
      this.decodedMutations = decodedMutations;
      this.encodedMutations = encodedMutations;
   }

   public static Batch createLocal(UUID id, long creationTime, Collection<Mutation> mutations) {
      return new Batch(id, creationTime, mutations, Collections.emptyList());
   }

   public static Batch createRemote(UUID id, long creationTime, Collection<ByteBuffer> mutations) {
      return new Batch(id, creationTime, Collections.emptyList(), mutations);
   }

   public int size() {
      return this.decodedMutations.size() + this.encodedMutations.size();
   }

   public StagedScheduler getScheduler() {
      return TPC.getForKey(Keyspace.open("system"), SystemKeyspace.decorateBatchKey(this.id));
   }

   public TracingAwareExecutor getRequestExecutor() {
      return this.getScheduler().forTaskType(TPCTaskType.BATCH_STORE);
   }

   public TracingAwareExecutor getResponseExecutor() {
      return this.getScheduler().forTaskType(TPCTaskType.BATCH_STORE_RESPONSE);
   }

   static final class BatchSerializer extends VersionDependent<WriteVerbs.WriteVersion> implements Serializer<Batch> {
      private final Serializer<Mutation> mutationSerializer;

      private BatchSerializer(WriteVerbs.WriteVersion version) {
         super(version);
         this.mutationSerializer = (Serializer)Mutation.serializers.get(version);
      }

      public long serializedSize(Batch batch) {
         assert batch.encodedMutations.isEmpty() : "attempted to serialize a 'remote' batch";

         long size = UUIDSerializer.serializer.serializedSize(batch.id);
         size += (long)TypeSizes.sizeof(batch.creationTime);
         size += (long)TypeSizes.sizeofUnsignedVInt((long)batch.decodedMutations.size());

         long mutationSize;
         for(Iterator var4 = batch.decodedMutations.iterator(); var4.hasNext(); size += mutationSize) {
            Mutation mutation = (Mutation)var4.next();
            mutationSize = this.mutationSerializer.serializedSize(mutation);
            size += (long)TypeSizes.sizeofUnsignedVInt(mutationSize);
         }

         return size;
      }

      public void serialize(Batch batch, DataOutputPlus out) throws IOException {
         assert batch.encodedMutations.isEmpty() : "attempted to serialize a 'remote' batch";

         UUIDSerializer.serializer.serialize(batch.id, out);
         out.writeLong(batch.creationTime);
         out.writeUnsignedVInt((long)batch.decodedMutations.size());
         Iterator var3 = batch.decodedMutations.iterator();

         while(var3.hasNext()) {
            Mutation mutation = (Mutation)var3.next();
            out.writeUnsignedVInt(this.mutationSerializer.serializedSize(mutation));
            this.mutationSerializer.serialize(mutation, out);
         }

      }

      public Batch deserialize(DataInputPlus in) throws IOException {
         UUID id = UUIDSerializer.serializer.deserialize(in);
         long creationTime = in.readLong();
         return ((WriteVerbs.WriteVersion)this.version).encodingVersion == EncodingVersion.last()?Batch.createRemote(id, creationTime, this.readEncodedMutations(in)):Batch.createLocal(id, creationTime, this.decodeMutations(in));
      }

      private Collection<ByteBuffer> readEncodedMutations(DataInputPlus in) throws IOException {
         int count = (int)in.readUnsignedVInt();
         ArrayList<ByteBuffer> mutations = new ArrayList(count);

         for(int i = 0; i < count; ++i) {
            mutations.add(ByteBufferUtil.readWithVIntLength(in));
         }

         return mutations;
      }

      private Collection<Mutation> decodeMutations(DataInputPlus in) throws IOException {
         int count = (int)in.readUnsignedVInt();
         ArrayList<Mutation> mutations = new ArrayList(count);

         for(int i = 0; i < count; ++i) {
            in.readUnsignedVInt();
            mutations.add(this.mutationSerializer.deserialize(in));
         }

         return mutations;
      }
   }
}
