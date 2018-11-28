package org.apache.cassandra.db;

import io.reactivex.Completable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.cassandra.concurrent.SchedulableMessage;
import org.apache.cassandra.concurrent.StagedScheduler;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCScheduler;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.concurrent.TracingAwareExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.exceptions.UnknownKeyspaceException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.versioning.VersionDependent;
import org.apache.cassandra.utils.versioning.Versioned;
import org.apache.commons.lang3.StringUtils;

public class Mutation implements IMutation, SchedulableMessage {
   public static final Versioned<EncodingVersion, Mutation.MutationSerializer> rawSerializers = EncodingVersion.versioned((x$0) -> {
      return new Mutation.MutationSerializer(x$0);
   });
   public static final Versioned<WriteVerbs.WriteVersion, Serializer<Mutation>> serializers = WriteVerbs.WriteVersion.versioned((v) -> {
      return (Mutation.MutationSerializer)rawSerializers.get(v.encodingVersion);
   });
   private final String keyspaceName;
   private final DecoratedKey key;
   private final Map<TableId, PartitionUpdate> modifications;
   public final long createdAt;
   public long viewLockAcquireStart;
   private boolean cdcEnabled;
   private static final int CACHED_SERIALIZATIONS = EncodingVersion.values().length;
   private final ByteBuffer[] cachedSerializations;
   private final transient TPCScheduler scheduler;
   private final transient TracingAwareExecutor requestExecutor;
   private final transient TracingAwareExecutor responseExecutor;
   private final TPCTaskType writeType;

   public Mutation(String keyspaceName, DecoratedKey key) {
      this(keyspaceName, key, new HashMap(), TPCTaskType.WRITE_LOCAL);
   }

   public Mutation(PartitionUpdate update) {
      this(update.metadata().keyspace, update.partitionKey(), Collections.singletonMap(update.metadata().id, update), TPCTaskType.WRITE_LOCAL);
   }

   protected Mutation(String keyspaceName, DecoratedKey key, Map<TableId, PartitionUpdate> modifications, TPCTaskType writeType) {
      this.createdAt = System.currentTimeMillis();
      this.viewLockAcquireStart = 0L;
      this.cdcEnabled = false;
      this.cachedSerializations = new ByteBuffer[CACHED_SERIALIZATIONS];
      this.keyspaceName = keyspaceName;
      this.key = key;
      this.modifications = modifications;

      PartitionUpdate pu;
      for(Iterator var5 = modifications.values().iterator(); var5.hasNext(); this.cdcEnabled |= pu.metadata().params.cdc) {
         pu = (PartitionUpdate)var5.next();
      }

      if(SchemaConstants.isInternalKeyspace(keyspaceName)) {
         writeType = TPCTaskType.WRITE_INTERNAL;
      }

      this.scheduler = createScheduler(keyspaceName, key);
      this.requestExecutor = this.scheduler == null?null:this.scheduler.forTaskType(writeType);
      this.responseExecutor = this.scheduler == null?null:this.scheduler.forTaskType(TPCTaskType.WRITE_RESPONSE);
      this.writeType = writeType;
   }

   private static TPCScheduler createScheduler(String keyspaceName, DecoratedKey key) {
      try {
         return TPC.getForKey(Keyspace.open(keyspaceName), key);
      } catch (UnknownKeyspaceException | IllegalStateException var3) {
         return null;
      }
   }

   public Mutation copy() {
      return new Mutation(this.keyspaceName, this.key, new HashMap(this.modifications), this.writeType);
   }

   public Mutation without(Set<TableId> tableIds) {
      if(tableIds.isEmpty()) {
         return this;
      } else {
         Mutation copy = this.copy();
         copy.modifications.keySet().removeAll(tableIds);
         copy.cdcEnabled = false;

         PartitionUpdate pu;
         for(Iterator var3 = this.modifications.values().iterator(); var3.hasNext(); copy.cdcEnabled |= pu.metadata().params.cdc) {
            pu = (PartitionUpdate)var3.next();
         }

         return copy;
      }
   }

   public Mutation without(TableId tableId) {
      return this.without(Collections.singleton(tableId));
   }

   public String getKeyspaceName() {
      return this.keyspaceName;
   }

   public Collection<TableId> getTableIds() {
      return this.modifications.keySet();
   }

   public DecoratedKey key() {
      return this.key;
   }

   public Collection<PartitionUpdate> getPartitionUpdates() {
      return this.modifications.values();
   }

   public PartitionUpdate getPartitionUpdate(TableMetadata table) {
      return table == null?null:(PartitionUpdate)this.modifications.get(table.id);
   }

   public Mutation add(PartitionUpdate update) {
      assert update != null;

      assert update.partitionKey().getPartitioner() == this.key.getPartitioner();

      this.cdcEnabled |= update.metadata().params.cdc;
      PartitionUpdate prev = (PartitionUpdate)this.modifications.put(update.metadata().id, update);
      if(prev != null) {
         throw new IllegalArgumentException("Table " + update.metadata().name + " already has modifications in this mutation: " + prev);
      } else {
         return this;
      }
   }

   public PartitionUpdate get(TableMetadata metadata) {
      return (PartitionUpdate)this.modifications.get(metadata.id);
   }

   public boolean isEmpty() {
      return this.modifications.isEmpty();
   }

   public static Mutation merge(List<Mutation> mutations) {
      assert (!mutations.isEmpty());
      if (mutations.size() == 1) {
         return mutations.get(0);
      }
      HashSet<TableId> updatedTables = new HashSet<TableId>();
      String ks = null;
      DecoratedKey key = null;
      for (Mutation mutation : mutations) {
         updatedTables.addAll(mutation.modifications.keySet());
         if (ks != null && !ks.equals(mutation.keyspaceName)) {
            throw new IllegalArgumentException();
         }
         if (key != null && !key.equals(mutation.key)) {
            throw new IllegalArgumentException();
         }
         ks = mutation.keyspaceName;
         key = mutation.key;
      }
      ArrayList<PartitionUpdate> updates = new ArrayList<PartitionUpdate>(mutations.size());
      HashMap<TableId, PartitionUpdate> modifications = new HashMap<TableId, PartitionUpdate>(updatedTables.size());
      for (TableId table : updatedTables) {
         for (Mutation mutation : mutations) {
            PartitionUpdate upd = mutation.modifications.get(table);
            if (upd == null) continue;
            updates.add(upd);
         }
         if (updates.isEmpty()) continue;
         modifications.put(table, updates.size() == 1 ? updates.get(0) : PartitionUpdate.merge(updates));
         updates.clear();
      }
      return new Mutation(ks, key, modifications, TPCTaskType.WRITE_LOCAL);
   }

   public StagedScheduler getScheduler() {
      return this.scheduler;
   }

   public TracingAwareExecutor getRequestExecutor() {
      return this.requestExecutor;
   }

   public TracingAwareExecutor getResponseExecutor() {
      return this.responseExecutor;
   }

   public Completable applyAsync(boolean durableWrites, boolean isDroppable) {
      Keyspace ks = Keyspace.open(this.keyspaceName);
      return ks.apply(this, durableWrites, true, isDroppable);
   }

   public CompletableFuture<?> applyFuture() {
      return TPCUtils.toFuture(this.applyAsync());
   }

   public Completable applyAsync() {
      return this.applyAsync(Keyspace.open(this.keyspaceName).getMetadata().params.durableWrites, true);
   }

   public void apply(boolean durableWrites) {
      TPCUtils.blockingAwait(this.applyAsync(durableWrites, true));
   }

   public void apply() {
      this.apply(Keyspace.open(this.keyspaceName).getMetadata().params.durableWrites);
   }

   public void applyUnsafe() {
      this.apply(false);
   }

   public long getTimeout() {
      return DatabaseDescriptor.getWriteRpcTimeout();
   }

   public int smallestGCGS() {
      int gcgs = 2147483647;

      PartitionUpdate update;
      for(Iterator var2 = this.getPartitionUpdates().iterator(); var2.hasNext(); gcgs = Math.min(gcgs, update.metadata().params.gcGraceSeconds)) {
         update = (PartitionUpdate)var2.next();
      }

      return gcgs;
   }

   public boolean trackedByCDC() {
      return this.cdcEnabled;
   }

   public String toString() {
      return this.toString(false);
   }

   public String toString(boolean shallow) {
      StringBuilder buff = new StringBuilder("Mutation(");
      buff.append("keyspace='").append(this.keyspaceName).append('\'');
      buff.append(", key='").append(ByteBufferUtil.bytesToHex(this.key.getKey())).append('\'');
      buff.append(", modifications=[");
      if(shallow) {
         List<String> cfnames = new ArrayList(this.modifications.size());
         Iterator var4 = this.modifications.keySet().iterator();

         while(var4.hasNext()) {
            TableId tableId = (TableId)var4.next();
            TableMetadata cfm = Schema.instance.getTableMetadata(tableId);
            cfnames.add(cfm == null?"-dropped-":cfm.name);
         }

         buff.append(StringUtils.join(cfnames, ", "));
      } else {
         buff.append("\n  ").append(StringUtils.join(this.modifications.values(), "\n  ")).append('\n');
      }

      return buff.append("])").toString();
   }

   public static Mutation.SimpleBuilder simpleBuilder(String keyspaceName, DecoratedKey partitionKey) {
      return new SimpleBuilders.MutationBuilder(keyspaceName, partitionKey);
   }

   public static class MutationSerializer extends VersionDependent<EncodingVersion> implements Serializer<Mutation> {
      private final PartitionUpdate.PartitionUpdateSerializer serializer;

      private MutationSerializer(EncodingVersion version) {
         super(version);
         this.serializer = (PartitionUpdate.PartitionUpdateSerializer)PartitionUpdate.serializers.get(version);
      }

      public void serialize(Mutation mutation, DataOutputPlus out) throws IOException {
         out.write(this.cachedSerialization(mutation));
      }

      public ByteBuffer serializedBuffer(Mutation mutation) {
         return this.cachedSerialization(mutation).duplicate();
      }

      public long serializedSize(Mutation mutation) {
         return (long)this.cachedSerialization(mutation).remaining();
      }

      private ByteBuffer cachedSerialization(Mutation mutation) {
         int versionIndex = ((EncodingVersion)this.version).ordinal();
         ByteBuffer cachedSerialization = mutation.cachedSerializations[versionIndex];
         if(cachedSerialization == null) {
            try {
               DataOutputBuffer dob = (DataOutputBuffer)DataOutputBuffer.scratchBuffer.get();
               Throwable var5 = null;

               try {
                  this.serializeInternal(mutation, dob);
                  cachedSerialization = dob.asNewBuffer();
               } catch (Throwable var15) {
                  var5 = var15;
                  throw var15;
               } finally {
                  if(dob != null) {
                     if(var5 != null) {
                        try {
                           dob.close();
                        } catch (Throwable var14) {
                           var5.addSuppressed(var14);
                        }
                     } else {
                        dob.close();
                     }
                  }

               }
            } catch (IOException var17) {
               throw new RuntimeException(var17);
            }

            mutation.cachedSerializations[versionIndex] = cachedSerialization;
         }

         return cachedSerialization;
      }

      private void serializeInternal(Mutation mutation, DataOutputPlus out) throws IOException {
         Map<TableId, PartitionUpdate> modifications = mutation.modifications;
         int size = modifications.size();
         out.writeUnsignedVInt((long)size);

         assert size > 0;

         Iterator var5 = modifications.entrySet().iterator();

         while(var5.hasNext()) {
            Entry<TableId, PartitionUpdate> entry = (Entry)var5.next();
            this.serializer.serialize((PartitionUpdate)entry.getValue(), out);
         }

      }

      public Mutation deserialize(DataInputPlus in, SerializationHelper.Flag flag) throws IOException {
         TPCTaskType writeType = flag == SerializationHelper.Flag.LOCAL?TPCTaskType.WRITE_LOCAL:TPCTaskType.WRITE_REMOTE;
         int size = (int)in.readUnsignedVInt();

         assert size > 0;

         PartitionUpdate update = this.serializer.deserialize(in, flag);
         if(size == 1) {
            return new Mutation(update.metadata().keyspace, update.partitionKey(), Collections.singletonMap(update.metadata().id, update), writeType);
         } else {
            Map<TableId, PartitionUpdate> modifications = new HashMap(size);
            DecoratedKey dk = update.partitionKey();
            modifications.put(update.metadata().id, update);

            for(int i = 1; i < size; ++i) {
               update = this.serializer.deserialize(in, flag);
               modifications.put(update.metadata().id, update);
            }

            return new Mutation(update.metadata().keyspace, dk, modifications, writeType);
         }
      }

      public Mutation deserialize(DataInputPlus in) throws IOException {
         return this.deserialize(in, SerializationHelper.Flag.FROM_REMOTE);
      }
   }

   public interface SimpleBuilder {
      Mutation.SimpleBuilder timestamp(long var1);

      Mutation.SimpleBuilder ttl(int var1);

      PartitionUpdate.SimpleBuilder update(TableMetadata var1);

      PartitionUpdate.SimpleBuilder update(String var1);

      Mutation build();
   }
}
