package org.apache.cassandra.batchlog;

import java.io.IOException;
import java.util.UUID;
import org.apache.cassandra.concurrent.SchedulableMessage;
import org.apache.cassandra.concurrent.StagedScheduler;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.concurrent.TracingAwareExecutor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.UUIDSerializer;

public class BatchRemove implements SchedulableMessage {
   public static final Serializer<BatchRemove> serializer = new BatchRemove.BatchRemoveSerializer();
   public final UUID id;

   public BatchRemove(UUID id) {
      this.id = id;
   }

   public StagedScheduler getScheduler() {
      return TPC.getForKey(Keyspace.open("system"), SystemKeyspace.decorateBatchKey(this.id));
   }

   public TracingAwareExecutor getRequestExecutor() {
      return this.getScheduler().forTaskType(TPCTaskType.BATCH_REMOVE);
   }

   static final class BatchRemoveSerializer implements Serializer<BatchRemove> {
      BatchRemoveSerializer() {
      }

      public void serialize(BatchRemove batchRemove, DataOutputPlus out) throws IOException {
         UUIDSerializer.serializer.serialize(batchRemove.id, out);
      }

      public BatchRemove deserialize(DataInputPlus in) throws IOException {
         return new BatchRemove(UUIDSerializer.serializer.deserialize(in));
      }

      public long serializedSize(BatchRemove batchRemove) {
         return UUIDSerializer.serializer.serializedSize(batchRemove.id);
      }
   }
}
