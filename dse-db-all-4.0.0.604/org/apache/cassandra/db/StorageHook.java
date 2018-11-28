package org.apache.cassandra.db;

import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.FBUtilities;

public interface StorageHook {
   StorageHook instance = createHook();

   void reportWrite(TableId var1, PartitionUpdate var2);

   void reportRead(TableId var1, DecoratedKey var2);

   public static StorageHook createHook() {
      String className = System.getProperty("cassandra.storage_hook");
      return className != null?(StorageHook)FBUtilities.construct(className, StorageHook.class.getSimpleName()):new StorageHook() {
         public void reportWrite(TableId tableId, PartitionUpdate partitionUpdate) {
         }

         public void reportRead(TableId tableId, DecoratedKey key) {
         }
      };
   }
}
