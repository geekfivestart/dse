package org.apache.cassandra.cql3.statements;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.CounterMutation;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;

final class UpdatesCollector {
   private final Map<TableId, RegularAndStaticColumns> updatedColumns;
   private final int updatedRows;
   private final Map<String, Map<ByteBuffer, IMutation>> mutations = new HashMap();

   public UpdatesCollector(Map<TableId, RegularAndStaticColumns> updatedColumns, int updatedRows) {
      this.updatedColumns = updatedColumns;
      this.updatedRows = updatedRows;
   }

   public PartitionUpdate getPartitionUpdate(TableMetadata metadata, DecoratedKey dk, ConsistencyLevel consistency) {
      Mutation mut = this.getMutation(metadata, dk, consistency);
      PartitionUpdate upd = mut.get(metadata);
      if(upd == null) {
         RegularAndStaticColumns columns = (RegularAndStaticColumns)this.updatedColumns.get(metadata.id);

         assert columns != null;

         upd = new PartitionUpdate(metadata, dk, columns, this.updatedRows);
         mut.add(upd);
      }

      return upd;
   }

   public void validateIndexedColumns() {
      Iterator var1 = this.mutations.values().iterator();

      while(var1.hasNext()) {
         Map<ByteBuffer, IMutation> perKsMutations = (Map)var1.next();
         Iterator var3 = perKsMutations.values().iterator();

         while(var3.hasNext()) {
            IMutation mutation = (IMutation)var3.next();
            Iterator var5 = mutation.getPartitionUpdates().iterator();

            while(var5.hasNext()) {
               PartitionUpdate update = (PartitionUpdate)var5.next();
               Keyspace.openAndGetStore(update.metadata()).indexManager.validate(update);
            }
         }
      }

   }

   private Mutation getMutation(TableMetadata metadata, DecoratedKey dk, ConsistencyLevel consistency) {
      String ksName = metadata.keyspace;
      IMutation mutation = (IMutation)this.keyspaceMap(ksName).get(dk.getKey());
      if(mutation == null) {
         Mutation mut = new Mutation(ksName, dk);
         mutation = metadata.isCounter()?new CounterMutation(mut, consistency):mut;
         this.keyspaceMap(ksName).put(dk.getKey(), mutation);
         return mut;
      } else {
         return metadata.isCounter()?((CounterMutation)mutation).getMutation():(Mutation)mutation;
      }
   }

   public Collection<IMutation> toMutations() {
      if(this.mutations.size() == 1) {
         return ((Map)this.mutations.values().iterator().next()).values();
      } else {
         List<IMutation> ms = new ArrayList();
         Iterator var2 = this.mutations.values().iterator();

         while(var2.hasNext()) {
            Map<ByteBuffer, IMutation> ksMap = (Map)var2.next();
            ms.addAll(ksMap.values());
         }

         return ms;
      }
   }

   private Map<ByteBuffer, IMutation> keyspaceMap(String ksName) {
      Map<ByteBuffer, IMutation> ksMap = (Map)this.mutations.get(ksName);
      if(ksMap == null) {
         ksMap = new HashMap();
         this.mutations.put(ksName, ksMap);
      }

      return (Map)ksMap;
   }
}
