package org.apache.cassandra.cql3.statements;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.UpdateParameters;
import org.apache.cassandra.cql3.conditions.ColumnCondition;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.CASRequest;
import org.apache.cassandra.utils.Pair;

public class CQL3CasRequest implements CASRequest {
   public final TableMetadata metadata;
   public final DecoratedKey key;
   public final boolean isBatch;
   private final RegularAndStaticColumns conditionColumns;
   private final boolean updatesRegularRows;
   private final boolean updatesStaticRow;
   private boolean hasExists;
   private CQL3CasRequest.RowCondition staticConditions;
   private final TreeMap<Clustering, CQL3CasRequest.RowCondition> conditions;
   private final List<CQL3CasRequest.RowUpdate> updates = new ArrayList();
   private final List<CQL3CasRequest.RangeDeletion> rangeDeletions = new ArrayList();

   public CQL3CasRequest(TableMetadata metadata, DecoratedKey key, boolean isBatch, RegularAndStaticColumns conditionColumns, boolean updatesRegularRows, boolean updatesStaticRow) {
      this.metadata = metadata;
      this.key = key;
      this.conditions = new TreeMap(metadata.comparator);
      this.isBatch = isBatch;
      this.conditionColumns = conditionColumns;
      this.updatesRegularRows = updatesRegularRows;
      this.updatesStaticRow = updatesStaticRow;
   }

   public void addRowUpdate(Clustering clustering, ModificationStatement stmt, QueryOptions options, long timestamp) {
      this.updates.add(new CQL3CasRequest.RowUpdate(clustering, stmt, options, timestamp));
   }

   public void addRangeDeletion(Slice slice, ModificationStatement stmt, QueryOptions options, long timestamp) {
      this.rangeDeletions.add(new CQL3CasRequest.RangeDeletion(slice, stmt, options, timestamp));
   }

   public void addNotExist(Clustering clustering) throws InvalidRequestException {
      this.addExistsCondition(clustering, new CQL3CasRequest.NotExistCondition(clustering), true);
   }

   public void addExist(Clustering clustering) throws InvalidRequestException {
      this.addExistsCondition(clustering, new CQL3CasRequest.ExistCondition(clustering), false);
   }

   private void addExistsCondition(Clustering clustering, CQL3CasRequest.RowCondition condition, boolean isNotExist) {
      assert condition instanceof CQL3CasRequest.ExistCondition || condition instanceof CQL3CasRequest.NotExistCondition;

      CQL3CasRequest.RowCondition previous = this.getConditionsForRow(clustering);
      if(previous != null) {
         if(previous.getClass().equals(condition.getClass())) {
            assert this.hasExists;

         } else {
            throw !(previous instanceof CQL3CasRequest.NotExistCondition) && !(previous instanceof CQL3CasRequest.ExistCondition)?new InvalidRequestException("Cannot mix IF conditions and IF " + (isNotExist?"NOT ":"") + "EXISTS for the same row"):new InvalidRequestException("Cannot mix IF EXISTS and IF NOT EXISTS conditions for the same row");
         }
      } else {
         this.setConditionsForRow(clustering, condition);
         this.hasExists = true;
      }
   }

   public void addConditions(Clustering clustering, Collection<ColumnCondition> conds, QueryOptions options) throws InvalidRequestException {
      CQL3CasRequest.RowCondition condition = this.getConditionsForRow(clustering);
      if(condition == null) {
         condition = new CQL3CasRequest.ColumnsConditions(clustering);
         this.setConditionsForRow(clustering, (CQL3CasRequest.RowCondition)condition);
      } else if(!(condition instanceof CQL3CasRequest.ColumnsConditions)) {
         throw new InvalidRequestException("Cannot mix IF conditions and IF NOT EXISTS for the same row");
      }

      ((CQL3CasRequest.ColumnsConditions)condition).addConditions(conds, options);
   }

   private CQL3CasRequest.RowCondition getConditionsForRow(Clustering clustering) {
      return clustering == Clustering.STATIC_CLUSTERING?this.staticConditions:(CQL3CasRequest.RowCondition)this.conditions.get(clustering);
   }

   private void setConditionsForRow(Clustering clustering, CQL3CasRequest.RowCondition condition) {
      if(clustering == Clustering.STATIC_CLUSTERING) {
         assert this.staticConditions == null;

         this.staticConditions = condition;
      } else {
         CQL3CasRequest.RowCondition previous = (CQL3CasRequest.RowCondition)this.conditions.put(clustering, condition);

         assert previous == null;
      }

   }

   private RegularAndStaticColumns columnsToRead() {
      if(this.hasExists) {
         RegularAndStaticColumns allColumns = this.metadata.regularAndStaticColumns();
         Columns statics = this.updatesStaticRow?allColumns.statics:Columns.NONE;
         Columns regulars = this.updatesRegularRows?allColumns.regulars:Columns.NONE;
         return new RegularAndStaticColumns(statics, regulars);
      } else {
         return this.conditionColumns;
      }
   }

   public SinglePartitionReadCommand readCommand(int nowInSec) {
      assert this.staticConditions != null || !this.conditions.isEmpty();

      ColumnFilter columnFilter = ColumnFilter.selection(this.metadata, this.columnsToRead());
      if(this.conditions.isEmpty()) {
         return SinglePartitionReadCommand.create(this.metadata, nowInSec, columnFilter, RowFilter.NONE, DataLimits.cqlLimits(1), this.key, new ClusteringIndexSliceFilter(Slices.ALL, false));
      } else {
         ClusteringIndexNamesFilter filter = new ClusteringIndexNamesFilter(this.conditions.navigableKeySet(), false);
         return SinglePartitionReadCommand.create(this.metadata, nowInSec, this.key, columnFilter, filter);
      }
   }

   public boolean appliesTo(FilteredPartition current) throws InvalidRequestException {
      if(this.staticConditions != null && !this.staticConditions.appliesTo(current)) {
         return false;
      } else {
         Iterator var2 = this.conditions.values().iterator();

         CQL3CasRequest.RowCondition condition;
         do {
            if(!var2.hasNext()) {
               return true;
            }

            condition = (CQL3CasRequest.RowCondition)var2.next();
         } while(condition.appliesTo(current));

         return false;
      }
   }

   private RegularAndStaticColumns updatedColumns() {
      RegularAndStaticColumns.Builder builder = RegularAndStaticColumns.builder();
      Iterator var2 = this.updates.iterator();

      while(var2.hasNext()) {
         CQL3CasRequest.RowUpdate upd = (CQL3CasRequest.RowUpdate)var2.next();
         builder.addAll(upd.stmt.updatedColumns());
      }

      return builder.build();
   }

   public PartitionUpdate makeUpdates(FilteredPartition current) throws InvalidRequestException {
      PartitionUpdate update = new PartitionUpdate(this.metadata, this.key, this.updatedColumns(), this.conditions.size());
      Iterator var3 = this.updates.iterator();

      while(var3.hasNext()) {
         CQL3CasRequest.RowUpdate upd = (CQL3CasRequest.RowUpdate)var3.next();
         upd.applyUpdates(current, update);
      }

      var3 = this.rangeDeletions.iterator();

      while(var3.hasNext()) {
         CQL3CasRequest.RangeDeletion upd = (CQL3CasRequest.RangeDeletion)var3.next();
         upd.applyUpdates(current, update);
      }

      Keyspace.openAndGetStore(this.metadata).indexManager.validate(update);
      return update;
   }

   private static class ColumnsConditions extends CQL3CasRequest.RowCondition {
      private final Multimap<Pair<ColumnIdentifier, ByteBuffer>, ColumnCondition.Bound> conditions;

      private ColumnsConditions(Clustering clustering) {
         super(clustering);
         this.conditions = HashMultimap.create();
      }

      public void addConditions(Collection<ColumnCondition> conds, QueryOptions options) throws InvalidRequestException {
         Iterator var3 = conds.iterator();

         while(var3.hasNext()) {
            ColumnCondition condition = (ColumnCondition)var3.next();
            ColumnCondition.Bound current = condition.bind(options);
            this.conditions.put(Pair.create(condition.column.name, current.getCollectionElementValue()), current);
         }

      }

      public boolean appliesTo(FilteredPartition current) throws InvalidRequestException {
         Row row = current.getRow(this.clustering);
         Iterator var3 = this.conditions.values().iterator();

         ColumnCondition.Bound condition;
         do {
            if(!var3.hasNext()) {
               return true;
            }

            condition = (ColumnCondition.Bound)var3.next();
         } while(condition.appliesTo(row));

         return false;
      }
   }

   private static class ExistCondition extends CQL3CasRequest.RowCondition {
      private ExistCondition(Clustering clustering) {
         super(clustering);
      }

      public boolean appliesTo(FilteredPartition current) {
         return current.getRow(this.clustering) != null;
      }
   }

   private static class NotExistCondition extends CQL3CasRequest.RowCondition {
      private NotExistCondition(Clustering clustering) {
         super(clustering);
      }

      public boolean appliesTo(FilteredPartition current) {
         return current.getRow(this.clustering) == null;
      }
   }

   private abstract static class RowCondition {
      public final Clustering clustering;

      protected RowCondition(Clustering clustering) {
         this.clustering = clustering;
      }

      public abstract boolean appliesTo(FilteredPartition var1) throws InvalidRequestException;
   }

   private class RangeDeletion {
      private final Slice slice;
      private final ModificationStatement stmt;
      private final QueryOptions options;
      private final long timestamp;

      private RangeDeletion(Slice slice, ModificationStatement stmt, QueryOptions options, long timestamp) {
         this.slice = slice;
         this.stmt = stmt;
         this.options = options;
         this.timestamp = timestamp;
      }

      public void applyUpdates(FilteredPartition current, PartitionUpdate updates) throws InvalidRequestException {
         Map<DecoratedKey, Partition> map = this.stmt.requiresRead()?Collections.singletonMap(CQL3CasRequest.this.key, current):null;
         UpdateParameters params = new UpdateParameters(CQL3CasRequest.this.metadata, updates.columns(), this.options, this.timestamp, this.stmt.getTimeToLive(this.options), map);
         this.stmt.addUpdateForKey(updates, this.slice, params);
      }
   }

   private class RowUpdate {
      private final Clustering clustering;
      private final ModificationStatement stmt;
      private final QueryOptions options;
      private final long timestamp;

      private RowUpdate(Clustering clustering, ModificationStatement stmt, QueryOptions options, long timestamp) {
         this.clustering = clustering;
         this.stmt = stmt;
         this.options = options;
         this.timestamp = timestamp;
      }

      public void applyUpdates(FilteredPartition current, PartitionUpdate updates) throws InvalidRequestException {
         Map<DecoratedKey, Partition> map = this.stmt.requiresRead()?Collections.singletonMap(CQL3CasRequest.this.key, current):null;
         UpdateParameters params = new UpdateParameters(CQL3CasRequest.this.metadata, updates.columns(), this.options, this.timestamp, this.stmt.getTimeToLive(this.options), map);
         this.stmt.addUpdateForKey(updates, this.clustering, params);
      }
   }
}
