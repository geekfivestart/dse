package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CounterId;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

public abstract class SimpleBuilders {
   private SimpleBuilders() {
   }

   private static DecoratedKey makePartitonKey(TableMetadata metadata, Object... partitionKey) {
      if(partitionKey.length == 1 && partitionKey[0] instanceof DecoratedKey) {
         return (DecoratedKey)partitionKey[0];
      } else {
         ByteBuffer key = metadata.partitionKeyAsClusteringComparator().make(partitionKey).serializeAsPartitionKey();
         return metadata.partitioner.decorateKey(key);
      }
   }

   private static Clustering makeClustering(TableMetadata metadata, Object... clusteringColumns) {
      if(clusteringColumns.length == 1 && clusteringColumns[0] instanceof Clustering) {
         return (Clustering)clusteringColumns[0];
      } else if(clusteringColumns.length == 0) {
         assert metadata.comparator.size() == 0 || !metadata.staticColumns().isEmpty();

         return metadata.comparator.size() == 0?Clustering.EMPTY:Clustering.STATIC_CLUSTERING;
      } else {
         return metadata.comparator.make(clusteringColumns);
      }
   }

   public static class RowBuilder extends SimpleBuilders.AbstractBuilder<Row.SimpleBuilder> implements Row.SimpleBuilder {
      private final TableMetadata metadata;
      private final Set<ColumnMetadata> columns = new HashSet();
      private final Row.Builder builder;
      private boolean initiated;
      private boolean noPrimaryKeyLivenessInfo;

      public RowBuilder(TableMetadata metadata, Object... clusteringColumns) {
         super(null);
         this.metadata = metadata;
         this.builder = Row.Builder.unsorted(FBUtilities.nowInSeconds());
         this.builder.newRow(SimpleBuilders.makeClustering(metadata, clusteringColumns));
      }

      Set<ColumnMetadata> columns() {
         return this.columns;
      }

      private void maybeInit() {
         if(!this.initiated) {
            if(this.metadata.isCQLTable() && !this.noPrimaryKeyLivenessInfo) {
               this.builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(this.timestamp, this.ttl, this.nowInSec));
            }

            this.initiated = true;
         }
      }

      public Row.SimpleBuilder add(String columnName, Object value) {
         return this.add(columnName, value, true);
      }

      public Row.SimpleBuilder appendAll(String columnName, Object value) {
         return this.add(columnName, value, false);
      }

      private Row.SimpleBuilder add(String columnName, Object value, boolean overwriteForCollection) {
         this.maybeInit();
         ColumnMetadata column = this.getColumn(columnName);
         if(overwriteForCollection || column.type.isMultiCell() && column.type.isCollection()) {
            this.columns.add(column);
            if(!column.type.isMultiCell()) {
               this.builder.addCell(this.cell(column, this.toByteBuffer(value, column.type), (CellPath)null));
               return this;
            } else {
               assert column.type instanceof CollectionType : "Collection are the only multi-cell types supported so far";

               if(value == null) {
                  this.builder.addComplexDeletion(column, new DeletionTime(this.timestamp, this.nowInSec));
                  return this;
               } else {
                  if(overwriteForCollection) {
                     this.builder.addComplexDeletion(column, new DeletionTime(this.timestamp - 1L, this.nowInSec));
                  }

                  switch(null.$SwitchMap$org$apache$cassandra$db$marshal$CollectionType$Kind[((CollectionType)column.type).kind.ordinal()]) {
                  case 1:
                     ListType lt = (ListType)column.type;

                     assert value instanceof List;

                     Iterator var10 = ((List)value).iterator();

                     while(var10.hasNext()) {
                        Object elt = var10.next();
                        this.builder.addCell(this.cell(column, this.toByteBuffer(elt, lt.getElementsType()), CellPath.create(ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes()))));
                     }

                     return this;
                  case 2:
                     SetType st = (SetType)column.type;

                     assert value instanceof Set;

                     Iterator var11 = ((Set)value).iterator();

                     while(var11.hasNext()) {
                        Object elt = var11.next();
                        this.builder.addCell(this.cell(column, ByteBufferUtil.EMPTY_BYTE_BUFFER, CellPath.create(this.toByteBuffer(elt, st.getElementsType()))));
                     }

                     return this;
                  case 3:
                     MapType mt = (MapType)column.type;

                     assert value instanceof Map;

                     Iterator var8 = ((Map)value).entrySet().iterator();

                     while(var8.hasNext()) {
                        Entry entry = (Entry)var8.next();
                        this.builder.addCell(this.cell(column, this.toByteBuffer(entry.getValue(), mt.getValuesType()), CellPath.create(this.toByteBuffer(entry.getKey(), mt.getKeysType()))));
                     }

                     return this;
                  default:
                     throw new AssertionError();
                  }
               }
            }
         } else {
            throw new IllegalArgumentException("appendAll() can only be called on non-frozen colletions");
         }
      }

      public Row.SimpleBuilder delete() {
         assert !this.initiated : "If called, delete() should be called before any other column value addition";

         this.builder.addRowDeletion(Row.Deletion.regular(new DeletionTime(this.timestamp, this.nowInSec)));
         return this;
      }

      public Row.SimpleBuilder delete(String columnName) {
         return this.add(columnName, (Object)null);
      }

      public Row.SimpleBuilder noPrimaryKeyLivenessInfo() {
         this.noPrimaryKeyLivenessInfo = true;
         return this;
      }

      public Row build() {
         this.maybeInit();
         return this.builder.build();
      }

      private ColumnMetadata getColumn(String columnName) {
         ColumnMetadata column = this.metadata.getColumn(new ColumnIdentifier(columnName, true));

         assert column != null : "Cannot find column " + columnName;

         assert !column.isPrimaryKeyColumn();

         assert !column.isStatic() || this.builder.clustering() == Clustering.STATIC_CLUSTERING : "Cannot add non-static column to static-row";

         return column;
      }

      private Cell cell(ColumnMetadata column, ByteBuffer value, CellPath path) {
         return value == null?BufferCell.tombstone(column, this.timestamp, this.nowInSec, path):(this.ttl == 0?BufferCell.live(column, this.timestamp, value, path):BufferCell.expiring(column, this.timestamp, this.ttl, this.nowInSec, value, path));
      }

      private ByteBuffer toByteBuffer(Object value, AbstractType<?> type) {
         if(value == null) {
            return null;
         } else if(value instanceof ByteBuffer) {
            return (ByteBuffer)value;
         } else if(type.isCounter()) {
            assert value instanceof Long : "Attempted to adjust Counter cell with non-long value.";

            return CounterContext.instance().createGlobal(CounterId.getLocalId(), 1L, ((Long)value).longValue());
         } else {
            return type.decompose(value);
         }
      }
   }

   public static class PartitionUpdateBuilder extends SimpleBuilders.AbstractBuilder<PartitionUpdate.SimpleBuilder> implements PartitionUpdate.SimpleBuilder {
      private final TableMetadata metadata;
      private final DecoratedKey key;
      private final Map<Clustering, SimpleBuilders.RowBuilder> rowBuilders = new HashMap();
      private List<SimpleBuilders.PartitionUpdateBuilder.RTBuilder> rangeBuilders = null;
      private DeletionTime partitionDeletion;

      public PartitionUpdateBuilder(TableMetadata metadata, Object... partitionKeyValues) {
         super(null);
         this.partitionDeletion = DeletionTime.LIVE;
         this.metadata = metadata;
         this.key = SimpleBuilders.makePartitonKey(metadata, partitionKeyValues);
      }

      public TableMetadata metadata() {
         return this.metadata;
      }

      public Row.SimpleBuilder row(Object... clusteringValues) {
         Clustering clustering = SimpleBuilders.makeClustering(this.metadata, clusteringValues);
         SimpleBuilders.RowBuilder builder = (SimpleBuilders.RowBuilder)this.rowBuilders.get(clustering);
         if(builder == null) {
            builder = new SimpleBuilders.RowBuilder(this.metadata, new Object[]{clustering});
            this.rowBuilders.put(clustering, builder);
         }

         this.copyParams(builder);
         return builder;
      }

      public PartitionUpdate.SimpleBuilder delete() {
         this.partitionDeletion = new DeletionTime(this.timestamp, this.nowInSec);
         return this;
      }

      public PartitionUpdate.SimpleBuilder.RangeTombstoneBuilder addRangeTombstone() {
         if(this.rangeBuilders == null) {
            this.rangeBuilders = new ArrayList();
         }

         SimpleBuilders.PartitionUpdateBuilder.RTBuilder builder = new SimpleBuilders.PartitionUpdateBuilder.RTBuilder(this.metadata.comparator, new DeletionTime(this.timestamp, this.nowInSec), null);
         this.rangeBuilders.add(builder);
         return builder;
      }

      public PartitionUpdate build() {
         RegularAndStaticColumns.Builder columns = RegularAndStaticColumns.builder();
         Iterator var2 = this.rowBuilders.values().iterator();

         while(var2.hasNext()) {
            SimpleBuilders.RowBuilder builder = (SimpleBuilders.RowBuilder)var2.next();
            columns.addAll((Iterable)builder.columns());
         }

         PartitionUpdate update = new PartitionUpdate(this.metadata, this.key, columns.build(), this.rowBuilders.size());
         update.addPartitionDeletion(this.partitionDeletion);
         Iterator var6;
         if(this.rangeBuilders != null) {
            var6 = this.rangeBuilders.iterator();

            while(var6.hasNext()) {
               SimpleBuilders.PartitionUpdateBuilder.RTBuilder builder = (SimpleBuilders.PartitionUpdateBuilder.RTBuilder)var6.next();
               update.add(builder.build());
            }
         }

         var6 = this.rowBuilders.values().iterator();

         while(var6.hasNext()) {
            SimpleBuilders.RowBuilder builder = (SimpleBuilders.RowBuilder)var6.next();
            update.add(builder.build());
         }

         return update;
      }

      public Mutation buildAsMutation() {
         return new Mutation(this.build());
      }

      private static class RTBuilder implements PartitionUpdate.SimpleBuilder.RangeTombstoneBuilder {
         private final ClusteringComparator comparator;
         private final DeletionTime deletionTime;
         private Object[] start;
         private Object[] end;
         private boolean startInclusive;
         private boolean endInclusive;

         private RTBuilder(ClusteringComparator comparator, DeletionTime deletionTime) {
            this.startInclusive = true;
            this.endInclusive = true;
            this.comparator = comparator;
            this.deletionTime = deletionTime;
         }

         public PartitionUpdate.SimpleBuilder.RangeTombstoneBuilder start(Object... values) {
            this.start = values;
            return this;
         }

         public PartitionUpdate.SimpleBuilder.RangeTombstoneBuilder end(Object... values) {
            this.end = values;
            return this;
         }

         public PartitionUpdate.SimpleBuilder.RangeTombstoneBuilder inclStart() {
            this.startInclusive = true;
            return this;
         }

         public PartitionUpdate.SimpleBuilder.RangeTombstoneBuilder exclStart() {
            this.startInclusive = false;
            return this;
         }

         public PartitionUpdate.SimpleBuilder.RangeTombstoneBuilder inclEnd() {
            this.endInclusive = true;
            return this;
         }

         public PartitionUpdate.SimpleBuilder.RangeTombstoneBuilder exclEnd() {
            this.endInclusive = false;
            return this;
         }

         private RangeTombstone build() {
            ClusteringBound startBound = ClusteringBound.create(this.comparator, true, this.startInclusive, this.start);
            ClusteringBound endBound = ClusteringBound.create(this.comparator, false, this.endInclusive, this.end);
            return new RangeTombstone(Slice.make(startBound, endBound), this.deletionTime);
         }
      }
   }

   public static class MutationBuilder extends SimpleBuilders.AbstractBuilder<Mutation.SimpleBuilder> implements Mutation.SimpleBuilder {
      private final String keyspaceName;
      private final DecoratedKey key;
      private final Map<TableId, SimpleBuilders.PartitionUpdateBuilder> updateBuilders = new HashMap();

      public MutationBuilder(String keyspaceName, DecoratedKey key) {
         super(null);
         this.keyspaceName = keyspaceName;
         this.key = key;
      }

      public PartitionUpdate.SimpleBuilder update(TableMetadata metadata) {
         assert metadata.keyspace.equals(this.keyspaceName);

         SimpleBuilders.PartitionUpdateBuilder builder = (SimpleBuilders.PartitionUpdateBuilder)this.updateBuilders.get(metadata.id);
         if(builder == null) {
            builder = new SimpleBuilders.PartitionUpdateBuilder(metadata, new Object[]{this.key});
            this.updateBuilders.put(metadata.id, builder);
         }

         this.copyParams(builder);
         return builder;
      }

      public PartitionUpdate.SimpleBuilder update(String tableName) {
         TableMetadata metadata = Schema.instance.getTableMetadata(this.keyspaceName, tableName);

         assert metadata != null : "Unknown table " + tableName + " in keyspace " + this.keyspaceName;

         return this.update(metadata);
      }

      public Mutation build() {
         assert !this.updateBuilders.isEmpty() : "Cannot create empty mutation";

         if(this.updateBuilders.size() == 1) {
            return new Mutation(((SimpleBuilders.PartitionUpdateBuilder)this.updateBuilders.values().iterator().next()).build());
         } else {
            Mutation mutation = new Mutation(this.keyspaceName, this.key);
            Iterator var2 = this.updateBuilders.values().iterator();

            while(var2.hasNext()) {
               SimpleBuilders.PartitionUpdateBuilder builder = (SimpleBuilders.PartitionUpdateBuilder)var2.next();
               mutation.add(builder.build());
            }

            return mutation;
         }
      }
   }

   private static class AbstractBuilder<T> {
      protected long timestamp;
      protected int ttl;
      protected int nowInSec;

      private AbstractBuilder() {
         this.timestamp = FBUtilities.timestampMicros();
         this.ttl = 0;
         this.nowInSec = FBUtilities.nowInSeconds();
      }

      protected void copyParams(SimpleBuilders.AbstractBuilder<?> other) {
         other.timestamp = this.timestamp;
         other.ttl = this.ttl;
         other.nowInSec = this.nowInSec;
      }

      public T timestamp(long timestamp) {
         this.timestamp = timestamp;
         return this;
      }

      public T ttl(int ttl) {
         this.ttl = ttl;
         return this;
      }

      public T nowInSec(int nowInSec) {
         this.nowInSec = nowInSec;
         return this;
      }
   }
}
