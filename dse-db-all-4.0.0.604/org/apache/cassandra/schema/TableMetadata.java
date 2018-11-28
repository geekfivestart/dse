package org.apache.cassandra.schema;

import com.google.common.base.MoreObjects;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.cassandra.auth.DataResource;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.CompactTables;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.compaction.MemoryOnlyStrategy;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.AbstractIterator;
import org.github.jamm.Unmetered;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Unmetered
public final class TableMetadata {
   private static final Logger logger = LoggerFactory.getLogger(TableMetadata.class);
   private static final ImmutableSet<TableMetadata.Flag> DEFAULT_CQL_FLAGS;
   private static final ImmutableSet<TableMetadata.Flag> DEPRECATED_CS_FLAGS;
   private static final String COMPACT_STORAGE_DEPRECATION_MESSAGE = "Incorrect set of flags is was detected in table {}.{}: '{}'. \nStarting with version 4.0, '{}' flags are deprecated and every table has to have COMPOUND flag. \nForcing the following set of flags: '{}'";
   public final String keyspace;
   public final String name;
   public final TableId id;
   public final IPartitioner partitioner;
   public final TableParams params;
   public final ImmutableSet<TableMetadata.Flag> flags;
   private final boolean isView;
   private final String indexName;
   public final ImmutableMap<ByteBuffer, DroppedColumn> droppedColumns;
   final ImmutableMap<ByteBuffer, ColumnMetadata> columns;
   private final ImmutableList<ColumnMetadata> partitionKeyColumns;
   private final ImmutableList<ColumnMetadata> clusteringColumns;
   private final RegularAndStaticColumns regularAndStaticColumns;
   private final ClusteringComparator partitionKeyClusteringComparator;
   public final Indexes indexes;
   public final Triggers triggers;
   public final AbstractType<?> partitionKeyType;
   public final ClusteringComparator comparator;
   public final ColumnMetadata compactValueColumn;
   public final boolean hasMulticellOrCounterColumn;
   public final DataResource resource;
   public final Config.AccessMode diskAccessMode;
   public final Config.AccessMode indexAccessMode;

   private TableMetadata(TableMetadata.Builder builder) {
      if(!TableMetadata.Flag.isCQLCompatible(builder.flags)) {
         this.flags = ImmutableSet.copyOf(Sets.union(Sets.difference(builder.flags, DEPRECATED_CS_FLAGS), DEFAULT_CQL_FLAGS));
         logger.warn("Incorrect set of flags is was detected in table {}.{}: '{}'. \nStarting with version 4.0, '{}' flags are deprecated and every table has to have COMPOUND flag. \nForcing the following set of flags: '{}'", new Object[]{builder.keyspace, builder.name, builder.flags, DEPRECATED_CS_FLAGS, this.flags});
      } else {
         this.flags = Sets.immutableEnumSet(builder.flags);
      }

      this.keyspace = builder.keyspace;
      this.name = builder.name;
      this.id = builder.id;
      this.partitioner = builder.partitioner;
      this.params = builder.params.build();
      this.isView = builder.isView;
      this.indexName = this.name.contains(".")?this.name.substring(this.name.indexOf(46) + 1):null;
      this.droppedColumns = ImmutableMap.copyOf(builder.droppedColumns);
      Collections.sort(builder.partitionKeyColumns);
      this.partitionKeyColumns = ImmutableList.copyOf(builder.partitionKeyColumns);
      Collections.sort(builder.clusteringColumns);
      this.clusteringColumns = ImmutableList.copyOf(builder.clusteringColumns);
      this.regularAndStaticColumns = RegularAndStaticColumns.builder().addAll((Iterable)builder.regularAndStaticColumns).build();
      this.columns = ImmutableMap.copyOf(builder.columns);
      this.partitionKeyClusteringComparator = new ClusteringComparator((Iterable)this.partitionKeyColumns.stream().map((c) -> {
         return c.type;
      }).collect(Collectors.toList()));
      this.indexes = builder.indexes;
      this.triggers = builder.triggers;
      this.partitionKeyType = (AbstractType)(this.partitionKeyColumns.size() == 1?((ColumnMetadata)this.partitionKeyColumns.get(0)).type:CompositeType.getInstance(Iterables.transform(this.partitionKeyColumns, (t) -> {
         return t.type;
      })));
      this.comparator = new ClusteringComparator(Iterables.transform(this.clusteringColumns, (c) -> {
         return c.type;
      }));
      this.compactValueColumn = this.isCompactTable()?CompactTables.getCompactValueColumn(this.regularAndStaticColumns, this.isSuper()):null;
      this.hasMulticellOrCounterColumn = Iterables.any(this.regularAndStaticColumns, (c) -> {
         return c.type.isMultiCell() || c.type.isCounter();
      });
      this.resource = DataResource.table(this.keyspace, this.name);
      this.diskAccessMode = this.params.compaction.klass().equals(MemoryOnlyStrategy.class)?Config.AccessMode.mmap:DatabaseDescriptor.getDiskAccessMode();
      this.indexAccessMode = this.params.compaction.klass().equals(MemoryOnlyStrategy.class)?Config.AccessMode.mmap:DatabaseDescriptor.getIndexAccessMode();
   }

   public static TableMetadata.Builder builder(String keyspace, String table) {
      return new TableMetadata.Builder(keyspace, table, null);
   }

   public static TableMetadata.Builder builder(String keyspace, String table, TableId id) {
      return new TableMetadata.Builder(keyspace, table, id);
   }

   public TableMetadata.Builder unbuild() {
      return builder(this.keyspace, this.name, this.id).partitioner(this.partitioner).params(this.params).flags(this.flags).isView(this.isView).addColumns(this.columns()).droppedColumns(this.droppedColumns).indexes(this.indexes).triggers(this.triggers);
   }

   public boolean isView() {
      return this.isView;
   }

   public boolean isIndex() {
      return this.indexName != null;
   }

   public Optional<String> indexName() {
      return Optional.ofNullable(this.indexName);
   }

   public boolean isDense() {
      return this.flags.contains(TableMetadata.Flag.DENSE);
   }

   public boolean isCompound() {
      return this.flags.contains(TableMetadata.Flag.COMPOUND);
   }

   public boolean isSuper() {
      return this.flags.contains(TableMetadata.Flag.SUPER);
   }

   public boolean isCounter() {
      return this.flags.contains(TableMetadata.Flag.COUNTER);
   }

   public boolean isCQLTable() {
      return !this.isSuper() && !this.isDense() && this.isCompound();
   }

   public boolean isCompactTable() {
      return !this.isCQLTable();
   }

   public boolean isStaticCompactTable() {
      return !this.isSuper() && !this.isDense() && !this.isCompound();
   }

   public ImmutableCollection<ColumnMetadata> columns() {
      return this.columns.values();
   }

   public Iterable<ColumnMetadata> primaryKeyColumns() {
      return Iterables.concat(this.partitionKeyColumns, this.clusteringColumns);
   }

   public ImmutableList<ColumnMetadata> partitionKeyColumns() {
      return this.partitionKeyColumns;
   }

   public ImmutableList<ColumnMetadata> clusteringColumns() {
      return this.clusteringColumns;
   }

   public RegularAndStaticColumns regularAndStaticColumns() {
      return this.regularAndStaticColumns;
   }

   public Columns regularColumns() {
      return this.regularAndStaticColumns.regulars;
   }

   public Columns staticColumns() {
      return this.regularAndStaticColumns.statics;
   }

   public Iterator<ColumnMetadata> allColumnsInSelectOrder() {
      final boolean isStaticCompactTable = this.isStaticCompactTable();
      final boolean noNonPkColumns = this.isCompactTable() && CompactTables.hasEmptyCompactValue(this);
      return new AbstractIterator<ColumnMetadata>() {
         private final Iterator<ColumnMetadata> partitionKeyIter;
         private final Iterator<ColumnMetadata> clusteringIter;
         private final Iterator<ColumnMetadata> otherColumns;

         {
            this.partitionKeyIter = TableMetadata.this.partitionKeyColumns.iterator();
            this.clusteringIter = (Iterator)(isStaticCompactTable?Collections.emptyIterator():TableMetadata.this.clusteringColumns.iterator());
            this.otherColumns = noNonPkColumns?Collections.emptyIterator():(isStaticCompactTable?TableMetadata.this.staticColumns().selectOrderIterator():TableMetadata.this.regularAndStaticColumns.selectOrderIterator());
         }

         protected ColumnMetadata computeNext() {
            return this.partitionKeyIter.hasNext()?(ColumnMetadata)this.partitionKeyIter.next():(this.clusteringIter.hasNext()?(ColumnMetadata)this.clusteringIter.next():(this.otherColumns.hasNext()?(ColumnMetadata)this.otherColumns.next():(ColumnMetadata)this.endOfData()));
         }
      };
   }

   public ColumnMetadata getColumn(ColumnIdentifier name) {
      return this.getColumn(name.bytes);
   }

   public ColumnMetadata getColumn(ByteBuffer name) {
      return (ColumnMetadata)this.columns.get(name);
   }

   public ColumnMetadata getDroppedColumn(ByteBuffer name) {
      DroppedColumn dropped = (DroppedColumn)this.droppedColumns.get(name);
      return dropped == null?null:dropped.column;
   }

   public ColumnMetadata getDroppedColumn(ByteBuffer name, boolean isStatic) {
      DroppedColumn dropped = (DroppedColumn)this.droppedColumns.get(name);
      return dropped == null?null:(isStatic && !dropped.column.isStatic()?ColumnMetadata.staticColumn(this, name, dropped.column.type):dropped.column);
   }

   public boolean hasStaticColumns() {
      return !this.staticColumns().isEmpty();
   }

   public void validate() {
      if(!IndexMetadata.isNameValid(this.keyspace)) {
         this.except("Keyspace name must not be empty, more than %s characters long, or contain non-alphanumeric-underscore characters (got \"%s\")", new Object[]{Integer.valueOf(48), this.keyspace});
      }

      if(!IndexMetadata.isNameValid(this.name)) {
         this.except("Table name must not be empty, more than %s characters long, or contain non-alphanumeric-underscore characters (got \"%s\")", new Object[]{Integer.valueOf(48), this.name});
      }

      this.params.validate();
      if(this.partitionKeyColumns.stream().anyMatch((c) -> {
         return c.type.isCounter();
      })) {
         this.except("PRIMARY KEY columns cannot contain counters", new Object[0]);
      }

      Iterator var1;
      ColumnMetadata column;
      if(this.isCounter()) {
         var1 = this.regularAndStaticColumns.iterator();

         while(var1.hasNext()) {
            column = (ColumnMetadata)var1.next();
            if(!column.type.isCounter() && !CompactTables.isSuperColumnMapColumn(column)) {
               this.except("Cannot have a non counter column (\"%s\") in a counter table", new Object[]{column.name});
            }
         }
      } else {
         var1 = this.regularAndStaticColumns.iterator();

         while(var1.hasNext()) {
            column = (ColumnMetadata)var1.next();
            if(column.type.isCounter()) {
               this.except("Cannot have a counter column (\"%s\") in a non counter column table", new Object[]{column.name});
            }
         }
      }

      if(this.partitionKeyColumns.isEmpty()) {
         this.except("Missing partition keys for table %s", new Object[]{this.toString()});
      }

      if(this.isCompactTable() && this.clusteringColumns.isEmpty()) {
         this.except("For table %s, isDense=%b, isCompound=%b, clustering=%s", new Object[]{this.toString(), Boolean.valueOf(this.isDense()), Boolean.valueOf(this.isCompound()), this.clusteringColumns});
      }

      if(!this.indexes.isEmpty() && this.isSuper()) {
         this.except("Secondary indexes are not supported on super column families", new Object[0]);
      }

      this.indexes.validate(this);
   }

   void validateCompatibility(TableMetadata other) {
      if(!this.isIndex()) {
         if(!other.keyspace.equals(this.keyspace)) {
            this.except("Keyspace mismatch (found %s; expected %s)", new Object[]{other.keyspace, this.keyspace});
         }

         if(!other.name.equals(this.name)) {
            this.except("Table mismatch (found %s; expected %s)", new Object[]{other.name, this.name});
         }

         if(!other.id.equals(this.id)) {
            this.except("Table ID mismatch (found %s; expected %s)", new Object[]{other.id, this.id});
         }

         if(!other.flags.equals(this.flags)) {
            this.except("Table type mismatch (found %s; expected %s)", new Object[]{other.flags, this.flags});
         }

         if(other.partitionKeyColumns.size() != this.partitionKeyColumns.size()) {
            this.except("Partition keys of different length (found %s; expected %s)", new Object[]{Integer.valueOf(other.partitionKeyColumns.size()), Integer.valueOf(this.partitionKeyColumns.size())});
         }

         int i;
         for(i = 0; i < this.partitionKeyColumns.size(); ++i) {
            if(!((ColumnMetadata)other.partitionKeyColumns.get(i)).type.isCompatibleWith(((ColumnMetadata)this.partitionKeyColumns.get(i)).type)) {
               this.except("Partition key column mismatch (found %s; expected %s)", new Object[]{((ColumnMetadata)other.partitionKeyColumns.get(i)).type, ((ColumnMetadata)this.partitionKeyColumns.get(i)).type});
            }
         }

         if(other.clusteringColumns.size() != this.clusteringColumns.size()) {
            this.except("Clustering columns of different length (found %s; expected %s)", new Object[]{Integer.valueOf(other.clusteringColumns.size()), Integer.valueOf(this.clusteringColumns.size())});
         }

         for(i = 0; i < this.clusteringColumns.size(); ++i) {
            if(!((ColumnMetadata)other.clusteringColumns.get(i)).type.isCompatibleWith(((ColumnMetadata)this.clusteringColumns.get(i)).type)) {
               this.except("Clustering column mismatch (found %s; expected %s)", new Object[]{((ColumnMetadata)other.clusteringColumns.get(i)).type, ((ColumnMetadata)this.clusteringColumns.get(i)).type});
            }
         }

         Iterator var5 = other.regularAndStaticColumns.iterator();

         while(var5.hasNext()) {
            ColumnMetadata otherColumn = (ColumnMetadata)var5.next();
            ColumnMetadata column = this.getColumn(otherColumn.name);
            if(column != null && !otherColumn.type.isCompatibleWith(column.type)) {
               this.except("Column mismatch (found %s; expected %s", new Object[]{otherColumn, column});
            }
         }

      }
   }

   public ClusteringComparator partitionKeyAsClusteringComparator() {
      return this.partitionKeyClusteringComparator;
   }

   public AbstractType<?> staticCompactOrSuperTableColumnNameType() {
      if(!this.isSuper()) {
         assert this.isStaticCompactTable();

         return ((ColumnMetadata)this.clusteringColumns.get(0)).type;
      } else {
         assert this.compactValueColumn != null && this.compactValueColumn.type instanceof MapType;

         return ((MapType)this.compactValueColumn.type).nameComparator();
      }
   }

   public AbstractType<?> columnDefinitionNameComparator(ColumnMetadata.Kind kind) {
      return (AbstractType)((!this.isSuper() || kind != ColumnMetadata.Kind.REGULAR) && (!this.isStaticCompactTable() || kind != ColumnMetadata.Kind.STATIC)?UTF8Type.instance:this.staticCompactOrSuperTableColumnNameType());
   }

   public String indexTableName(IndexMetadata info) {
      return this.name + "." + info.name;
   }

   boolean changeAffectsPreparedStatements(TableMetadata updated) {
      return !this.partitionKeyColumns.equals(updated.partitionKeyColumns) || !this.clusteringColumns.equals(updated.clusteringColumns) || !this.regularAndStaticColumns.equals(updated.regularAndStaticColumns) || !this.indexes.equals(updated.indexes) || this.params.defaultTimeToLive != updated.params.defaultTimeToLive || this.params.gcGraceSeconds != updated.params.gcGraceSeconds;
   }

   public static TableMetadata minimal(String keyspace, String name) {
      return builder(keyspace, name).addPartitionKeyColumn((String)"key", BytesType.instance).build();
   }

   public TableMetadata updateIndexTableMetadata(TableParams baseTableParams) {
      TableParams.Builder builder = baseTableParams.unbuild().readRepairChance(0.0D).dcLocalReadRepairChance(0.0D).gcGraceSeconds(0);
      builder.caching(baseTableParams.caching.cacheKeys()?CachingParams.CACHE_KEYS:CachingParams.CACHE_NOTHING);
      return this.unbuild().params(builder.build()).build();
   }

   private void except(String format, Object... args) {
      throw new ConfigurationException(this.keyspace + "." + this.name + ": " + String.format(format, args));
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(!(o instanceof TableMetadata)) {
         return false;
      } else {
         TableMetadata tm = (TableMetadata)o;
         return this.equalsIgnoringParameters(tm) && this.params.equals(tm.params);
      }
   }

   private boolean equalsIgnoringParameters(TableMetadata tm) {
      return this.keyspace.equals(tm.keyspace) && this.name.equals(tm.name) && this.id.equals(tm.id) && this.partitioner.equals(tm.partitioner) && this.flags.equals(tm.flags) && this.isView == tm.isView && this.columns.equals(tm.columns) && this.droppedColumns.equals(tm.droppedColumns) && this.indexes.equals(tm.indexes) && this.triggers.equals(tm.triggers);
   }

   public boolean equalsIgnoringNodeSync(TableMetadata tm) {
      return this.equalsIgnoringParameters(tm) && this.params.equalsIgnoringNodeSync(tm.params);
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.keyspace, this.name, this.id, this.partitioner, this.params, this.flags, Boolean.valueOf(this.isView), this.columns, this.droppedColumns, this.indexes, this.triggers});
   }

   public String toString() {
      return String.format("%s.%s", new Object[]{ColumnIdentifier.maybeQuote(this.keyspace), ColumnIdentifier.maybeQuote(this.name)});
   }

   public String toDebugString() {
      return MoreObjects.toStringHelper(this).add("keyspace", this.keyspace).add("table", this.name).add("id", this.id).add("partitioner", this.partitioner).add("params", this.params).add("flags", this.flags).add("isView", this.isView).add("columns", this.columns()).add("droppedColumns", this.droppedColumns.values()).add("indexes", this.indexes).add("triggers", this.triggers).toString();
   }

   public boolean enforceStrictLiveness() {
      return this.isView && Keyspace.open(this.keyspace).viewManager.getByName(this.name).enforceStrictLiveness();
   }

   static {
      DEFAULT_CQL_FLAGS = ImmutableSet.of(TableMetadata.Flag.COMPOUND);
      DEPRECATED_CS_FLAGS = ImmutableSet.of(TableMetadata.Flag.DENSE, TableMetadata.Flag.SUPER);
   }

   public static final class Builder {
      final String keyspace;
      final String name;
      private TableId id;
      private IPartitioner partitioner;
      private TableParams.Builder params;
      private Set<TableMetadata.Flag> flags;
      private Triggers triggers;
      private Indexes indexes;
      private final Map<ByteBuffer, DroppedColumn> droppedColumns;
      private final Map<ByteBuffer, ColumnMetadata> columns;
      private final List<ColumnMetadata> partitionKeyColumns;
      private final List<ColumnMetadata> clusteringColumns;
      private final List<ColumnMetadata> regularAndStaticColumns;
      private boolean isView;

      private Builder(String keyspace, String name, TableId id) {
         this.params = TableParams.builder();
         this.flags = EnumSet.of(TableMetadata.Flag.COMPOUND);
         this.triggers = Triggers.none();
         this.indexes = Indexes.none();
         this.droppedColumns = new HashMap();
         this.columns = new HashMap();
         this.partitionKeyColumns = new ArrayList();
         this.clusteringColumns = new ArrayList();
         this.regularAndStaticColumns = new ArrayList();
         this.keyspace = keyspace;
         this.name = name;
         this.id = id;
      }

      private Builder(String keyspace, String name) {
         this.params = TableParams.builder();
         this.flags = EnumSet.of(TableMetadata.Flag.COMPOUND);
         this.triggers = Triggers.none();
         this.indexes = Indexes.none();
         this.droppedColumns = new HashMap();
         this.columns = new HashMap();
         this.partitionKeyColumns = new ArrayList();
         this.clusteringColumns = new ArrayList();
         this.regularAndStaticColumns = new ArrayList();
         this.keyspace = keyspace;
         this.name = name;
      }

      public TableMetadata build() {
         if(this.partitioner == null) {
            this.partitioner = DatabaseDescriptor.getPartitioner();
         }

         if(this.id == null) {
            this.id = TableId.generate();
         }

         return new TableMetadata(this);
      }

      public TableMetadata.Builder id(TableId val) {
         this.id = val;
         return this;
      }

      public TableMetadata.Builder partitioner(IPartitioner val) {
         this.partitioner = val;
         return this;
      }

      public TableMetadata.Builder params(TableParams val) {
         this.params = val.unbuild();
         return this;
      }

      public TableMetadata.Builder bloomFilterFpChance(double val) {
         this.params.bloomFilterFpChance(val);
         return this;
      }

      public TableMetadata.Builder caching(CachingParams val) {
         this.params.caching(val);
         return this;
      }

      public TableMetadata.Builder comment(String val) {
         this.params.comment(val);
         return this;
      }

      public TableMetadata.Builder compaction(CompactionParams val) {
         this.params.compaction(val);
         return this;
      }

      public TableMetadata.Builder compression(CompressionParams val) {
         this.params.compression(val);
         return this;
      }

      public TableMetadata.Builder dcLocalReadRepairChance(double val) {
         this.params.dcLocalReadRepairChance(val);
         return this;
      }

      public TableMetadata.Builder defaultTimeToLive(int val) {
         this.params.defaultTimeToLive(val);
         return this;
      }

      public TableMetadata.Builder gcGraceSeconds(int val) {
         this.params.gcGraceSeconds(val);
         return this;
      }

      public TableMetadata.Builder maxIndexInterval(int val) {
         this.params.maxIndexInterval(val);
         return this;
      }

      public TableMetadata.Builder memtableFlushPeriod(int val) {
         this.params.memtableFlushPeriodInMs(val);
         return this;
      }

      public TableMetadata.Builder minIndexInterval(int val) {
         this.params.minIndexInterval(val);
         return this;
      }

      public TableMetadata.Builder readRepairChance(double val) {
         this.params.readRepairChance(val);
         return this;
      }

      public TableMetadata.Builder crcCheckChance(double val) {
         this.params.crcCheckChance(val);
         return this;
      }

      public TableMetadata.Builder speculativeRetry(SpeculativeRetryParam val) {
         this.params.speculativeRetry(val);
         return this;
      }

      public TableMetadata.Builder nodesync(NodeSyncParams val) {
         this.params.nodeSync(val);
         return this;
      }

      public TableMetadata.Builder extensions(Map<String, ByteBuffer> val) {
         this.params.extensions(val);
         return this;
      }

      public TableMetadata.Builder isView(boolean val) {
         this.isView = val;
         return this;
      }

      public TableMetadata.Builder flags(Set<TableMetadata.Flag> val) {
         this.flags = val;
         return this;
      }

      public TableMetadata.Builder isSuper(boolean val) {
         return this.flag(TableMetadata.Flag.SUPER, val);
      }

      public TableMetadata.Builder isCounter(boolean val) {
         return this.flag(TableMetadata.Flag.COUNTER, val);
      }

      public TableMetadata.Builder isDense(boolean val) {
         return this.flag(TableMetadata.Flag.DENSE, val);
      }

      public TableMetadata.Builder isCompound(boolean val) {
         return this.flag(TableMetadata.Flag.COMPOUND, val);
      }

      private TableMetadata.Builder flag(TableMetadata.Flag flag, boolean set) {
         if(set) {
            this.flags.add(flag);
         } else {
            this.flags.remove(flag);
         }

         return this;
      }

      public TableMetadata.Builder triggers(Triggers val) {
         this.triggers = val;
         return this;
      }

      public TableMetadata.Builder indexes(Indexes val) {
         this.indexes = val;
         return this;
      }

      public TableMetadata.Builder addPartitionKeyColumn(String name, AbstractType type) {
         return this.addPartitionKeyColumn(ColumnIdentifier.getInterned(name, false), type);
      }

      public TableMetadata.Builder addPartitionKeyColumn(ColumnIdentifier name, AbstractType type) {
         return this.addColumn(new ColumnMetadata(this.keyspace, this.name, name, type, this.partitionKeyColumns.size(), ColumnMetadata.Kind.PARTITION_KEY));
      }

      public TableMetadata.Builder addClusteringColumn(String name, AbstractType type) {
         return this.addClusteringColumn(ColumnIdentifier.getInterned(name, false), type);
      }

      public TableMetadata.Builder addClusteringColumn(ColumnIdentifier name, AbstractType type) {
         return this.addColumn(new ColumnMetadata(this.keyspace, this.name, name, type, this.clusteringColumns.size(), ColumnMetadata.Kind.CLUSTERING));
      }

      public TableMetadata.Builder addRegularColumn(String name, AbstractType type) {
         return this.addRegularColumn(ColumnIdentifier.getInterned(name, false), type);
      }

      public TableMetadata.Builder addRegularColumn(ColumnIdentifier name, AbstractType type) {
         return this.addColumn(new ColumnMetadata(this.keyspace, this.name, name, type, -1, ColumnMetadata.Kind.REGULAR));
      }

      public TableMetadata.Builder addStaticColumn(String name, AbstractType type) {
         return this.addStaticColumn(ColumnIdentifier.getInterned(name, false), type);
      }

      public TableMetadata.Builder addStaticColumn(ColumnIdentifier name, AbstractType type) {
         return this.addColumn(new ColumnMetadata(this.keyspace, this.name, name, type, -1, ColumnMetadata.Kind.STATIC));
      }

      public TableMetadata.Builder addColumn(ColumnMetadata column) {
         if (this.columns.containsKey(column.name.bytes)) {
            throw new IllegalArgumentException();
         }
         switch (column.kind) {
            case PARTITION_KEY: {
               this.partitionKeyColumns.add(column);
               Collections.sort(this.partitionKeyColumns);
               break;
            }
            case CLUSTERING: {
               column.type.checkComparable();
               this.clusteringColumns.add(column);
               Collections.sort(this.clusteringColumns);
               break;
            }
            default: {
               this.regularAndStaticColumns.add(column);
            }
         }
         this.columns.put(column.name.bytes, column);
         return this;
      }

      public TableMetadata.Builder addColumns(Iterable<ColumnMetadata> columns) {
         columns.forEach(this::addColumn);
         return this;
      }

      public TableMetadata.Builder droppedColumns(Map<ByteBuffer, DroppedColumn> droppedColumns) {
         this.droppedColumns.clear();
         this.droppedColumns.putAll(droppedColumns);
         return this;
      }

      public TableMetadata.Builder recordDeprecatedSystemColumn(String name, AbstractType<?> type) {
         assert SchemaConstants.isLocalSystemKeyspace(this.keyspace);

         this.recordColumnDrop(ColumnMetadata.regularColumn(this.keyspace, this.name, name, type), 9223372036854775807L);
         return this;
      }

      public TableMetadata.Builder recordColumnDrop(ColumnMetadata column, long timeMicros) {
         this.droppedColumns.put(column.name.bytes, new DroppedColumn(column, timeMicros));
         return this;
      }

      public Iterable<ColumnMetadata> columns() {
         return this.columns.values();
      }

      public Set<String> columnNames() {
         return (Set)this.columns.values().stream().map((c) -> {
            return c.name.toString();
         }).collect(Collectors.toSet());
      }

      public ColumnMetadata getColumn(ColumnIdentifier identifier) {
         return (ColumnMetadata)this.columns.get(identifier.bytes);
      }

      public ColumnMetadata getColumn(ByteBuffer name) {
         return (ColumnMetadata)this.columns.get(name);
      }

      public boolean hasRegularColumns() {
         return this.regularAndStaticColumns.stream().anyMatch(ColumnMetadata::isRegular);
      }

      public TableMetadata.Builder removeRegularOrStaticColumn(ColumnIdentifier identifier) {
         ColumnMetadata column = (ColumnMetadata)this.columns.get(identifier.bytes);
         if(column != null && !column.isPrimaryKeyColumn()) {
            this.columns.remove(identifier.bytes);
            this.regularAndStaticColumns.remove(column);
            return this;
         } else {
            throw new IllegalArgumentException();
         }
      }

      public TableMetadata.Builder renamePrimaryKeyColumn(ColumnIdentifier from, ColumnIdentifier to) {
         if(this.columns.containsKey(to.bytes)) {
            throw new IllegalArgumentException();
         } else {
            ColumnMetadata column = (ColumnMetadata)this.columns.get(from.bytes);
            if(column != null && column.isPrimaryKeyColumn()) {
               ColumnMetadata newColumn = column.withNewName(to);
               if(column.isPartitionKey()) {
                  this.partitionKeyColumns.set(column.position(), newColumn);
               } else {
                  this.clusteringColumns.set(column.position(), newColumn);
               }

               this.columns.remove(from.bytes);
               this.columns.put(to.bytes, newColumn);
               return this;
            } else {
               throw new IllegalArgumentException();
            }
         }
      }

      public TableMetadata.Builder alterColumnType(ColumnIdentifier name, AbstractType<?> type) {

         ColumnMetadata column = this.columns.get(name.bytes);
         if (column == null) {
            throw new IllegalArgumentException();
         }
         ColumnMetadata newColumn = column.withNewType(type);
         switch (column.kind) {
            case PARTITION_KEY: {
               this.partitionKeyColumns.set(column.position(), newColumn);
               break;
            }
            case CLUSTERING: {
               this.clusteringColumns.set(column.position(), newColumn);
               break;
            }
            case REGULAR:
            case STATIC: {
               this.regularAndStaticColumns.remove(column);
               this.regularAndStaticColumns.add(newColumn);
            }
         }
         this.columns.put(column.name.bytes, newColumn);
         return this;
      }
   }

   public static enum Flag {
      SUPER,
      COUNTER,
      DENSE,
      COMPOUND;

      private Flag() {
      }

      public static boolean isCQLCompatible(Set<TableMetadata.Flag> flags) {
         return !flags.contains(DENSE) && !flags.contains(SUPER) && flags.contains(COMPOUND);
      }

      public static Set<TableMetadata.Flag> fromStringSet(Set<String> strings) {
         return (Set)strings.stream().map(String::toUpperCase).map(TableMetadata.Flag::valueOf).collect(Collectors.toSet());
      }

      public static Set<String> toStringSet(Set<TableMetadata.Flag> flags) {
         return (Set)flags.stream().map(Enum::toString).map(String::toLowerCase).collect(Collectors.toSet());
      }
   }
}
