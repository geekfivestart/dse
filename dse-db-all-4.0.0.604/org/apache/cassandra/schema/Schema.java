package org.apache.cassandra.schema;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Sets;
import com.google.common.collect.MapDifference.ValueDifference;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.functions.UDAggregate;
import org.apache.cassandra.cql3.functions.UDFunction;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.KeyspaceNotDefinedException;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MessagingVersion;
import org.apache.cassandra.utils.Pair;
import org.jctools.maps.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Schema {
   private static final Logger logger = LoggerFactory.getLogger(Schema.class);
   public static final int COMPATIBILITY_VERSION = 0;
   public static final Schema instance = new Schema();
   private volatile Keyspaces keyspaces = Keyspaces.none();
   private final Map<TableId, TableMetadataRef> metadataRefs = new NonBlockingHashMap();
   private final Map<Pair<String, String>, TableMetadataRef> indexMetadataRefs = new NonBlockingHashMap();
   private final Map<String, Keyspace> keyspaceInstances = new NonBlockingHashMap();
   private volatile UUID version;
   private final List<SchemaChangeListener> changeListeners = new CopyOnWriteArrayList();
   private final Map<InetAddress, Integer> endpointCompatibilityVersions = new ConcurrentHashMap();
   @VisibleForTesting
   public static boolean loadedFromDisk;

   private Schema() {
      if(DatabaseDescriptor.isDaemonInitialized() || DatabaseDescriptor.isToolInitialized()) {
         this.load(SchemaKeyspace.metadata());
         this.load(SystemKeyspace.metadata());
         loadedFromDisk = true;
      }

   }

   public static void validateKeyspaceNotSystem(String keyspace) {
      if(SchemaConstants.isLocalSystemKeyspace(keyspace)) {
         throw new InvalidRequestException(String.format("%s keyspace is not user-modifiable", new Object[]{keyspace}));
      }
   }

   public void loadFromDisk() {
      this.loadFromDisk(true);
   }

   public void loadFromDisk(boolean updateVersion) {
      this.load((Iterable)SchemaKeyspace.fetchNonSystemKeyspaces());
      if(updateVersion) {
         TPCUtils.blockingAwait(this.updateVersion());
      }

      loadedFromDisk = true;
   }

   private void load(Iterable<KeyspaceMetadata> keyspaceDefs) {
      keyspaceDefs.forEach(this::load);
   }

   public synchronized void load(KeyspaceMetadata ksm) {
      KeyspaceMetadata previous = this.keyspaces.getNullable(ksm.name);
      if(previous == null) {
         this.loadNew(ksm);
      } else {
         this.reload(previous, ksm);
      }

      this.keyspaces = this.keyspaces.withAddedOrUpdated(ksm);
   }

   private void loadNew(KeyspaceMetadata ksm) {
      ksm.tablesAndViews().forEach((metadata) -> {
         TableMetadataRef var10000 = (TableMetadataRef)this.metadataRefs.put(metadata.id, new TableMetadataRef(metadata));
      });
      ksm.tables.indexTables().forEach((name, metadata) -> {
         TableMetadataRef var10000 = (TableMetadataRef)this.indexMetadataRefs.put(Pair.create(ksm.name, name), new TableMetadataRef(metadata));
      });
   }

   private void reload(KeyspaceMetadata previous, KeyspaceMetadata updated) {
      Keyspace keyspace = this.getKeyspaceInstance(updated.name);
      if(keyspace != null) {
         keyspace.setMetadata(updated);
      }

      MapDifference<TableId, TableMetadata> tablesDiff = previous.tables.diff(updated.tables);
      MapDifference<TableId, ViewMetadata> viewsDiff = previous.views.diff(updated.views);
      MapDifference<String, TableMetadata> indexesDiff = previous.tables.indexesDiff(updated.tables);
      tablesDiff.entriesOnlyOnLeft().values().forEach((table) -> {
         TableMetadataRef var10000 = (TableMetadataRef)this.metadataRefs.remove(table.id);
      });
      viewsDiff.entriesOnlyOnLeft().values().forEach((view) -> {
         TableMetadataRef var10000 = (TableMetadataRef)this.metadataRefs.remove(view.metadata.id);
      });
      indexesDiff.entriesOnlyOnLeft().values().forEach((indexTable) -> {
         TableMetadataRef var10000 = (TableMetadataRef)this.indexMetadataRefs.remove(Pair.create(indexTable.keyspace, indexTable.indexName().get()));
      });
      tablesDiff.entriesOnlyOnRight().values().forEach((table) -> {
         TableMetadataRef var10000 = (TableMetadataRef)this.metadataRefs.put(table.id, new TableMetadataRef(table));
      });
      viewsDiff.entriesOnlyOnRight().values().forEach((view) -> {
         TableMetadataRef var10000 = (TableMetadataRef)this.metadataRefs.put(view.metadata.id, new TableMetadataRef(view.metadata));
      });
      indexesDiff.entriesOnlyOnRight().values().forEach((indexTable) -> {
         TableMetadataRef var10000 = (TableMetadataRef)this.indexMetadataRefs.put(Pair.create(indexTable.keyspace, indexTable.indexName().get()), new TableMetadataRef(indexTable));
      });
      tablesDiff.entriesDiffering().values().forEach((diff) -> {
         ((TableMetadataRef)this.metadataRefs.get(((TableMetadata)diff.rightValue()).id)).set((TableMetadata)diff.rightValue());
      });
      viewsDiff.entriesDiffering().values().forEach((diff) -> {
         ((TableMetadataRef)this.metadataRefs.get(((ViewMetadata)diff.rightValue()).metadata.id)).set(((ViewMetadata)diff.rightValue()).metadata);
      });
      indexesDiff.entriesDiffering().values().stream().map(ValueDifference::rightValue).forEach((indexTable) -> {
         ((TableMetadataRef)this.indexMetadataRefs.get(Pair.create(indexTable.keyspace, indexTable.indexName().get()))).set(indexTable);
      });
   }

   public void registerListenerEarly(SchemaChangeListener listener) {
      if(!this.changeListeners.isEmpty()) {
         this.changeListeners.add(0, listener);
      } else {
         this.changeListeners.add(listener);
      }

   }

   public void registerListener(SchemaChangeListener listener) {
      this.changeListeners.add(listener);
   }

   public void unregisterListener(SchemaChangeListener listener) {
      this.changeListeners.remove(listener);
   }

   public Keyspace getKeyspaceInstance(String keyspaceName) {
      return (Keyspace)this.keyspaceInstances.get(keyspaceName);
   }

   public ColumnFamilyStore getColumnFamilyStoreInstance(TableId id) {
      TableMetadata metadata = this.getTableMetadata(id);
      if(metadata == null) {
         return null;
      } else {
         Keyspace instance = this.getKeyspaceInstance(metadata.keyspace);
         return instance == null?null:(instance.hasColumnFamilyStore(metadata.id)?instance.getColumnFamilyStore(metadata.id):null);
      }
   }

   public void storeKeyspaceInstance(Keyspace keyspace) {
      if(this.keyspaceInstances.containsKey(keyspace.getName())) {
         throw new IllegalArgumentException(String.format("Keyspace %s was already initialized.", new Object[]{keyspace.getName()}));
      } else {
         this.keyspaceInstances.put(keyspace.getName(), keyspace);
      }
   }

   public Keyspace removeKeyspaceInstance(String keyspaceName) {
      return (Keyspace)this.keyspaceInstances.remove(keyspaceName);
   }

   @VisibleForTesting
   public synchronized void unload(KeyspaceMetadata ksm) {
      this.keyspaces = this.keyspaces.without(ksm.name);
      ksm.tablesAndViews().forEach((t) -> {
         TableMetadataRef var10000 = (TableMetadataRef)this.metadataRefs.remove(t.id);
      });
      ksm.tables.indexTables().keySet().forEach((name) -> {
         TableMetadataRef var10000 = (TableMetadataRef)this.indexMetadataRefs.remove(Pair.create(ksm.name, name));
      });
   }

   @VisibleForTesting
   public synchronized void unsafeUnload(TableMetadata table) {
      KeyspaceMetadata keyspace = this.getKeyspaceMetadata(table.keyspace);
      keyspace = keyspace.withSwapped(keyspace.tables.without(table.name));
      this.keyspaces = this.keyspaces.withAddedOrUpdated(keyspace);
      this.metadataRefs.remove(table.id);
   }

   @VisibleForTesting
   public synchronized void unsafeReplace(TableMetadata table) {
      KeyspaceMetadata keyspace = this.getKeyspaceMetadata(table.keyspace);
      keyspace = keyspace.withSwapped(keyspace.tables.without(table.name).with(table));
      this.keyspaces = this.keyspaces.withAddedOrUpdated(keyspace);
      this.metadataRefs.remove(table.id);
   }

   public int getNumberOfTables() {
      return this.keyspaces.stream().mapToInt((k) -> {
         return Iterables.size(k.tablesAndViews());
      }).sum();
   }

   public ViewMetadata getView(String keyspaceName, String viewName) {
      assert keyspaceName != null;

      KeyspaceMetadata ksm = this.keyspaces.getNullable(keyspaceName);
      return ksm == null?null:ksm.views.getNullable(viewName);
   }

   public KeyspaceMetadata getKeyspaceMetadata(String keyspaceName) {
      assert keyspaceName != null;

      return this.keyspaces.getNullable(keyspaceName);
   }

   private Set<String> getNonSystemKeyspacesSet() {
      return Sets.difference(this.keyspaces.names(), SchemaConstants.LOCAL_SYSTEM_KEYSPACE_NAMES);
   }

   public ImmutableList<String> getNonSystemKeyspaces() {
      return ImmutableList.copyOf(this.getNonSystemKeyspacesSet());
   }

   public List<String> getNonLocalStrategyKeyspaces() {
      return (List)this.keyspaces.stream().filter((keyspace) -> {
         return keyspace.params.replication.klass != LocalStrategy.class;
      }).map((keyspace) -> {
         return keyspace.name;
      }).collect(Collectors.toList());
   }

   public List<String> getPartitionedKeyspaces() {
      return (List)this.keyspaces.stream().filter((keyspace) -> {
         return Keyspace.open(keyspace.name).getReplicationStrategy().isPartitioned();
      }).map((keyspace) -> {
         return keyspace.name;
      }).collect(Collectors.toList());
   }

   public List<String> getUserKeyspaces() {
      return ImmutableList.copyOf(Sets.difference(this.getNonSystemKeyspacesSet(), SchemaConstants.REPLICATED_SYSTEM_KEYSPACE_NAMES));
   }

   public Iterable<TableMetadata> getTablesAndViews(String keyspaceName) {
      assert keyspaceName != null;

      KeyspaceMetadata ksm = this.keyspaces.getNullable(keyspaceName);

      assert ksm != null;

      return ksm.tablesAndViews();
   }

   public Set<String> getKeyspaces() {
      return this.keyspaces.names();
   }

   public TableMetadataRef getTableMetadataRef(String keyspace, String table) {
      TableMetadata tm = this.getTableMetadata(keyspace, table);
      return tm == null?null:(TableMetadataRef)this.metadataRefs.get(tm.id);
   }

   public TableMetadataRef getIndexTableMetadataRef(String keyspace, String index) {
      return (TableMetadataRef)this.indexMetadataRefs.get(Pair.create(keyspace, index));
   }

   public TableMetadataRef getTableMetadataRef(TableId id) {
      return (TableMetadataRef)this.metadataRefs.get(id);
   }

   public TableMetadataRef getTableMetadataRef(Descriptor descriptor) {
      return this.getTableMetadataRef(descriptor.ksname, descriptor.cfname);
   }

   public TableMetadata getTableMetadata(String keyspace, String table) {
      assert keyspace != null;

      assert table != null;

      KeyspaceMetadata ksm = this.keyspaces.getNullable(keyspace);
      return ksm == null?null:ksm.getTableOrViewNullable(table);
   }

   public TableMetadata getTableMetadata(TableId id) {
      return this.keyspaces.getTableOrViewNullable(id);
   }

   public TableMetadata validateTable(String keyspaceName, String tableName) {
      if(tableName.isEmpty()) {
         throw new InvalidRequestException("non-empty table is required");
      } else {
         KeyspaceMetadata keyspace = this.keyspaces.getNullable(keyspaceName);
         if(keyspace == null) {
            throw new KeyspaceNotDefinedException(String.format("keyspace %s does not exist", new Object[]{keyspaceName}));
         } else {
            TableMetadata metadata = keyspace.getTableOrViewNullable(tableName);
            if(metadata == null) {
               throw new InvalidRequestException(String.format("table %s does not exist", new Object[]{tableName}));
            } else {
               return metadata;
            }
         }
      }
   }

   public TableMetadata getTableMetadata(Descriptor descriptor) {
      return this.getTableMetadata(descriptor.ksname, descriptor.cfname);
   }

   public TableMetadata getExistingTableMetadata(TableId id) throws UnknownTableException {
      TableMetadata metadata = this.getTableMetadata(id);
      if(metadata != null) {
         return metadata;
      } else {
         String message = String.format("Couldn't find table with id %s. If a table was just created, this is likely due to the schema not being fully propagated.  Please wait for schema agreement on table creation.", new Object[]{id});
         throw new UnknownTableException(message, id);
      }
   }

   public Collection<org.apache.cassandra.cql3.functions.Function> getFunctions(FunctionName name) {
      if(!name.hasKeyspace()) {
         throw new IllegalArgumentException(String.format("Function name must be fully qualified: got %s", new Object[]{name}));
      } else {
         KeyspaceMetadata ksm = this.getKeyspaceMetadata(name.keyspace);
         return (Collection)(ksm == null?Collections.emptyList():ksm.functions.get(name));
      }
   }

   public Optional<org.apache.cassandra.cql3.functions.Function> findFunction(FunctionName name, List<AbstractType<?>> argTypes) {
      if(!name.hasKeyspace()) {
         throw new IllegalArgumentException(String.format("Function name must be fully quallified: got %s", new Object[]{name}));
      } else {
         KeyspaceMetadata ksm = this.getKeyspaceMetadata(name.keyspace);
         return ksm == null?Optional.empty():ksm.functions.find(name, argTypes);
      }
   }

   public UUID getVersion() {
      return this.version;
   }

   public boolean isSameVersion(UUID schemaVersion) {
      return schemaVersion != null && schemaVersion.equals(this.version);
   }

   public boolean isEmpty() {
      return SchemaConstants.emptyVersion.equals(this.version);
   }

   public CompletableFuture<Void> updateVersion() {
      this.version = SchemaKeyspace.calculateSchemaDigest();
      return SystemKeyspace.updateSchemaVersion(this.version);
   }

   public void updateVersionAndAnnounce() {
      TPCUtils.blockingAwait(this.updateVersion());
      MigrationManager.passiveAnnounce(this.version);
   }

   public synchronized void clear() {
      this.getNonSystemKeyspaces().forEach((k) -> {
         this.unload(this.getKeyspaceMetadata(k));
      });
      this.updateVersionAndAnnounce();
   }

   public synchronized void reloadSchemaAndAnnounceVersion() {
      this.reloadSchema();
      this.updateVersionAndAnnounce();
   }

   public synchronized void reloadSchema() {
      Keyspaces before = this.keyspaces.filter((k) -> {
         return !SchemaConstants.isLocalSystemKeyspace(k.name);
      });
      Keyspaces after = SchemaKeyspace.fetchNonSystemKeyspaces();
      this.merge(before, after);
   }

   synchronized void mergeAndAnnounceVersion(SchemaMigration schema) {
      assert schema.isCompatible;

      this.merge(schema);
      this.updateVersionAndAnnounce();
   }

   synchronized void merge(SchemaMigration schema) {
      Set<String> affectedKeyspaces = SchemaKeyspace.affectedKeyspaces(schema.mutations);
      Keyspaces before = this.keyspaces.filter((k) -> {
         return affectedKeyspaces.contains(k.name);
      });
      SchemaKeyspace.applyChanges(schema.mutations);
      Keyspaces after = SchemaKeyspace.fetchKeyspaces(affectedKeyspaces);
      this.merge(before, after);
   }

   private synchronized void merge(Keyspaces before, Keyspaces after) {
      MapDifference<String, KeyspaceMetadata> keyspacesDiff = before.diff(after);
      keyspacesDiff.entriesOnlyOnLeft().values().forEach(this::dropKeyspace);
      keyspacesDiff.entriesOnlyOnRight().values().forEach(this::createKeyspace);
      keyspacesDiff.entriesDiffering().entrySet().forEach((diff) -> {
         this.alterKeyspace((KeyspaceMetadata)((ValueDifference)diff.getValue()).leftValue(), (KeyspaceMetadata)((ValueDifference)diff.getValue()).rightValue());
      });
   }

   private void alterKeyspace(KeyspaceMetadata before, KeyspaceMetadata after) {
      MapDifference<TableId, TableMetadata> tablesDiff = before.tables.diff(after.tables);
      MapDifference<TableId, ViewMetadata> viewsDiff = before.views.diff(after.views);
      MapDifference<ByteBuffer, UserType> typesDiff = before.types.diff(after.types);
      MapDifference<Pair<FunctionName, List<String>>, UDFunction> udfsDiff = before.functions.udfsDiff(after.functions);
      MapDifference<Pair<FunctionName, List<String>>, UDAggregate> udasDiff = before.functions.udasDiff(after.functions);
      viewsDiff.entriesOnlyOnLeft().values().forEach(this::dropView);
      tablesDiff.entriesOnlyOnLeft().values().forEach(this::dropTable);
      this.load(after);
      tablesDiff.entriesOnlyOnRight().values().forEach(this::createTable);
      viewsDiff.entriesOnlyOnRight().values().forEach(this::createView);
      tablesDiff.entriesDiffering().values().forEach((diff) -> {
         this.alterTable((TableMetadata)diff.rightValue());
      });
      viewsDiff.entriesDiffering().values().forEach((diff) -> {
         this.alterView((ViewMetadata)diff.rightValue());
      });
      Keyspace.open(before.name).viewManager.reload(true);
      udasDiff.entriesOnlyOnLeft().values().forEach(this::notifyDropAggregate);
      udfsDiff.entriesOnlyOnLeft().values().forEach(this::notifyDropFunction);
      viewsDiff.entriesOnlyOnLeft().values().forEach(this::notifyDropView);
      tablesDiff.entriesOnlyOnLeft().values().forEach(this::notifyDropTable);
      typesDiff.entriesOnlyOnLeft().values().forEach(this::notifyDropType);
      typesDiff.entriesOnlyOnRight().values().forEach(this::notifyCreateType);
      tablesDiff.entriesOnlyOnRight().values().forEach(this::notifyCreateTable);
      viewsDiff.entriesOnlyOnRight().values().forEach(this::notifyCreateView);
      udfsDiff.entriesOnlyOnRight().values().forEach(this::notifyCreateFunction);
      udasDiff.entriesOnlyOnRight().values().forEach(this::notifyCreateAggregate);
      if(!before.params.equals(after.params)) {
         this.notifyAlterKeyspace(after);
      }

      typesDiff.entriesDiffering().values().forEach((diff) -> {
         this.notifyAlterType((UserType)diff.rightValue());
      });
      tablesDiff.entriesDiffering().values().forEach((diff) -> {
         this.notifyAlterTable((TableMetadata)diff.leftValue(), (TableMetadata)diff.rightValue());
      });
      viewsDiff.entriesDiffering().values().forEach((diff) -> {
         this.notifyAlterView((ViewMetadata)diff.leftValue(), (ViewMetadata)diff.rightValue());
      });
      udfsDiff.entriesDiffering().values().forEach((diff) -> {
         this.notifyAlterFunction((UDFunction)diff.rightValue());
      });
      udasDiff.entriesDiffering().values().forEach((diff) -> {
         this.notifyAlterAggregate((UDAggregate)diff.rightValue());
      });
   }

   private void createKeyspace(KeyspaceMetadata keyspace) {
      this.load(keyspace);
      Keyspace.open(keyspace.name);
      this.notifyCreateKeyspace(keyspace);
      keyspace.types.forEach(this::notifyCreateType);
      keyspace.tables.forEach(this::notifyCreateTable);
      keyspace.views.forEach(this::notifyCreateView);
      keyspace.functions.udfs().forEach(this::notifyCreateFunction);
      keyspace.functions.udas().forEach(this::notifyCreateAggregate);
   }

   private void dropKeyspace(KeyspaceMetadata keyspace) {
      keyspace.views.forEach(this::dropView);
      keyspace.tables.forEach(this::dropTable);
      Keyspace.clear(keyspace.name);
      this.unload(keyspace);
      Keyspace.writeOrder.awaitNewBarrier();
      keyspace.functions.udas().forEach(this::notifyDropAggregate);
      keyspace.functions.udfs().forEach(this::notifyDropFunction);
      keyspace.views.forEach(this::notifyDropView);
      keyspace.tables.forEach(this::notifyDropTable);
      keyspace.types.forEach(this::notifyDropType);
      this.notifyDropKeyspace(keyspace);
   }

   private void dropView(ViewMetadata metadata) {
      Keyspace.open(metadata.keyspace).viewManager.dropView(metadata.name);
      this.dropTable(metadata.metadata);
   }

   private void dropTable(TableMetadata metadata) {
      ColumnFamilyStore cfs = Keyspace.open(metadata.keyspace).getColumnFamilyStore(metadata.name);

      assert cfs != null;

      cfs.indexManager.markAllIndexesRemoved();
      CompactionManager.instance.interruptCompactionFor(Collections.singleton(metadata));
      if(DatabaseDescriptor.isAutoSnapshot()) {
         cfs.snapshot(Keyspace.getTimestampedSnapshotNameWithPrefix(cfs.name, "dropped"));
      }

      CommitLog.instance.forceRecycleAllSegments(Collections.singleton(metadata.id));
      Keyspace.open(metadata.keyspace).dropCf(metadata.id);
   }

   private void createTable(TableMetadata table) {
      Keyspace.open(table.keyspace).initCf((TableMetadataRef)this.metadataRefs.get(table.id), true);
   }

   private void createView(ViewMetadata view) {
      Keyspace.open(view.keyspace).initCf((TableMetadataRef)this.metadataRefs.get(view.metadata.id), true);
   }

   private void alterTable(TableMetadata updated) {
      Keyspace.open(updated.keyspace).getColumnFamilyStore(updated.name).reload();
   }

   private void alterView(ViewMetadata updated) {
      Keyspace.open(updated.keyspace).getColumnFamilyStore(updated.name).reload();
   }

   private void notifyCreateKeyspace(KeyspaceMetadata ksm) {
      this.changeListeners.forEach((l) -> {
         l.onCreateKeyspace(ksm.name);
      });
   }

   private void notifyCreateTable(TableMetadata metadata) {
      this.changeListeners.forEach((l) -> {
         l.onCreateTable(metadata.keyspace, metadata.name);
      });
   }

   private void notifyCreateView(ViewMetadata view) {
      this.changeListeners.forEach((l) -> {
         l.onCreateView(view.keyspace, view.name);
      });
   }

   private void notifyCreateType(UserType ut) {
      this.changeListeners.forEach((l) -> {
         l.onCreateType(ut.keyspace, ut.getNameAsString());
      });
   }

   private void notifyCreateFunction(UDFunction udf) {
      this.changeListeners.forEach((l) -> {
         l.onCreateFunction(udf.name().keyspace, udf.name().name, udf.argTypes());
      });
   }

   private void notifyCreateAggregate(UDAggregate udf) {
      this.changeListeners.forEach((l) -> {
         l.onCreateAggregate(udf.name().keyspace, udf.name().name, udf.argTypes());
      });
   }

   private void notifyAlterKeyspace(KeyspaceMetadata ksm) {
      this.changeListeners.forEach((l) -> {
         l.onAlterKeyspace(ksm.name);
      });
   }

   private void notifyAlterTable(TableMetadata current, TableMetadata updated) {
      boolean changeAffectedPreparedStatements = current.changeAffectsPreparedStatements(updated);
      this.changeListeners.forEach((l) -> {
         l.onAlterTable(updated.keyspace, updated.name, changeAffectedPreparedStatements);
      });
   }

   private void notifyAlterView(ViewMetadata current, ViewMetadata updated) {
      boolean changeAffectedPreparedStatements = current.metadata.changeAffectsPreparedStatements(updated.metadata);
      this.changeListeners.forEach((l) -> {
         l.onAlterView(updated.keyspace, updated.name, changeAffectedPreparedStatements);
      });
   }

   private void notifyAlterType(UserType ut) {
      this.changeListeners.forEach((l) -> {
         l.onAlterType(ut.keyspace, ut.getNameAsString());
      });
   }

   private void notifyAlterFunction(UDFunction udf) {
      this.changeListeners.forEach((l) -> {
         l.onAlterFunction(udf.name().keyspace, udf.name().name, udf.argTypes());
      });
   }

   private void notifyAlterAggregate(UDAggregate udf) {
      this.changeListeners.forEach((l) -> {
         l.onAlterAggregate(udf.name().keyspace, udf.name().name, udf.argTypes());
      });
   }

   private void notifyDropKeyspace(KeyspaceMetadata ksm) {
      this.changeListeners.forEach((l) -> {
         l.onDropKeyspace(ksm.name);
      });
   }

   private void notifyDropTable(TableMetadata metadata) {
      this.changeListeners.forEach((l) -> {
         l.onDropTable(metadata.keyspace, metadata.name);
      });
   }

   private void notifyDropView(ViewMetadata view) {
      this.changeListeners.forEach((l) -> {
         l.onDropView(view.keyspace, view.name);
      });
   }

   private void notifyDropType(UserType ut) {
      this.changeListeners.forEach((l) -> {
         l.onDropType(ut.keyspace, ut.getNameAsString());
      });
   }

   private void notifyDropFunction(UDFunction udf) {
      this.changeListeners.forEach((l) -> {
         l.onDropFunction(udf.name().keyspace, udf.name().name, udf.argTypes());
      });
   }

   private void notifyDropAggregate(UDAggregate udf) {
      this.changeListeners.forEach((l) -> {
         l.onDropAggregate(udf.name().keyspace, udf.name().name, udf.argTypes());
      });
   }

   public void updateEndpointCompatibilityVersion(InetAddress endpoint, int compatibilityVersion) {
      Integer previous = (Integer)this.endpointCompatibilityVersions.put(endpoint, Integer.valueOf(compatibilityVersion));
      if(previous == null) {
         logger.debug("Received schema compatibility version {} from {}", Integer.valueOf(compatibilityVersion), endpoint);
      } else {
         logger.debug("Received update of schema compatibility version from {} to {} for {}", new Object[]{previous, Integer.valueOf(compatibilityVersion), endpoint});
      }

   }

   public boolean isSchemaCompatibleWith(InetAddress endpoint) {
      Integer endpointVersion = (Integer)this.endpointCompatibilityVersions.get(endpoint);
      if(endpointVersion != null) {
         return this.isSchemaCompatibleWith(endpointVersion.intValue());
      } else {
         Optional<MessagingVersion> endpointMessagingVersion = MessagingService.instance().getVersion(endpoint);
         return endpointMessagingVersion.isPresent() && endpointMessagingVersion.get() == MessagingVersion.DSE_60;
      }
   }

   public boolean isSchemaCompatibleWith(int remoteSchemaCompatibilityVersion) {
      return remoteSchemaCompatibilityVersion == 0;
   }

   public static String schemaVersionToString(UUID version) {
      return version == null?"unknown":(SchemaConstants.emptyVersion.equals(version)?"(empty)":version.toString());
   }
}
