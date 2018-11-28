package org.apache.cassandra.cql3.statements;

import com.datastax.bdp.db.audit.AuditableEventType;
import com.datastax.bdp.db.audit.CoreAuditableEventType;
import com.google.common.collect.Iterables;
import io.reactivex.Maybe;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.DateTieredCompactionStrategy;
import org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.view.View;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.DroppedColumn;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Indexes;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.schema.ViewMetadata;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Event;
import io.reactivex.MaybeSource;

public class AlterTableStatement extends SchemaAlteringStatement implements TableStatement {
   public final AlterTableStatement.Type oType;
   private final TableAttributes attrs;
   private final Map<ColumnMetadata.Raw, ColumnMetadata.Raw> renames;
   private final List<AlterTableStatementColumn> colNameList;
   private final Long deleteTimestamp;

   public AlterTableStatement(CFName name, AlterTableStatement.Type type, List<AlterTableStatementColumn> colDataList, TableAttributes attrs, Map<ColumnMetadata.Raw, ColumnMetadata.Raw> renames, Long deleteTimestamp) {
      super(name);
      this.oType = type;
      this.colNameList = colDataList;
      this.attrs = attrs;
      this.renames = renames;
      this.deleteTimestamp = deleteTimestamp;
   }

   public AuditableEventType getAuditEventType() {
      return CoreAuditableEventType.UPDATE_CF;
   }

   public void checkAccess(QueryState state) {
      state.checkTablePermission(this.keyspace(), this.columnFamily(), CorePermission.ALTER);
   }

   public void validate(QueryState state) {
   }

   public Maybe<Event.SchemaChange> announceMigration(QueryState queryState, boolean isLocalOnly) throws RequestValidationException {
      TableMetadata current = Schema.instance.validateTable(this.keyspace(), this.columnFamily());
      if (current.isView()) {
         return this.error("Cannot use ALTER TABLE on Materialized View");
      }
      TableMetadata.Builder builder = current.unbuild();
      ColumnIdentifier columnName = null;
      ColumnMetadata def = null;
      CQL3Type.Raw dataType = null;
      boolean isStatic = false;
      CQL3Type validator = null;
      ArrayList<ViewMetadata> viewUpdates = new ArrayList<ViewMetadata>();
      Iterable<ViewMetadata> views = View.findAll(this.keyspace(), this.columnFamily());
      if (!SchemaConstants.isUserKeyspace(this.keyspace()) && this.oType != Type.OPTS) {
         return this.error("Cannot alter schema (adding, renaming or removing columns) of system keyspace " + this.keyspace());
      }
      switch (this.oType) {
         case ALTER: {
            return this.error("Altering of types is not allowed");
         }
         case ADD: {
            if (current.isDense()) {
               return this.error("Cannot add new column to a COMPACT STORAGE table");
            }
            for (AlterTableStatementColumn colData : this.colNameList) {
               columnName = colData.getColumnName().getIdentifier(current);
               def = builder.getColumn(columnName);
               dataType = colData.getColumnType();
               assert (dataType != null);
               isStatic = colData.getStaticType();
               validator = dataType.prepare(this.keyspace());
               if (isStatic) {
                  if (!current.isCompound()) {
                     return this.error("Static columns are not allowed in COMPACT STORAGE tables");
                  }
                  if (current.clusteringColumns().isEmpty()) {
                     return this.error("Static columns are only useful (and thus allowed) if the table has at least one clustering column");
                  }
               }
               if (def != null) {
                  switch (def.kind) {
                     case PARTITION_KEY:
                     case CLUSTERING: {
                        return this.error(String.format("Invalid column name %s because it conflicts with a PRIMARY KEY part", columnName));
                     }
                  }
                  return this.error(String.format("Invalid column name %s because it conflicts with an existing column", columnName));
               }
               if (current.isCounter() && current.getDroppedColumn(columnName.bytes) != null) {
                  return this.error(String.format("Cannot re-add previously dropped counter column %s", columnName));
               }
               AbstractType<?> type = validator.getType();
               if (type.isCollection() && type.isMultiCell()) {
                  if (!current.isCompound()) {
                     return this.error("Cannot use non-frozen collections in COMPACT STORAGE tables");
                  }
                  if (current.isSuper()) {
                     return this.error("Cannot use non-frozen collections with super column families");
                  }
                  DroppedColumn dropped = (DroppedColumn)current.droppedColumns.get((Object)columnName.bytes);
                  if (dropped != null && dropped.column.type instanceof CollectionType && dropped.column.type.isMultiCell() && !type.isCompatibleWith(dropped.column.type)) {
                     String message = String.format("Cannot add a collection with the name %s because a collection with the same name and a different type (%s) has already been used in the past", columnName, dropped.column.type.asCQL3Type());
                     return this.error(message);
                  }
               }
               builder.addColumn(isStatic ? ColumnMetadata.staticColumn(current, columnName.bytes, type) : ColumnMetadata.regularColumn(current, columnName.bytes, type));
               if (isStatic) continue;
               for (ViewMetadata view : views) {
                  if (!view.includeAllColumns) continue;
                  viewUpdates.add(view.withAddedRegularColumn(ColumnMetadata.regularColumn(view.metadata, columnName.bytes, type)));
               }
            }
            break;
         }
         case DROP: {
            if (!current.isCQLTable()) {
               return this.error("Cannot drop columns from a non-CQL3 table");
            }
            for (AlterTableStatementColumn colData : this.colNameList) {
               columnName = colData.getColumnName().getIdentifier(current);
               def = builder.getColumn(columnName);
               if (def == null) {
                  return this.error(String.format("Column %s was not found in table %s", columnName, this.columnFamily()));
               }
               switch (def.kind) {
                  case PARTITION_KEY:
                  case CLUSTERING: {
                     return this.error(String.format("Cannot drop PRIMARY KEY part %s", columnName));
                  }
                  case REGULAR:
                  case STATIC: {
                     builder.removeRegularOrStaticColumn(def.name);
                     builder.recordColumnDrop(def, this.deleteTimestamp == null ? queryState.getTimestamp() : this.deleteTimestamp.longValue());
                  }
               }
               Indexes allIndexes = current.indexes;
               if (!allIndexes.isEmpty()) {
                  ColumnFamilyStore store = Keyspace.openAndGetStore(current);
                  Set<IndexMetadata> dependentIndexes = store.indexManager.getDependentIndexes(def);
                  if (!dependentIndexes.isEmpty()) {
                     Object[] arrobject = new Object[2];
                     arrobject[0] = def;
                     arrobject[1] = dependentIndexes.stream().map(i -> i.name).collect(Collectors.joining(","));
                     return this.error(String.format("Cannot drop column %s because it has dependent secondary indexes (%s)", arrobject));
                  }
               }
               if (Iterables.isEmpty(views)) continue;
               throw new InvalidRequestException(String.format("Cannot drop column %s on base table %s with materialized views.", columnName.toString(), this.columnFamily()));
            }
            break;
         }
         case OPTS: {
            if (this.attrs == null) {
               return this.error("ALTER TABLE WITH invoked, but no parameters found");
            }
            this.attrs.validate();
            if (!(SchemaConstants.isUserKeyspace(this.keyspace()) || current.keyspace.equals("system_distributed") || this.attrs.size() == 1 && this.attrs.hasOption(TableParams.Option.NODESYNC))) {
               return this.error("Only the " + (Object)((Object)TableParams.Option.NODESYNC) + " option is user-modifiable on system table " + current);
            }
            TableParams newParams = this.attrs.asAlteredTableParams(current.params);
            if (newParams.compaction.klass().equals(DateTieredCompactionStrategy.class) && !current.params.compaction.klass().equals(DateTieredCompactionStrategy.class)) {
               DateTieredCompactionStrategy.deprecatedWarning(this.keyspace(), this.columnFamily());
            }
            if (!TimeWindowCompactionStrategy.shouldLogNodeSyncSplitDuringFlushWarning(current, current.params) && TimeWindowCompactionStrategy.shouldLogNodeSyncSplitDuringFlushWarning(current, newParams)) {
               ClientWarn.instance.warn(TimeWindowCompactionStrategy.getNodeSyncSplitDuringFlushWarning(this.keyspace(), this.columnFamily()));
            }
            if (!Iterables.isEmpty(views) && newParams.gcGraceSeconds == 0) {
               return this.error("Cannot alter gc_grace_seconds of the base table of a materialized view to 0, since this value is used to TTL undelivered updates. Setting gc_grace_seconds too low might cause undelivered updates to expire before being replayed.");
            }
            if (current.isCounter() && newParams.defaultTimeToLive > 0) {
               return this.error("Cannot set default_time_to_live on a table with counters");
            }
            builder.params(newParams);
            break;
         }
         case RENAME: {
            for (Map.Entry<ColumnMetadata.Raw, ColumnMetadata.Raw> entry : this.renames.entrySet()) {
               ColumnIdentifier from = entry.getKey().getIdentifier(current);
               ColumnIdentifier to = entry.getValue().getIdentifier(current);
               def = current.getColumn(from);
               if (def == null) {
                  throw new InvalidRequestException(String.format("Cannot rename unknown column %s in table %s", from, current.name));
               }
               if (current.getColumn(to) != null) {
                  throw new InvalidRequestException(String.format("Cannot rename column %s to %s in table %s; another column of that name already exist", from, to, current.name));
               }
               if (!def.isPrimaryKeyColumn()) {
                  throw new InvalidRequestException(String.format("Cannot rename non PRIMARY KEY part %s", from));
               }
               if (!current.indexes.isEmpty()) {
                  ColumnFamilyStore store = Keyspace.openAndGetStore(current);
                  Set<IndexMetadata> dependentIndexes = store.indexManager.getDependentIndexes(def);
                  if (!dependentIndexes.isEmpty()) {
                     Object[] arrobject = new Object[2];
                     arrobject[0] = from;
                     arrobject[1] = dependentIndexes.stream().map(i -> i.name).collect(Collectors.joining(","));
                     throw new InvalidRequestException(String.format("Cannot rename column %s because it has dependent secondary indexes (%s)", arrobject));
                  }
               }
               builder.renamePrimaryKeyColumn(from, to);
               for (ViewMetadata view : views) {
                  if (!view.includes(from)) continue;
                  ColumnIdentifier viewFrom = entry.getKey().getIdentifier(view.metadata);
                  ColumnIdentifier viewTo = entry.getValue().getIdentifier(view.metadata);
                  viewUpdates.add(view.renamePrimaryKeyColumn(viewFrom, viewTo));
               }
            }
            break;
         }
      }
      return MigrationManager.announceTableUpdate(builder.build(), viewUpdates, isLocalOnly).andThen((MaybeSource)Maybe.just((Object)new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.TABLE, this.keyspace(), this.columnFamily())));
   }

   public String toString() {
      return String.format("AlterTableStatement(name=%s, type=%s)", new Object[]{this.cfName, this.oType});
   }

   public static enum Type {
      ADD,
      ALTER,
      DROP,
      OPTS,
      RENAME;

      private Type() {
      }
   }
}
