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
      if(current.isView()) {
         return this.error("Cannot use ALTER TABLE on Materialized View");
      } else {
         TableMetadata.Builder builder = current.unbuild();
         ColumnIdentifier columnName = null;
         ColumnMetadata def = null;
         CQL3Type.Raw dataType = null;
         boolean isStatic = false;
         CQL3Type validator = null;
         List<ViewMetadata> viewUpdates = new ArrayList();
         Iterable<ViewMetadata> views = View.findAll(this.keyspace(), this.columnFamily());
         if(!SchemaConstants.isUserKeyspace(this.keyspace()) && this.oType != AlterTableStatement.Type.OPTS) {
            return this.error("Cannot alter schema (adding, renaming or removing columns) of system keyspace " + this.keyspace());
         } else {
            Iterator var21;
            AlterTableStatementColumn colData;
            switch(null.$SwitchMap$org$apache$cassandra$cql3$statements$AlterTableStatement$Type[this.oType.ordinal()]) {
            case 1:
               return this.error("Altering of types is not allowed");
            case 2:
               if(current.isDense()) {
                  return this.error("Cannot add new column to a COMPACT STORAGE table");
               }

               var21 = this.colNameList.iterator();

               while(var21.hasNext()) {
                  colData = (AlterTableStatementColumn)var21.next();
                  columnName = colData.getColumnName().getIdentifier(current);
                  def = builder.getColumn(columnName);
                  dataType = colData.getColumnType();

                  assert dataType != null;

                  isStatic = colData.getStaticType().booleanValue();
                  validator = dataType.prepare(this.keyspace());
                  if(isStatic) {
                     if(!current.isCompound()) {
                        return this.error("Static columns are not allowed in COMPACT STORAGE tables");
                     }

                     if(current.clusteringColumns().isEmpty()) {
                        return this.error("Static columns are only useful (and thus allowed) if the table has at least one clustering column");
                     }
                  }

                  if(def != null) {
                     switch(null.$SwitchMap$org$apache$cassandra$schema$ColumnMetadata$Kind[def.kind.ordinal()]) {
                     case 1:
                     case 2:
                        return this.error(String.format("Invalid column name %s because it conflicts with a PRIMARY KEY part", new Object[]{columnName}));
                     default:
                        return this.error(String.format("Invalid column name %s because it conflicts with an existing column", new Object[]{columnName}));
                     }
                  }

                  if(current.isCounter() && current.getDroppedColumn(columnName.bytes) != null) {
                     return this.error(String.format("Cannot re-add previously dropped counter column %s", new Object[]{columnName}));
                  }

                  AbstractType<?> type = validator.getType();
                  if(type.isCollection() && type.isMultiCell()) {
                     if(!current.isCompound()) {
                        return this.error("Cannot use non-frozen collections in COMPACT STORAGE tables");
                     }

                     if(current.isSuper()) {
                        return this.error("Cannot use non-frozen collections with super column families");
                     }

                     DroppedColumn dropped = (DroppedColumn)current.droppedColumns.get(columnName.bytes);
                     if(dropped != null && dropped.column.type instanceof CollectionType && dropped.column.type.isMultiCell() && !type.isCompatibleWith(dropped.column.type)) {
                        String message = String.format("Cannot add a collection with the name %s because a collection with the same name and a different type (%s) has already been used in the past", new Object[]{columnName, dropped.column.type.asCQL3Type()});
                        return this.error(message);
                     }
                  }

                  builder.addColumn(isStatic?ColumnMetadata.staticColumn(current, columnName.bytes, type):ColumnMetadata.regularColumn(current, columnName.bytes, type));
                  if(!isStatic) {
                     Iterator var27 = views.iterator();

                     while(var27.hasNext()) {
                        ViewMetadata view = (ViewMetadata)var27.next();
                        if(view.includeAllColumns) {
                           viewUpdates.add(view.withAddedRegularColumn(ColumnMetadata.regularColumn(view.metadata, columnName.bytes, type)));
                        }
                     }
                  }
               }

               return MigrationManager.announceTableUpdate(builder.build(), viewUpdates, isLocalOnly).andThen(Maybe.just(new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.TABLE, this.keyspace(), this.columnFamily())));
            case 3:
               if(!current.isCQLTable()) {
                  return this.error("Cannot drop columns from a non-CQL3 table");
               }

               var21 = this.colNameList.iterator();

               while(var21.hasNext()) {
                  colData = (AlterTableStatementColumn)var21.next();
                  columnName = colData.getColumnName().getIdentifier(current);
                  def = builder.getColumn(columnName);
                  if(def == null) {
                     return this.error(String.format("Column %s was not found in table %s", new Object[]{columnName, this.columnFamily()}));
                  }

                  switch(null.$SwitchMap$org$apache$cassandra$schema$ColumnMetadata$Kind[def.kind.ordinal()]) {
                  case 1:
                  case 2:
                     return this.error(String.format("Cannot drop PRIMARY KEY part %s", new Object[]{columnName}));
                  case 3:
                  case 4:
                     builder.removeRegularOrStaticColumn(def.name);
                     builder.recordColumnDrop(def, this.deleteTimestamp == null?queryState.getTimestamp():this.deleteTimestamp.longValue());
                  default:
                     Indexes allIndexes = current.indexes;
                     if(!allIndexes.isEmpty()) {
                        ColumnFamilyStore store = Keyspace.openAndGetStore(current);
                        Set<IndexMetadata> dependentIndexes = store.indexManager.getDependentIndexes(def);
                        if(!dependentIndexes.isEmpty()) {
                           return this.error(String.format("Cannot drop column %s because it has dependent secondary indexes (%s)", new Object[]{def, dependentIndexes.stream().map((i) -> {
                              return i.name;
                           }).collect(Collectors.joining(","))}));
                        }
                     }

                     if(!Iterables.isEmpty(views)) {
                        throw new InvalidRequestException(String.format("Cannot drop column %s on base table %s with materialized views.", new Object[]{columnName.toString(), this.columnFamily()}));
                     }
                  }
               }

               return MigrationManager.announceTableUpdate(builder.build(), viewUpdates, isLocalOnly).andThen(Maybe.just(new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.TABLE, this.keyspace(), this.columnFamily())));
            case 4:
               if(this.attrs == null) {
                  return this.error("ALTER TABLE WITH invoked, but no parameters found");
               }

               this.attrs.validate();
               if(!SchemaConstants.isUserKeyspace(this.keyspace()) && !current.keyspace.equals("system_distributed") && (this.attrs.size() != 1 || !this.attrs.hasOption(TableParams.Option.NODESYNC))) {
                  return this.error("Only the " + TableParams.Option.NODESYNC + " option is user-modifiable on system table " + current);
               }

               TableParams newParams = this.attrs.asAlteredTableParams(current.params);
               if(newParams.compaction.klass().equals(DateTieredCompactionStrategy.class) && !current.params.compaction.klass().equals(DateTieredCompactionStrategy.class)) {
                  DateTieredCompactionStrategy.deprecatedWarning(this.keyspace(), this.columnFamily());
               }

               if(!TimeWindowCompactionStrategy.shouldLogNodeSyncSplitDuringFlushWarning(current, current.params) && TimeWindowCompactionStrategy.shouldLogNodeSyncSplitDuringFlushWarning(current, newParams)) {
                  ClientWarn.instance.warn(TimeWindowCompactionStrategy.getNodeSyncSplitDuringFlushWarning(this.keyspace(), this.columnFamily()));
               }

               if(!Iterables.isEmpty(views) && newParams.gcGraceSeconds == 0) {
                  return this.error("Cannot alter gc_grace_seconds of the base table of a materialized view to 0, since this value is used to TTL undelivered updates. Setting gc_grace_seconds too low might cause undelivered updates to expire before being replayed.");
               }

               if(current.isCounter() && newParams.defaultTimeToLive > 0) {
                  return this.error("Cannot set default_time_to_live on a table with counters");
               }

               builder.params(newParams);
               break;
            case 5:
               Iterator var13 = this.renames.entrySet().iterator();

               while(var13.hasNext()) {
                  Entry<ColumnMetadata.Raw, ColumnMetadata.Raw> entry = (Entry)var13.next();
                  ColumnIdentifier from = ((ColumnMetadata.Raw)entry.getKey()).getIdentifier(current);
                  ColumnIdentifier to = ((ColumnMetadata.Raw)entry.getValue()).getIdentifier(current);
                  def = current.getColumn(from);
                  if(def == null) {
                     throw new InvalidRequestException(String.format("Cannot rename unknown column %s in table %s", new Object[]{from, current.name}));
                  }

                  if(current.getColumn(to) != null) {
                     throw new InvalidRequestException(String.format("Cannot rename column %s to %s in table %s; another column of that name already exist", new Object[]{from, to, current.name}));
                  }

                  if(!def.isPrimaryKeyColumn()) {
                     throw new InvalidRequestException(String.format("Cannot rename non PRIMARY KEY part %s", new Object[]{from}));
                  }

                  if(!current.indexes.isEmpty()) {
                     ColumnFamilyStore store = Keyspace.openAndGetStore(current);
                     Set<IndexMetadata> dependentIndexes = store.indexManager.getDependentIndexes(def);
                     if(!dependentIndexes.isEmpty()) {
                        throw new InvalidRequestException(String.format("Cannot rename column %s because it has dependent secondary indexes (%s)", new Object[]{from, dependentIndexes.stream().map((i) -> {
                           return i.name;
                        }).collect(Collectors.joining(","))}));
                     }
                  }

                  builder.renamePrimaryKeyColumn(from, to);
                  Iterator var31 = views.iterator();

                  while(var31.hasNext()) {
                     ViewMetadata view = (ViewMetadata)var31.next();
                     if(view.includes(from)) {
                        ColumnIdentifier viewFrom = ((ColumnMetadata.Raw)entry.getKey()).getIdentifier(view.metadata);
                        ColumnIdentifier viewTo = ((ColumnMetadata.Raw)entry.getValue()).getIdentifier(view.metadata);
                        viewUpdates.add(view.renamePrimaryKeyColumn(viewFrom, viewTo));
                     }
                  }
               }
            }

            return MigrationManager.announceTableUpdate(builder.build(), viewUpdates, isLocalOnly).andThen(Maybe.just(new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.TABLE, this.keyspace(), this.columnFamily())));
         }
      }
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
