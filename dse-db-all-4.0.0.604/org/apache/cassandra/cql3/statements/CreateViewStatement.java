package org.apache.cassandra.cql3.statements;

import com.datastax.bdp.db.audit.AuditableEventType;
import com.datastax.bdp.db.audit.CoreAuditableEventType;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.collect.UnmodifiableIterator;
import io.reactivex.Maybe;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.WhereClause;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.cql3.selection.RawSelector;
import org.apache.cassandra.cql3.selection.Selectable;
import org.apache.cassandra.db.compaction.DateTieredCompactionStrategy;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.DurationType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.view.View;
import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.schema.ViewMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Event;

public class CreateViewStatement extends SchemaAlteringStatement implements TableStatement {
   private final CFName baseName;
   private final List<RawSelector> selectClause;
   private final WhereClause whereClause;
   private final List<ColumnMetadata.Raw> partitionKeys;
   private final List<ColumnMetadata.Raw> clusteringKeys;
   public final CFProperties properties = new CFProperties();
   private final boolean ifNotExists;

   public CreateViewStatement(CFName viewName, CFName baseName, List<RawSelector> selectClause, WhereClause whereClause, List<ColumnMetadata.Raw> partitionKeys, List<ColumnMetadata.Raw> clusteringKeys, boolean ifNotExists) {
      super(viewName);
      this.baseName = baseName;
      this.selectClause = selectClause;
      this.whereClause = whereClause;
      this.partitionKeys = partitionKeys;
      this.clusteringKeys = clusteringKeys;
      this.ifNotExists = ifNotExists;
   }

   public AuditableEventType getAuditEventType() {
      return CoreAuditableEventType.CREATE_VIEW;
   }

   public void checkAccess(QueryState state) {
      if(!this.baseName.hasKeyspace()) {
         this.baseName.setKeyspace(this.keyspace(), true);
      }

      state.checkTablePermission(this.keyspace(), this.baseName.getColumnFamily(), CorePermission.ALTER);
   }

   public void validate(QueryState state) throws RequestValidationException {
   }

   private void add(TableMetadata baseCfm, Iterable<ColumnIdentifier> columns, CreateViewStatement.AddColumn adder) {
      ColumnIdentifier column;
      Object type;
      for(Iterator var4 = columns.iterator(); var4.hasNext(); adder.add(column, (AbstractType)type)) {
         column = (ColumnIdentifier)var4.next();
         type = baseCfm.getColumn(column).type;
         if(this.properties.definedOrdering.containsKey(column)) {
            boolean desc = ((Boolean)this.properties.definedOrdering.get(column)).booleanValue();
            if(!desc && ((AbstractType)type).isReversed()) {
               type = ((ReversedType)type).baseType;
            } else if(desc && !((AbstractType)type).isReversed()) {
               type = ReversedType.getInstance((AbstractType)type);
            }
         }
      }

   }

   public Maybe<Event.SchemaChange> announceMigration(QueryState queryState, boolean isLocalOnly) throws RequestValidationException {
      this.properties.validate();
      if(this.properties.useCompactStorage) {
         return this.error("Cannot use 'COMPACT STORAGE' when defining a materialized view");
      } else if(!this.baseName.getKeyspace().equals(this.keyspace())) {
         return this.error("Cannot create a materialized view on a table in a separate keyspace");
      } else {
         TableMetadata metadata = Schema.instance.validateTable(this.baseName.getKeyspace(), this.baseName.getColumnFamily());
         if(metadata.isCounter()) {
            return this.error("Materialized views are not supported on counter tables");
         } else if(metadata.isView()) {
            return this.error("Materialized views cannot be created against other materialized views");
         } else if(metadata.params.gcGraceSeconds == 0) {
            return this.error(String.format("Cannot create materialized view '%s' for base table '%s' with gc_grace_seconds of 0, since this value is used to TTL undelivered updates. Setting gc_grace_seconds too low might cause undelivered updates to expire before being replayed.", new Object[]{this.cfName.getColumnFamily(), this.baseName.getColumnFamily()}));
         } else {
            Set<ColumnIdentifier> included = Sets.newHashSetWithExpectedSize(this.selectClause.size());
            Iterator var5 = this.selectClause.iterator();

            while(var5.hasNext()) {
               RawSelector selector = (RawSelector)var5.next();
               Selectable.Raw selectable = selector.selectable;
               if(selectable instanceof Selectable.WithFieldSelection.Raw) {
                  return this.error("Cannot select out a part of type when defining a materialized view");
               }

               if(selectable instanceof Selectable.WithFunction.Raw) {
                  return this.error("Cannot use function when defining a materialized view");
               }

               if(selectable instanceof Selectable.WritetimeOrTTL.Raw) {
                  return this.error("Cannot use function when defining a materialized view");
               }

               if(selectable instanceof Selectable.WithElementSelection.Raw) {
                  return this.error("Cannot use collection element selection when defining a materialized view");
               }

               if(selectable instanceof Selectable.WithSliceSelection.Raw) {
                  return this.error("Cannot use collection slice selection when defining a materialized view");
               }

               if(selector.alias != null) {
                  return this.error("Cannot use alias when defining a materialized view");
               }

               Selectable s = selectable.prepare(metadata);
               if(s instanceof Term.Raw) {
                  return this.error("Cannot use terms in selection when defining a materialized view");
               }

               ColumnMetadata cdef = (ColumnMetadata)s;
               included.add(cdef.name);
            }

            Set<ColumnMetadata.Raw> targetPrimaryKeys = new HashSet();
            Iterator var29 = Iterables.concat(this.partitionKeys, this.clusteringKeys).iterator();

            while(var29.hasNext()) {
               ColumnMetadata.Raw identifier = (ColumnMetadata.Raw)var29.next();
               if(!targetPrimaryKeys.add(identifier)) {
                  return this.error("Duplicate entry found in PRIMARY KEY: " + identifier);
               }

               ColumnMetadata cdef = identifier.prepare(metadata);
               if(cdef.type.isMultiCell()) {
                  return this.error(String.format("Cannot use MultiCell column '%s' in PRIMARY KEY of materialized view", new Object[]{identifier}));
               }

               if(cdef.isStatic()) {
                  return this.error(String.format("Cannot use Static column '%s' in PRIMARY KEY of materialized view", new Object[]{identifier}));
               }

               if(cdef.type instanceof DurationType) {
                  return this.error(String.format("Cannot use Duration column '%s' in PRIMARY KEY of materialized view", new Object[]{identifier}));
               }
            }

            Map<ColumnMetadata.Raw, Boolean> orderings = Collections.emptyMap();
            List<Selectable.Raw> groups = Collections.emptyList();
            SelectStatement.Parameters parameters = new SelectStatement.Parameters(orderings, groups, false, true, false);
            SelectStatement.RawStatement rawSelect = new SelectStatement.RawStatement(this.baseName, parameters, this.selectClause, this.whereClause, (Term.Raw)null, (Term.Raw)null);
            ClientState state = ClientState.forInternalCalls();
            state.setKeyspace(this.keyspace());
            rawSelect.prepareKeyspace(state);
            rawSelect.setBoundVariables(this.getBoundVariables());
            ParsedStatement.Prepared prepared = rawSelect.prepare(true);
            SelectStatement select = (SelectStatement)prepared.statement;
            StatementRestrictions restrictions = select.getRestrictions();
            if(!prepared.boundNames.isEmpty()) {
               return this.error("Cannot use query parameters in CREATE MATERIALIZED VIEW statements");
            } else {
               boolean allowFilteringNonKeyColumns = Boolean.parseBoolean(System.getProperty("cassandra.mv.allow_filtering_nonkey_columns_unsafe", "false"));
               if(!restrictions.nonPKRestrictedColumns(false).isEmpty() && !allowFilteringNonKeyColumns) {
                  throw new InvalidRequestException(String.format("Non-primary key columns cannot be restricted in the SELECT statement used for materialized view creation (got restrictions on: %s)", new Object[]{restrictions.nonPKRestrictedColumns(false).stream().map((def) -> {
                     return def.name.toString();
                  }).collect(Collectors.joining(", "))}));
               } else {
                  String whereClauseText = View.relationsToWhereClause(this.whereClause.relations);
                  Set<ColumnIdentifier> basePrimaryKeyCols = new HashSet();
                  Iterator var17 = Iterables.concat(metadata.partitionKeyColumns(), metadata.clusteringColumns()).iterator();

                  while(var17.hasNext()) {
                     ColumnMetadata definition = (ColumnMetadata)var17.next();
                     basePrimaryKeyCols.add(definition.name);
                  }

                  List<ColumnIdentifier> targetClusteringColumns = new ArrayList();
                  List<ColumnIdentifier> targetPartitionKeys = new ArrayList();
                  boolean hasNonPKColumn = false;

                  Iterator var20;
                  ColumnMetadata.Raw raw;
                  for(var20 = this.partitionKeys.iterator(); var20.hasNext(); hasNonPKColumn |= getColumnIdentifier(metadata, basePrimaryKeyCols, hasNonPKColumn, raw, targetPartitionKeys, restrictions)) {
                     raw = (ColumnMetadata.Raw)var20.next();
                  }

                  for(var20 = this.clusteringKeys.iterator(); var20.hasNext(); hasNonPKColumn |= getColumnIdentifier(metadata, basePrimaryKeyCols, hasNonPKColumn, raw, targetClusteringColumns, restrictions)) {
                     raw = (ColumnMetadata.Raw)var20.next();
                  }

                  boolean missingClusteringColumns = false;
                  StringBuilder columnNames = new StringBuilder();
                  List<ColumnIdentifier> includedColumns = new ArrayList();
                  UnmodifiableIterator var23 = metadata.columns().iterator();

                  while(var23.hasNext()) {
                     ColumnMetadata def = (ColumnMetadata)var23.next();
                     ColumnIdentifier identifier = def.name;
                     boolean includeDef = included.isEmpty() || included.contains(identifier);
                     if(includeDef && def.isStatic()) {
                        return this.error(String.format("Unable to include static column '%s' which would be included by Materialized View SELECT * statement", new Object[]{identifier}));
                     }

                     boolean defInTargetPrimaryKey = targetClusteringColumns.contains(identifier) || targetPartitionKeys.contains(identifier);
                     if(includeDef && !defInTargetPrimaryKey) {
                        includedColumns.add(identifier);
                     }

                     if(def.isPrimaryKeyColumn() && !defInTargetPrimaryKey) {
                        if(missingClusteringColumns) {
                           columnNames.append(',');
                        } else {
                           missingClusteringColumns = true;
                        }

                        columnNames.append(identifier);
                     }
                  }

                  if(missingClusteringColumns) {
                     return this.error(String.format("Cannot create Materialized View %s without primary key columns from base %s (%s)", new Object[]{this.columnFamily(), this.baseName.getColumnFamily(), columnNames.toString()}));
                  } else if(targetPartitionKeys.isEmpty()) {
                     return this.error("Must select at least a column for a Materialized View");
                  } else if(targetClusteringColumns.isEmpty()) {
                     return this.error("No columns are defined for Materialized View other than primary key");
                  } else {
                     TableParams params = this.properties.properties.asNewTableParams();
                     if(params.compaction.klass().equals(DateTieredCompactionStrategy.class)) {
                        DateTieredCompactionStrategy.deprecatedWarning(this.keyspace(), this.columnFamily());
                     }

                     if(params.defaultTimeToLive > 0) {
                        throw new InvalidRequestException("Cannot set default_time_to_live for a materialized view. Data in a materialized view always expire at the same time than the corresponding data in the parent table.");
                     } else {
                        TableMetadata.Builder builder = TableMetadata.builder(this.keyspace(), this.columnFamily(), this.properties.properties.getId()).isView(true).params(params);
                        this.add(metadata, targetPartitionKeys, builder::addPartitionKeyColumn);
                        this.add(metadata, targetClusteringColumns, builder::addClusteringColumn);
                        this.add(metadata, includedColumns, builder::addRegularColumn);
                        ViewMetadata definition = new ViewMetadata(this.keyspace(), this.columnFamily(), metadata.id, metadata.name, included.isEmpty(), rawSelect, whereClauseText, builder.build());
                        return MigrationManager.announceNewView(definition, isLocalOnly).andThen(Maybe.just(new Event.SchemaChange(Event.SchemaChange.Change.CREATED, Event.SchemaChange.Target.TABLE, this.keyspace(), this.columnFamily()))).onErrorResumeNext((e) -> {
                           return e instanceof AlreadyExistsException && this.ifNotExists?Maybe.empty():Maybe.error(e);
                        });
                     }
                  }
               }
            }
         }
      }
   }

   private static boolean getColumnIdentifier(TableMetadata cfm, Set<ColumnIdentifier> basePK, boolean hasNonPKColumn, ColumnMetadata.Raw raw, List<ColumnIdentifier> columns, StatementRestrictions restrictions) {
      ColumnMetadata def = raw.prepare(cfm);
      boolean isPk = basePK.contains(def.name);
      if(!isPk && hasNonPKColumn) {
         throw new InvalidRequestException(String.format("Cannot include more than one non-primary key column '%s' in materialized view primary key", new Object[]{def.name}));
      } else {
         boolean isSinglePartitionKey = def.isPartitionKey() && cfm.partitionKeyColumns().size() == 1;
         if(!isSinglePartitionKey && !restrictions.isRestricted(def)) {
            throw new InvalidRequestException(String.format("Primary key column '%s' is required to be filtered by 'IS NOT NULL'", new Object[]{def.name}));
         } else {
            columns.add(def.name);
            return !isPk;
         }
      }
   }

   private interface AddColumn {
      void add(ColumnIdentifier var1, AbstractType<?> var2);
   }
}
