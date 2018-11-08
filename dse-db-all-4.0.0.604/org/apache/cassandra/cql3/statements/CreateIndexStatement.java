package org.apache.cassandra.cql3.statements;

import com.datastax.bdp.db.audit.AuditableEventType;
import com.datastax.bdp.db.audit.CoreAuditableEventType;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import io.reactivex.Maybe;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.IndexName;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.index.sasi.SASIIndex;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Indexes;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateIndexStatement extends SchemaAlteringStatement implements TableStatement {
   private static final Logger logger = LoggerFactory.getLogger(CreateIndexStatement.class);
   private final String indexName;
   private final List<IndexTarget.Raw> rawTargets;
   private final IndexPropDefs properties;
   private final boolean ifNotExists;

   public CreateIndexStatement(CFName name, IndexName indexName, List<IndexTarget.Raw> targets, IndexPropDefs properties, boolean ifNotExists) {
      super(name);
      this.indexName = indexName.getIdx();
      this.rawTargets = targets;
      this.properties = properties;
      this.ifNotExists = ifNotExists;
   }

   public AuditableEventType getAuditEventType() {
      return CoreAuditableEventType.CREATE_INDEX;
   }

   public void checkAccess(QueryState state) {
      state.checkTablePermission(this.keyspace(), this.columnFamily(), CorePermission.ALTER);
   }

   public void validate(QueryState state) throws RequestValidationException {
      TableMetadata table = Schema.instance.validateTable(this.keyspace(), this.columnFamily());
      if(table.isCounter()) {
         throw new InvalidRequestException("Secondary indexes are not supported on counter tables");
      } else if(table.isView()) {
         throw new InvalidRequestException("Secondary indexes are not supported on materialized views");
      } else if(table.isCompactTable() && !table.isStaticCompactTable()) {
         throw new InvalidRequestException("Secondary indexes are not supported on COMPACT STORAGE tables that have clustering columns");
      } else {
         List<IndexTarget> targets = new ArrayList(this.rawTargets.size());
         Iterator var4 = this.rawTargets.iterator();

         while(var4.hasNext()) {
            IndexTarget.Raw rawTarget = (IndexTarget.Raw)var4.next();
            targets.add(rawTarget.prepare(table));
         }

         if(targets.isEmpty() && !this.properties.isCustom) {
            throw new InvalidRequestException("Only CUSTOM indexes can be created without specifying a target column");
         } else {
            if(targets.size() > 1) {
               this.validateTargetsForMultiColumnIndex(targets);
            }

            ColumnMetadata cd;
            for(var4 = targets.iterator(); var4.hasNext(); RequestValidations.checkFalse(cd.type.isUDT() && cd.type.isMultiCell(), "Secondary indexes are not supported on non-frozen UDTs")) {
               IndexTarget target = (IndexTarget)var4.next();
               cd = table.getColumn(target.column);
               if(cd == null) {
                  throw new InvalidRequestException("No column definition found for column " + target.column);
               }

               if(cd.type.referencesDuration()) {
                  RequestValidations.checkFalse(cd.type.isCollection(), "Secondary indexes are not supported on collections containing durations");
                  RequestValidations.checkFalse(cd.type.isTuple(), "Secondary indexes are not supported on tuples containing durations");
                  RequestValidations.checkFalse(cd.type.isUDT(), "Secondary indexes are not supported on UDTs containing durations");
                  throw RequestValidations.invalidRequest("Secondary indexes are not supported on duration columns");
               }

               if(table.isCompactTable() && cd.isPrimaryKeyColumn()) {
                  throw new InvalidRequestException("Secondary indexes are not supported on PRIMARY KEY columns in COMPACT STORAGE tables");
               }

               if(cd.kind == ColumnMetadata.Kind.PARTITION_KEY && table.partitionKeyColumns().size() == 1) {
                  throw new InvalidRequestException(String.format("Cannot create secondary index on partition key column %s", new Object[]{target.column}));
               }

               boolean isMap = cd.type instanceof MapType;
               boolean isFrozenCollection = cd.type.isCollection() && !cd.type.isMultiCell();
               if(isFrozenCollection) {
                  this.validateForFrozenCollection(target);
               } else {
                  this.validateNotFullIndex(target);
                  this.validateIsSimpleIndexIfTargetColumnNotCollection(cd, target);
                  this.validateTargetColumnIsMapIfIndexInvolvesKeys(isMap, target);
               }
            }

            if(!Strings.isNullOrEmpty(this.indexName) && Schema.instance.getKeyspaceMetadata(this.keyspace()).existingIndexNames((String)null).contains(this.indexName)) {
               if(!this.ifNotExists) {
                  throw new InvalidRequestException(String.format("Index %s already exists", new Object[]{this.indexName}));
               }
            } else {
               this.properties.validate();
            }
         }
      }
   }

   private void validateForFrozenCollection(IndexTarget target) throws InvalidRequestException {
      if(target.type != IndexTarget.Type.FULL) {
         throw new InvalidRequestException(String.format("Cannot create %s() index on frozen column %s. Frozen collections only support full() indexes", new Object[]{target.type, target.column}));
      }
   }

   private void validateNotFullIndex(IndexTarget target) throws InvalidRequestException {
      if(target.type == IndexTarget.Type.FULL) {
         throw new InvalidRequestException("full() indexes can only be created on frozen collections");
      }
   }

   private void validateIsSimpleIndexIfTargetColumnNotCollection(ColumnMetadata cd, IndexTarget target) throws InvalidRequestException {
      if(!cd.type.isCollection() && target.type != IndexTarget.Type.SIMPLE) {
         throw new InvalidRequestException(String.format("Cannot create %s() index on %s. Non-collection columns support only simple indexes", new Object[]{target.type.toString(), target.column}));
      }
   }

   private void validateTargetColumnIsMapIfIndexInvolvesKeys(boolean isMap, IndexTarget target) throws InvalidRequestException {
      if((target.type == IndexTarget.Type.KEYS || target.type == IndexTarget.Type.KEYS_AND_VALUES) && !isMap) {
         throw new InvalidRequestException(String.format("Cannot create index on %s of column %s with non-map type", new Object[]{target.type, target.column}));
      }
   }

   private void validateTargetsForMultiColumnIndex(List<IndexTarget> targets) {
      if(!this.properties.isCustom) {
         throw new InvalidRequestException("Only CUSTOM indexes support multiple columns");
      } else {
         Set<ColumnIdentifier> columns = Sets.newHashSetWithExpectedSize(targets.size());
         Iterator var3 = targets.iterator();

         IndexTarget target;
         do {
            if(!var3.hasNext()) {
               return;
            }

            target = (IndexTarget)var3.next();
         } while(columns.add(target.column));

         throw new InvalidRequestException("Duplicate column " + target.column + " in index target list");
      }
   }

   public Maybe<Event.SchemaChange> announceMigration(QueryState queryState, boolean isLocalOnly) throws RequestValidationException {
      TableMetadata current = Schema.instance.getTableMetadata(this.keyspace(), this.columnFamily());
      List<IndexTarget> targets = new ArrayList(this.rawTargets.size());
      Iterator var5 = this.rawTargets.iterator();

      while(var5.hasNext()) {
         IndexTarget.Raw rawTarget = (IndexTarget.Raw)var5.next();
         targets.add(rawTarget.prepare(current));
      }

      String acceptedName = this.indexName;
      if(Strings.isNullOrEmpty(acceptedName)) {
         acceptedName = Indexes.getAvailableIndexName(this.keyspace(), this.columnFamily(), targets.size() == 1?((IndexTarget)targets.get(0)).column.toString():null);
      }

      if(Schema.instance.getKeyspaceMetadata(this.keyspace()).existingIndexNames((String)null).contains(acceptedName)) {
         return this.ifNotExists?Maybe.empty():this.error(String.format("Index %s already exists", new Object[]{acceptedName}));
      } else {
         Map indexOptions;
         IndexMetadata.Kind kind;
         if(this.properties.isCustom) {
            kind = IndexMetadata.Kind.CUSTOM;
            indexOptions = this.properties.getOptions();
            if(this.properties.customClass.equals(SASIIndex.class.getName())) {
               String warning = String.format("SASI index was enabled for '%s.%s'. SASI is still in beta, take extra caution when using it in production.", new Object[]{this.keyspace(), this.columnFamily()});
               logger.warn(warning);
               ClientWarn.instance.warn(warning);
            }
         } else {
            indexOptions = Collections.emptyMap();
            kind = current.isCompound()?IndexMetadata.Kind.COMPOSITES:IndexMetadata.Kind.KEYS;
         }

         IndexMetadata index = IndexMetadata.fromIndexTargets(targets, acceptedName, kind, indexOptions);
         Optional<IndexMetadata> existingIndex = Iterables.tryFind(current.indexes, (existing) -> {
            return existing.equalsWithoutName(index);
         });
         if(existingIndex.isPresent()) {
            return this.ifNotExists?Maybe.empty():this.error(String.format("Index %s is a duplicate of existing index %s", new Object[]{index.name, ((IndexMetadata)existingIndex.get()).name}));
         } else {
            TableMetadata updated = current.unbuild().indexes(current.indexes.with(index)).build();
            logger.trace("Updating index definition for {}", this.indexName);
            return MigrationManager.announceTableUpdate(updated, isLocalOnly).andThen(Maybe.just(new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.TABLE, this.keyspace(), this.columnFamily())));
         }
      }
   }
}
