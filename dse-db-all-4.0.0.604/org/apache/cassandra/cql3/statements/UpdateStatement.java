package org.apache.cassandra.cql3.statements;

import com.datastax.bdp.db.audit.AuditableEventType;
import com.datastax.bdp.db.audit.CoreAuditableEventType;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.cassandra.cql3.Attributes;
import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Json;
import org.apache.cassandra.cql3.Operation;
import org.apache.cassandra.cql3.Operations;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.Relation;
import org.apache.cassandra.cql3.SingleColumnRelation;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.UpdateParameters;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.WhereClause;
import org.apache.cassandra.cql3.conditions.ColumnCondition;
import org.apache.cassandra.cql3.conditions.Conditions;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.CompactTables;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

public class UpdateStatement extends ModificationStatement {
   private static final Constants.Value EMPTY;

   private UpdateStatement(StatementType type, int boundTerms, TableMetadata metadata, Operations operations, StatementRestrictions restrictions, Conditions conditions, Attributes attrs) {
      super(type, boundTerms, metadata, operations, restrictions, conditions, attrs);
   }

   public AuditableEventType getAuditEventType() {
      return CoreAuditableEventType.CQL_UPDATE;
   }

   public boolean requireFullClusteringKey() {
      return true;
   }

   public void addUpdateForKey(PartitionUpdate update, Clustering clustering, UpdateParameters params) {
      if(this.updatesRegularRows()) {
         params.newRow(clustering);
         if(this.type.isInsert() && this.metadata().isCQLTable()) {
            params.addPrimaryKeyLivenessInfo();
         }

         List<Operation> updates = this.getRegularOperations();
         if(this.metadata().isCompactTable() && updates.isEmpty()) {
            RequestValidations.checkTrue(CompactTables.hasEmptyCompactValue(this.metadata), "Column %s is mandatory for this COMPACT STORAGE table", this.metadata().compactValueColumn.name);
            updates = Collections.singletonList(new Constants.Setter(this.metadata().compactValueColumn, EMPTY));
         }

         Iterator var5 = updates.iterator();

         while(var5.hasNext()) {
            Operation op = (Operation)var5.next();
            op.execute(update.partitionKey(), params);
         }

         update.add(params.buildRow());
      }

      if(this.updatesStaticRow()) {
         params.newRow(Clustering.STATIC_CLUSTERING);
         Iterator var7 = this.getStaticOperations().iterator();

         while(var7.hasNext()) {
            Operation op = (Operation)var7.next();
            op.execute(update.partitionKey(), params);
         }

         update.add(params.buildRow());
      }

   }

   public void addUpdateForKey(PartitionUpdate update, Slice slice, UpdateParameters params) {
      throw new UnsupportedOperationException();
   }

   static {
      EMPTY = new Constants.Value(ByteBufferUtil.EMPTY_BYTE_BUFFER);
   }

   public static class ParsedUpdate extends ModificationStatement.Parsed {
      private final List<Pair<ColumnMetadata.Raw, Operation.RawUpdate>> updates;
      private final WhereClause whereClause;

      public ParsedUpdate(CFName name, Attributes.Raw attrs, List<Pair<ColumnMetadata.Raw, Operation.RawUpdate>> updates, WhereClause whereClause, List<Pair<ColumnMetadata.Raw, ColumnCondition.Raw>> conditions, boolean ifExists) {
         super(name, StatementType.UPDATE, attrs, conditions, false, ifExists);
         this.updates = updates;
         this.whereClause = whereClause;
      }

      protected ModificationStatement prepareInternal(TableMetadata metadata, VariableSpecifications boundNames, Conditions conditions, Attributes attrs) {
         Operations operations = new Operations(this.type);
         Iterator var6 = this.updates.iterator();

         while(var6.hasNext()) {
            Pair<ColumnMetadata.Raw, Operation.RawUpdate> entry = (Pair)var6.next();
            ColumnMetadata def = getColumnDefinition(metadata, (ColumnMetadata.Raw)entry.left);
            RequestValidations.checkFalse(def.isPrimaryKeyColumn(), "PRIMARY KEY part %s found in SET part", def.name);
            Operation operation = ((Operation.RawUpdate)entry.right).prepare(metadata, def);
            operation.collectMarkerSpecification(boundNames);
            operations.add(operation);
         }

         StatementRestrictions restrictions = this.newRestrictions(metadata, boundNames, operations, this.whereClause, conditions);
         return new UpdateStatement(this.type, boundNames.size(), metadata, operations, restrictions, conditions, attrs);
      }
   }

   public static class ParsedInsertJson extends ModificationStatement.Parsed {
      private final Json.Raw jsonValue;
      private final boolean defaultUnset;

      public ParsedInsertJson(CFName name, Attributes.Raw attrs, Json.Raw jsonValue, boolean defaultUnset, boolean ifNotExists) {
         super(name, StatementType.INSERT, attrs, (List)null, ifNotExists, false);
         this.jsonValue = jsonValue;
         this.defaultUnset = defaultUnset;
      }

      protected ModificationStatement prepareInternal(TableMetadata metadata, VariableSpecifications boundNames, Conditions conditions, Attributes attrs) {
         RequestValidations.checkFalse(metadata.isCounter(), "INSERT statements are not allowed on counter tables, use UPDATE instead");
         Collection<ColumnMetadata> defs = metadata.columns();
         Json.Prepared prepared = this.jsonValue.prepareAndCollectMarkers(metadata, defs, boundNames);
         WhereClause.Builder whereClause = new WhereClause.Builder();
         Operations operations = new Operations(this.type);
         boolean hasClusteringColumnsSet = false;
         Iterator var10 = defs.iterator();

         while(var10.hasNext()) {
            ColumnMetadata def = (ColumnMetadata)var10.next();
            if(def.isClusteringColumn()) {
               hasClusteringColumnsSet = true;
            }

            Term.Raw raw = prepared.getRawTermForColumn(def, this.defaultUnset);
            if(def.isPrimaryKeyColumn()) {
               whereClause.add((Relation)(new SingleColumnRelation(ColumnMetadata.Raw.forColumn(def), Operator.EQ, raw)));
            } else {
               Operation operation = (new Operation.SetValue(raw)).prepare(metadata, def);
               operation.collectMarkerSpecification(boundNames);
               operations.add(operation);
            }
         }

         boolean applyOnlyToStaticColumns = !hasClusteringColumnsSet && ModificationStatement.appliesOnlyToStaticColumns(operations, conditions);
         StatementRestrictions restrictions = new StatementRestrictions(this.type, metadata, whereClause.build(), boundNames, applyOnlyToStaticColumns, false, false);
         return new UpdateStatement(this.type, boundNames.size(), metadata, operations, restrictions, conditions, attrs);
      }
   }

   public static class ParsedInsert extends ModificationStatement.Parsed {
      private final List<ColumnMetadata.Raw> columnNames;
      private final List<Term.Raw> columnValues;

      public ParsedInsert(CFName name, Attributes.Raw attrs, List<ColumnMetadata.Raw> columnNames, List<Term.Raw> columnValues, boolean ifNotExists) {
         super(name, StatementType.INSERT, attrs, (List)null, ifNotExists, false);
         this.columnNames = columnNames;
         this.columnValues = columnValues;
      }

      protected ModificationStatement prepareInternal(TableMetadata metadata, VariableSpecifications boundNames, Conditions conditions, Attributes attrs) {
         RequestValidations.checkFalse(metadata.isCounter(), "INSERT statements are not allowed on counter tables, use UPDATE instead");
         RequestValidations.checkFalse(this.columnNames == null, "Column names for INSERT must be provided when using VALUES");
         RequestValidations.checkFalse(this.columnNames.isEmpty(), "No columns provided to INSERT");
         RequestValidations.checkFalse(this.columnNames.size() != this.columnValues.size(), "Unmatched column names/values");
         RequestValidations.checkContainsNoDuplicates(this.columnNames, "The column names contains duplicates");
         WhereClause.Builder whereClause = new WhereClause.Builder();
         Operations operations = new Operations(this.type);
         boolean hasClusteringColumnsSet = false;

         for(int i = 0; i < this.columnNames.size(); ++i) {
            ColumnMetadata def = getColumnDefinition(metadata, (ColumnMetadata.Raw)this.columnNames.get(i));
            if(def.isClusteringColumn()) {
               hasClusteringColumnsSet = true;
            }

            Term.Raw value = (Term.Raw)this.columnValues.get(i);
            if(def.isPrimaryKeyColumn()) {
               whereClause.add((Relation)(new SingleColumnRelation((ColumnMetadata.Raw)this.columnNames.get(i), Operator.EQ, value)));
            } else {
               Operation operation = (new Operation.SetValue(value)).prepare(metadata, def);
               operation.collectMarkerSpecification(boundNames);
               operations.add(operation);
            }
         }

         boolean applyOnlyToStaticColumns = !hasClusteringColumnsSet && ModificationStatement.appliesOnlyToStaticColumns(operations, conditions);
         StatementRestrictions restrictions = new StatementRestrictions(this.type, metadata, whereClause.build(), boundNames, applyOnlyToStaticColumns, false, false);
         return new UpdateStatement(this.type, boundNames.size(), metadata, operations, restrictions, conditions, attrs);
      }
   }
}
