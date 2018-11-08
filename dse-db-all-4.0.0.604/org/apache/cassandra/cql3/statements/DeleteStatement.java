package org.apache.cassandra.cql3.statements;

import com.datastax.bdp.db.audit.AuditableEventType;
import com.datastax.bdp.db.audit.CoreAuditableEventType;
import java.util.Iterator;
import java.util.List;
import org.apache.cassandra.cql3.Attributes;
import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.cql3.Operation;
import org.apache.cassandra.cql3.Operations;
import org.apache.cassandra.cql3.UpdateParameters;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.WhereClause;
import org.apache.cassandra.cql3.conditions.ColumnCondition;
import org.apache.cassandra.cql3.conditions.Conditions;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Pair;

public class DeleteStatement extends ModificationStatement {
   private DeleteStatement(int boundTerms, TableMetadata cfm, Operations operations, StatementRestrictions restrictions, Conditions conditions, Attributes attrs) {
      super(StatementType.DELETE, boundTerms, cfm, operations, restrictions, conditions, attrs);
   }

   public AuditableEventType getAuditEventType() {
      return CoreAuditableEventType.CQL_DELETE;
   }

   public void addUpdateForKey(PartitionUpdate update, Clustering clustering, UpdateParameters params) throws InvalidRequestException {
      TableMetadata metadata = this.metadata();
      List<Operation> regularDeletions = this.getRegularOperations();
      List<Operation> staticDeletions = this.getStaticOperations();
      if(regularDeletions.isEmpty() && staticDeletions.isEmpty()) {
         if(clustering.size() == 0) {
            update.addPartitionDeletion(params.deletionTime());
         } else if(clustering.size() == metadata.clusteringColumns().size()) {
            params.newRow(clustering);
            params.addRowDeletion();
            update.add(params.buildRow());
         } else {
            update.add(params.makeRangeTombstone(metadata.comparator, clustering));
         }
      } else {
         Iterator var7;
         Operation op;
         if(!regularDeletions.isEmpty()) {
            RequestValidations.checkFalse(clustering.size() == 0 && metadata.clusteringColumns().size() != 0, "Range deletions are not supported for specific columns");
            params.newRow(clustering);
            var7 = regularDeletions.iterator();

            while(var7.hasNext()) {
               op = (Operation)var7.next();
               op.execute(update.partitionKey(), params);
            }

            update.add(params.buildRow());
         }

         if(!staticDeletions.isEmpty()) {
            params.newRow(Clustering.STATIC_CLUSTERING);
            var7 = staticDeletions.iterator();

            while(var7.hasNext()) {
               op = (Operation)var7.next();
               op.execute(update.partitionKey(), params);
            }

            update.add(params.buildRow());
         }
      }

   }

   public void addUpdateForKey(PartitionUpdate update, Slice slice, UpdateParameters params) {
      List<Operation> regularDeletions = this.getRegularOperations();
      List<Operation> staticDeletions = this.getStaticOperations();
      RequestValidations.checkTrue(regularDeletions.isEmpty() && staticDeletions.isEmpty(), "Range deletions are not supported for specific columns");
      update.add(params.makeRangeTombstone(slice));
   }

   public static class Parsed extends ModificationStatement.Parsed {
      private final List<Operation.RawDeletion> deletions;
      private final WhereClause whereClause;

      public Parsed(CFName name, Attributes.Raw attrs, List<Operation.RawDeletion> deletions, WhereClause whereClause, List<Pair<ColumnMetadata.Raw, ColumnCondition.Raw>> conditions, boolean ifExists) {
         super(name, StatementType.DELETE, attrs, conditions, false, ifExists);
         this.deletions = deletions;
         this.whereClause = whereClause;
      }

      protected ModificationStatement prepareInternal(TableMetadata metadata, VariableSpecifications boundNames, Conditions conditions, Attributes attrs) {
         Operations operations = new Operations(this.type);
         Iterator var6 = this.deletions.iterator();

         while(var6.hasNext()) {
            Operation.RawDeletion deletion = (Operation.RawDeletion)var6.next();
            ColumnMetadata def = getColumnDefinition(metadata, deletion.affectedColumn());
            RequestValidations.checkFalse(def.isPrimaryKeyColumn(), "Invalid identifier %s for deletion (should not be a PRIMARY KEY part)", def.name);
            Operation op = deletion.prepare(metadata.keyspace, def, metadata);
            op.collectMarkerSpecification(boundNames);
            operations.add(op);
         }

         StatementRestrictions restrictions = this.newRestrictions(metadata, boundNames, operations, this.whereClause, conditions);
         DeleteStatement stmt = new DeleteStatement(boundNames.size(), metadata, operations, restrictions, conditions, attrs);
         if(stmt.hasConditions() && !restrictions.hasAllPKColumnsRestrictedByEqualities()) {
            RequestValidations.checkFalse(operations.appliesToRegularColumns(), "DELETE statements must restrict all PRIMARY KEY columns with equality relations in order to delete non static columns");
            RequestValidations.checkFalse(conditions.appliesToRegularColumns(), "DELETE statements must restrict all PRIMARY KEY columns with equality relations in order to use IF condition on non static columns");
         }

         return stmt;
      }
   }
}
