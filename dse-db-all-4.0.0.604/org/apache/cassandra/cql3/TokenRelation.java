package org.apache.cassandra.cql3;

import com.google.common.base.Joiner;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.cassandra.cql3.restrictions.Restriction;
import org.apache.cassandra.cql3.restrictions.TokenRestriction;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.cql3.statements.RequestValidations;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

public final class TokenRelation extends Relation {
   private final List<ColumnMetadata.Raw> entities;
   private final Term.Raw value;

   public TokenRelation(List<ColumnMetadata.Raw> entities, Operator type, Term.Raw value) {
      this.entities = entities;
      this.relationType = type;
      this.value = value;
   }

   public boolean onToken() {
      return true;
   }

   public Term.Raw getValue() {
      return this.value;
   }

   public List<? extends Term.Raw> getInValues() {
      return null;
   }

   protected Restriction newEQRestriction(TableMetadata table, VariableSpecifications boundNames) {
      List<ColumnMetadata> columnDefs = this.getColumnDefinitions(table);
      Term term = this.toTerm(toReceivers(table, columnDefs), this.value, table.keyspace, boundNames);
      return new TokenRestriction.EQRestriction(table, columnDefs, term);
   }

   protected Restriction newINRestriction(TableMetadata table, VariableSpecifications boundNames) {
      throw RequestValidations.invalidRequest("%s cannot be used with the token function", new Object[]{this.operator()});
   }

   protected Restriction newSliceRestriction(TableMetadata table, VariableSpecifications boundNames, Bound bound, boolean inclusive) {
      List<ColumnMetadata> columnDefs = this.getColumnDefinitions(table);
      Term term = this.toTerm(toReceivers(table, columnDefs), this.value, table.keyspace, boundNames);
      return new TokenRestriction.SliceRestriction(table, columnDefs, bound, inclusive, term);
   }

   protected Restriction newContainsRestriction(TableMetadata table, VariableSpecifications boundNames, boolean isKey) {
      throw RequestValidations.invalidRequest("%s cannot be used with the token function", new Object[]{this.operator()});
   }

   protected Restriction newIsNotRestriction(TableMetadata table, VariableSpecifications boundNames) {
      throw RequestValidations.invalidRequest("%s cannot be used with the token function", new Object[]{this.operator()});
   }

   protected Restriction newLikeRestriction(TableMetadata table, VariableSpecifications boundNames, Operator operator) {
      throw RequestValidations.invalidRequest("%s cannot be used with the token function", new Object[]{operator});
   }

   protected Term toTerm(List<? extends ColumnSpecification> receivers, Term.Raw raw, String keyspace, VariableSpecifications boundNames) throws InvalidRequestException {
      Term term = raw.prepare(keyspace, (ColumnSpecification)receivers.get(0));
      term.collectMarkerSpecification(boundNames);
      return term;
   }

   public Relation renameIdentifier(ColumnMetadata.Raw from, ColumnMetadata.Raw to) {
      if(!this.entities.contains(from)) {
         return this;
      } else {
         List<ColumnMetadata.Raw> newEntities = (List)this.entities.stream().map((e) -> {
            return e.equals(from)?to:e;
         }).collect(Collectors.toList());
         return new TokenRelation(newEntities, this.operator(), this.value);
      }
   }

   public String toString() {
      return String.format("token%s %s %s", new Object[]{Tuples.tupleToString(this.entities), this.relationType, this.value});
   }

   private List<ColumnMetadata> getColumnDefinitions(TableMetadata table) {
      List<ColumnMetadata> columnDefs = new ArrayList(this.entities.size());
      Iterator var3 = this.entities.iterator();

      while(var3.hasNext()) {
         ColumnMetadata.Raw raw = (ColumnMetadata.Raw)var3.next();
         columnDefs.add(raw.prepare(table));
      }

      return columnDefs;
   }

   private static List<? extends ColumnSpecification> toReceivers(TableMetadata table, List<ColumnMetadata> columnDefs) throws InvalidRequestException {
      if(!columnDefs.equals(table.partitionKeyColumns())) {
         RequestValidations.checkTrue(columnDefs.containsAll(table.partitionKeyColumns()), "The token() function must be applied to all partition key components or none of them");
         RequestValidations.checkContainsNoDuplicates(columnDefs, "The token() function contains duplicate partition key components");
         RequestValidations.checkContainsOnly(columnDefs, table.partitionKeyColumns(), "The token() function must contains only partition key components");
         throw RequestValidations.invalidRequest("The token function arguments must be in the partition key order: %s", new Object[]{Joiner.on(", ").join(ColumnMetadata.toIdentifiers(table.partitionKeyColumns()))});
      } else {
         ColumnMetadata firstColumn = (ColumnMetadata)columnDefs.get(0);
         return Collections.singletonList(new ColumnSpecification(firstColumn.ksName, firstColumn.cfName, new ColumnIdentifier("partition key token", true), table.partitioner.getTokenValidator()));
      }
   }
}
