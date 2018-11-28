package org.apache.cassandra.cql3;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.cassandra.cql3.restrictions.Restriction;
import org.apache.cassandra.cql3.restrictions.SingleColumnRestriction;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.cql3.statements.RequestValidations;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

public class SingleColumnRelation extends Relation {
   private final ColumnMetadata.Raw entity;
   private final Term.Raw mapKey;
   private final Term.Raw value;
   private final List<Term.Raw> inValues;

   protected SingleColumnRelation(ColumnMetadata.Raw entity, Term.Raw mapKey, Operator type, Term.Raw value, List<Term.Raw> inValues) {
      this.entity = entity;
      this.mapKey = mapKey;
      this.relationType = type;
      this.value = value;
      this.inValues = inValues;

      assert type != Operator.IS_NOT || value == Constants.NULL_LITERAL;

   }

   public SingleColumnRelation(ColumnMetadata.Raw entity, Term.Raw mapKey, Operator type, Term.Raw value) {
      this(entity, mapKey, type, value, (List)null);
   }

   public SingleColumnRelation(ColumnMetadata.Raw entity, Operator type, Term.Raw value) {
      this(entity, (Term.Raw)null, type, value);
   }

   public Term.Raw getValue() {
      return this.value;
   }

   public List<? extends Term.Raw> getInValues() {
      return this.inValues;
   }

   public static SingleColumnRelation createInRelation(ColumnMetadata.Raw entity, List<Term.Raw> inValues) {
      return new SingleColumnRelation(entity, (Term.Raw)null, Operator.IN, (Term.Raw)null, inValues);
   }

   public ColumnMetadata.Raw getEntity() {
      return this.entity;
   }

   public Term.Raw getMapKey() {
      return this.mapKey;
   }

   protected Term toTerm(List<? extends ColumnSpecification> receivers, Term.Raw raw, String keyspace, VariableSpecifications boundNames) throws InvalidRequestException {
      assert receivers.size() == 1;

      Term term = raw.prepare(keyspace, (ColumnSpecification)receivers.get(0));
      term.collectMarkerSpecification(boundNames);
      return term;
   }

   public SingleColumnRelation withNonStrictOperator() {
      switch (this.relationType) {
         case GT: {
            return new SingleColumnRelation(this.entity, Operator.GTE, this.value);
         }
         case LT: {
            return new SingleColumnRelation(this.entity, Operator.LTE, this.value);
         }
      }
      return this;
   }

   public Relation renameIdentifier(ColumnMetadata.Raw from, ColumnMetadata.Raw to) {
      return this.entity.equals(from)?new SingleColumnRelation(to, this.mapKey, this.operator(), this.value, this.inValues):this;
   }

   public String toString() {
      String entityAsString = this.entity.toString();
      if(this.mapKey != null) {
         entityAsString = String.format("%s[%s]", new Object[]{entityAsString, this.mapKey});
      }

      return this.isIN()?String.format("%s IN %s", new Object[]{entityAsString, this.inValues}):String.format("%s %s %s", new Object[]{entityAsString, this.relationType, this.value});
   }

   protected Restriction newEQRestriction(TableMetadata table, VariableSpecifications boundNames) {
      ColumnMetadata columnDef = this.entity.prepare(table);
      if(this.mapKey == null) {
         Term term = this.toTerm(this.toReceivers(columnDef), this.value, table.keyspace, boundNames);
         return new SingleColumnRestriction.EQRestriction(columnDef, term);
      } else {
         List<? extends ColumnSpecification> receivers = this.toReceivers(columnDef);
         Term entryKey = this.toTerm(Collections.singletonList(receivers.get(0)), this.mapKey, table.keyspace, boundNames);
         Term entryValue = this.toTerm(Collections.singletonList(receivers.get(1)), this.value, table.keyspace, boundNames);
         return new SingleColumnRestriction.ContainsRestriction(columnDef, entryKey, entryValue);
      }
   }

   protected Restriction newINRestriction(TableMetadata table, VariableSpecifications boundNames) {
      ColumnMetadata columnDef = this.entity.prepare(table);
      List<? extends ColumnSpecification> receivers = this.toReceivers(columnDef);
      List<Term> terms = this.toTerms(receivers, this.inValues, table.keyspace, boundNames);
      if(terms == null) {
         Term term = this.toTerm(receivers, this.value, table.keyspace, boundNames);
         return new SingleColumnRestriction.InRestrictionWithMarker(columnDef, (Lists.Marker)term);
      } else {
         return (Restriction)(terms.size() == 1?new SingleColumnRestriction.EQRestriction(columnDef, (Term)terms.get(0)):new SingleColumnRestriction.InRestrictionWithValues(columnDef, terms));
      }
   }

   protected Restriction newSliceRestriction(TableMetadata table, VariableSpecifications boundNames, Bound bound, boolean inclusive) {
      ColumnMetadata columnDef = this.entity.prepare(table);
      if(columnDef.type.referencesDuration()) {
         RequestValidations.checkFalse(columnDef.type.isCollection(), "Slice restrictions are not supported on collections containing durations");
         RequestValidations.checkFalse(columnDef.type.isTuple(), "Slice restrictions are not supported on tuples containing durations");
         RequestValidations.checkFalse(columnDef.type.isUDT(), "Slice restrictions are not supported on UDTs containing durations");
         throw RequestValidations.invalidRequest("Slice restrictions are not supported on duration columns");
      } else {
         Term term = this.toTerm(this.toReceivers(columnDef), this.value, table.keyspace, boundNames);
         return new SingleColumnRestriction.SliceRestriction(columnDef, bound, inclusive, term);
      }
   }

   protected Restriction newContainsRestriction(TableMetadata table, VariableSpecifications boundNames, boolean isKey) throws InvalidRequestException {
      ColumnMetadata columnDef = this.entity.prepare(table);
      Term term = this.toTerm(this.toReceivers(columnDef), this.value, table.keyspace, boundNames);
      return new SingleColumnRestriction.ContainsRestriction(columnDef, term, isKey);
   }

   protected Restriction newIsNotRestriction(TableMetadata table, VariableSpecifications boundNames) throws InvalidRequestException {
      ColumnMetadata columnDef = this.entity.prepare(table);

      assert this.value == Constants.NULL_LITERAL : "Expected null literal for IS NOT relation: " + this.toString();

      return new SingleColumnRestriction.IsNotNullRestriction(columnDef);
   }

   protected Restriction newLikeRestriction(TableMetadata table, VariableSpecifications boundNames, Operator operator) {
      if(this.mapKey != null) {
         throw RequestValidations.invalidRequest("%s can't be used with collections.", new Object[]{this.operator()});
      } else {
         ColumnMetadata columnDef = this.entity.prepare(table);
         Term term = this.toTerm(this.toReceivers(columnDef), this.value, table.keyspace, boundNames);
         return new SingleColumnRestriction.LikeRestriction(columnDef, operator, term);
      }
   }

   protected List<? extends ColumnSpecification> toReceivers(ColumnMetadata columnDef) throws InvalidRequestException {
      ColumnSpecification receiver = columnDef;
      if(this.isIN()) {
         RequestValidations.checkFalse(!columnDef.isPrimaryKeyColumn() && !this.canHaveOnlyOneValue(), "IN predicates on non-primary-key columns (%s) is not yet supported", columnDef.name);
      }

      RequestValidations.checkFalse(this.isContainsKey() && !(columnDef.type instanceof MapType), "Cannot use CONTAINS KEY on non-map column %s", columnDef.name);
      RequestValidations.checkFalse(this.isContains() && !columnDef.type.isCollection(), "Cannot use CONTAINS on non-collection column %s", columnDef.name);
      if(this.mapKey != null) {
         RequestValidations.checkFalse(columnDef.type instanceof ListType, "Indexes on list entries (%s[index] = value) are not currently supported.", columnDef.name);
         RequestValidations.checkTrue(columnDef.type instanceof MapType, "Column %s cannot be used as a map", columnDef.name);
         RequestValidations.checkTrue(columnDef.type.isMultiCell(), "Map-entry equality predicates on frozen map column %s are not supported", columnDef.name);
         RequestValidations.checkTrue(this.isEQ(), "Only EQ relations are supported on map entries");
      }

      RequestValidations.checkFalse(columnDef.type.isUDT() && columnDef.type.isMultiCell(), "Non-frozen UDT column '%s' (%s) cannot be restricted by any relation", columnDef.name, columnDef.type.asCQL3Type());
      if(columnDef.type.isCollection()) {
         RequestValidations.checkFalse(columnDef.type.isMultiCell() && !this.isLegalRelationForNonFrozenCollection(), "Collection column '%s' (%s) cannot be restricted by a '%s' relation", columnDef.name, columnDef.type.asCQL3Type(), this.operator());
         if(!this.isContainsKey() && !this.isContains()) {
            if(columnDef.type.isMultiCell() && this.mapKey != null && this.isEQ()) {
               List<ColumnSpecification> receivers = new ArrayList(2);
               receivers.add(makeCollectionReceiver(columnDef, true));
               receivers.add(makeCollectionReceiver(columnDef, false));
               return receivers;
            }
         } else {
            receiver = makeCollectionReceiver(columnDef, this.isContainsKey());
         }
      }

      return Collections.singletonList(receiver);
   }

   protected static ColumnSpecification makeCollectionReceiver(ColumnSpecification receiver, boolean forKey) {
      return ((CollectionType)receiver.type).makeCollectionReceiver(receiver, forKey);
   }

   private boolean isLegalRelationForNonFrozenCollection() {
      return this.isContainsKey() || this.isContains() || this.isMapEntryEquality();
   }

   private boolean isMapEntryEquality() {
      return this.mapKey != null && this.isEQ();
   }

   private boolean canHaveOnlyOneValue() {
      return this.isEQ() || this.isLIKE() || this.isIN() && this.inValues != null && this.inValues.size() == 1;
   }
}
