package org.apache.cassandra.cql3;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.cassandra.cql3.restrictions.MultiColumnRestriction;
import org.apache.cassandra.cql3.restrictions.Restriction;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.cql3.statements.RequestValidations;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

public class MultiColumnRelation extends Relation {
   private final List<ColumnMetadata.Raw> entities;
   private final Term.MultiColumnRaw valuesOrMarker;
   private final List<? extends Term.MultiColumnRaw> inValues;
   private final Tuples.INRaw inMarker;

   private MultiColumnRelation(List<ColumnMetadata.Raw> entities, Operator relationType, Term.MultiColumnRaw valuesOrMarker, List<? extends Term.MultiColumnRaw> inValues, Tuples.INRaw inMarker) {
      this.entities = entities;
      this.relationType = relationType;
      this.valuesOrMarker = valuesOrMarker;
      this.inValues = inValues;
      this.inMarker = inMarker;
   }

   public static MultiColumnRelation createNonInRelation(List<ColumnMetadata.Raw> entities, Operator relationType, Term.MultiColumnRaw valuesOrMarker) {
      assert relationType != Operator.IN;

      return new MultiColumnRelation(entities, relationType, valuesOrMarker, (List)null, (Tuples.INRaw)null);
   }

   public static MultiColumnRelation createInRelation(List<ColumnMetadata.Raw> entities, List<? extends Term.MultiColumnRaw> inValues) {
      return new MultiColumnRelation(entities, Operator.IN, (Term.MultiColumnRaw)null, inValues, (Tuples.INRaw)null);
   }

   public static MultiColumnRelation createSingleMarkerInRelation(List<ColumnMetadata.Raw> entities, Tuples.INRaw inMarker) {
      return new MultiColumnRelation(entities, Operator.IN, (Term.MultiColumnRaw)null, (List)null, inMarker);
   }

   public List<ColumnMetadata.Raw> getEntities() {
      return this.entities;
   }

   public Term.MultiColumnRaw getValue() {
      return (Term.MultiColumnRaw)(this.relationType == Operator.IN?this.inMarker:this.valuesOrMarker);
   }

   public List<? extends Term.Raw> getInValues() {
      assert this.relationType == Operator.IN;

      return this.inValues;
   }

   public boolean isMultiColumn() {
      return true;
   }

   protected Restriction newEQRestriction(TableMetadata table, VariableSpecifications boundNames) {
      List<ColumnMetadata> receivers = this.receivers(table);
      Term term = this.toTerm(receivers, this.getValue(), table.keyspace, boundNames);
      return new MultiColumnRestriction.EQRestriction(receivers, term);
   }

   protected Restriction newINRestriction(TableMetadata table, VariableSpecifications boundNames) {
      List<ColumnMetadata> receivers = this.receivers(table);
      List<Term> terms = this.toTerms(receivers, this.inValues, table.keyspace, boundNames);
      if(terms == null) {
         Term term = this.toTerm(receivers, this.getValue(), table.keyspace, boundNames);
         return new MultiColumnRestriction.InRestrictionWithMarker(receivers, (AbstractMarker)term);
      } else {
         return (Restriction)(terms.size() == 1?new MultiColumnRestriction.EQRestriction(receivers, (Term)terms.get(0)):new MultiColumnRestriction.InRestrictionWithValues(receivers, terms));
      }
   }

   protected Restriction newSliceRestriction(TableMetadata table, VariableSpecifications boundNames, Bound bound, boolean inclusive) {
      List<ColumnMetadata> receivers = this.receivers(table);
      Term term = this.toTerm(this.receivers(table), this.getValue(), table.keyspace, boundNames);
      return new MultiColumnRestriction.SliceRestriction(receivers, bound, inclusive, term);
   }

   protected Restriction newContainsRestriction(TableMetadata table, VariableSpecifications boundNames, boolean isKey) {
      throw RequestValidations.invalidRequest("%s cannot be used for multi-column relations", new Object[]{this.operator()});
   }

   protected Restriction newIsNotRestriction(TableMetadata table, VariableSpecifications boundNames) {
      throw new AssertionError(String.format("%s cannot be used for multi-column relations", new Object[]{this.operator()}));
   }

   protected Restriction newLikeRestriction(TableMetadata table, VariableSpecifications boundNames, Operator operator) {
      throw RequestValidations.invalidRequest("%s cannot be used for multi-column relations", new Object[]{this.operator()});
   }

   protected Term toTerm(List<? extends ColumnSpecification> receivers, Term.Raw raw, String keyspace, VariableSpecifications boundNames) throws InvalidRequestException {
      Term term = ((Term.MultiColumnRaw)raw).prepare(keyspace, receivers);
      term.collectMarkerSpecification(boundNames);
      return term;
   }

   protected List<ColumnMetadata> receivers(TableMetadata table) throws InvalidRequestException {
      List<ColumnMetadata> names = new ArrayList(this.getEntities().size());
      int previousPosition = -1;

      ColumnMetadata def;
      for(Iterator var4 = this.getEntities().iterator(); var4.hasNext(); previousPosition = def.position()) {
         ColumnMetadata.Raw raw = (ColumnMetadata.Raw)var4.next();
         def = raw.prepare(table);
         RequestValidations.checkTrue(def.isClusteringColumn(), "Multi-column relations can only be applied to clustering columns but was applied to: %s", def.name);
         RequestValidations.checkFalse(names.contains(def), "Column \"%s\" appeared twice in a relation: %s", def.name, this);
         RequestValidations.checkFalse(previousPosition != -1 && def.position() != previousPosition + 1, "Clustering columns must appear in the PRIMARY KEY order in multi-column relations: %s", this);
         names.add(def);
      }

      return names;
   }

   public Relation renameIdentifier(ColumnMetadata.Raw from, ColumnMetadata.Raw to) {
      if(!this.entities.contains(from)) {
         return this;
      } else {
         List<ColumnMetadata.Raw> newEntities = (List)this.entities.stream().map((e) -> {
            return e.equals(from)?to:e;
         }).collect(Collectors.toList());
         return new MultiColumnRelation(newEntities, this.operator(), this.valuesOrMarker, this.inValues, this.inMarker);
      }
   }

   public String toString() {
      StringBuilder builder = new StringBuilder(Tuples.tupleToString(this.entities));
      return this.isIN()?builder.append(" IN ").append(this.inMarker != null?Character.valueOf('?'):Tuples.tupleToString(this.inValues)).toString():builder.append(" ").append(this.relationType).append(" ").append(this.valuesOrMarker).toString();
   }
}
