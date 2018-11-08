package org.apache.cassandra.cql3;

import java.util.ArrayList;
import java.util.List;
import org.apache.cassandra.cql3.restrictions.Restriction;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.cql3.statements.RequestValidations;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

public abstract class Relation {
   protected Operator relationType;

   public Relation() {
   }

   public Operator operator() {
      return this.relationType;
   }

   public abstract Term.Raw getValue();

   public abstract List<? extends Term.Raw> getInValues();

   public boolean isMultiColumn() {
      return false;
   }

   public boolean onToken() {
      return false;
   }

   public final boolean isContains() {
      return this.relationType == Operator.CONTAINS;
   }

   public final boolean isContainsKey() {
      return this.relationType == Operator.CONTAINS_KEY;
   }

   public final boolean isIN() {
      return this.relationType == Operator.IN;
   }

   public final boolean isEQ() {
      return this.relationType == Operator.EQ;
   }

   public final boolean isLIKE() {
      return this.relationType == Operator.LIKE_PREFIX || this.relationType == Operator.LIKE_SUFFIX || this.relationType == Operator.LIKE_CONTAINS || this.relationType == Operator.LIKE_MATCHES || this.relationType == Operator.LIKE;
   }

   public final boolean isSlice() {
      return this.relationType == Operator.GT || this.relationType == Operator.GTE || this.relationType == Operator.LTE || this.relationType == Operator.LT;
   }

   public Restriction toRestriction(TableMetadata table, VariableSpecifications boundNames) {
      switch(null.$SwitchMap$org$apache$cassandra$cql3$Operator[this.relationType.ordinal()]) {
      case 1:
         return this.newEQRestriction(table, boundNames);
      case 2:
         return this.newSliceRestriction(table, boundNames, Bound.END, false);
      case 3:
         return this.newSliceRestriction(table, boundNames, Bound.END, true);
      case 4:
         return this.newSliceRestriction(table, boundNames, Bound.START, true);
      case 5:
         return this.newSliceRestriction(table, boundNames, Bound.START, false);
      case 6:
         return this.newINRestriction(table, boundNames);
      case 7:
         return this.newContainsRestriction(table, boundNames, false);
      case 8:
         return this.newContainsRestriction(table, boundNames, true);
      case 9:
         return this.newIsNotRestriction(table, boundNames);
      case 10:
      case 11:
      case 12:
      case 13:
      case 14:
         return this.newLikeRestriction(table, boundNames, this.relationType);
      default:
         throw RequestValidations.invalidRequest("Unsupported \"!=\" relation: %s", new Object[]{this});
      }
   }

   protected abstract Restriction newEQRestriction(TableMetadata var1, VariableSpecifications var2);

   protected abstract Restriction newINRestriction(TableMetadata var1, VariableSpecifications var2);

   protected abstract Restriction newSliceRestriction(TableMetadata var1, VariableSpecifications var2, Bound var3, boolean var4);

   protected abstract Restriction newContainsRestriction(TableMetadata var1, VariableSpecifications var2, boolean var3);

   protected abstract Restriction newIsNotRestriction(TableMetadata var1, VariableSpecifications var2);

   protected abstract Restriction newLikeRestriction(TableMetadata var1, VariableSpecifications var2, Operator var3);

   protected abstract Term toTerm(List<? extends ColumnSpecification> var1, Term.Raw var2, String var3, VariableSpecifications var4);

   protected final List<Term> toTerms(List<? extends ColumnSpecification> receivers, List<? extends Term.Raw> raws, String keyspace, VariableSpecifications boundNames) {
      if(raws == null) {
         return null;
      } else {
         List<Term> terms = new ArrayList(raws.size());
         int i = 0;

         for(int m = raws.size(); i < m; ++i) {
            terms.add(this.toTerm(receivers, (Term.Raw)raws.get(i), keyspace, boundNames));
         }

         return terms;
      }
   }

   public abstract Relation renameIdentifier(ColumnMetadata.Raw var1, ColumnMetadata.Raw var2);
}
