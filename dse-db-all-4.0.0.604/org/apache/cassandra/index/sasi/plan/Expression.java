package org.apache.cassandra.index.sasi.plan;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sasi.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sasi.conf.ColumnIndex;
import org.apache.cassandra.index.sasi.disk.OnDiskIndex;
import org.apache.cassandra.index.sasi.utils.TypeUtil;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Expression {
   private static final Logger logger = LoggerFactory.getLogger(Expression.class);
   private final QueryController controller;
   public final AbstractAnalyzer analyzer;
   public final ColumnIndex index;
   public final AbstractType<?> validator;
   public final boolean isLiteral;
   @VisibleForTesting
   protected Expression.Op operation;
   public Expression.Bound lower;
   public Expression.Bound upper;
   public List<ByteBuffer> exclusions;

   public Expression(Expression other) {
      this(other.controller, other.index);
      this.operation = other.operation;
   }

   public Expression(QueryController controller, ColumnIndex columnIndex) {
      this.exclusions = new ArrayList();
      this.controller = controller;
      this.index = columnIndex;
      this.analyzer = columnIndex.getAnalyzer();
      this.validator = columnIndex.getValidator();
      this.isLiteral = columnIndex.isLiteral();
   }

   @VisibleForTesting
   public Expression(String name, AbstractType<?> validator) {
      this((QueryController)null, (ColumnIndex)(new ColumnIndex(UTF8Type.instance, ColumnMetadata.regularColumn("sasi", "internal", name, validator), (IndexMetadata)null)));
   }

   public Expression setLower(Expression.Bound newLower) {
      this.lower = newLower == null?null:new Expression.Bound(newLower.value, newLower.inclusive);
      return this;
   }

   public Expression setUpper(Expression.Bound newUpper) {
      this.upper = newUpper == null?null:new Expression.Bound(newUpper.value, newUpper.inclusive);
      return this;
   }

   public Expression setOp(Expression.Op op) {
      this.operation = op;
      return this;
   }


   public Expression add(final Operator op, final ByteBuffer value) {
      boolean lowerInclusive = false;
      boolean upperInclusive = false;
      Label_0247: {
         switch (op) {
            case EQ:
            case LIKE_PREFIX:
            case LIKE_SUFFIX:
            case LIKE_CONTAINS:
            case LIKE_MATCHES: {
               this.lower = new Bound(value, true);
               this.upper = this.lower;
               this.operation = Op.valueOf(op);
               break;
            }
            case NEQ: {
               if (this.operation == null) {
                  this.operation = Op.NOT_EQ;
                  this.lower = new Bound(value, true);
                  this.upper = this.lower;
                  break;
               }
               this.exclusions.add(value);
               break;
            }
            case LTE: {
               if (this.index.getDefinition().isReversedType()) {
                  lowerInclusive = true;
                  break Label_0247;
               }
               upperInclusive = true;
               break Label_0247;
            }
            case LT: {
               this.operation = Op.RANGE;
               if (this.index.getDefinition().isReversedType()) {
                  this.lower = new Bound(value, lowerInclusive);
                  break;
               }
               this.upper = new Bound(value, upperInclusive);
               break;
            }
            case GTE: {
               if (this.index.getDefinition().isReversedType()) {
                  upperInclusive = true;
                  break Label_0247;
               }
               lowerInclusive = true;
               break Label_0247;
            }
            case GT: {
               this.operation = Op.RANGE;
               if (this.index.getDefinition().isReversedType()) {
                  this.upper = new Bound(value, upperInclusive);
                  break;
               }
               this.lower = new Bound(value, lowerInclusive);
               break;
            }
         }
      }
      return this;
   }


   public Expression addExclusion(ByteBuffer value) {
      this.exclusions.add(value);
      return this;
   }

   public boolean isSatisfiedBy(ByteBuffer value) {
      int cmp;
      if(!TypeUtil.isValid(value, this.validator)) {
         cmp = value.remaining();
         if((value = TypeUtil.tryUpcast(value, this.validator)) == null) {
            logger.error("Can't cast value for {} to size accepted by {}, value size is {}.", new Object[]{this.index.getColumnName(), this.validator, FBUtilities.prettyPrintMemory((long)cmp)});
            return false;
         }
      }

      if(this.lower != null) {
         if(this.isLiteral) {
            if(!this.validateStringValue(value, this.lower.value)) {
               return false;
            }
         } else {
            cmp = this.validator.compare(this.lower.value, value);
            if(this.operation == Expression.Op.EQ || this.operation == Expression.Op.NOT_EQ) {
               return cmp == 0;
            }

            if(cmp > 0 || cmp == 0 && !this.lower.inclusive) {
               return false;
            }
         }
      }

      if(this.upper != null && this.lower != this.upper) {
         if(this.isLiteral) {
            if(!this.validateStringValue(value, this.upper.value)) {
               return false;
            }
         } else {
            cmp = this.validator.compare(this.upper.value, value);
            if(cmp < 0 || cmp == 0 && !this.upper.inclusive) {
               return false;
            }
         }
      }

      Iterator var4 = this.exclusions.iterator();

      ByteBuffer term;
      do {
         if(!var4.hasNext()) {
            return true;
         }

         term = (ByteBuffer)var4.next();
      } while((!this.isLiteral || !this.validateStringValue(value, term)) && this.validator.compare(term, value) != 0);

      return false;
   }


   private boolean validateStringValue(ByteBuffer columnValue, ByteBuffer requestedValue) {
      this.analyzer.reset(columnValue.duplicate());
      while (this.analyzer.hasNext()) {
         ByteBuffer term = this.analyzer.next();
         boolean isMatch = false;
         switch (this.operation) {
            case EQ:
            case MATCH:
            case NOT_EQ: {
               isMatch = this.validator.compare(term, requestedValue) == 0;
               break;
            }
            case RANGE: {
               isMatch = this.isLowerSatisfiedBy(term) && this.isUpperSatisfiedBy(term);
               break;
            }
            case PREFIX: {
               isMatch = ByteBufferUtil.startsWith(term, requestedValue);
               break;
            }
            case SUFFIX: {
               isMatch = ByteBufferUtil.endsWith(term, requestedValue);
               break;
            }
            case CONTAINS: {
               isMatch = ByteBufferUtil.contains(term, requestedValue);
            }
         }
         if (!isMatch) continue;
         return true;
      }
      return false;
   }

   public Expression.Op getOp() {
      return this.operation;
   }

   public void checkpoint() {
      if(this.controller != null) {
         this.controller.checkpoint();
      }
   }

   public boolean hasLower() {
      return this.lower != null;
   }

   public boolean hasUpper() {
      return this.upper != null;
   }

   public boolean isLowerSatisfiedBy(ByteBuffer value) {
      if(!this.hasLower()) {
         return true;
      } else {
         int cmp = this.validator.compare(value, this.lower.value);
         return cmp > 0 || cmp == 0 && this.lower.inclusive;
      }
   }

   public boolean isUpperSatisfiedBy(ByteBuffer value) {
      if(!this.hasUpper()) {
         return true;
      } else {
         int cmp = this.validator.compare(value, this.upper.value);
         return cmp < 0 || cmp == 0 && this.upper.inclusive;
      }
   }

   public boolean isLowerSatisfiedBy(OnDiskIndex.DataTerm term) {
      if(!this.hasLower()) {
         return true;
      } else {
         int cmp = term.compareTo(this.validator, this.lower.value, false);
         return cmp > 0 || cmp == 0 && this.lower.inclusive;
      }
   }

   public boolean isUpperSatisfiedBy(OnDiskIndex.DataTerm term) {
      if(!this.hasUpper()) {
         return true;
      } else {
         int cmp = term.compareTo(this.validator, this.upper.value, false);
         return cmp < 0 || cmp == 0 && this.upper.inclusive;
      }
   }

   public boolean isIndexed() {
      return this.index.isIndexed();
   }

   public String toString() {
      Object[] arrobject = new Object[7];
      arrobject[0] = this.index.getColumnName();
      arrobject[1] = this.operation;
      arrobject[2] = this.lower == null ? "null" : this.validator.getString(this.lower.value);
      arrobject[3] = this.lower != null && this.lower.inclusive;
      arrobject[4] = this.upper == null ? "null" : this.validator.getString(this.upper.value);
      arrobject[5] = this.upper != null && this.upper.inclusive;
      arrobject[6] = Iterators.toString((Iterator)Iterators.transform(this.exclusions.iterator(), this.validator::getString));
      return String.format("Expression{name: %s, op: %s, lower: (%s, %s), upper: (%s, %s), exclusions: %s}", arrobject);
   }

   public int hashCode() {
      return (new HashCodeBuilder()).append(this.index.getColumnName()).append(this.operation).append(this.validator).append(this.lower).append(this.upper).append(this.exclusions).build().intValue();
   }

   public boolean equals(Object other) {
      if(!(other instanceof Expression)) {
         return false;
      } else if(this == other) {
         return true;
      } else {
         Expression o = (Expression)other;
         return Objects.equals(this.index.getColumnName(), o.index.getColumnName()) && this.validator.equals(o.validator) && this.operation == o.operation && Objects.equals(this.lower, o.lower) && Objects.equals(this.upper, o.upper) && this.exclusions.equals(o.exclusions);
      }
   }

   public static class Bound {
      public final ByteBuffer value;
      public final boolean inclusive;

      public Bound(ByteBuffer value, boolean inclusive) {
         this.value = value;
         this.inclusive = inclusive;
      }

      public boolean equals(Object other) {
         if(!(other instanceof Expression.Bound)) {
            return false;
         } else {
            Expression.Bound o = (Expression.Bound)other;
            return this.value.equals(o.value) && this.inclusive == o.inclusive;
         }
      }

      public int hashCode() {
         HashCodeBuilder builder = new HashCodeBuilder();
         builder.append(this.value);
         builder.append(this.inclusive);
         return builder.toHashCode();
      }
   }

   public static enum Op {
      EQ,
      MATCH,
      PREFIX,
      SUFFIX,
      CONTAINS,
      NOT_EQ,
      RANGE;

      private Op() {
      }


      public static Op valueOf(Operator operator) {
         switch (operator) {
            case EQ: {
               return EQ;
            }
            case NEQ: {
               return NOT_EQ;
            }
            case LT:
            case GT:
            case LTE:
            case GTE: {
               return RANGE;
            }
            case LIKE_PREFIX: {
               return PREFIX;
            }
            case LIKE_SUFFIX: {
               return SUFFIX;
            }
            case LIKE_CONTAINS: {
               return CONTAINS;
            }
            case LIKE_MATCHES: {
               return MATCH;
            }
         }
         throw new IllegalArgumentException("unknown operator: " + (Object)((Object)operator));
      }
   }
}
