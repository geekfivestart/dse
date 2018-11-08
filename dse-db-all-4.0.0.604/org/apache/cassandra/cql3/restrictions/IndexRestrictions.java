package org.apache.cassandra.cql3.restrictions;

import com.google.common.collect.ImmutableList;
import org.apache.cassandra.cql3.IndexName;
import org.apache.cassandra.cql3.statements.RequestValidations;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.TableMetadata;

public final class IndexRestrictions {
   private static final IndexRestrictions EMPTY_RESTRICTIONS = new IndexRestrictions(ImmutableList.of(), ImmutableList.of());
   public static final String INDEX_NOT_FOUND = "Invalid index expression, index %s not found for %s";
   public static final String INVALID_INDEX = "Target index %s cannot be used to query %s";
   public static final String CUSTOM_EXPRESSION_NOT_SUPPORTED = "Index %s does not support custom expressions";
   public static final String NON_CUSTOM_INDEX_IN_EXPRESSION = "Only CUSTOM indexes may be used in custom index expressions, %s is not valid";
   public static final String MULTIPLE_EXPRESSIONS = "Multiple custom index expressions in a single query are not supported";
   private final ImmutableList<Restrictions> regularRestrictions;
   private final ImmutableList<ExternalRestriction> externalRestrictions;

   private IndexRestrictions(ImmutableList<Restrictions> regularRestrictions, ImmutableList<ExternalRestriction> externalExpressions) {
      this.regularRestrictions = regularRestrictions;
      this.externalRestrictions = externalExpressions;
   }

   public static IndexRestrictions of() {
      return EMPTY_RESTRICTIONS;
   }

   public static IndexRestrictions.Builder builder() {
      return new IndexRestrictions.Builder();
   }

   public boolean isEmpty() {
      return this.regularRestrictions.isEmpty() && this.externalRestrictions.isEmpty();
   }

   public ImmutableList<Restrictions> getRestrictions() {
      return this.regularRestrictions;
   }

   public ImmutableList<ExternalRestriction> getExternalExpressions() {
      return this.externalRestrictions;
   }

   static InvalidRequestException invalidIndex(IndexName indexName, TableMetadata table) {
      return new InvalidRequestException(String.format("Target index %s cannot be used to query %s", new Object[]{indexName.getIdx(), table.toString()}));
   }

   static InvalidRequestException indexNotFound(IndexName indexName, TableMetadata table) {
      return new InvalidRequestException(String.format("Invalid index expression, index %s not found for %s", new Object[]{indexName.getIdx(), table.toString()}));
   }

   static InvalidRequestException nonCustomIndexInExpression(IndexName indexName) {
      return RequestValidations.invalidRequest("Only CUSTOM indexes may be used in custom index expressions, %s is not valid", new Object[]{indexName.getIdx()});
   }

   static InvalidRequestException customExpressionNotSupported(IndexName indexName) {
      return RequestValidations.invalidRequest("Index %s does not support custom expressions", new Object[]{indexName.getIdx()});
   }

   public static final class Builder {
      private com.google.common.collect.ImmutableList.Builder<Restrictions> regularRestrictions;
      private com.google.common.collect.ImmutableList.Builder<ExternalRestriction> externalRestrictions;

      private Builder() {
         this.regularRestrictions = ImmutableList.builder();
         this.externalRestrictions = ImmutableList.builder();
      }

      public IndexRestrictions.Builder add(Restrictions restrictions) {
         this.regularRestrictions.add(restrictions);
         return this;
      }

      public IndexRestrictions.Builder add(IndexRestrictions restrictions) {
         this.regularRestrictions.addAll(restrictions.regularRestrictions);
         this.externalRestrictions.addAll(restrictions.externalRestrictions);
         return this;
      }

      public IndexRestrictions.Builder add(ExternalRestriction restriction) {
         this.externalRestrictions.add(restriction);
         return this;
      }

      public IndexRestrictions build() {
         return new IndexRestrictions(this.regularRestrictions.build(), this.externalRestrictions.build());
      }
   }
}
