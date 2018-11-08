package org.apache.cassandra.cql3.conditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.CQL3CasRequest;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.schema.ColumnMetadata;

public final class ColumnConditions extends AbstractConditions {
   private final List<ColumnCondition> columnConditions;
   private final List<ColumnCondition> staticConditions;

   private ColumnConditions(ColumnConditions.Builder builder) {
      this.columnConditions = builder.columnConditions;
      this.staticConditions = builder.staticConditions;
   }

   public boolean appliesToStaticColumns() {
      return !this.staticConditions.isEmpty();
   }

   public boolean appliesToRegularColumns() {
      return !this.columnConditions.isEmpty();
   }

   public Collection<ColumnMetadata> getColumns() {
      return (Collection)Stream.concat(this.columnConditions.stream(), this.staticConditions.stream()).map((e) -> {
         return e.column;
      }).collect(Collectors.toList());
   }

   public boolean isEmpty() {
      return this.columnConditions.isEmpty() && this.staticConditions.isEmpty();
   }

   public void addConditionsTo(CQL3CasRequest request, Clustering clustering, QueryOptions options) {
      if(!this.columnConditions.isEmpty()) {
         request.addConditions(clustering, this.columnConditions, options);
      }

      if(!this.staticConditions.isEmpty()) {
         request.addConditions(Clustering.STATIC_CLUSTERING, this.staticConditions, options);
      }

   }

   public void addFunctionsTo(List<org.apache.cassandra.cql3.functions.Function> functions) {
      this.columnConditions.forEach((p) -> {
         p.addFunctionsTo(functions);
      });
      this.staticConditions.forEach((p) -> {
         p.addFunctionsTo(functions);
      });
   }

   public static ColumnConditions.Builder newBuilder() {
      return new ColumnConditions.Builder();
   }

   public static final class Builder {
      private List<ColumnCondition> columnConditions;
      private List<ColumnCondition> staticConditions;

      public ColumnConditions.Builder add(ColumnCondition condition) {
         List conds;
         if(condition.column.isStatic()) {
            if(this.staticConditions.isEmpty()) {
               this.staticConditions = new ArrayList();
            }

            conds = this.staticConditions;
         } else {
            if(this.columnConditions.isEmpty()) {
               this.columnConditions = new ArrayList();
            }

            conds = this.columnConditions;
         }

         conds.add(condition);
         return this;
      }

      public ColumnConditions build() {
         return new ColumnConditions(this);
      }

      private Builder() {
         this.columnConditions = Collections.emptyList();
         this.staticConditions = Collections.emptyList();
      }
   }
}
