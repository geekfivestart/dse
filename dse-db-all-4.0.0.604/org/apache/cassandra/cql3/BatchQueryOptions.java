package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.service.QueryState;

public abstract class BatchQueryOptions {
   public static BatchQueryOptions DEFAULT;
   protected final QueryOptions wrapped;
   private final List<Object> queryOrIdList;

   protected BatchQueryOptions(QueryOptions wrapped, List<Object> queryOrIdList) {
      this.wrapped = wrapped;
      this.queryOrIdList = queryOrIdList;
   }

   public static BatchQueryOptions withoutPerStatementVariables(QueryOptions options) {
      return new BatchQueryOptions.WithoutPerStatementVariables(options, Collections.emptyList());
   }

   public static BatchQueryOptions withPerStatementVariables(QueryOptions options, List<List<ByteBuffer>> variables, List<Object> queryOrIdList) {
      return new BatchQueryOptions.WithPerStatementVariables(options, variables, queryOrIdList);
   }

   public abstract QueryOptions forStatement(int var1);

   public void prepareStatement(int i, List<ColumnSpecification> boundNames) {
      this.forStatement(i).prepare(boundNames);
   }

   public ConsistencyLevel getConsistency() {
      return this.wrapped.getConsistency();
   }

   public String getKeyspace() {
      return this.wrapped.getKeyspace();
   }

   public ConsistencyLevel getSerialConsistency() {
      return this.wrapped.getSerialConsistency();
   }

   public List<Object> getQueryOrIdList() {
      return this.queryOrIdList;
   }

   public long getTimestamp(QueryState state) {
      return this.wrapped.getTimestamp(state);
   }

   static {
      DEFAULT = withoutPerStatementVariables(QueryOptions.DEFAULT);
   }

   private static class WithPerStatementVariables extends BatchQueryOptions {
      private final List<QueryOptions> perStatementOptions;

      private WithPerStatementVariables(QueryOptions wrapped, List<List<ByteBuffer>> variables, List<Object> queryOrIdList) {
         super(wrapped, queryOrIdList);
         this.perStatementOptions = new ArrayList(variables.size());
         Iterator var4 = variables.iterator();

         while(var4.hasNext()) {
            final List<ByteBuffer> vars = (List)var4.next();
            this.perStatementOptions.add(new QueryOptions.QueryOptionsWrapper(wrapped) {
               public List<ByteBuffer> getValues() {
                  return vars;
               }
            });
         }

      }

      public QueryOptions forStatement(int i) {
         return (QueryOptions)this.perStatementOptions.get(i);
      }

      public void prepareStatement(int i, List<ColumnSpecification> boundNames) {
         QueryOptions options = (QueryOptions)this.perStatementOptions.get(i);
         options.prepare(boundNames);
         options = QueryOptions.addColumnSpecifications(options, boundNames);
         this.perStatementOptions.set(i, options);
      }
   }

   private static class WithoutPerStatementVariables extends BatchQueryOptions {
      private WithoutPerStatementVariables(QueryOptions wrapped, List<Object> queryOrIdList) {
         super(wrapped, queryOrIdList);
      }

      public QueryOptions forStatement(int i) {
         return this.wrapped;
      }
   }
}
