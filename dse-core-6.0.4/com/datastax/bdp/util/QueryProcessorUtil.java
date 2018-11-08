package com.datastax.bdp.util;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.google.common.base.Preconditions;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.cql3.BatchQueryOptions;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.UntypedResultSet.Row;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement.Prepared;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.transport.messages.ResultMessage.Kind;
import org.apache.cassandra.transport.messages.ResultMessage.Rows;
import org.apache.cassandra.utils.flow.Flow;

public class QueryProcessorUtil {
   public QueryProcessorUtil() {
   }

   public static void processPreparedBlocking(CQLStatement statement, ConsistencyLevel cl, List<ByteBuffer> values) {
      TPCUtils.blockingGet(statement.execute(QueryState.forInternalCalls(), QueryOptions.forInternalCalls(cl, values), System.nanoTime()));
   }

   public static void processBatchBlocking(BatchStatement statement, ConsistencyLevel cl, List<List<ByteBuffer>> values) {
      BatchQueryOptions options = BatchQueryOptions.withPerStatementVariables(QueryOptions.forInternalCalls(cl, Collections.emptyList()), values, Collections.emptyList());
      TPCUtils.blockingGet(QueryProcessor.instance.processBatch(statement, QueryState.forInternalCalls(), options, System.nanoTime()));
   }

   public static UntypedResultSet processPreparedSelect(CQLStatement statement, ConsistencyLevel cl) {
      return processPreparedSelect(statement, cl, Collections.emptyList());
   }

   public static UntypedResultSet processPreparedSelect(CQLStatement statement, ConsistencyLevel cl, List<ByteBuffer> values) {
      ResultMessage result = (ResultMessage)TPCUtils.blockingGet(statement.execute(QueryState.forInternalCalls(), QueryOptions.forInternalCalls(cl, values), System.nanoTime()));
      if(result.kind.equals(Kind.ROWS)) {
         return UntypedResultSet.create(((Rows)result).result);
      } else {
         throw new RuntimeException("Unexpected result type returned for select statement: " + result.kind);
      }
   }

   public static ConsistencyLevel getSerialCL(ConsistencyLevel quorumCL) {
      if(quorumCL.equals(ConsistencyLevel.LOCAL_QUORUM)) {
         return ConsistencyLevel.LOCAL_SERIAL;
      } else if(quorumCL.equals(ConsistencyLevel.QUORUM)) {
         return ConsistencyLevel.SERIAL;
      } else {
         throw new AssertionError("CL should be either QUORUM or LOCAL_QUORUM, not " + quorumCL.toString());
      }
   }

   public static boolean wasLwtApplied(UntypedResultSet res) {
      return res != null && !res.isEmpty() && res.one().getBoolean("[applied]");
   }

   public static QueryOptions getLwtQueryOptions(ConsistencyLevel cl, List<ByteBuffer> parameters) {
      return QueryOptions.create(cl, parameters, false, -1, (PagingState)null, getSerialCL(cl), ProtocolVersion.CURRENT, (String)null);
   }

   public static String getFullTableName(String keyspace, String table) {
      return String.format("\"%s\".\"%s\"", new Object[]{keyspace, table});
   }

   public static QueryProcessorUtil.LwtUntypedResultSet executeLwt(String queryStr, ConsistencyLevel cl, Object... values) {
      Prepared stmt = QueryProcessor.getStatement(queryStr, QueryState.forInternalCalls());
      return executeLwt(stmt, cl, values);
   }

   public static QueryProcessorUtil.LwtUntypedResultSet executeLwt(Prepared stmt, ConsistencyLevel cl, Object... values) {
      Preconditions.checkArgument(stmt.statement instanceof ModificationStatement, "This method can be used to execute only modification statements");
      List<ByteBuffer> bbList = toByteBufferList(stmt.boundNames, values);
      QueryOptions queryOptions = getLwtQueryOptions(cl, bbList);
      return new QueryProcessorUtil.LwtUntypedResultSet(execute(stmt, QueryState.forInternalCalls(), queryOptions));
   }

   public static UntypedResultSet execute(String queryStr, ConsistencyLevel cl, Object... values) {
      Prepared stmt = QueryProcessor.getStatement(queryStr, QueryState.forInternalCalls());
      List<ByteBuffer> bbList = toByteBufferList(stmt.boundNames, values);
      QueryOptions queryOptions = QueryOptions.forInternalCalls(cl, bbList);
      return execute(stmt, QueryState.forInternalCalls(), queryOptions);
   }

   private static UntypedResultSet execute(Prepared prepared, QueryState queryState, QueryOptions queryOptions) {
      ResultMessage resultMessage = (ResultMessage)TPCUtils.blockingGet(QueryProcessor.instance.processStatement(prepared, queryState, queryOptions, System.nanoTime()));
      if(resultMessage.kind == Kind.ROWS) {
         return UntypedResultSet.create(((Rows)resultMessage).result);
      } else if(resultMessage.kind == Kind.VOID) {
         return null;
      } else {
         throw new RuntimeException("Unexpected result kind " + resultMessage.kind);
      }
   }

   public static UntypedResultSet executeQuery(ConsistencyLevel cl, BuiltStatement stmt) {
      ByteBuffer[] valuesArray = stmt.getValues(com.datastax.driver.core.ProtocolVersion.NEWEST_SUPPORTED, CodecRegistry.DEFAULT_INSTANCE);
      List<ByteBuffer> values = valuesArray != null?Arrays.asList(valuesArray):Collections.EMPTY_LIST;
      String query = stmt.getQueryString();
      return (UntypedResultSet)TPCUtils.blockingGet(QueryProcessor.process(query, cl, values));
   }

   private static List<ByteBuffer> toByteBufferList(List<ColumnSpecification> boundNames, Object[] values) {
      if(boundNames.size() != values.length) {
         throw new IllegalArgumentException(String.format("Invalid number of values. Expecting %d but got %d", new Object[]{Integer.valueOf(boundNames.size()), Integer.valueOf(values.length)}));
      } else {
         ArrayList<ByteBuffer> boundValues = new ArrayList(values.length);

         for(int i = 0; i < values.length; ++i) {
            Object value = values[i];
            AbstractType type = ((ColumnSpecification)boundNames.get(i)).type;
            ByteBuffer bb = !(value instanceof ByteBuffer) && value != null?type.decompose(value):(ByteBuffer)value;
            boundValues.add(bb);
         }

         return boundValues;
      }
   }

   public static class LwtUntypedResultSet extends UntypedResultSet {
      private final UntypedResultSet resultSet;

      private LwtUntypedResultSet(UntypedResultSet resultSet) {
         this.resultSet = resultSet;
      }

      public int size() {
         return this.resultSet.size();
      }

      public Row one() {
         return this.resultSet.one();
      }

      public Flow<Row> rows() {
         return this.resultSet.rows();
      }

      public List<ColumnSpecification> metadata() {
         return this.resultSet.metadata();
      }

      public Iterator<Row> iterator() {
         return this.resultSet.iterator();
      }

      public boolean wasApplied() {
         return QueryProcessorUtil.wasLwtApplied(this.resultSet);
      }

      public void ifApplied(Runnable code) {
         if(this.wasApplied()) {
            code.run();
         }

      }

      public <T> Optional<T> map(Supplier<T> code) {
         return this.wasApplied()?Optional.of(code.get()):Optional.empty();
      }
   }
}
