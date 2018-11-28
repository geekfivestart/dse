package com.datastax.bdp.util.rpc;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.Iterators;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.UntypedResultSet.Row;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.service.QueryState;
import org.apache.commons.lang3.StringUtils;

public class RpcUtil {
   public RpcUtil() {
   }

   public static Row[] callNoExtract(QueryState qs, String objName, String methodName, Object... params) {
      String query = getFormat(objName, methodName, params);
      UntypedResultSet rs = QueryProcessor.execute(query, ConsistencyLevel.LOCAL_ONE, qs, params);
      return rs != null?(Row[])Iterators.toArray(rs.iterator(), Row.class):null;
   }

   public static <T> T call(Session session, String objName, String methodName, Object... params) {
      return callInternal(session, objName, methodName, (Boolean)null, params);
   }

   public static <T> T callIdempotent(Session session, String objName, String methodName, Object... params) {
      return callInternal(session, objName, methodName, Boolean.valueOf(true), params);
   }

   private static <T> T callInternal(Session session, String objName, String methodName, Boolean isIdempotent, Object... params) {
      String query = getFormat(objName, methodName, params);
      Statement statement = new SimpleStatement(query, params);
      if(isIdempotent != null) {
         statement.setIdempotent(isIdempotent.booleanValue());
      }

      ResultSet rs = session.execute(statement);
      if(rs.isExhausted()) {
         return null;
      } else {
         Object result = rs.one().getObject("result");
         return (T)result;
      }
   }

   public static ResultSet callNoExtract(Session session, String objName, String methodName, Object... params) {
      return callNoExtractInternal(session, objName, methodName, (Boolean)null, params);
   }

   private static ResultSet callNoExtractInternal(Session session, String objName, String methodName, Boolean isIdempotent, Object... params) {
      String query = getFormat(objName, methodName, params);
      Statement statement = new SimpleStatement(query, params);
      if(isIdempotent != null) {
         statement.setIdempotent(isIdempotent.booleanValue());
      }

      return session.execute(statement);
   }

   private static String getFormat(String objName, String methodName, Object[] params) {
      return String.format("CALL %s.%s (%s)", new Object[]{QueryBuilder.quote(objName), QueryBuilder.quote(methodName), StringUtils.repeat("?", ",", params.length)});
   }
}
