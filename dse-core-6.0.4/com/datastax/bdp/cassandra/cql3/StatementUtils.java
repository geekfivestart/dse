package com.datastax.bdp.cassandra.cql3;

import com.datastax.bdp.cassandra.metrics.LatencyValues;
import io.reactivex.Single;
import io.reactivex.functions.Function;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.cassandra.auth.user.UserRolesAndPermissions;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.CFStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement.Prepared;
import org.apache.cassandra.cql3.statements.SelectStatement.RawStatement;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.MD5Digest;
import org.apache.cassandra.utils.Pair;

public class StatementUtils {
   private StatementUtils() {
   }

   public static CQLStatement prepareStatementBlocking(String cql, QueryState queryState, String errorMessage) {
      try {
         Prepared stmt;
         MD5Digest stmtId;
         for(stmt = null; stmt == null; stmt = QueryProcessor.instance.getPrepared(stmtId)) {
            stmtId = ((org.apache.cassandra.transport.messages.ResultMessage.Prepared)TPCUtils.blockingGet(QueryProcessor.prepare(cql, queryState))).statementId;
         }

         return stmt.statement;
      } catch (RequestValidationException var5) {
         throw new RuntimeException(errorMessage, var5);
      }
   }

   public static CQLStatement prepareStatementBlocking(String cql, QueryState queryState) {
      return prepareStatementBlocking(cql, queryState, "Error preparing CQL statement");
   }

   private static Single<Pair<Prepared, org.apache.cassandra.transport.messages.ResultMessage.Prepared>> prepareStatementOrNull(String cql, DseQueryOperationFactory operationFactory, QueryState queryState, Map<String, ByteBuffer> customPayload) {
      return buildPreparedMaybeCustomized(cql, operationFactory, queryState.getClientState(), customPayload).map((prepared) -> {
         Prepared preparedStatement = QueryProcessor.instance.getPrepared(prepared.statementId);
         return preparedStatement != null?Pair.create(preparedStatement, prepared):null;
      });
   }

   public static Single<Pair<Prepared, org.apache.cassandra.transport.messages.ResultMessage.Prepared>> prepareStatementMaybeCustom(String cql, DseQueryOperationFactory operationFactory, QueryState queryState, Map<String, ByteBuffer> customPayload) throws RequestValidationException {
      return prepareStatementOrNull(cql, operationFactory, queryState, customPayload).flatMap((p) -> {
         return p != null?Single.just(p):prepareStatementMaybeCustom(cql, operationFactory, queryState, customPayload);
      });
   }

   public static ParsedStatement parseMaybeInjectCustomValidation(String query, DseQueryOperationFactory operationFactory, ClientState clientState, Map<String, ByteBuffer> customPayload) {
      Tracing.trace("Parsing {}", query);
      ParsedStatement onlyParsed = QueryProcessor.parseStatement(query);
      if(onlyParsed instanceof RawStatement) {
         ((RawStatement)onlyParsed).prepareKeyspace(clientState);
         onlyParsed = operationFactory.maybeInjectCustomRestrictions(onlyParsed, customPayload);
      }

      return onlyParsed;
   }

   public static QueryState queryStateBlocking(ClientState clientState) {
      return clientState.getUser() == null?new QueryState(clientState, UserRolesAndPermissions.SYSTEM):new QueryState(clientState, (UserRolesAndPermissions)TPCUtils.blockingGet(DatabaseDescriptor.getAuthManager().getUserRolesAndPermissions(clientState.getUser())));
   }

   public static LatencyValues.EventType getInteractionType(CQLStatement stmt) {
      return !(stmt instanceof ModificationStatement) && !(stmt instanceof BatchStatement)?(stmt instanceof SelectStatement?LatencyValues.EventType.READ:null):LatencyValues.EventType.WRITE;
   }

   private static Single<org.apache.cassandra.transport.messages.ResultMessage.Prepared> buildPreparedMaybeCustomized(String queryString, DseQueryOperationFactory operationFactory, ClientState clientState, Map<String, ByteBuffer> customPayload) throws RequestValidationException {
      org.apache.cassandra.transport.messages.ResultMessage.Prepared existing = QueryProcessor.getStoredPreparedStatement(queryString, clientState.getRawKeyspace());
      if(existing != null) {
         return Single.just(existing);
      } else {
         ParsedStatement parsed = parseMaybeInjectCustomValidation(queryString, operationFactory, clientState, customPayload);
         if(parsed instanceof CFStatement) {
            ((CFStatement)parsed).prepareKeyspace(clientState);
         }

         Tracing.trace("Preparing statement");
         Prepared prepared = parsed.prepare();
         prepared.rawCQLStatement = queryString;
         QueryProcessor.validateBindingMarkers(prepared);
         return QueryProcessor.storePreparedStatement(queryString, clientState.getRawKeyspace(), prepared, true);
      }
   }
}
