package org.apache.cassandra.cql3;

import io.reactivex.Single;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.MD5Digest;

public interface QueryHandler {
   Single<ResultMessage> process(String var1, QueryState var2, QueryOptions var3, Map<String, ByteBuffer> var4, long var5);

   Single<ResultMessage.Prepared> prepare(String var1, QueryState var2, Map<String, ByteBuffer> var3);

   ParsedStatement.Prepared getPrepared(MD5Digest var1);

   Single<ResultMessage> processPrepared(ParsedStatement.Prepared var1, QueryState var2, QueryOptions var3, Map<String, ByteBuffer> var4, long var5);

   Single<ResultMessage> processBatch(BatchStatement var1, QueryState var2, BatchQueryOptions var3, Map<String, ByteBuffer> var4, long var5);
}
