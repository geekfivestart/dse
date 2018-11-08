package com.datastax.bdp.graph;

import com.datastax.bdp.cassandra.cql3.DseQueryOperationFactory;
import io.reactivex.Single;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

public interface DseGraphQueryOperationFactory extends DseQueryOperationFactory {
   Single<ResultMessage> process(String var1, QueryState var2, QueryOptions var3, Map<String, ByteBuffer> var4, long var5) throws RequestExecutionException, RequestValidationException;
}
