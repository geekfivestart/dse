package com.datastax.bdp.cassandra.cql3;

import com.datastax.bdp.cassandra.audit.DseAuditableEventType;
import com.datastax.bdp.db.audit.AuditableEventType;
import com.datastax.bdp.util.rpc.RpcMethod;
import com.datastax.bdp.util.rpc.RpcRegistry;
import com.google.common.base.MoreObjects;
import io.reactivex.Single;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.cassandra.concurrent.StagedScheduler;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.Term.Raw;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement.Prepared;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.github.jamm.Unmetered;

public class RpcCallStatement extends ParsedStatement implements CQLStatement {
   private final String object;
   private final String method;
   @Unmetered
   private final Optional<RpcMethod> rpcMethod;
   private final List<Raw> values;
   public static final Duration RPC_ROUTED_CALL_TIMEOUT = Duration.ofHours(12L);

   public RpcCallStatement(String object, String method, List<Raw> values) {
      this.object = object;
      this.method = method;
      this.rpcMethod = RpcRegistry.lookupMethod(object, method);
      this.values = values;
   }

   public AuditableEventType getAuditEventType() {
      return DseAuditableEventType.RPC_CALL_STATEMENT;
   }

   public int getBoundTerms() {
      return this.getBoundVariables().size();
   }

   public Prepared prepare() throws InvalidRequestException {
      if(this.rpcMethod.isPresent()) {
         if(((RpcMethod)this.rpcMethod.get()).getArgumentCount() != this.values.size()) {
            throw new InvalidRequestException(String.format("Incorrect number of arguments received for method %s.%s", new Object[]{this.object, this.method}));
         }

         int argumentIndex = 0;
         int bindIndex = 0;
         Iterator var3 = this.values.iterator();

         while(var3.hasNext()) {
            Raw value = (Raw)var3.next();
            ColumnSpecification spec = ((RpcMethod)this.rpcMethod.get()).getArgumentSpecification(argumentIndex++);
            if(value.prepare(spec.ksName, spec).containsBindMarker()) {
               this.getBoundVariables().add(bindIndex++, spec);
            }
         }
      }

      return new Prepared(this, this.getBoundVariables(), (short[])null);
   }

   public void checkAccess(QueryState state) throws UnauthorizedException {
      if(this.rpcMethod.isPresent()) {
         ((RpcMethod)this.rpcMethod.get()).checkAccess(state);
      }

   }

   public void validate(QueryState state) throws InvalidRequestException {
      if(!this.rpcMethod.isPresent()) {
         throw new InvalidRequestException(String.format("The method %s.%s does not exist. Make sure that the required component for that method is active/enabled", new Object[]{this.object, this.method}));
      }
   }

   public Single<? extends ResultMessage> execute(QueryState state, QueryOptions options, long queryStartNanoTime) throws RequestExecutionException {
      List<ByteBuffer> args = this.buildParameters(options);
      ResultMessage resultMessage = ((RpcMethod)this.rpcMethod.get()).execute(state.getClientState(), args);
      return Single.just(resultMessage);
   }

   public Single<? extends ResultMessage> executeInternal(QueryState state, QueryOptions options) throws RequestExecutionException {
      return this.execute(state, options, System.nanoTime());
   }

   @Nullable
   public StagedScheduler getScheduler() {
      return TPC.ioScheduler();
   }

   private List<ByteBuffer> buildParameters(QueryOptions options) {
      List<ByteBuffer> parameters = new ArrayList();
      int argumentIndex = 0;
      Iterator var4 = this.values.iterator();

      while(var4.hasNext()) {
         Raw value = (Raw)var4.next();
         ColumnSpecification spec = ((RpcMethod)this.rpcMethod.get()).getArgumentSpecification(argumentIndex++);
         parameters.add(value.prepare(spec.ksName, spec).bindAndGet(options));
      }

      return parameters;
   }

   public String toString() {
      return MoreObjects.toStringHelper(this).add("object", this.object).add("method", this.method).add("values", this.values).toString();
   }
}
