package com.datastax.bdp.cassandra.cql3;

import com.datastax.bdp.cassandra.auth.RowLevelAccessControlAuthorizer;
import io.reactivex.Single;
import io.reactivex.SingleObserver;
import io.reactivex.disposables.Disposable;
import io.reactivex.functions.Function;
import io.reactivex.internal.operators.single.SingleJust;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.restrictions.ExternalRestriction;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadVerbs.ReadVersion;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.filter.RowFilter.UserExpression;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.serializers.UTF8Serializer;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.flow.Flow;
import org.apache.cassandra.utils.flow.Threads;

public class RLACExpression extends UserExpression implements ExternalRestriction {
   private static final ByteBuffer FAKE_COLUMN_NAME = ByteBuffer.wrap("RLSExpression".getBytes());
   private volatile QueryState queryState;
   private final ClientState clientState;
   private final AuthenticatedUser user;

   public void addToRowFilter(RowFilter filter, TableMetadata cfm, QueryOptions options) {
      filter.addUserExpression(this);
   }

   public static RLACExpression newExpression(TableMetadata cfm, QueryState queryState) {
      ByteBuffer userBytes = UTF8Type.instance.decompose(queryState.getUser().getName());
      return new RLACExpression(cfm, userBytes, queryState);
   }

   private static ColumnMetadata makeDefinition(TableMetadata cfm) {
      return ColumnMetadata.regularColumn(cfm, FAKE_COLUMN_NAME, UTF8Type.instance);
   }

   private RLACExpression(TableMetadata cfm, ByteBuffer value, ClientState clientState) {
      super(makeDefinition(cfm), Operator.EQ, value);
      this.user = clientState.getUser();
      this.clientState = clientState;
      this.queryState = null;
   }

   private RLACExpression(TableMetadata cfm, ByteBuffer value, QueryState queryState) {
      super(makeDefinition(cfm), Operator.EQ, value);
      this.user = queryState.getUser();
      this.clientState = queryState.getClientState();
      this.queryState = queryState;
   }

   public Flow<Boolean> isSatisfiedBy(TableMetadata metadata, DecoratedKey partitionKey, Row row) {
      if(this.queryState != null) {
         return Flow.just(Boolean.valueOf(RowLevelAccessControlAuthorizer.authorizeLocalRead(this.queryState, metadata, partitionKey, row)));
      } else {
         Single<QueryState> qss = this.queryStateSingle();
         if(qss instanceof SingleJust) {
            qss.subscribe(new SingleObserver<QueryState>() {
               public void onSubscribe(Disposable d) {
               }

               public void onSuccess(QueryState queryState) {
                  RLACExpression.this.queryState = queryState;
               }

               public void onError(Throwable e) {
               }
            });
            return Flow.just(Boolean.valueOf(RowLevelAccessControlAuthorizer.authorizeLocalRead(this.queryState, metadata, partitionKey, row)));
         } else {
            return Threads.evaluateOnIO(() -> {
               QueryState qs = (QueryState)TPCUtils.blockingGet(this.queryStateSingle());
               this.queryState = qs;
               return Boolean.valueOf(RowLevelAccessControlAuthorizer.authorizeLocalRead(qs, metadata, partitionKey, row));
            }, TPCTaskType.AUTHENTICATION);
         }
      }
   }

   private Single<QueryState> queryStateSingle() {
      return DatabaseDescriptor.getAuthManager().getUserRolesAndPermissions(this.clientState.getUser()).map((u) -> {
         QueryState newQs = new QueryState(this.clientState, u);
         this.queryState = newQs;
         return newQs;
      });
   }

   public void serialize(DataOutputPlus out, ReadVersion version) throws IOException {
      ByteBufferUtil.writeWithShortLength(this.value, out);
   }

   public long serializedSize(ReadVersion version) {
      return (long)ByteBufferUtil.serializedSizeWithShortLength(this.value);
   }

   public String toString() {
      return String.format("RLS filter on user %s", new Object[]{this.user == null?"<none>":this.user.getName()});
   }

   public static class Deserializer extends org.apache.cassandra.db.filter.RowFilter.UserExpression.Deserializer {
      public Deserializer() {
      }

      public RLACExpression deserialize(DataInputPlus in, ReadVersion version, TableMetadata metadata) throws IOException {
         ByteBuffer userBytes = ByteBufferUtil.readWithShortLength(in);
         ClientState state = ClientState.forExternalCalls(new AuthenticatedUser(UTF8Serializer.instance.deserialize(userBytes)));
         return new RLACExpression(metadata, userBytes, state);
      }
   }
}
