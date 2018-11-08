package org.apache.cassandra.transport.messages;

import io.netty.buffer.ByteBuf;
import io.reactivex.Single;
import io.reactivex.functions.Function;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BinaryOperator;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.continuous.paging.ContinuousPagingService;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.JVMStabilityInspector;

public class ReviseRequestMessage extends Message.Request {
   private static final ColumnIdentifier RESULT_COLUMN = new ColumnIdentifier("[status]", false);
   private static final ColumnSpecification RESULT_COLUMN_SPEC;
   private static final ResultSet.ResultMetadata RESULT_METADATA;
   public static final Message.Codec<ReviseRequestMessage> codec;
   private final ReviseRequestMessage.RevisionType revisionType;
   private final int id;
   private final int nextPages;

   private ReviseRequestMessage(ReviseRequestMessage.RevisionType revisionType, int id) {
      this(revisionType, id, 0);
   }

   private ReviseRequestMessage(ReviseRequestMessage.RevisionType revisionType, int id, int nextPages) {
      super(Message.Type.REVISE_REQUEST);
      this.revisionType = revisionType;
      this.id = id;
      this.nextPages = nextPages;
   }

   public Single<Message.Response> execute(Single<QueryState> queryState, long queryStartNanoTime) {
      Single ret;
      if(this.revisionType == ReviseRequestMessage.RevisionType.CONTINUOUS_PAGING_CANCEL) {
         ret = ContinuousPagingService.cancel(queryState, this.id);
      } else if(this.revisionType == ReviseRequestMessage.RevisionType.CONTINUOUS_PAGING_BACKPRESSURE) {
         ret = ContinuousPagingService.updateBackpressure(queryState, this.id, this.nextPages);
      } else {
         ret = Single.error(new InvalidRequestException(String.format("Unknown update type: %s", new Object[]{this.revisionType})));
      }

      return ret.map((res) -> {
         List<List<ByteBuffer>> rows = Collections.singletonList(Collections.singletonList(BooleanType.instance.decompose(res)));
         return new ResultMessage.Rows(new ResultSet(RESULT_METADATA, rows));
      }).onErrorReturn((err) -> {
         JVMStabilityInspector.inspectThrowable(err);
         return ErrorMessage.fromException(err);
      });
   }

   public String toString() {
      return String.format("REVISE REQUEST %d with revision %s (nextPages %s)", new Object[]{Integer.valueOf(this.id), this.revisionType, Integer.valueOf(this.nextPages)});
   }

   static {
      RESULT_COLUMN_SPEC = new ColumnSpecification("", "", RESULT_COLUMN, BooleanType.instance);
      RESULT_METADATA = new ResultSet.ResultMetadata(Collections.singletonList(RESULT_COLUMN_SPEC));
      codec = new Message.Codec<ReviseRequestMessage>() {
         public ReviseRequestMessage decode(ByteBuf body, ProtocolVersion version) {
            ReviseRequestMessage.RevisionType revisionType = ReviseRequestMessage.RevisionType.decode(body.readInt());
            if(revisionType == ReviseRequestMessage.RevisionType.CONTINUOUS_PAGING_CANCEL) {
               return new ReviseRequestMessage(revisionType, body.readInt(), null);
            } else if(revisionType == ReviseRequestMessage.RevisionType.CONTINUOUS_PAGING_BACKPRESSURE) {
               return new ReviseRequestMessage(revisionType, body.readInt(), body.readInt(), null);
            } else {
               throw new InvalidRequestException(String.format("Unknown update type: %s", new Object[]{revisionType}));
            }
         }

         public void encode(ReviseRequestMessage msg, ByteBuf dest, ProtocolVersion version) {
            dest.writeInt(msg.revisionType.id);
            dest.writeInt(msg.id);
            if(msg.revisionType == ReviseRequestMessage.RevisionType.CONTINUOUS_PAGING_BACKPRESSURE) {
               dest.writeInt(msg.nextPages);
            }

         }

         public int encodedSize(ReviseRequestMessage msg, ProtocolVersion version) {
            int ret = 8;
            if(msg.revisionType == ReviseRequestMessage.RevisionType.CONTINUOUS_PAGING_BACKPRESSURE) {
               ret += 4;
            }

            return ret;
         }
      };
   }

   public static enum RevisionType {
      CONTINUOUS_PAGING_CANCEL(1),
      CONTINUOUS_PAGING_BACKPRESSURE(2);

      private final int id;
      private static final ReviseRequestMessage.RevisionType[] REVISION_TYPES;

      private RevisionType(int id) {
         this.id = id;
      }

      static ReviseRequestMessage.RevisionType decode(int id) {
         if(id < REVISION_TYPES.length && REVISION_TYPES[id] != null) {
            return REVISION_TYPES[id];
         } else {
            throw new ProtocolException(String.format("Unknown operation type %d", new Object[]{Integer.valueOf(id)}));
         }
      }

      static {
         int maxId = ((Integer)Arrays.stream(values()).map((ot) -> {
            return Integer.valueOf(ot.id);
         }).reduce(Integer.valueOf(0), Math::max)).intValue();
         REVISION_TYPES = new ReviseRequestMessage.RevisionType[maxId + 1];
         ReviseRequestMessage.RevisionType[] var1 = values();
         int var2 = var1.length;

         for(int var3 = 0; var3 < var2; ++var3) {
            ReviseRequestMessage.RevisionType ut = var1[var3];
            if(REVISION_TYPES[ut.id] != null) {
               throw new IllegalStateException("Duplicate operation type");
            }

            REVISION_TYPES[ut.id] = ut;
         }

      }
   }
}
