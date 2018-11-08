package org.apache.cassandra.transport.messages;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.MD5Digest;

public abstract class ResultMessage extends Message.Response {
   public static final Message.Codec<ResultMessage> codec = new Message.Codec<ResultMessage>() {
      public ResultMessage decode(ByteBuf body, ProtocolVersion version) {
         ResultMessage.Kind kind = ResultMessage.Kind.fromId(body.readInt());
         return (ResultMessage)kind.subcodec.decode(body, version);
      }

      public void encode(ResultMessage msg, ByteBuf dest, ProtocolVersion version) {
         dest.writeInt(msg.kind.id);
         msg.kind.subcodec.encode(msg, dest, version);
      }

      public int encodedSize(ResultMessage msg, ProtocolVersion version) {
         return 4 + msg.kind.subcodec.encodedSize(msg, version);
      }
   };
   public final ResultMessage.Kind kind;

   protected ResultMessage(ResultMessage.Kind kind) {
      this(kind, true);
   }

   protected ResultMessage(ResultMessage.Kind kind, boolean sendToClient) {
      super(Message.Type.RESULT, sendToClient);
      this.kind = kind;
   }

   public static class SchemaChange extends ResultMessage {
      public final Event.SchemaChange change;
      public static final Message.Codec<ResultMessage> subcodec = new Message.Codec<ResultMessage>() {
         public ResultMessage decode(ByteBuf body, ProtocolVersion version) {
            return new ResultMessage.SchemaChange(Event.SchemaChange.deserializeEvent(body, version));
         }

         public void encode(ResultMessage msg, ByteBuf dest, ProtocolVersion version) {
            assert msg instanceof ResultMessage.SchemaChange;

            ResultMessage.SchemaChange scm = (ResultMessage.SchemaChange)msg;
            scm.change.serializeEvent(dest, version);
         }

         public int encodedSize(ResultMessage msg, ProtocolVersion version) {
            assert msg instanceof ResultMessage.SchemaChange;

            ResultMessage.SchemaChange scm = (ResultMessage.SchemaChange)msg;
            return scm.change.eventSerializedSize(version);
         }
      };

      public SchemaChange(Event.SchemaChange change) {
         super(ResultMessage.Kind.SCHEMA_CHANGE);
         this.change = change;
      }

      public String toString() {
         return "RESULT schema change " + this.change;
      }
   }

   public static class Prepared extends ResultMessage {
      public static final Message.Codec<ResultMessage> subcodec = new Message.Codec<ResultMessage>() {
         public ResultMessage decode(ByteBuf body, ProtocolVersion version) {
            MD5Digest id = MD5Digest.wrap(CBUtil.readBytes(body));
            MD5Digest resultMetadataId = null;
            if(version.isGreaterOrEqualTo(ProtocolVersion.V5, ProtocolVersion.DSE_V2)) {
               resultMetadataId = MD5Digest.wrap(CBUtil.readBytes(body));
            }

            ResultSet.PreparedMetadata metadata = (ResultSet.PreparedMetadata)ResultSet.PreparedMetadata.codec.decode(body, version);
            ResultSet.ResultMetadata resultMetadata = ResultSet.ResultMetadata.EMPTY;
            if(version.isGreaterThan(ProtocolVersion.V1)) {
               resultMetadata = (ResultSet.ResultMetadata)ResultSet.ResultMetadata.codec.decode(body, version);
            }

            return new ResultMessage.Prepared(id, resultMetadataId, -1, metadata, resultMetadata, null);
         }

         public void encode(ResultMessage msg, ByteBuf dest, ProtocolVersion version) {
            assert msg instanceof ResultMessage.Prepared;

            ResultMessage.Prepared prepared = (ResultMessage.Prepared)msg;

            assert prepared.statementId != null;

            CBUtil.writeBytes(prepared.statementId.bytes, dest);
            if(version.isGreaterOrEqualTo(ProtocolVersion.V5, ProtocolVersion.DSE_V2)) {
               CBUtil.writeBytes(prepared.resultMetadataId.bytes, dest);
            }

            ResultSet.PreparedMetadata.codec.encode(prepared.metadata, dest, version);
            if(version.isGreaterThan(ProtocolVersion.V1)) {
               ResultSet.ResultMetadata.codec.encode(prepared.resultMetadata, dest, version);
            }

         }

         public int encodedSize(ResultMessage msg, ProtocolVersion version) {
            assert msg instanceof ResultMessage.Prepared;

            ResultMessage.Prepared prepared = (ResultMessage.Prepared)msg;

            assert prepared.statementId != null;

            int size = 0;
            int sizex = size + CBUtil.sizeOfBytes(prepared.statementId.bytes);
            if(version.isGreaterOrEqualTo(ProtocolVersion.V5, ProtocolVersion.DSE_V2)) {
               sizex += CBUtil.sizeOfBytes(prepared.resultMetadataId.bytes);
            }

            sizex += ResultSet.PreparedMetadata.codec.encodedSize(prepared.metadata, version);
            if(version.isGreaterThan(ProtocolVersion.V1)) {
               sizex += ResultSet.ResultMetadata.codec.encodedSize(prepared.resultMetadata, version);
            }

            return sizex;
         }
      };
      public final MD5Digest statementId;
      public final MD5Digest resultMetadataId;
      public final ResultSet.PreparedMetadata metadata;
      public final ResultSet.ResultMetadata resultMetadata;
      private final int thriftStatementId;

      public Prepared(MD5Digest statementId, MD5Digest resultMetadataId, ParsedStatement.Prepared prepared) {
         this(statementId, resultMetadataId, -1, ResultSet.PreparedMetadata.fromPrepared(prepared), ResultSet.ResultMetadata.fromPrepared(prepared));
      }

      public Prepared(MD5Digest statementId, MD5Digest resultMetadataId, ResultSet.PreparedMetadata metadata, ResultSet.ResultMetadata resultMetadata) {
         this(statementId, resultMetadataId, -1, metadata, resultMetadata);
      }

      private Prepared(MD5Digest statementId, MD5Digest resultMetadataId, int thriftStatementId, ResultSet.PreparedMetadata metadata, ResultSet.ResultMetadata resultMetadata) {
         super(ResultMessage.Kind.PREPARED);
         this.statementId = statementId;
         this.resultMetadataId = resultMetadataId;
         this.thriftStatementId = thriftStatementId;
         this.metadata = metadata;
         this.resultMetadata = resultMetadata;
      }

      @VisibleForTesting
      public ResultMessage.Prepared withResultMetadata(ResultSet.ResultMetadata resultMetadata) {
         return new ResultMessage.Prepared(this.statementId, resultMetadata.getResultMetadataId(), this.metadata, resultMetadata);
      }

      public String toString() {
         return "RESULT PREPARED " + this.statementId + ' ' + this.metadata + " (resultMetadata=" + this.resultMetadata + ')';
      }
   }

   public static class Rows extends ResultMessage {
      public static final Message.Codec<ResultMessage> subcodec = new Message.Codec<ResultMessage>() {
         public ResultMessage decode(ByteBuf body, ProtocolVersion version) {
            return new ResultMessage.Rows(ResultSet.codec.decode(body, version));
         }

         public void encode(ResultMessage msg, ByteBuf dest, ProtocolVersion version) {
            assert msg instanceof ResultMessage.Rows;

            ResultMessage.Rows rowMsg = (ResultMessage.Rows)msg;
            ResultSet.codec.encode(rowMsg.result, dest, version);
         }

         public int encodedSize(ResultMessage msg, ProtocolVersion version) {
            assert msg instanceof ResultMessage.Rows;

            ResultMessage.Rows rowMsg = (ResultMessage.Rows)msg;
            return ResultSet.codec.encodedSize(rowMsg.result, version);
         }
      };
      public final ResultSet result;

      public Rows(ResultSet result) {
         this(result, true);
      }

      public Rows(ResultSet result, boolean sendToClient) {
         super(ResultMessage.Kind.ROWS, sendToClient);
         this.result = result;

         assert sendToClient || result.isEmpty();

      }

      public String toString() {
         return "ROWS " + this.result;
      }
   }

   public static class SetKeyspace extends ResultMessage {
      public final String keyspace;
      public static final Message.Codec<ResultMessage> subcodec = new Message.Codec<ResultMessage>() {
         public ResultMessage decode(ByteBuf body, ProtocolVersion version) {
            String keyspace = CBUtil.readString(body);
            return new ResultMessage.SetKeyspace(keyspace);
         }

         public void encode(ResultMessage msg, ByteBuf dest, ProtocolVersion version) {
            assert msg instanceof ResultMessage.SetKeyspace;

            CBUtil.writeString(((ResultMessage.SetKeyspace)msg).keyspace, dest);
         }

         public int encodedSize(ResultMessage msg, ProtocolVersion version) {
            assert msg instanceof ResultMessage.SetKeyspace;

            return CBUtil.sizeOfString(((ResultMessage.SetKeyspace)msg).keyspace);
         }
      };

      public SetKeyspace(String keyspace) {
         super(ResultMessage.Kind.SET_KEYSPACE);
         this.keyspace = keyspace;
      }

      public String toString() {
         return "RESULT set keyspace " + this.keyspace;
      }
   }

   public static class Void extends ResultMessage {
      public static final Message.Codec<ResultMessage> subcodec = new Message.Codec<ResultMessage>() {
         public ResultMessage decode(ByteBuf body, ProtocolVersion version) {
            return new ResultMessage.Void();
         }

         public void encode(ResultMessage msg, ByteBuf dest, ProtocolVersion version) {
            assert msg instanceof ResultMessage.Void;

         }

         public int encodedSize(ResultMessage msg, ProtocolVersion version) {
            return 0;
         }
      };

      public Void() {
         super(ResultMessage.Kind.VOID);
      }

      public String toString() {
         return "EMPTY RESULT";
      }
   }

   public static enum Kind {
      VOID(1, ResultMessage.Void.subcodec),
      ROWS(2, ResultMessage.Rows.subcodec),
      SET_KEYSPACE(3, ResultMessage.SetKeyspace.subcodec),
      PREPARED(4, ResultMessage.Prepared.subcodec),
      SCHEMA_CHANGE(5, ResultMessage.SchemaChange.subcodec);

      public final int id;
      public final Message.Codec<ResultMessage> subcodec;
      private static final ResultMessage.Kind[] ids;

      private Kind(int id, Message.Codec<ResultMessage> subcodec) {
         this.id = id;
         this.subcodec = subcodec;
      }

      public static ResultMessage.Kind fromId(int id) {
         ResultMessage.Kind k = ids[id];
         if(k == null) {
            throw new ProtocolException(String.format("Unknown kind id %d in RESULT message", new Object[]{Integer.valueOf(id)}));
         } else {
            return k;
         }
      }

      static {
         int maxId = -1;
         ResultMessage.Kind[] var1 = values();
         int var2 = var1.length;

         int var3;
         ResultMessage.Kind k;
         for(var3 = 0; var3 < var2; ++var3) {
            k = var1[var3];
            maxId = Math.max(maxId, k.id);
         }

         ids = new ResultMessage.Kind[maxId + 1];
         var1 = values();
         var2 = var1.length;

         for(var3 = 0; var3 < var2; ++var3) {
            k = var1[var3];
            if(ids[k.id] != null) {
               throw new IllegalStateException("Duplicate kind id");
            }

            ids[k.id] = k;
         }

      }
   }
}
