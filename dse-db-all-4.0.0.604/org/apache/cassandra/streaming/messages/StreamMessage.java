package org.apache.cassandra.streaming.messages;

import java.io.IOException;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.function.Function;
import org.apache.cassandra.dht.BoundsVersion;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.net.ProtocolVersion;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.utils.versioning.Version;
import org.apache.cassandra.utils.versioning.Versioned;

public abstract class StreamMessage {
   public static final StreamMessage.StreamVersion CURRENT_VERSION = (StreamMessage.StreamVersion)Version.last(StreamMessage.StreamVersion.class);
   private transient volatile boolean sent = false;
   public final StreamMessage.Type type;

   public static void serialize(StreamMessage message, DataOutputStreamPlus out, StreamMessage.StreamVersion version, StreamSession session) throws IOException {
      ByteBuffer buff = ByteBuffer.allocate(1);
      buff.put(message.type.type);
      buff.flip();
      out.write(buff);
      message.type.outSerializer.serialize(message, out, version, session);
   }

   public static StreamMessage deserialize(ReadableByteChannel in, StreamMessage.StreamVersion version, StreamSession session) throws IOException {
      ByteBuffer buff = ByteBuffer.allocate(1);
      int readBytes = in.read(buff);
      if(readBytes > 0) {
         buff.flip();
         StreamMessage.Type type = StreamMessage.Type.get(buff.get());
         return type.inSerializer.deserialize(in, version, session);
      } else if(readBytes == 0) {
         return null;
      } else {
         throw new SocketException("End-of-stream reached");
      }
   }

   public void sent() {
      this.sent = true;
   }

   public boolean wasSent() {
      return this.sent;
   }

   protected StreamMessage(StreamMessage.Type type) {
      this.type = type;
   }

   public int getPriority() {
      return this.type.priority;
   }

   public static enum Type {
      PREPARE(1, 5, PrepareMessage.serializer),
      FILE(2, 0, IncomingFileMessage.serializer, OutgoingFileMessage.serializer),
      RECEIVED(3, 4, ReceivedMessage.serializer),
      RETRY(4, 4, RetryMessage.serializer),
      COMPLETE(5, 1, CompleteMessage.serializer),
      SESSION_FAILED(6, 5, SessionFailedMessage.serializer),
      KEEP_ALIVE(7, 5, KeepAliveMessage.serializer);

      private final byte type;
      public final int priority;
      public final StreamMessage.Serializer<StreamMessage> inSerializer;
      public final StreamMessage.Serializer<StreamMessage> outSerializer;

      public static StreamMessage.Type get(byte type) {
         StreamMessage.Type[] var1 = values();
         int var2 = var1.length;

         for(int var3 = 0; var3 < var2; ++var3) {
            StreamMessage.Type t = var1[var3];
            if(t.type == type) {
               return t;
            }
         }

         throw new IllegalArgumentException("Unknown type " + type);
      }

      private Type(int type, int priority, StreamMessage.Serializer serializer) {
         this(type, priority, serializer, serializer);
      }

      private Type(int type, int priority, StreamMessage.Serializer inSerializer, StreamMessage.Serializer outSerializer) {
         this.type = (byte)type;
         this.priority = priority;
         this.inSerializer = inSerializer;
         this.outSerializer = outSerializer;
      }
   }

   public interface Serializer<V extends StreamMessage> {
      V deserialize(ReadableByteChannel var1, StreamMessage.StreamVersion var2, StreamSession var3) throws IOException;

      void serialize(V var1, DataOutputStreamPlus var2, StreamMessage.StreamVersion var3, StreamSession var4) throws IOException;
   }

   public static enum StreamVersion implements Version<StreamMessage.StreamVersion> {
      OSS_30(ProtocolVersion.oss(4), BoundsVersion.OSS_30),
      OSS_40(ProtocolVersion.oss(5), BoundsVersion.OSS_30),
      DSE_60(ProtocolVersion.dse(1), BoundsVersion.OSS_30);

      public final ProtocolVersion protocolVersion;
      public final BoundsVersion boundsVersion;

      private StreamVersion(ProtocolVersion protocolVersion, BoundsVersion boundsVersion) {
         this.protocolVersion = protocolVersion;
         this.boundsVersion = boundsVersion;
      }

      public static StreamMessage.StreamVersion of(ProtocolVersion protocolVersion) {
         StreamMessage.StreamVersion[] var1 = values();
         int var2 = var1.length;

         for(int var3 = 0; var3 < var2; ++var3) {
            StreamMessage.StreamVersion version = var1[var3];
            if(version.protocolVersion.equals(protocolVersion)) {
               return version;
            }
         }

         return null;
      }

      public static <T> Versioned<StreamMessage.StreamVersion, T> versioned(Function<StreamMessage.StreamVersion, ? extends T> function) {
         return new Versioned(StreamMessage.StreamVersion.class, function);
      }
   }
}
