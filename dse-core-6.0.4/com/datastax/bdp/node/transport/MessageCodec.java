package com.datastax.bdp.node.transport;

import com.datastax.bdp.node.transport.internal.FailedProcessorSerializer;
import com.datastax.bdp.node.transport.internal.HandshakeSerializer;
import com.datastax.bdp.node.transport.internal.SystemMessageTypes;
import com.datastax.bdp.node.transport.internal.UnsupportedMessageException;
import com.datastax.bdp.node.transport.internal.UnsupportedMessageSerializer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.ByteToMessageCodec;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MessageCodec {
   public static final short MAGIC_NUMBER = 2605;
   public static final byte STATIC_VERSION = -1;
   public static final int MAGIC_SIZE = 2;
   public static final int LENGTH_SIZE = 4;
   private final ConcurrentMap<MessageCodec.VersionedMessageType, MessageBodySerializer> serializers = new ConcurrentHashMap();
   private final byte currentVersion;
   private final int maxFrameLengthInBytes;

   public MessageCodec(byte currentVersion, int maxFrameLengthInMB) {
      this.currentVersion = currentVersion;
      this.maxFrameLengthInBytes = maxFrameLengthInMB * 1024 * 1024;
   }

   public void addSerializer(MessageType type, MessageBodySerializer serializer, byte... versions) {
      byte[] var4 = versions;
      int var5 = versions.length;

      for(int var6 = 0; var6 < var5; ++var6) {
         byte version = var4[var6];
         if(version <= 0) {
            throw new IllegalArgumentException("Cannot accept non-positive version values!");
         }

         if(this.serializers.putIfAbsent(new MessageCodec.VersionedMessageType(type, version), serializer) != null) {
            throw new IllegalArgumentException("Type already exists: " + type);
         }
      }

   }

   public byte getCurrentVersion() {
      return this.currentVersion;
   }

   ChannelHandler[] newPipeline() {
      return new ChannelHandler[]{new MessageCodec.FrameEncoder(), new MessageCodec.FrameDecoder(this.maxFrameLengthInBytes), new MessageCodec.MessageSerDe(this.serializers)};
   }

   public static class VersionedMessageBodySerializer {
      public final byte version;
      public final MessageBodySerializer serializer;

      public VersionedMessageBodySerializer(byte version, MessageBodySerializer serializer) {
         this.version = version;
         this.serializer = serializer;
      }
   }

   @VisibleForTesting
   public static class VersionedMessageType {
      public final MessageType type;
      public final byte version;

      public VersionedMessageType(MessageType type, byte version) {
         this.type = type;
         this.version = version;
      }

      public boolean equals(Object obj) {
         if(!(obj instanceof MessageCodec.VersionedMessageType)) {
            return false;
         } else {
            MessageCodec.VersionedMessageType other = (MessageCodec.VersionedMessageType)obj;
            return other.type.equals(this.type) && other.version == this.version;
         }
      }

      public int hashCode() {
         return Objects.hashCode(new Object[]{this.type, Byte.valueOf(this.version)});
      }
   }

   @VisibleForTesting
   public static class MessageSerDe extends ByteToMessageCodec<Message> {
      private final ConcurrentMap<MessageCodec.VersionedMessageType, MessageBodySerializer> serializers;

      @VisibleForTesting
      public MessageSerDe(Map<MessageCodec.VersionedMessageType, MessageBodySerializer> serializers) {
         this.serializers = new ConcurrentHashMap(serializers);
         this.serializers.put(new MessageCodec.VersionedMessageType(SystemMessageTypes.HANDSHAKE, -1), new HandshakeSerializer());
         this.serializers.put(new MessageCodec.VersionedMessageType(SystemMessageTypes.UNSUPPORTED_MESSAGE, -1), new UnsupportedMessageSerializer());
         this.serializers.put(new MessageCodec.VersionedMessageType(SystemMessageTypes.FAILED_PROCESSOR, -1), new FailedProcessorSerializer());
      }

      @VisibleForTesting
      public void encode(ChannelHandlerContext ctx, Message message, ByteBuf buffer) throws Exception {
         this.prepareEncoding(buffer);
         MessageType type = message.getType();
         byte version = message.getVersion();
         MessageCodec.VersionedMessageBodySerializer serializer = this.findVersionedSerializer(type, version);
         if(serializer.serializer != null) {
            ByteBufOutputStream outputStream = new ByteBufOutputStream(buffer);
            MessageCodec.MessageSerDe.Header h = new MessageCodec.MessageSerDe.Header();
            h.flags = message.getFlags();
            h.id = message.getId();
            h.type = type;
            h.version = serializer.version;
            h.write(outputStream);
            serializer.serializer.serialize(message.getBody(), outputStream);
         } else {
            throw new IllegalStateException(String.format("Undefined serializer for message type: %s", new Object[]{type}));
         }
      }

      @VisibleForTesting
      public void decode(ChannelHandlerContext ctx, ByteBuf input, List<Object> collector) throws Exception {
         this.prepareDecoding(input);
         ByteBufInputStream inputStream = new ByteBufInputStream(input);
         MessageCodec.MessageSerDe.Header h = new MessageCodec.MessageSerDe.Header();
         h.read(inputStream);
         MessageBodySerializer serializer = (MessageBodySerializer)this.serializers.get(new MessageCodec.VersionedMessageType(h.type, h.version));
         Message message;
         if(serializer != null) {
            Object body = serializer.deserialize(inputStream);
            message = new Message(h.flags, h.id, h.type, body);
            message.trySetVersion(h.version);
            collector.add(message);
         } else {
            input.skipBytes(input.readableBytes());
            UnsupportedMessageException ex = new UnsupportedMessageException(Long.valueOf(h.id), h.type, Byte.valueOf(h.version));
            message = new Message(EnumSet.of(Message.Flag.UNSUPPORTED_MESSAGE), ex.messageId.longValue(), SystemMessageTypes.UNSUPPORTED_MESSAGE, ex);
            message.trySetVersion(-1);
            collector.add(message);
         }

      }

      private void prepareEncoding(ByteBuf buffer) {
         buffer.writeZero(6);
      }

      private void prepareDecoding(ByteBuf buffer) {
         buffer.skipBytes(6);
      }

      private MessageCodec.VersionedMessageBodySerializer findVersionedSerializer(MessageType type, byte version) {
         MessageCodec.VersionedMessageType versionedMessageType = new MessageCodec.VersionedMessageType(type, version);

         MessageBodySerializer serializer;
         for(serializer = null; (serializer = (MessageBodySerializer)this.serializers.get(versionedMessageType)) == null && version > 1; versionedMessageType = new MessageCodec.VersionedMessageType(type, version)) {
            --version;
         }

         return new MessageCodec.VersionedMessageBodySerializer(version, serializer);
      }

      private static class Header {
         public EnumSet<Message.Flag> flags;
         public long id;
         public MessageType type;
         public byte version;

         private Header() {
         }

         private void write(ByteBufOutputStream outputStream) throws IOException {
            outputStream.writeInt(Message.Flag.serialize(this.flags));
            outputStream.writeLong(this.id);
            outputStream.writeShort(this.type.getSerialized());
            outputStream.write(this.version);
         }

         public void read(ByteBufInputStream inputStream) throws IOException {
            this.flags = Message.Flag.deserialize(inputStream.readInt());
            this.id = inputStream.readLong();
            this.type = new MessageType(inputStream.readShort());
            this.version = inputStream.readByte();
         }
      }
   }

   public static class FrameDecoder extends LengthFieldBasedFrameDecoder {
      public FrameDecoder(int maxFrameLengthInBytes) {
         super(maxFrameLengthInBytes, 2, 4, 0, 0);
      }

      protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
         if(in.readableBytes() >= 2) {
            short magic = in.getShort(in.readerIndex());
            if(magic != 2605) {
               in.skipBytes(in.readableBytes());
               throw new IllegalStateException("Unexpected protocol magic number!");
            } else {
               return super.decode(ctx, in);
            }
         } else {
            return null;
         }
      }
   }

   public static class FrameEncoder extends ChannelOutboundHandlerAdapter {
      public FrameEncoder() {
      }

      public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
         ByteBuf buffer = (ByteBuf)msg;
         buffer.setShort(buffer.readerIndex(), 2605);
         buffer.setInt(buffer.readerIndex() + 2, buffer.readableBytes() - 2 - 4);
         ctx.writeAndFlush(msg, promise);
      }
   }
}
