package org.apache.cassandra.transport;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.util.Attribute;
import java.io.IOException;
import java.util.List;
import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCEventLoop;
import org.apache.cassandra.concurrent.TPCMetrics;
import org.apache.cassandra.concurrent.TPCRunnable;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.transport.messages.ErrorMessage;
import org.apache.cassandra.utils.Flags;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.TimeSource;
import org.jctools.queues.SpscArrayQueue;

public class Frame {
   public static final byte PROTOCOL_VERSION_MASK = 127;
   public final Frame.Header header;
   public final ByteBuf body;

   private Frame(Frame.Header header, ByteBuf body) {
      this.header = header;
      this.body = body;
   }

   public void retain() {
      this.body.retain();
   }

   public boolean release() {
      return this.body.release();
   }

   public static Frame create(TimeSource timeSource, Message.Type type, int streamId, ProtocolVersion version, int flags, ByteBuf body) {
      Frame.Header header = new Frame.Header(version, flags, streamId, type, timeSource.nanoTime());
      return new Frame(header, body);
   }

   public Frame with(ByteBuf newBody) {
      return new Frame(this.header, newBody);
   }

   private static long discard(ByteBuf buffer, long remainingToDiscard) {
      int availableToDiscard = (int)Math.min(remainingToDiscard, (long)buffer.readableBytes());
      buffer.skipBytes(availableToDiscard);
      return remainingToDiscard - (long)availableToDiscard;
   }

   public static class AsyncProcessor implements Runnable {
      private final TPCEventLoop eventLoop;
      private final TPCMetrics metrics;
      private final SpscArrayQueue<Pair<ChannelHandlerContext, Frame>> frames;

      public AsyncProcessor(TPCEventLoop eventLoop) {
         this.eventLoop = eventLoop;
         this.metrics = TPC.metrics(eventLoop.coreId());
         this.frames = new SpscArrayQueue(65536);
      }

      public void maybeDoAsync(ChannelHandlerContext context, Frame frame, List<Object> out) {
         if(!frame.header.type.supportsBackpressure || !this.eventLoop.shouldBackpressure(false) && this.frames.isEmpty()) {
            out.add(frame);
         } else {
            if(this.frames.isEmpty()) {
               this.schedule();
            }

            if(!this.frames.offer(Pair.create(context, frame))) {
               throw new OverloadedException("Too many pending client requests, dropping the current request.");
            }

            this.metrics.backpressureDelayedTaskCount(1);
         }

      }

      public void run() {
         int processed;
         for(processed = 0; !this.frames.isEmpty() && !this.eventLoop.shouldBackpressure(false); ++processed) {
            Pair<ChannelHandlerContext, Frame> contextAndFrame = (Pair)this.frames.poll();
            if(((ChannelHandlerContext)contextAndFrame.left).channel().isActive()) {
               ((ChannelHandlerContext)contextAndFrame.left).fireChannelRead(contextAndFrame.right);
            } else {
               ((Frame)contextAndFrame.right).release();
            }
         }

         this.metrics.backpressureDelayedTaskCount(-processed);
         if(!this.frames.isEmpty()) {
            this.schedule();
         }

      }

      private void schedule() {
         this.eventLoop.execute(new TPCRunnable(this, ExecutorLocals.create(), TPCTaskType.FRAME_DECODE, this.eventLoop.coreId()));
      }
   }

   @Sharable
   public static class Compressor extends MessageToMessageEncoder<Frame> {
      public Compressor() {
      }

      public void encode(ChannelHandlerContext ctx, Frame frame, List<Object> results) throws IOException {
         Connection connection = (Connection)ctx.channel().attr(Connection.attributeKey).get();
         if(frame.header.type != Message.Type.STARTUP && connection != null) {
            FrameCompressor compressor = connection.getCompressor();
            if(compressor == null) {
               results.add(frame);
            } else {
               frame.header.flags = Flags.add(frame.header.flags, 1);
               results.add(compressor.compress(frame));
            }
         } else {
            results.add(frame);
         }
      }
   }

   @Sharable
   public static class Decompressor extends MessageToMessageDecoder<Frame> {
      public Decompressor() {
      }

      public void decode(ChannelHandlerContext ctx, Frame frame, List<Object> results) throws IOException {
         Connection connection = (Connection)ctx.channel().attr(Connection.attributeKey).get();
         if(Flags.contains(frame.header.flags, 1) && connection != null) {
            FrameCompressor compressor = connection.getCompressor();
            if(compressor == null) {
               results.add(frame);
            } else {
               results.add(compressor.decompress(frame));
            }
         } else {
            results.add(frame);
         }
      }
   }

   @Sharable
   public static class Encoder extends MessageToMessageEncoder<Frame> {
      public Encoder() {
      }

      public void encode(ChannelHandlerContext ctx, Frame frame, List<Object> results) throws IOException {
         ByteBuf header = CBUtil.allocator.buffer(9);
         Message.Type type = frame.header.type;
         header.writeByte(type.direction.addToVersion(frame.header.version.asInt()));
         header.writeByte(frame.header.flags);
         if(frame.header.version.isGreaterOrEqualTo(ProtocolVersion.V3)) {
            header.writeShort(frame.header.streamId);
         } else {
            header.writeByte(frame.header.streamId);
         }

         header.writeByte(type.opcode);
         header.writeInt(frame.body.readableBytes());
         results.add(header);
         results.add(frame.body);
      }
   }

   public static class Decoder extends ByteToMessageDecoder {
      private static final int MAX_FRAME_LENGTH = DatabaseDescriptor.getNativeTransportMaxFrameSize();
      private boolean discardingTooLongFrame;
      private long tooLongFrameLength;
      private long bytesToDiscard;
      private int tooLongStreamId;
      private final TimeSource timeSource;
      private final Connection.Factory factory;
      private final Frame.AsyncProcessor processor;

      public Decoder(TimeSource timeSource, Connection.Factory factory) {
         this(timeSource, factory, (Frame.AsyncProcessor)null);
      }

      public Decoder(TimeSource timeSource, Connection.Factory factory, Frame.AsyncProcessor processor) {
         this.timeSource = timeSource;
         this.factory = factory;
         this.processor = processor;
      }

      @VisibleForTesting
      Frame decodeFrame(ByteBuf buffer) throws Exception {
         if(this.discardingTooLongFrame) {
            this.bytesToDiscard = Frame.discard(buffer, this.bytesToDiscard);
            if(this.bytesToDiscard <= 0L) {
               this.fail();
            }

            return null;
         } else {
            int readableBytes = buffer.readableBytes();
            if(readableBytes == 0) {
               return null;
            } else {
               int idx = buffer.readerIndex();
               int firstByte = buffer.getByte(idx++);
               Message.Direction direction = Message.Direction.extractFromVersion(firstByte);
               int versionNum = firstByte & 127;
               ProtocolVersion version = ProtocolVersion.decode(versionNum);
               if(readableBytes < 9) {
                  return null;
               } else {
                  int flags = buffer.getByte(idx++);
                  if(version.isBeta() && !Flags.contains(flags, 16)) {
                     throw new ProtocolException(String.format("Beta version of the protocol used (%s), but USE_BETA flag is unset", new Object[]{version}), version);
                  } else {
                     int streamId = buffer.getShort(idx);
                     idx += 2;

                     Message.Type type;
                     try {
                        type = Message.Type.fromOpcode(buffer.getUnsignedByte(idx++), direction);
                     } catch (ProtocolException var16) {
                        throw ErrorMessage.wrap(var16, streamId);
                     }

                     long bodyLength = buffer.getUnsignedInt(idx);
                     idx += 4;
                     long frameLength = bodyLength + 9L;
                     if(frameLength > (long)MAX_FRAME_LENGTH) {
                        this.discardingTooLongFrame = true;
                        this.tooLongStreamId = streamId;
                        this.tooLongFrameLength = frameLength;
                        this.bytesToDiscard = Frame.discard(buffer, frameLength);
                        if(this.bytesToDiscard <= 0L) {
                           this.fail();
                        }

                        return null;
                     } else if((long)buffer.readableBytes() < frameLength) {
                        return null;
                     } else {
                        ByteBuf body = buffer.slice(idx, (int)bodyLength);
                        idx = (int)((long)idx + bodyLength);
                        buffer.readerIndex(idx);
                        return new Frame(new Frame.Header(version, flags, streamId, type, this.timeSource.nanoTime()), body.retain());
                     }
                  }
               }
            }
         }
      }

      protected void decode(ChannelHandlerContext ctx, ByteBuf buffer, List<Object> results) throws Exception {
         Frame frame = null;

         try {
            frame = this.decodeFrame(buffer);
            if(frame != null) {
               Attribute<Connection> attrConn = ctx.channel().attr(Connection.attributeKey);
               Connection connection = (Connection)attrConn.get();
               if(connection == null) {
                  connection = this.factory.newConnection(ctx.channel(), frame.header.version);
                  attrConn.set(connection);
               } else if(connection.getVersion() != frame.header.version) {
                  throw ErrorMessage.wrap(new ProtocolException(String.format("Invalid message version. Got %s but previous messages on this connection had version %s", new Object[]{frame.header.version, connection.getVersion()})), frame.header.streamId);
               }

               if(this.processor != null) {
                  this.processor.maybeDoAsync(ctx, frame, results);
               } else {
                  results.add(frame);
               }

            }
         } catch (Throwable var7) {
            if(frame != null) {
               frame.release();
            }

            throw var7;
         }
      }

      private void fail() {
         long tooLongFrameLength = this.tooLongFrameLength;
         this.tooLongFrameLength = 0L;
         this.discardingTooLongFrame = false;
         String msg = String.format("Request is too big: length %d exceeds maximum allowed length %d.", new Object[]{Long.valueOf(tooLongFrameLength), Integer.valueOf(MAX_FRAME_LENGTH)});
         throw ErrorMessage.wrap(new InvalidRequestException(msg), this.tooLongStreamId);
      }
   }

   public static class Header {
      public static final int LENGTH = 9;
      public static final int BODY_LENGTH_SIZE = 4;
      public final ProtocolVersion version;
      public int flags;
      public final int streamId;
      public final Message.Type type;
      public final long queryStartNanoTime;

      private Header(ProtocolVersion version, int flags, int streamId, Message.Type type, long queryStartNanoTime) {
         this.version = version;
         this.flags = flags;
         this.streamId = streamId;
         this.type = type;
         this.queryStartNanoTime = queryStartNanoTime;
      }

      public interface HeaderFlag {
         int NONE = 0;
         int COMPRESSED = 1;
         int TRACING = 2;
         int CUSTOM_PAYLOAD = 4;
         int WARNING = 8;
         int USE_BETA = 16;
      }
   }
}
