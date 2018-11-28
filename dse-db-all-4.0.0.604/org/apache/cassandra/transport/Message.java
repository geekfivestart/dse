package org.apache.cassandra.transport;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoop;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.util.internal.shaded.org.jctools.queues.MpscArrayQueue;
import io.reactivex.Single;
import io.reactivex.functions.Consumer;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.messages.AuthChallenge;
import org.apache.cassandra.transport.messages.AuthResponse;
import org.apache.cassandra.transport.messages.AuthSuccess;
import org.apache.cassandra.transport.messages.AuthenticateMessage;
import org.apache.cassandra.transport.messages.BatchMessage;
import org.apache.cassandra.transport.messages.ErrorMessage;
import org.apache.cassandra.transport.messages.EventMessage;
import org.apache.cassandra.transport.messages.ExecuteMessage;
import org.apache.cassandra.transport.messages.OptionsMessage;
import org.apache.cassandra.transport.messages.PrepareMessage;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.apache.cassandra.transport.messages.ReadyMessage;
import org.apache.cassandra.transport.messages.RegisterMessage;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.transport.messages.ReviseRequestMessage;
import org.apache.cassandra.transport.messages.StartupMessage;
import org.apache.cassandra.transport.messages.SupportedMessage;
import org.apache.cassandra.utils.Flags;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.NoSpamLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Message {
   protected static final Logger logger = LoggerFactory.getLogger(Message.class);
   private static final NoSpamLogger noSpamLogger;
   private static final Set<String> ioExceptionsAtDebugLevel;
   public final Message.Type type;
   protected Connection connection;
   private int streamId;
   private Frame sourceFrame;
   private Map<String, ByteBuffer> customPayload;
   protected ProtocolVersion forcedProtocolVersion = null;

   protected Message(Message.Type type) {
      this.type = type;
   }

   public void attach(Connection connection) {
      this.connection = connection;
   }

   public Connection connection() {
      return this.connection;
   }

   public Message setStreamId(int streamId) {
      this.streamId = streamId;
      return this;
   }

   public int getStreamId() {
      return this.streamId;
   }

   public void setSourceFrame(Frame sourceFrame) {
      this.sourceFrame = sourceFrame;
   }

   public Frame getSourceFrame() {
      return this.sourceFrame;
   }

   public Map<String, ByteBuffer> getCustomPayload() {
      return this.customPayload;
   }

   public void setCustomPayload(Map<String, ByteBuffer> customPayload) {
      this.customPayload = customPayload;
   }

   public long getQueryStartNanoTime() {
      return this.sourceFrame.header.queryStartNanoTime;
   }

   static {
      noSpamLogger = NoSpamLogger.getLogger(logger, 10L, TimeUnit.SECONDS);
      ioExceptionsAtDebugLevel = ImmutableSet.<String>builder().add("Connection reset by peer").add("Broken pipe").add("Connection timed out").build();
   }

   static final class UnexpectedChannelExceptionHandler implements Predicate<Throwable> {
      private final Channel channel;
      private final boolean alwaysLogAtError;

      UnexpectedChannelExceptionHandler(Channel channel, boolean alwaysLogAtError) {
         this.channel = channel;
         this.alwaysLogAtError = alwaysLogAtError;
      }

      public boolean apply(Throwable exception) {
         String channelInfo;
         if (exception instanceof RuntimeException && exception.getCause() != null && exception.getCause() instanceof IOException) {
            exception = exception.getCause();
         }
         String message = "Unexpected exception during request; channel = {}";
         try {
            channelInfo = String.valueOf((Object)this.channel);
         }
         catch (Exception ignore) {
            channelInfo = "<unprintable>";
         }
         if (!this.alwaysLogAtError && exception instanceof IOException) {
            if (exception.getMessage() != null) {
               if (ioExceptionsAtDebugLevel.stream().anyMatch(exception.getMessage()::contains)) {
                  Message.logger.trace(message, (Object)channelInfo, (Object)exception);
                  return true;
               }
            }
            noSpamLogger.info(message, channelInfo, exception);
            return true;
         }
         Message.logger.error(message, (Object)channelInfo, (Object)exception);
         return true;
      }
   }

   @Sharable
   public static final class ExceptionHandler extends ChannelInboundHandlerAdapter {
      public ExceptionHandler() {
      }

      public void exceptionCaught(final ChannelHandlerContext ctx, Throwable cause) {
         Message.UnexpectedChannelExceptionHandler handler = new Message.UnexpectedChannelExceptionHandler(ctx.channel(), false);
         ErrorMessage errorMessage = ErrorMessage.fromException(cause, handler);
         if(ctx.channel().isOpen()) {
            ChannelFuture future = ctx.writeAndFlush(errorMessage);
            if(cause instanceof ProtocolException) {
               future.addListener(new ChannelFutureListener() {
                  public void operationComplete(ChannelFuture future) {
                     ctx.close();
                  }
               });
            }
         }

      }
   }

   @Sharable
   public static class Dispatcher extends SimpleChannelInboundHandler<Message.Request> {
      private static final ConcurrentMap<EventLoop, Message.Dispatcher.Flusher> flusherLookup = new ConcurrentHashMap();

      public Dispatcher() {
         super(false);
      }

      public void channelRead0(ChannelHandlerContext ctx, Message.Request request) {
         long queryStartNanoTime = request.getQueryStartNanoTime();

         try {
            assert request.connection() instanceof ServerConnection;

            ServerConnection connection = (ServerConnection)request.connection();
            connection.onNewRequest();
            ExecutorLocals.set((ExecutorLocals)null);
            if(connection.getVersion().isGreaterOrEqualTo(ProtocolVersion.V4)) {
               ClientWarn.instance.captureWarnings();
            }

            Single<QueryState> qstate = connection.validateNewMessage(request, connection.getVersion());
            if(Message.logger.isTraceEnabled()) {
               Message.logger.trace("Received: {}, v={} ON {}", new Object[]{request, connection.getVersion(), Thread.currentThread().getName()});
            }

            Single<? extends Message.Response> req = request.execute(qstate, queryStartNanoTime);
            req.subscribe((response) -> {
               try {
                  if(!response.sendToClient) {
                     request.getSourceFrame().release();
                     return;
                  }

                  response.setStreamId(request.getStreamId());
                  response.setWarnings(ClientWarn.instance.getWarnings());
                  response.attach(connection);
                  connection.applyStateTransition(request.type, response.type);
                  if(Message.logger.isTraceEnabled()) {
                     Message.logger.trace("Responding: {}, v={} ON {}", new Object[]{response, connection.getVersion(), Thread.currentThread().getName()});
                  }

                  this.flush(new Message.Dispatcher.FlushItem(ctx, response, request.getSourceFrame()));
               } catch (Throwable var9) {
                  request.getSourceFrame().release();
                  JVMStabilityInspector.inspectThrowable(var9);
                  Message.logger.error("Failed to reply, got another error whilst writing reply: {}", var9.getMessage(), var9);
               } finally {
                  connection.onRequestCompleted();
                  ClientWarn.instance.resetWarnings();
               }

            }, (t) -> {
               this.handleError(ctx, request, t);
            });
         } catch (Throwable var8) {
            Throwable t = var8;
            if(var8 instanceof NullPointerException && var8.getCause() != null) {
               t = var8.getCause();
            }

            this.handleError(ctx, request, t);
         }

      }

      private void handleError(ChannelHandlerContext ctx, Message.Request request, Throwable error) {
         try {
            if(Message.logger.isTraceEnabled()) {
               Message.logger.trace("Responding with error: {}, v={} ON {}", new Object[]{error.getMessage(), request.connection().getVersion(), Thread.currentThread().getName()});
            }

            JVMStabilityInspector.inspectThrowable(error);
            Message.UnexpectedChannelExceptionHandler handler = new Message.UnexpectedChannelExceptionHandler(ctx.channel(), true);
            this.flush(new Message.Dispatcher.FlushItem(ctx, ErrorMessage.fromException(error, handler).setStreamId(request.getStreamId()), request.getSourceFrame()));
         } catch (Throwable var8) {
            request.getSourceFrame().release();
            JVMStabilityInspector.inspectThrowable(var8);
            Message.logger.error("Failed to reply with error {}, got error whilst writing error reply: {}", new Object[]{error.getMessage(), var8.getMessage(), var8});
         } finally {
            if(request.connection() instanceof ServerConnection) {
               ((ServerConnection)((ServerConnection)request.connection())).onRequestCompleted();
            }

            ClientWarn.instance.resetWarnings();
         }

      }

      private void flush(Message.Dispatcher.FlushItem item) {
         EventLoop loop = item.ctx.channel().eventLoop();
         Message.Dispatcher.Flusher flusher = (Message.Dispatcher.Flusher)flusherLookup.get(loop);
         if(flusher == null) {
            Message.Dispatcher.Flusher alt = (Message.Dispatcher.Flusher)flusherLookup.putIfAbsent(loop, flusher = new Message.Dispatcher.Flusher(loop));
            if(alt != null) {
               flusher = alt;
            }
         }

         if(!flusher.queued.offer(item)) {
            throw new OverloadedException("Too many outgoing requests");
         } else {
            flusher.start();
         }
      }

      private static final class Flusher implements Runnable {
         final EventLoop eventLoop;
         final MpscArrayQueue<Message.Dispatcher.FlushItem> queued;
         final AtomicBoolean running;
         final Map<ChannelHandlerContext, Message.Dispatcher.ChannelFlusher> channels;
         final List<ChannelHandlerContext> finishedChannels;
         int runsWithNoWork;

         private Flusher(EventLoop eventLoop) {
            this.queued = new MpscArrayQueue(65536);
            this.running = new AtomicBoolean(false);
            this.channels = new IdentityHashMap();
            this.finishedChannels = new ArrayList();
            this.runsWithNoWork = 0;
            this.eventLoop = eventLoop;
         }

         void start() {
            if(!this.running.get() && this.running.compareAndSet(false, true)) {
               this.eventLoop.execute(this);
            }

         }

         public void run() {
            boolean doneWork;
            Message.Dispatcher.FlushItem item;
            for(doneWork = false; null != (item = (Message.Dispatcher.FlushItem)this.queued.poll()); doneWork = true) {
               ((Message.Dispatcher.ChannelFlusher)this.channels.computeIfAbsent(item.ctx, Message.Dispatcher.ChannelFlusher::new)).add(item);
            }

            Iterator var3 = this.channels.entrySet().iterator();

            while(var3.hasNext()) {
               Entry<ChannelHandlerContext, Message.Dispatcher.ChannelFlusher> c = (Entry)var3.next();
               if(((ChannelHandlerContext)c.getKey()).channel().isActive()) {
                  ((Message.Dispatcher.ChannelFlusher)c.getValue()).maybeFlush();
               } else {
                  ((Message.Dispatcher.ChannelFlusher)c.getValue()).release();
                  this.finishedChannels.add(c.getKey());
               }
            }

            var3 = this.finishedChannels.iterator();

            while(var3.hasNext()) {
               ChannelHandlerContext c = (ChannelHandlerContext)var3.next();
               this.channels.remove(c);
            }

            this.finishedChannels.clear();
            if(doneWork) {
               this.runsWithNoWork = 0;
            } else if(++this.runsWithNoWork > 5) {
               this.running.set(false);
               if(this.queued.isEmpty()) {
                  return;
               }

               if(!this.running.compareAndSet(false, true)) {
                  return;
               }
            }

            this.eventLoop.schedule(this, 10000L, TimeUnit.NANOSECONDS);
         }
      }

      private static class ChannelFlusher {
         final ChannelHandlerContext ctx;
         final List<Message.Dispatcher.FlushItem> flushItems = new ArrayList();
         int runsSinceFlush = 0;

         ChannelFlusher(ChannelHandlerContext ctx) {
            this.ctx = ctx;
         }

         void add(Message.Dispatcher.FlushItem item) {
            this.ctx.write(item.response, this.ctx.voidPromise());
            this.flushItems.add(item);
         }

         void release() {
            Iterator var1 = this.flushItems.iterator();

            while(var1.hasNext()) {
               Message.Dispatcher.FlushItem item = (Message.Dispatcher.FlushItem)var1.next();
               item.sourceFrame.release();
            }

            this.flushItems.clear();
         }

         boolean maybeFlush() {
            if(this.runsSinceFlush <= 2 && this.flushItems.size() <= 50) {
               ++this.runsSinceFlush;
               return false;
            } else {
               this.ctx.flush();
               this.release();
               this.runsSinceFlush = 0;
               return true;
            }
         }
      }

      private static class FlushItem {
         final ChannelHandlerContext ctx;
         final Object response;
         final Frame sourceFrame;

         private FlushItem(ChannelHandlerContext ctx, Object response, Frame sourceFrame) {
            this.ctx = ctx;
            this.sourceFrame = sourceFrame;
            this.response = response;
         }
      }
   }

   @Sharable
   public static class ProtocolEncoder extends MessageToMessageEncoder<Message> {
      public ProtocolEncoder() {
      }

      public void encode(ChannelHandlerContext ctx, Message message, List results) {
         Connection connection = (Connection)ctx.channel().attr(Connection.attributeKey).get();
         ProtocolVersion version = connection == null ? ProtocolVersion.CURRENT : connection.getVersion();
         Codec<Message> codec = (Codec<Message>)message.type.codec;
         Frame frame = null;
         int messageSize = codec.encodedSize(message, version);
         try {
            frame = ProtocolEncoder.makeFrame(message, messageSize, version);
            codec.encode(message, frame.body, version);
            results.add(frame);
         }
         catch (Throwable e) {
            if (frame != null) {
               frame.body.release();
            }
            throw ErrorMessage.wrap(e, message.getStreamId());
         }
      }

      public static Frame makeFrame(Message message, int messageSize, ProtocolVersion version) {
         int flags = 0;
         ByteBuf body = null;

         try {
            if(message instanceof Message.Response) {
               UUID tracingId = ((Message.Response)message).getTracingId();
               Map<String, ByteBuffer> customPayload = message.getCustomPayload();
               if(tracingId != null) {
                  messageSize += CBUtil.sizeOfUUID(tracingId);
               }

               List<String> warnings = ((Message.Response)message).getWarnings();
               if(warnings != null) {
                  if(version.isSmallerThan(ProtocolVersion.V4)) {
                     throw new ProtocolException("Must not send frame with WARNING flag for native protocol version < 4");
                  }

                  messageSize += CBUtil.sizeOfStringList(warnings);
               }

               if(customPayload != null) {
                  if(version.isSmallerThan(ProtocolVersion.V4)) {
                     throw new ProtocolException("Must not send frame with CUSTOM_PAYLOAD flag for native protocol version < 4");
                  }

                  messageSize += CBUtil.sizeOfBytesMap(customPayload);
               }

               body = CBUtil.allocator.buffer(messageSize);
               if(tracingId != null) {
                  CBUtil.writeUUID(tracingId, body);
                  flags = Flags.add(flags, 2);
               }

               if(warnings != null) {
                  CBUtil.writeStringList(warnings, body);
                  flags = Flags.add(flags, 8);
               }

               if(customPayload != null) {
                  CBUtil.writeBytesMap(customPayload, body);
                  flags = Flags.add(flags, 4);
               }
            } else {
               assert message instanceof Message.Request;

               if(((Message.Request)message).isTracingRequested()) {
                  flags = Flags.add(flags, 2);
               }

               Map<String, ByteBuffer> payload = message.getCustomPayload();
               if(payload != null) {
                  messageSize += CBUtil.sizeOfBytesMap(payload);
               }

               body = CBUtil.allocator.buffer(messageSize);
               if(payload != null) {
                  CBUtil.writeBytesMap(payload, body);
                  flags = Flags.add(flags, 4);
               }
            }

            ProtocolVersion responseVersion = message.forcedProtocolVersion == null?version:message.forcedProtocolVersion;
            if(responseVersion.isBeta()) {
               flags = Flags.add(flags, 16);
            }

            return Frame.create(Server.TIME_SOURCE, message.type, message.getStreamId(), responseVersion, flags, body);
         } catch (Throwable var8) {
            if(body != null) {
               body.release();
            }

            throw var8;
         }
      }
   }

   @Sharable
   public static class ProtocolDecoder extends MessageToMessageDecoder<Frame> {
      public ProtocolDecoder() {
      }

      public void decode(ChannelHandlerContext ctx, Frame frame, List results) {
         boolean isRequest = frame.header.type.direction == Message.Direction.REQUEST;
         int flags = frame.header.flags;
         boolean isTracing = Flags.contains(flags, 2);
         boolean isCustomPayload = Flags.contains(flags, 4);
         boolean hasWarning = Flags.contains(flags, 8);
         UUID tracingId = !isRequest && isTracing?CBUtil.readUUID(frame.body):null;
         List<String> warnings = !isRequest && hasWarning?CBUtil.readStringList(frame.body):null;
         Map customPayload = !isCustomPayload?null:CBUtil.readBytesMap(frame.body);

         try {
            if(isCustomPayload && frame.header.version.isSmallerThan(ProtocolVersion.V4)) {
               throw new ProtocolException("Received frame with CUSTOM_PAYLOAD flag for native protocol version < 4");
            } else {
               Message message = (Message)frame.header.type.codec.decode(frame.body, frame.header.version);
               message.setStreamId(frame.header.streamId);
               message.setSourceFrame(frame);
               message.setCustomPayload(customPayload);
               if(isRequest) {
                  assert message instanceof Message.Request;

                  Message.Request req = (Message.Request)message;
                  Connection connection = (Connection)ctx.channel().attr(Connection.attributeKey).get();
                  req.attach(connection);
                  if(isTracing) {
                     req.setTracingRequested();
                  }
               } else {
                  assert message instanceof Message.Response;

                  if(isTracing) {
                     ((Message.Response)message).setTracingId(tracingId);
                  }

                  if(hasWarning) {
                     ((Message.Response)message).setWarnings(warnings);
                  }
               }

               results.add(message);
            }
         } catch (Throwable var15) {
            frame.release();
            throw ErrorMessage.wrap(var15, frame.header.streamId);
         }
      }
   }

   public abstract static class Response extends Message {
      protected UUID tracingId;
      protected List<String> warnings;
      public final boolean sendToClient;

      protected Response(Message.Type type) {
         this(type, true);
      }

      protected Response(Message.Type type, boolean sendToClient) {
         super(type);
         if(type.direction != Message.Direction.RESPONSE) {
            throw new IllegalArgumentException();
         } else {
            this.sendToClient = sendToClient;
         }
      }

      public Message setTracingId(UUID tracingId) {
         this.tracingId = tracingId;
         return this;
      }

      public UUID getTracingId() {
         return this.tracingId;
      }

      public Message setWarnings(List<String> warnings) {
         this.warnings = warnings;
         return this;
      }

      public List<String> getWarnings() {
         return this.warnings;
      }
   }

   public abstract static class Request extends Message {
      protected boolean tracingRequested;

      protected Request(Message.Type type) {
         super(type);
         if(type.direction != Message.Direction.REQUEST) {
            throw new IllegalArgumentException();
         }
      }

      public abstract Single<? extends Message.Response> execute(Single<QueryState> var1, long var2);

      public void setTracingRequested() {
         this.tracingRequested = true;
      }

      public final boolean shouldTraceRequest() {
         return this.tracingRequested?true:StorageService.instance.shouldTraceRequest();
      }

      public boolean isTracingRequested() {
         return this.tracingRequested;
      }

      protected final InetSocketAddress getRemoteAddress() {
         return ((ServerConnection)this.connection).getRemoteAddress();
      }

      protected final InetAddress getClientAddress() {
         return ((ServerConnection)this.connection).getClientAddress();
      }

      protected void checkIsLoggedIn(QueryState state) {
         if(!state.hasUser()) {
            throw new UnauthorizedException("You have not logged in");
         }
      }
   }

   public static enum Type {
      ERROR(0, Message.Direction.RESPONSE, ErrorMessage.codec, false),
      STARTUP(1, Message.Direction.REQUEST, StartupMessage.codec, false),
      READY(2, Message.Direction.RESPONSE, ReadyMessage.codec, false),
      AUTHENTICATE(3, Message.Direction.RESPONSE, AuthenticateMessage.codec, false),
      OPTIONS(5, Message.Direction.REQUEST, OptionsMessage.codec, false),
      SUPPORTED(6, Message.Direction.RESPONSE, SupportedMessage.codec, false),
      QUERY(7, Message.Direction.REQUEST, QueryMessage.codec, true),
      RESULT(8, Message.Direction.RESPONSE, ResultMessage.codec, false),
      PREPARE(9, Message.Direction.REQUEST, PrepareMessage.codec, false),
      EXECUTE(10, Message.Direction.REQUEST, ExecuteMessage.codec, true),
      REGISTER(11, Message.Direction.REQUEST, RegisterMessage.codec, false),
      EVENT(12, Message.Direction.RESPONSE, EventMessage.codec, false),
      BATCH(13, Message.Direction.REQUEST, BatchMessage.codec, true),
      AUTH_CHALLENGE(14, Message.Direction.RESPONSE, AuthChallenge.codec, false),
      AUTH_RESPONSE(15, Message.Direction.REQUEST, AuthResponse.codec, false),
      AUTH_SUCCESS(16, Message.Direction.RESPONSE, AuthSuccess.codec, false),
      REVISE_REQUEST(255, Message.Direction.REQUEST, ReviseRequestMessage.codec, false);

      public final int opcode;
      public final Message.Direction direction;
      public final Message.Codec<?> codec;
      public final boolean supportsBackpressure;
      private static final Message.Type[] opcodeIdx;

      private Type(int opcode, Message.Direction direction, Message.Codec<?> codec, boolean supportsBackpressure) {
         this.opcode = opcode;
         this.direction = direction;
         this.codec = codec;
         this.supportsBackpressure = supportsBackpressure;
      }

      public static Message.Type fromOpcode(int opcode, Message.Direction direction) {
         if(opcode >= opcodeIdx.length) {
            throw new ProtocolException(String.format("Unknown opcode %d", new Object[]{Integer.valueOf(opcode)}));
         } else {
            Message.Type t = opcodeIdx[opcode];
            if(t == null) {
               throw new ProtocolException(String.format("Unknown opcode %d", new Object[]{Integer.valueOf(opcode)}));
            } else if(t.direction != direction) {
               throw new ProtocolException(String.format("Wrong protocol direction (expected %s, got %s) for opcode %d (%s)", new Object[]{t.direction, direction, Integer.valueOf(opcode), t}));
            } else {
               return t;
            }
         }
      }

      static {
         int maxOpcode = -1;
         Message.Type[] var1 = values();
         int var2 = var1.length;

         int var3;
         Message.Type type;
         for(var3 = 0; var3 < var2; ++var3) {
            type = var1[var3];
            maxOpcode = Math.max(maxOpcode, type.opcode);
         }

         opcodeIdx = new Message.Type[maxOpcode + 1];
         var1 = values();
         var2 = var1.length;

         for(var3 = 0; var3 < var2; ++var3) {
            type = var1[var3];
            if(opcodeIdx[type.opcode] != null) {
               throw new IllegalStateException("Duplicate opcode");
            }

            opcodeIdx[type.opcode] = type;
         }

      }
   }

   public static enum Direction {
      REQUEST,
      RESPONSE;

      private Direction() {
      }

      public static Message.Direction extractFromVersion(int versionWithDirection) {
         return (versionWithDirection & 128) == 0?REQUEST:RESPONSE;
      }

      public int addToVersion(int rawVersion) {
         return this == REQUEST?rawVersion & 127:rawVersion | 128;
      }
   }

   public interface Codec<M extends Message> extends CBCodec<M> {
   }
}
