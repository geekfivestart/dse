package org.apache.cassandra.streaming;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.util.concurrent.FastThreadLocalThread;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.BufferedDataOutputStreamPlus;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.io.util.WrappedDataOutputStreamPlus;
import org.apache.cassandra.net.IncomingStreamingConnection;
import org.apache.cassandra.streaming.messages.StreamInitMessage;
import org.apache.cassandra.streaming.messages.StreamMessage;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionHandler {
   private static final Logger logger = LoggerFactory.getLogger(ConnectionHandler.class);
   private final StreamSession session;
   private ConnectionHandler.IncomingMessageHandler incoming;
   private ConnectionHandler.OutgoingMessageHandler outgoing;
   private final boolean isPreview;

   ConnectionHandler(StreamSession session, boolean isPreview) {
      this.session = session;
      this.isPreview = isPreview;
      this.incoming = new ConnectionHandler.IncomingMessageHandler(session);
      this.outgoing = new ConnectionHandler.OutgoingMessageHandler(session);
   }

   public void initiate() throws IOException {
      logger.debug("[Stream #{}] Sending stream init for incoming stream", this.session.planId());
      Socket incomingSocket = this.session.createConnection();
      this.incoming.start(incomingSocket, StreamMessage.CURRENT_VERSION, true);
      logger.debug("[Stream #{}] Sending stream init for outgoing stream", this.session.planId());
      Socket outgoingSocket = this.session.createConnection();
      this.outgoing.start(outgoingSocket, StreamMessage.CURRENT_VERSION, true);
   }

   public void initiateOnReceivingSide(IncomingStreamingConnection connection, boolean isForOutgoing, StreamMessage.StreamVersion version) throws IOException {
      if(isForOutgoing) {
         this.outgoing.start(connection, version);
      } else {
         this.incoming.start(connection, version);
      }

   }

   public ListenableFuture<?> close() {
      logger.debug("[Stream #{}] Closing stream connection handler on {}", this.session.planId(), this.session.peer);
      ListenableFuture<?> inClosed = this.closeIncoming();
      ListenableFuture<?> outClosed = this.closeOutgoing();
      return Futures.allAsList(new ListenableFuture[]{inClosed, outClosed});
   }

   public ListenableFuture<?> closeOutgoing() {
      return this.outgoing == null?Futures.immediateFuture((Object)null):this.outgoing.close();
   }

   public ListenableFuture<?> closeIncoming() {
      return this.incoming == null?Futures.immediateFuture((Object)null):this.incoming.close();
   }

   public void sendMessages(Collection<? extends StreamMessage> messages) {
      Iterator var2 = messages.iterator();

      while(var2.hasNext()) {
         StreamMessage message = (StreamMessage)var2.next();
         this.sendMessage(message);
      }

   }

   public void sendMessage(StreamMessage message) {
      if(this.outgoing.isClosed()) {
         throw new RuntimeException("Outgoing stream handler has been closed");
      } else if(message.type == StreamMessage.Type.FILE && this.isPreview) {
         throw new RuntimeException("Cannot send file messages for preview streaming sessions");
      } else {
         this.outgoing.enqueue(message);
      }
   }

   public boolean isOutgoingConnected() {
      return this.outgoing != null && !this.outgoing.isClosed();
   }

   static class OutgoingMessageHandler extends ConnectionHandler.MessageHandler {
      private final PriorityBlockingQueue<StreamMessage> messageQueue = new PriorityBlockingQueue(64, new Comparator<StreamMessage>() {
         public int compare(StreamMessage o1, StreamMessage o2) {
            return o2.getPriority() - o1.getPriority();
         }
      });

      OutgoingMessageHandler(StreamSession session) {
         super(session, true);
      }

      protected String name() {
         return "STREAM-OUT";
      }

      public void enqueue(StreamMessage message) {
         this.messageQueue.put(message);
      }

      public void run() {
         try {
            DataOutputStreamPlus out = getWriteChannel(this.socket);

            StreamMessage next;
            while(!this.isClosed()) {
               if((next = (StreamMessage)this.messageQueue.poll(1L, TimeUnit.SECONDS)) != null) {
                  ConnectionHandler.logger.debug("[Stream #{}] Sending {}", this.session.planId(), next);
                  this.sendMessage(out, next);
                  ConnectionHandler.logger.debug("[Stream #{}] Sent {}", this.session.planId(), next);
                  if(next.type == StreamMessage.Type.SESSION_FAILED) {
                     ConnectionHandler.logger.debug("[Stream #{}] Closing due to SESSION_FAILED", this.session.planId());
                     this.close();
                  }
               }
            }

            ConnectionHandler.logger.debug("[Stream #{}] Closed - {} messages left in queue", this.session.planId(), Integer.valueOf(this.messageQueue.size()));

            while((next = (StreamMessage)this.messageQueue.poll()) != null) {
               ConnectionHandler.logger.debug("[Stream #{}] Closed - Trying to send message {}", this.session.planId(), next);
               this.sendMessage(out, next);
            }

            ConnectionHandler.logger.debug("[Stream #{}] Done", this.session.planId());
         } catch (InterruptedException var7) {
            ConnectionHandler.logger.debug("[Stream #{}] Interrupted", this.session.planId());
            throw new AssertionError(var7);
         } catch (Throwable var8) {
            ConnectionHandler.logger.debug("[Stream #{}] Error", this.session.planId(), var8);
            this.session.onError(var8);
         } finally {
            this.signalCloseDone();
         }

      }

      private void sendMessage(DataOutputStreamPlus out, StreamMessage message) {
         try {
            StreamMessage.serialize(message, out, this.version, this.session);
            out.flush();
            message.sent();
         } catch (SocketException var4) {
            this.session.onError(var4);
            this.close();
         } catch (IOException var5) {
            this.session.onError(var5);
         }

      }
   }

   static class IncomingMessageHandler extends ConnectionHandler.MessageHandler {
      IncomingMessageHandler(StreamSession session) {
         super(session, false);
      }

      public void start(Socket socket, StreamMessage.StreamVersion version, boolean initiator) throws IOException {
         int socketTimeout = (int)TimeUnit.SECONDS.toMillis((long)(2 * DatabaseDescriptor.getStreamingKeepAlivePeriod()));

         try {
            socket.setSoTimeout(socketTimeout);
         } catch (SocketException var6) {
            ConnectionHandler.logger.warn("Could not set incoming socket timeout to {}", Integer.valueOf(socketTimeout), var6);
         }

         super.start(socket, version, initiator);
      }

      protected String name() {
         return "STREAM-IN";
      }

      public void run() {
         try {
            StreamMessage message;
            try {
               for(ReadableByteChannel in = getReadChannel(this.socket); !this.isClosed(); ConnectionHandler.logger.debug("[Stream #{}] Processed {}", this.session.planId(), message)) {
                  message = StreamMessage.deserialize(in, this.version, this.session);
                  ConnectionHandler.logger.debug("[Stream #{}] Received {}", this.session.planId(), message);
                  if(message != null) {
                     this.session.messageReceived(message);
                  }
               }
            } catch (Throwable var6) {
               ConnectionHandler.logger.debug("[Stream #{}] Error", this.session.planId(), var6);
               JVMStabilityInspector.inspectThrowable(var6);
               this.session.onError(var6);
            }
         } finally {
            this.signalCloseDone();
         }

      }
   }

   abstract static class MessageHandler implements Runnable {
      protected final StreamSession session;
      protected StreamMessage.StreamVersion version;
      private final boolean isOutgoingHandler;
      protected Socket socket;
      private final AtomicReference<SettableFuture<?>> closeFuture = new AtomicReference();
      private IncomingStreamingConnection incomingConnection;

      protected MessageHandler(StreamSession session, boolean isOutgoingHandler) {
         this.session = session;
         this.isOutgoingHandler = isOutgoingHandler;
      }

      protected abstract String name();

      protected static DataOutputStreamPlus getWriteChannel(Socket socket) throws IOException {
         WritableByteChannel out = socket.getChannel();
         return (DataOutputStreamPlus)(out == null?new WrappedDataOutputStreamPlus(new BufferedOutputStream(socket.getOutputStream())):new BufferedDataOutputStreamPlus(out));
      }

      protected static ReadableByteChannel getReadChannel(Socket socket) throws IOException {
         return Channels.newChannel(socket.getInputStream());
      }

      private void sendInitMessage() throws IOException {
         StreamInitMessage message = new StreamInitMessage(FBUtilities.getBroadcastAddress(), this.session.sessionIndex(), this.session.planId(), this.session.streamOperation(), !this.isOutgoingHandler, this.session.keepSSTableLevel(), this.session.getPendingRepair(), this.session.getPreviewKind());
         ByteBuffer messageBuf = message.createMessage(false, this.version);
         DataOutputStreamPlus out = getWriteChannel(this.socket);
         out.write(messageBuf);
         out.flush();
      }

      public void start(IncomingStreamingConnection connection, StreamMessage.StreamVersion version) throws IOException {
         this.incomingConnection = connection;
         this.start(connection.socket, version, false);
      }

      public void start(Socket socket, StreamMessage.StreamVersion version, boolean initiator) throws IOException {
         this.socket = socket;
         this.version = version;
         if(initiator) {
            this.sendInitMessage();
         }

         (new FastThreadLocalThread(this, this.name() + "-" + socket.getRemoteSocketAddress())).start();
      }

      public ListenableFuture<?> close() {
         SettableFuture<?> future = SettableFuture.create();
         return (ListenableFuture)(this.closeFuture.compareAndSet((Object)null, future)?future:(ListenableFuture)this.closeFuture.get());
      }

      public boolean isClosed() {
         return this.closeFuture.get() != null;
      }

      protected void signalCloseDone() {
         if(!this.isClosed()) {
            this.close();
         }

         ((SettableFuture)this.closeFuture.get()).set((Object)null);
         if(this.incomingConnection != null) {
            this.incomingConnection.close();
         } else {
            try {
               this.socket.close();
            } catch (IOException var2) {
               ConnectionHandler.logger.debug("Unexpected error while closing streaming connection", var2);
            }
         }

      }
   }
}
