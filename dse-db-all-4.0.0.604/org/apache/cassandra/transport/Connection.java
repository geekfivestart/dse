package org.apache.cassandra.transport;

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.GenericFutureListener;

public class Connection {
   static final AttributeKey<Connection> attributeKey = AttributeKey.valueOf("CONN");
   private final Channel channel;
   private final ProtocolVersion version;
   private final Connection.Tracker tracker;
   private volatile FrameCompressor frameCompressor;

   public Connection(Channel channel, ProtocolVersion version, Connection.Tracker tracker) {
      this.channel = channel;
      this.version = version;
      this.tracker = tracker;
      tracker.addConnection(channel, this);
      channel.closeFuture().addListener((fut) -> {
         tracker.removeConnection(channel, this);
      });
   }

   public void setCompressor(FrameCompressor compressor) {
      this.frameCompressor = compressor;
   }

   public FrameCompressor getCompressor() {
      return this.frameCompressor;
   }

   public Connection.Tracker getTracker() {
      return this.tracker;
   }

   public ProtocolVersion getVersion() {
      return this.version;
   }

   public Channel channel() {
      return this.channel;
   }

   public interface Tracker {
      void addConnection(Channel var1, Connection var2);

      void removeConnection(Channel var1, Connection var2);
   }

   public interface Factory {
      Connection newConnection(Channel var1, ProtocolVersion var2);
   }
}
