package org.apache.cassandra.net;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.TrackedDataInputPlus;
import org.apache.cassandra.tracing.Tracing;

public abstract class Message<P> {
   static final Message<?> CLOSE_SENTINEL = new Message<Void>((InetAddress)null, (InetAddress)null, -1, (Message.Data)null) {
      public Verb<?, ?> verb() {
         return null;
      }

      public Message.Type type() {
         return null;
      }

      public Message<Void> addParameters(MessageParameters parameters) {
         throw new UnsupportedOperationException();
      }

      long payloadSerializedSize(MessagingVersion version) {
         return 0L;
      }

      public boolean isDroppable() {
         return false;
      }
   };
   private final InetAddress from;
   private final InetAddress to;
   private final int id;
   final Message.Data<P> messageData;

   protected Message(InetAddress from, InetAddress to, int id, Message.Data<P> messageData) {
      this.from = from;
      this.to = to;
      this.id = id;
      this.messageData = messageData;
   }

   public static Message.Serializer createSerializer(MessagingVersion version, long baseTimestampMillis) {
      return (Message.Serializer)(version.isDSE()?new MessageSerializer(version, baseTimestampMillis):new OSSMessageSerializer(version));
   }

   public InetAddress from() {
      return this.from;
   }

   public InetAddress to() {
      return this.to;
   }

   public VerbGroup<?> group() {
      return this.verb().group();
   }

   public abstract Verb<?, ?> verb();

   int id() {
      return this.id;
   }

   public abstract Message.Type type();

   public boolean isRequest() {
      return this.type() == Message.Type.REQUEST;
   }

   public boolean isResponse() {
      return this.type() == Message.Type.RESPONSE;
   }

   long operationStartMillis() {
      return this.messageData.createdAtMillis;
   }

   public long timeoutMillis() {
      return this.messageData.timeoutMillis;
   }

   boolean isTimedOut(long currentTimeMillis) {
      return !this.verb().isOneWay() && currentTimeMillis - this.operationStartMillis() > this.timeoutMillis();
   }

   long lifetimeMillis() {
      return Math.max(System.currentTimeMillis() - this.operationStartMillis(), 0L);
   }

   public boolean isLocal() {
      return this.locality() == Message.Locality.LOCAL;
   }

   public Message.Locality locality() {
      return this.to.equals(this.from)?Message.Locality.LOCAL:Message.Locality.REMOTE;
   }

   public MessageParameters parameters() {
      return this.messageData.parameters;
   }

   public abstract Message<P> addParameters(MessageParameters var1);

   public boolean isTraced() {
      return this.messageData.tracingInfo != null;
   }

   public Tracing.SessionInfo tracingInfo() {
      return this.messageData.tracingInfo;
   }

   public P payload() {
      return this.messageData.payload;
   }

   abstract long payloadSerializedSize(MessagingVersion var1);

   public Message.Kind kind() {
      return this.group() == Verbs.GOSSIP?Message.Kind.GOSSIP:(this.messageData.payloadSize > OutboundTcpConnectionPool.LARGE_MESSAGE_THRESHOLD?Message.Kind.LARGE:Message.Kind.SMALL);
   }

   public boolean isDroppable() {
      return this.group() == Verbs.GOSSIP?false:this.isRequest();
   }

   public boolean equals(Object other) {
      if(!(other instanceof Message)) {
         return false;
      } else {
         Message<?> that = (Message)other;
         return this.from.equals(that.from) && this.to.equals(that.to) && this.id == that.id && this.verb().equals(that.verb()) && this.messageData.equals(that.messageData);
      }
   }

   public String toString() {
      return String.format("%s (%d): %s", new Object[]{this.verb(), Integer.valueOf(this.id()), this.isRequest()?String.format("%s -> %s", new Object[]{this.from(), this.to()}):String.format("%s <- %s", new Object[]{this.to(), this.from()})});
   }

   public interface Serializer {
      <P> void serialize(Message<P> var1, DataOutputPlus var2) throws IOException;

      <P> long serializedSize(Message<P> var1);

      <P> Message<P> deserialize(TrackedDataInputPlus var1, int var2, InetAddress var3) throws IOException;

      default void writeSerializedSize(int serializedSize, DataOutputPlus out) throws IOException {
         out.writeInt(serializedSize);
      }

      default int readSerializedSize(DataInputPlus in) throws IOException {
         return in.readInt();
      }
   }

   static class Data<P> {
      private final P payload;
      final long payloadSize;
      private final long createdAtMillis;
      private final long timeoutMillis;
      private final MessageParameters parameters;
      @Nullable
      private final Tracing.SessionInfo tracingInfo;

      Data(P payload, long payloadSize, long createdAtMillis, long timeoutMillis, MessageParameters parameters, Tracing.SessionInfo tracingInfo) {
         this.payload = payload;
         this.payloadSize = payloadSize;
         this.createdAtMillis = createdAtMillis;
         this.timeoutMillis = timeoutMillis;
         this.parameters = parameters;
         this.tracingInfo = tracingInfo;
      }

      Data(P payload, long payloadSize, long createdAtMillis, long timeoutMillis) {
         this(payload, payloadSize, createdAtMillis, timeoutMillis, MessageParameters.EMPTY, Tracing.isTracing()?Tracing.instance.sessionInfo():null);
      }

      Data(P payload) {
         this(payload, -1L, System.currentTimeMillis(), 9223372036854775807L);
      }

      <Q> Message.Data<Q> withPayload(Q payload, long payloadSize) {
         return new Message.Data(payload, payloadSize, this.createdAtMillis, this.timeoutMillis, this.parameters, this.tracingInfo);
      }

      Message.Data<P> withAddedParameters(MessageParameters newParameters) {
         return new Message.Data(this.payload, this.payloadSize, this.createdAtMillis, this.timeoutMillis, this.parameters.unionWith(newParameters), this.tracingInfo);
      }

      public boolean equals(Object other) {
         if(!(other instanceof Message.Data)) {
            return false;
         } else {
            Message.Data that = (Message.Data)other;
            return Objects.equals(this.payload, that.payload) && this.parameters.equals(that.parameters) && Objects.equals(this.tracingInfo, that.tracingInfo);
         }
      }
   }

   static enum Kind {
      GOSSIP,
      SMALL,
      LARGE;

      private Kind() {
      }

      public String toString() {
         return this.name().substring(0, 1) + this.name().substring(1).toLowerCase();
      }
   }

   public static enum Locality {
      LOCAL,
      REMOTE;

      private Locality() {
      }

      public static ImmutableSet<Message.Locality> all() {
         return Sets.immutableEnumSet(LOCAL, new Message.Locality[]{REMOTE});
      }
   }

   public static enum Type {
      REQUEST,
      RESPONSE;

      private Type() {
      }

      public static ImmutableSet<Message.Type> all() {
         return Sets.immutableEnumSet(REQUEST, new Message.Type[]{RESPONSE});
      }
   }
}
