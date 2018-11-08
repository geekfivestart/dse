package org.apache.cassandra.net;

import com.google.common.annotations.VisibleForTesting;
import java.net.InetAddress;
import org.apache.cassandra.utils.FBUtilities;

public class Response<Q> extends Message<Q> {
   private final Verb<?, Q> verb;

   Response(InetAddress from, InetAddress to, int id, Verb<?, Q> verb, Message.Data<Q> data) {
      super(from, to, id, data);
      this.verb = verb;
   }

   public static <R> Response<R> local(Verb<?, R> verb, R payload, long createdAtMillis) {
      InetAddress local = FBUtilities.getLocalAddress();
      return new Response(local, local, -1, verb, new Message.Data(payload, -1L, createdAtMillis, 9223372036854775807L));
   }

   @VisibleForTesting
   public static <R> Response<R> testResponse(InetAddress from, InetAddress to, Verb<?, R> verb, R payload) {
      return new Response(from, to, -1, verb, new Message.Data(payload));
   }

   @VisibleForTesting
   public static <R> Response<R> localTestResponse(Verb<?, R> verb, R payload) {
      InetAddress local = FBUtilities.getLocalAddress();
      return testResponse(local, local, verb, payload);
   }

   @VisibleForTesting
   public static Response<EmptyPayload> testLocalAck(Verb<?, EmptyPayload> verb) {
      return localTestResponse(verb, EmptyPayload.instance);
   }

   public Message.Type type() {
      return Message.Type.RESPONSE;
   }

   public Verb<?, Q> verb() {
      return this.verb;
   }

   public Response<Q> addParameters(MessageParameters parameters) {
      return new Response(this.from(), this.to(), this.id(), this.verb(), this.messageData.withAddedParameters(parameters));
   }

   public boolean isFailure() {
      return false;
   }

   public void deliverTo(MessageCallback<Q> callback) {
      callback.onResponse(this);
   }

   long payloadSerializedSize(MessagingVersion version) {
      return this.messageData.payloadSize >= 0L && version == MessagingService.current_version?this.messageData.payloadSize:version.serializer(this.verb()).responseSerializer.serializedSize(this.payload());
   }
}
