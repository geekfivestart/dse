package org.apache.cassandra.net;

import java.net.InetAddress;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.utils.FBUtilities;

public class FailureResponse<Q> extends Response<Q> {
   private final RequestFailureReason reason;

   FailureResponse(InetAddress from, InetAddress to, int id, Verb<?, Q> verb, RequestFailureReason reason, Message.Data<Q> data) {
      super(from, to, id, verb, data);
      this.reason = reason;
   }

   public static <R> FailureResponse<R> local(Verb<?, R> verb, RequestFailureReason reason, long createdAtMillis) {
      InetAddress local = FBUtilities.getLocalAddress();
      return new FailureResponse(local, local, -1, verb, reason, new Message.Data((Object)null, -1L, createdAtMillis, 9223372036854775807L));
   }

   public RequestFailureReason reason() {
      return this.reason;
   }

   public boolean isFailure() {
      return true;
   }

   public FailureResponse<Q> addParameters(MessageParameters parameters) {
      return new FailureResponse(this.from(), this.to(), this.id(), this.verb(), this.reason, this.messageData.withAddedParameters(parameters));
   }

   public void deliverTo(MessageCallback<Q> callback) {
      callback.onFailure(this);
   }

   long payloadSerializedSize(MessagingVersion version) {
      return 0L;
   }

   public String toString() {
      return String.format("[error]@%d %s: %s %s %s", new Object[]{Integer.valueOf(this.id()), this.reason, this.from(), this.isRequest()?"->":"<-", this.to()});
   }
}
