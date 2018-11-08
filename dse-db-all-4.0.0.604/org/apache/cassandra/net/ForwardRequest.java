package org.apache.cassandra.net;

import java.net.InetAddress;
import org.apache.cassandra.exceptions.RequestFailureReason;

class ForwardRequest<P, Q> extends Request<P, Q> {
   final InetAddress replyTo;

   ForwardRequest(InetAddress from, InetAddress to, InetAddress replyTo, int id, Verb<P, Q> type, Message.Data<P> data) {
      super(from, to, id, type, data);
      this.replyTo = replyTo;
   }

   boolean isForwarded() {
      return true;
   }

   public Request<P, Q> addParameters(MessageParameters parameters) {
      throw new UnsupportedOperationException();
   }

   public Response<Q> respond(Q payload) {
      return new Response(local, this.replyTo, this.id(), this.verb(), this.responseData(payload));
   }

   public FailureResponse<Q> respondWithFailure(RequestFailureReason reason) {
      return new FailureResponse(local, this.replyTo, this.id(), this.verb(), reason, this.messageData.withPayload((Object)null, -1L));
   }
}
