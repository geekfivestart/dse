package org.apache.cassandra.net;

import java.io.IOException;
import java.net.InetAddress;
import org.apache.cassandra.exceptions.InternalRequestExecutionException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.tracing.Tracing;

class MessageDeserializationException extends RuntimeException {
   private final int unreadBytesOfMessage;
   private final FailureResponse<?> failureResponse;

   private MessageDeserializationException(int unreadBytesOfMessage, FailureResponse<?> failureResponse, Throwable cause) {
      super("Error deserializing inter-node message", cause);
      this.unreadBytesOfMessage = unreadBytesOfMessage;
      this.failureResponse = failureResponse;
   }

   static <Q> MessageDeserializationException forRequestPayloadDeserializationException(Exception e, InetAddress from, int id, Verb<?, Q> verb, long createAtMillis, long timeoutMillis, MessageParameters parameters, Tracing.SessionInfo tracingInfo, int unreadBytesOfMessage) {
      FailureResponse<Q> failureResponse = null;
      RequestFailureReason reason = reason(e);
      if(!verb.isOneWay() && !(e instanceof IOException)) {
         failureResponse = failureResponse(from, id, verb, createAtMillis, timeoutMillis, parameters, tracingInfo, reason);
      }

      String details = String.format("Error reading %s request message from %s", new Object[]{verb, from});
      if(reason.isSchemaRelated()) {
         details = details + " (this is likely due to schema update not having yet propagated to this node)";
      }

      return forException(e, verb, failureResponse, details, unreadBytesOfMessage);
   }

   static <Q> MessageDeserializationException forResponsePayloadDeserializationException(Exception e, InetAddress from, Verb<?, Q> verb, int unreadBytesOfMessage) {
      String details = String.format("Error reading %s response message from %s", new Object[]{verb, from});
      return forException(e, verb, (FailureResponse)null, details, unreadBytesOfMessage);
   }

   private static <Q> MessageDeserializationException forException(Exception e, Verb<?, Q> verb, FailureResponse<Q> failureResponse, String details, int unreadBytesOfMessage) {
      if(unreadBytesOfMessage < 0) {
         return nonRecoverable(e, failureResponse);
      } else if(!(e instanceof InternalRequestExecutionException)) {
         return nonRecoverable(e, failureResponse);
      } else {
         verb.errorHandler().handleError(wrap((InternalRequestExecutionException)e, details));
         return new MessageDeserializationException(unreadBytesOfMessage, failureResponse, e);
      }
   }

   private static InternalRequestExecutionException wrap(InternalRequestExecutionException e, String details) {
      return new InternalRequestExecutionException(e.reason, details + ": " + e.getMessage(), e) {
      };
   }

   private static RequestFailureReason reason(Exception e) {
      return e instanceof InternalRequestExecutionException?((InternalRequestExecutionException)e).reason:RequestFailureReason.UNKNOWN;
   }

   private static <Q> FailureResponse<Q> failureResponse(InetAddress from, int id, Verb<?, Q> verb, long createAtMillis, long timeoutMillis, MessageParameters parameters, Tracing.SessionInfo tracingInfo, RequestFailureReason reason) {
      return new FailureResponse(Request.local, from, id, verb, reason, new Message.Data((Object)null, -1L, createAtMillis, timeoutMillis, parameters, tracingInfo));
   }

   private static MessageDeserializationException nonRecoverable(Exception e, FailureResponse<?> failureResponse) {
      return new MessageDeserializationException(-1, failureResponse, e);
   }

   void maybeSendErrorResponse() {
      if(this.failureResponse != null) {
         MessagingService.instance().reply(this.failureResponse);
      }

   }

   boolean isRecoverable() {
      return this.unreadBytesOfMessage >= 0;
   }

   void recover(DataInputPlus in) throws IOException {
      assert this.isRecoverable();

      in.skipBytesFully(this.unreadBytesOfMessage);
   }
}
