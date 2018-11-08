package org.apache.cassandra.net;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import java.net.InetAddress;
import java.util.Collections;
import java.util.List;
import org.apache.cassandra.exceptions.RequestFailureReason;

public class OneWayRequest<P> extends Request<P, NoResponse> {
   private static final Runnable NOOP = () -> {
   };

   OneWayRequest(InetAddress from, InetAddress to, Verb.OneWay<P> verb, Message.Data<P> data) {
      this(from, to, verb, data, Collections.emptyList());
   }

   OneWayRequest(InetAddress from, InetAddress to, Verb.OneWay<P> verb, Message.Data<P> data, List<Request.Forward> forwards) {
      super(from, to, -1, verb, data, forwards);
   }

   void execute() {
      assert this.verb.isOneWay();

      this.execute(this.verb.EMPTY_RESPONSE_CONSUMER, NOOP);
   }

   public Verb.OneWay<P> verb() {
      return (Verb.OneWay)super.verb();
   }

   public OneWayRequest<P> addParameters(MessageParameters parameters) {
      return new OneWayRequest(this.from(), this.to(), this.verb(), this.messageData.withAddedParameters(parameters));
   }

   public Response<NoResponse> respond(NoResponse payload) {
      throw new UnsupportedOperationException();
   }

   public FailureResponse<NoResponse> respondWithFailure(RequestFailureReason reason) {
      throw new UnsupportedOperationException();
   }

   static class Dispatcher<P> extends Request.Dispatcher<P, NoResponse> {
      Dispatcher(MessageTargets targets, Verb.OneWay<P> verb, Message.Data<P> messageData) {
         super(targets, verb, messageData);
      }

      Verb.OneWay<P> verb() {
         return (Verb.OneWay)this.verb;
      }

      Iterable<OneWayRequest<P>> remoteRequests() {
         Iterable<OneWayRequest<P>> withoutForwards = Iterables.transform(this.targets.nonForwardingRemotes(), (to) -> {
            return this.verb().newRequest(to, this.messageData);
         });
         return !this.targets.hasForwards()?withoutForwards:Iterables.concat(withoutForwards, Iterables.transform(this.targets.remotesWithForwards(), (t) -> {
            return this.verb().newRequestWithForwards(t.target, this.messageData, t.forwards);
         }));
      }

      OneWayRequest<P> localRequest() {
         return this.verb().newRequest(Request.local, this.messageData);
      }
   }
}
