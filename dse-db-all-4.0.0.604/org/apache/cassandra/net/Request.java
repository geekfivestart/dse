package org.apache.cassandra.net;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.cassandra.concurrent.TracingAwareExecutor;
import org.apache.cassandra.db.monitoring.AbortedOperationException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Request<P, Q> extends Message<P> {
   private static final Logger logger = LoggerFactory.getLogger(Request.class);
   static final InetAddress local = FBUtilities.getBroadcastAddress();
   protected final Verb<P, Q> verb;
   private final List<Request.Forward> forwards;

   Request(InetAddress from, InetAddress to, int id, Verb<P, Q> verb, Message.Data<P> data) {
      this(from, to, id, verb, data, Collections.emptyList());
   }

   Request(InetAddress from, InetAddress to, int id, Verb<P, Q> verb, Message.Data<P> data, List<Request.Forward> forwards) {
      super(from, to, id, data);
      this.verb = verb;
      this.forwards = forwards;
   }

   @VisibleForTesting
   public static <P, Q> Request<P, Q> fakeTestRequest(InetAddress to, int id, Verb<P, Q> verb, P payload) {
      return fakeTestRequest(FBUtilities.getBroadcastAddress(), to, id, verb, payload, System.currentTimeMillis());
   }

   @VisibleForTesting
   public static <P, Q> Request<P, Q> fakeTestRequest(InetAddress from, InetAddress to, int id, Verb<P, Q> verb, P payload, long createAtMillis) {
      return new Request(from, to, id, verb, new Message.Data(payload, -1L, createAtMillis, verb.isOneWay()?9223372036854775807L:verb.timeoutSupplier().get(payload)));
   }

   public Message.Type type() {
      return Message.Type.REQUEST;
   }

   boolean isForwarded() {
      return false;
   }

   public Verb<P, Q> verb() {
      return this.verb;
   }

   List<Request.Forward> forwards() {
      return this.forwards;
   }

   public Response<Q> respond(Q payload) {
      return new Response(local, this.from(), this.id(), this.verb, this.responseData(payload));
   }

   Message.Data<Q> responseData(Q payload) {
      long serializedSize = this.from().equals(local)?-1L:MessagingService.current_version.serializer(this.verb()).responseSerializer.serializedSize(payload);
      return this.messageData.withPayload(payload, serializedSize);
   }

   public Request<P, Q> addParameters(MessageParameters parameters) {
      return new Request(this.from(), this.to(), this.id(), this.verb(), this.messageData.withAddedParameters(parameters), this.forwards);
   }

   public FailureResponse<Q> respondWithFailure(RequestFailureReason reason) {
      return new FailureResponse(local, this.from(), this.id(), this.verb, reason, this.messageData.withPayload((Object)null, -1L));
   }

   TracingAwareExecutor requestExecutor() {
      return this.verb().requestExecutor().get(this.payload());
   }

   TracingAwareExecutor responseExecutor() {
      return this.verb().responseExecutor().get(this.payload());
   }

   public void execute(Consumer<Response<Q>> responseHandler, Runnable onAborted) {
      try {
         CompletableFuture<Response<Q>> future = this.verb.handler().handle(this);

         assert future != null || this.verb.isOneWay();

         if(future != null) {
            future.thenAccept(responseHandler).exceptionally((e) -> {
               if(e instanceof CompletionException && e.getCause() != null) {
                  e = e.getCause();
               }

               if(e instanceof AbortedOperationException) {
                  onAborted.run();
               } else if(e instanceof DroppingResponseException) {
                  DroppingResponseException err = (DroppingResponseException)e;
                  logger.debug("Dropping response to {} as requested by the handler (it threw DroppingResponseException).{}", this.to(), err.getMessage() == null?"":String.format(" Reason: %s", new Object[]{err.getMessage()}));
               } else {
                  logger.error("Unexpected error thrown by {} handler; this should have been caught in the handler", this.verb, e);
               }

               return null;
            });
         }
      } catch (AbortedOperationException var4) {
         onAborted.run();
      } catch (DroppingResponseException var5) {
         logger.debug("Dropping response to {} as requested by the handler (it threw DroppingResponseException)", this.to());
      }

   }

   Iterable<ForwardRequest<P, Q>> forwardRequests() {
      return Iterables.transform(this.forwards, (fwd) -> {
         return new ForwardRequest(local, fwd.to, this.from(), fwd.id, this.verb, this.messageData);
      });
   }

   long payloadSerializedSize(MessagingVersion version) {
      return this.messageData.payloadSize >= 0L && version == MessagingService.current_version?this.messageData.payloadSize:version.serializer(this.verb()).requestSerializer.serializedSize(this.payload());
   }

   public static class Dispatcher<P, Q> {
      protected final MessageTargets targets;
      protected final Verb<P, Q> verb;
      protected final Message.Data<P> messageData;

      Dispatcher(MessageTargets targets, Verb<P, Q> verb, Message.Data<P> data) {
         this.targets = targets;
         this.verb = verb;
         this.messageData = data;
      }

      Verb<P, Q> verb() {
         return this.verb;
      }

      boolean hasLocalRequest() {
         return this.targets.hasLocal();
      }

      Iterable<? extends Request<P, Q>> remoteRequests() {
         Iterable<Request<P, Q>> withoutForwards = Iterables.transform(this.targets.nonForwardingRemotes(), (to) -> {
            return this.verb.newRequest(to, this.messageData);
         });
         return !this.targets.hasForwards()?withoutForwards:Iterables.concat(withoutForwards, Iterables.transform(this.targets.remotesWithForwards(), (t) -> {
            return this.verb.newRequestWithForwards(t.target, this.messageData, t.forwards);
         }));
      }

      Request<P, Q> localRequest() {
         return this.verb.newRequest(Request.local, this.messageData);
      }
   }

   static class Forward {
      final InetAddress to;
      final int id;

      Forward(InetAddress to, int id) {
         this.to = to;
         this.id = id;
      }

      private Forward(InetAddress to) {
         this(to, MessagingService.newMessageId());
      }

      static List<Request.Forward> from(List<InetAddress> hosts) {
         List<Request.Forward> r = new ArrayList(hosts.size());
         Iterator var2 = hosts.iterator();

         while(var2.hasNext()) {
            InetAddress host = (InetAddress)var2.next();
            r.add(new Request.Forward(host));
         }

         return r;
      }
   }
}
