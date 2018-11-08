package org.apache.cassandra.net.interceptors;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.cassandra.net.EmptyPayload;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Request;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.net.Verbs;

public class FakeWriteInterceptor extends AbstractInterceptor {
   private static final ImmutableSet<Verb<?, ?>> intercepted;

   public FakeWriteInterceptor(String name) {
      super(name, intercepted, Sets.immutableEnumSet(Message.Type.REQUEST, new Message.Type[0]), Sets.immutableEnumSet(MessageDirection.RECEIVING, new MessageDirection[0]), Sets.immutableEnumSet(Message.Locality.all()));
   }

   protected boolean allowModifyingIntercepted() {
      return false;
   }

   protected <M extends Message<?>> void handleIntercepted(M message, InterceptionContext<M> context) {
      assert message.isRequest() && intercepted.contains(message.verb()) && context.direction() == MessageDirection.RECEIVING;

      Request<?, EmptyPayload> request = (Request)message;
      context.drop(message);
      context.responseCallback().accept(request.respond(EmptyPayload.instance));
   }

   static {
      intercepted = ImmutableSet.of(Verbs.WRITES.WRITE, Verbs.WRITES.VIEW_WRITE, Verbs.WRITES.BATCH_STORE, Verbs.WRITES.COUNTER_FORWARDING, Verbs.WRITES.READ_REPAIR, Verbs.HINTS.HINT, new Verb[0]);
   }
}
