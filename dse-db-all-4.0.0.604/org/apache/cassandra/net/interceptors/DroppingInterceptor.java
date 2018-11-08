package org.apache.cassandra.net.interceptors;

import com.google.common.collect.ImmutableSet;
import org.apache.cassandra.net.Message;

public class DroppingInterceptor extends AbstractInterceptor {
   public DroppingInterceptor(String name) {
      super(name, ImmutableSet.of(), Message.Type.all(), MessageDirection.all(), Message.Locality.all());
   }

   protected <M extends Message<?>> void handleIntercepted(M message, InterceptionContext<M> context) {
      context.drop(message);
   }
}
