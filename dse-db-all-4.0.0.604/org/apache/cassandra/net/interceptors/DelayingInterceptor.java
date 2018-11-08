package org.apache.cassandra.net.interceptors;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.net.Message;

public class DelayingInterceptor extends AbstractInterceptor {
   private static final String DELAY_PROPERTY = "message_delay_ms";
   private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
   private final int messageDelay = this.messageDelay();

   public DelayingInterceptor(String name) {
      super(name, ImmutableSet.of(), Sets.immutableEnumSet(Message.Type.REQUEST, new Message.Type[0]), Sets.immutableEnumSet(MessageDirection.RECEIVING, new MessageDirection[0]), Sets.immutableEnumSet(Message.Locality.all()));
   }

   private int messageDelay() {
      String delayStr = getProperty("message_delay_ms");
      if(delayStr == null) {
         throw new ConfigurationException(String.format("Missing -D%s%s property for delaying interceptor", new Object[]{"dse.net.interceptors.", "message_delay_ms"}));
      } else {
         return Integer.valueOf(delayStr).intValue();
      }
   }

   protected <M extends Message<?>> void handleIntercepted(M message, InterceptionContext<M> context) {
      this.executor.schedule(() -> {
         context.passDown(message);
      }, (long)this.messageDelay, TimeUnit.MILLISECONDS);
   }
}
