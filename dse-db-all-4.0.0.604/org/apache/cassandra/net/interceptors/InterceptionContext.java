package org.apache.cassandra.net.interceptors;

import java.util.List;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InterceptionContext<M extends Message<?>> {
   private static final Logger logger = LoggerFactory.getLogger(InterceptionContext.class);
   private final MessageDirection direction;
   private final Consumer<M> handler;
   @Nullable
   private final Consumer<Response<?>> responseCallback;
   private final List<Interceptor> pipeline;
   private int next;

   InterceptionContext(MessageDirection direction, Consumer<M> handler, Consumer<Response<?>> responseCallback, List<Interceptor> pipeline) {
      this(direction, handler, responseCallback, pipeline, 0);
   }

   private InterceptionContext(MessageDirection direction, Consumer<M> handler, Consumer<Response<?>> responseCallback, List<Interceptor> pipeline, int next) {
      this.direction = direction;
      this.handler = handler;
      this.responseCallback = responseCallback;
      this.pipeline = pipeline;
      this.next = next;
   }

   public boolean isReceiving() {
      return this.direction() == MessageDirection.RECEIVING;
   }

   public boolean isSending() {
      return this.direction() == MessageDirection.SENDING;
   }

   public MessageDirection direction() {
      return this.direction;
   }

   public Consumer<Response<?>> responseCallback() {
      return this.responseCallback;
   }

   public void passDown(M message) {
      if(this.next >= this.pipeline.size()) {
         this.handler.accept(message);
      } else {
         ((Interceptor)this.pipeline.get(this.next++)).intercept(message, this);
      }

   }

   public void drop(M message) {
      logger.trace("Message {} dropped by interceptor", message);
   }
}
