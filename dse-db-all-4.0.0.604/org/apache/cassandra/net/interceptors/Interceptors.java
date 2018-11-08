package org.apache.cassandra.net.interceptors;

import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageCallback;
import org.apache.cassandra.net.Request;
import org.apache.cassandra.net.Response;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Interceptors {
   private static final String PROPERTY = "dse.net.interceptors";
   static final String PROPERTY_PREFIX = "dse.net.interceptors.";
   private static final Logger logger = LoggerFactory.getLogger(Interceptors.class);
   private final AtomicReference<List<Interceptor>> interceptors = new AtomicReference(Collections.emptyList());

   public Interceptors() {
   }

   public void load() {
      String names = System.getProperty("dse.net.interceptors");
      if(names != null) {
         names = names.trim();
         if(names.charAt(0) == 39 || names.charAt(0) == 34) {
            names = names.substring(1, names.length() - 1);
         }

         this.load(names);
      }
   }

   private void load(String classNames) {
      if(classNames != null && !classNames.isEmpty()) {
         List<String> names = Splitter.on(",").trimResults().omitEmptyStrings().splitToList(classNames);
         List<Interceptor> loaded = new ArrayList(names.size());
         Iterator var4 = names.iterator();

         while(var4.hasNext()) {
            String name = (String)var4.next();
            if(!name.contains(".")) {
               name = "org.apache.cassandra.net.interceptors." + name;
            }

            String jmxName;
            List parts;
            if(name.contains("=")) {
               parts = Splitter.on("=").trimResults().splitToList(name);
               if(parts.size() != 2) {
                  throw new ConfigurationException("Invalid interceptor name declaration: " + name);
               }

               name = (String)parts.get(0);
               jmxName = (String)parts.get(1);
            } else {
               parts = Splitter.on(".").trimResults().splitToList(name);
               jmxName = (String)parts.get(parts.size() - 1);
            }

            try {
               Class<Interceptor> cls = FBUtilities.classForName(name, "Interceptor");
               Constructor<Interceptor> ctor = cls.getConstructor(new Class[]{String.class});
               loaded.add(ctor.newInstance(new Object[]{jmxName}));
               logger.info("Using class {} to intercept internode messages (You shouldn't see this unless this is running a test)", name);
            } catch (Exception var9) {
               logger.error("Error instantiating internode message interceptor class {}: {}", name, var9.getMessage());
               throw new IllegalStateException(var9);
            }
         }

         this.interceptors.set(loaded);
      }
   }

   public void add(Interceptor interceptor) {
      List current;
      ArrayList updated;
      do {
         current = (List)this.interceptors.get();
         updated = new ArrayList(current.size() + 1);
         updated.addAll(current);
         updated.add(interceptor);
      } while(!this.interceptors.compareAndSet(current, updated));

   }

   public void remove(Interceptor interceptor) {
      List current;
      ArrayList updated;
      do {
         current = (List)this.interceptors.get();
         if(!current.contains(interceptor)) {
            return;
         }

         updated = new ArrayList(current.size() - 1);
         Iterables.addAll(updated, Iterables.filter(current, (i) -> {
            return !i.equals(interceptor);
         }));
      } while(!this.interceptors.compareAndSet(current, updated));

   }

   public void clear() {
      this.interceptors.set(Collections.emptyList());
   }

   public boolean isEmpty() {
      return this.interceptors.get() == null;
   }

   public <M extends Message<?>> void intercept(M message, Consumer<M> handler, Consumer<Response<?>> responseCallback) {
      List<Interceptor> snapshot = (List)this.interceptors.get();
      if(snapshot.isEmpty()) {
         handler.accept(message);
      } else if(message.isLocal()) {
         this.doIntercept(message, MessageDirection.SENDING, snapshot, (msg) -> {
            this.doIntercept(msg, MessageDirection.RECEIVING, snapshot, handler, responseCallback);
         }, responseCallback);
      } else {
         MessageDirection direction = message.from().equals(FBUtilities.getBroadcastAddress())?MessageDirection.SENDING:MessageDirection.RECEIVING;
         this.doIntercept(message, direction, snapshot, handler, responseCallback);
      }

   }

   public <P, Q, M extends Request<P, Q>> void interceptRequest(M request, Consumer<M> handler, MessageCallback<Q> responseCallback) {
      this.intercept(request, handler, (response) -> {
         response.deliverTo(responseCallback);
      });
   }

   private <M extends Message<?>> void doIntercept(M message, MessageDirection direction, List<Interceptor> pipeline, Consumer<M> handler, Consumer<Response<?>> responseCallback) {
      assert message.verb().isOneWay() || !(message instanceof Request) || direction != MessageDirection.SENDING || responseCallback != null;

      InterceptionContext<M> ctx = new InterceptionContext(direction, handler, responseCallback, pipeline);
      ctx.passDown(message);
   }
}
