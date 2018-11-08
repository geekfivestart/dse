package org.apache.cassandra.service;

import com.google.common.annotations.VisibleForTesting;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.metrics.AuthMetrics;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NativeTransportService {
   private static final Logger logger = LoggerFactory.getLogger(NativeTransportService.class);
   private static final int ON_CLOSE_WAIT_TIMEOUT_SECS = 5;
   private List<Server> servers = Collections.emptyList();
   private boolean initialized = false;
   private final InetAddress nativeAddr;
   private final int nativePort;

   @VisibleForTesting
   public NativeTransportService(InetAddress nativeAddr, int nativePort) {
      this.nativeAddr = nativeAddr;
      this.nativePort = nativePort;
   }

   public NativeTransportService() {
      this.nativeAddr = DatabaseDescriptor.getNativeTransportAddress();
      this.nativePort = DatabaseDescriptor.getNativeTransportPort();
   }

   @VisibleForTesting
   synchronized void initialize() {
      if(!this.initialized) {
         int nativePortSSL = DatabaseDescriptor.getNativeTransportPortSSL();
         Server.Builder builder;
         if(!DatabaseDescriptor.getClientEncryptionOptions().enabled) {
            this.servers = new ArrayList(1);
            builder = (new Server.Builder()).withHost(this.nativeAddr).withPort(this.nativePort).withSSL(false);
            this.servers.add(builder.build());
         } else {
            builder = (new Server.Builder()).withHost(this.nativeAddr);
            if(this.nativePort != nativePortSSL) {
               this.servers = Collections.unmodifiableList(Arrays.asList(new Server[]{builder.withSSL(false).withPort(this.nativePort).build(), builder.withSSL(true).withPort(nativePortSSL).build()}));
            } else {
               this.servers = Collections.singletonList(builder.withSSL(true).withPort(this.nativePort).build());
            }
         }

         ClientMetrics.instance.addCounter("connectedNativeClients", () -> {
            int ret = 0;

            Server server;
            for(Iterator var2 = this.servers.iterator(); var2.hasNext(); ret += server.getConnectedClients()) {
               server = (Server)var2.next();
            }

            return Integer.valueOf(ret);
         });
         AuthMetrics.init();
         this.initialized = true;
      }
   }

   public void start() {
      this.initialize();
      this.servers.forEach(Server::start);
   }

   public void stop() {
      try {
         this.stopAsync().get(5L, TimeUnit.SECONDS);
      } catch (Throwable var2) {
         JVMStabilityInspector.inspectThrowable(var2);
         logger.error("Failed to wait for native transport service to stop cleanly", var2);
      }

   }

   public CompletableFuture stopAsync() {
      return CompletableFuture.allOf((CompletableFuture[])this.servers.stream().map(Server::stop).toArray((x$0) -> {
         return new CompletableFuture[x$0];
      }));
   }

   public void destroy() {
      this.stop();
      this.servers = Collections.emptyList();
   }

   public boolean isRunning() {
      Iterator var1 = this.servers.iterator();

      Server server;
      do {
         if(!var1.hasNext()) {
            return false;
         }

         server = (Server)var1.next();
      } while(!server.isRunning());

      return true;
   }

   @VisibleForTesting
   Collection<Server> getServers() {
      return this.servers;
   }
}
