package com.datastax.bdp.db.tools.nodesync;

import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.VersionNumber;
import java.net.InetAddress;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.utils.Pair;

public class NodeProbes implements AutoCloseable {
   private static final VersionNumber PROXYING_MIN_VERSION = VersionNumber.parse("4.0.0.603");
   private final Metadata metadata;
   private final NodeProbeBuilder builder;
   private final InetAddress contactPointAddress;
   private final boolean contactPointSupportsProxying;
   private NodeProbe localProbe;
   private Pair<InetAddress, NodeProbe> lastRemoteProbe;

   public NodeProbes(Metadata metadata, NodeProbeBuilder builder, InetAddress contactPointAddress) {
      this.metadata = metadata;
      this.builder = builder;
      this.contactPointAddress = contactPointAddress;
      this.contactPointSupportsProxying = this.supportsProxying(contactPointAddress);
   }

   void startUserValidation(InetAddress address, Map<String, String> options) {
      this.request(address, (p) -> {
         p.startUserValidation(address, options);
      }, (p) -> {
         p.startUserValidation(options);
      });
   }

   void cancelUserValidation(InetAddress address, String id) {
      this.request(address, (p) -> {
         p.cancelUserValidation(address, id);
      }, (p) -> {
         p.cancelUserValidation(id);
      });
   }

   UUID currentNodeSyncTracingSession(InetAddress address) {
      return (UUID)this.requestResponse(address, (p) -> {
         return p.currentNodeSyncTracingSession(address);
      }, NodeProbe::currentNodeSyncTracingSession);
   }

   void enableNodeSyncTracing(InetAddress address, Map<String, String> options) {
      this.request(address, (p) -> {
         p.enableNodeSyncTracing(address, options);
      }, (p) -> {
         p.enableNodeSyncTracing(options);
      });
   }

   void disableNodeSyncTracing(InetAddress address) {
      this.request(address, (probe) -> {
         probe.disableNodeSyncTracing(address);
      }, NodeProbe::disableNodeSyncTracing);
   }

   private synchronized <K> K requestResponse(InetAddress address, Function<NodeProbe, K> proxyMethod, Function<NodeProbe, K> localMethod) {
      return address.equals(this.contactPointAddress)?localMethod.apply(this.localProbe()):(this.contactPointSupportsProxying && this.supportsProxying(address)?proxyMethod.apply(this.localProbe()):localMethod.apply(this.remoteProbe(address)));
   }

   private void request(InetAddress address, Consumer<NodeProbe> proxyMethod, Consumer<NodeProbe> localMethod) {
      this.requestResponse(address, asFunction(proxyMethod), asFunction(localMethod));
   }

   private static Function<NodeProbe, Void> asFunction(Consumer<NodeProbe> consumer) {
      return (p) -> {
         consumer.accept(p);
         return null;
      };
   }

   private boolean supportsProxying(InetAddress address) {
      return this.metadata.getAllHosts().stream().filter((h) -> {
         return h.getBroadcastAddress().equals(address);
      }).anyMatch((h) -> {
         return h.getCassandraVersion().compareTo(PROXYING_MIN_VERSION) >= 0;
      });
   }

   private NodeProbe localProbe() {
      if(this.localProbe == null) {
         this.localProbe = this.builder.build(this.contactPointAddress);
      }

      return this.localProbe;
   }

   private NodeProbe remoteProbe(InetAddress address) {
      if(this.lastRemoteProbe == null || !((InetAddress)this.lastRemoteProbe.left).equals(address)) {
         if(this.lastRemoteProbe != null) {
            close((InetAddress)this.lastRemoteProbe.left, (NodeProbe)this.lastRemoteProbe.right);
            this.lastRemoteProbe = null;
         }

         NodeProbe probe = this.builder.build(address);
         this.lastRemoteProbe = Pair.create(address, probe);
      }

      return (NodeProbe)this.lastRemoteProbe.right;
   }

   public synchronized void close() {
      if(this.localProbe != null) {
         close(this.contactPointAddress, this.localProbe);
         this.localProbe = null;
      }

      if(this.lastRemoteProbe != null) {
         close((InetAddress)this.lastRemoteProbe.left, (NodeProbe)this.lastRemoteProbe.right);
         this.lastRemoteProbe = null;
      }

   }

   private static void close(InetAddress address, NodeProbe probe) {
      try {
         probe.close();
      } catch (Exception var3) {
         System.err.printf("Unable to close JMX connection to %s: %s%n", new Object[]{address, var3.getMessage()});
      }

   }
}
