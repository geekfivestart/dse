package com.datastax.bdp.db.tools.nodesync;

import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableMap.Builder;
import io.airlift.airline.Command;
import java.net.InetAddress;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

@Command(
   name = "status",
   description = "Check status of NodeSync tracing"
)
public class TracingStatus extends NodeSyncTracingCommand {
   public TracingStatus() {
   }

   protected void execute(Metadata metadata, Session session, NodeProbes probes) {
      Set<InetAddress> nodes = this.nodes(metadata);
      TracingStatus.Status status = checkStatus(nodes, probes, (x$0) -> {
         this.printVerbose(x$0, new Object[0]);
      }, (x$0) -> {
         this.printWarning(x$0, new Object[0]);
      });
      if(!this.isOnAllNodes() && nodes.size() == 1) {
         System.out.println("Tracing is " + (status.enabled.isEmpty()?"disabled":"enabled"));
      } else if(status.disabled.isEmpty()) {
         System.out.println(String.format("Tracing is enabled on all%s nodes", new Object[]{this.isOnAllNodes()?"":" requested"}));
      } else if(status.enabled.isEmpty()) {
         System.out.println(String.format("Tracing is disabled on all%s nodes", new Object[]{this.isOnAllNodes()?"":" requested"}));
      } else {
         System.out.println("Tracing is only enabled on " + status.enabled.keySet());
      }

   }

   static TracingStatus.Status checkStatus(Set<InetAddress> nodes, NodeProbes probes, Consumer<String> verboseLogger, Consumer<String> warningLogger) {
      Builder<InetAddress, UUID> enabled = ImmutableMap.builder();
      com.google.common.collect.ImmutableSet.Builder<InetAddress> disabled = ImmutableSet.builder();
      Iterator var6 = nodes.iterator();

      while(var6.hasNext()) {
         InetAddress address = (InetAddress)var6.next();

         try {
            UUID id = probes.currentNodeSyncTracingSession(address);
            if(id == null) {
               verboseLogger.accept(String.format("Tracing disabled on %s", new Object[]{address}));
               disabled.add(address);
            } else {
               verboseLogger.accept(String.format("Tracing enabled on %s with id %s", new Object[]{address, id}));
               enabled.put(address, id);
            }
         } catch (Exception var9) {
            warningLogger.accept(String.format("failed to retrieve tracing status on %s: %s", new Object[]{address, var9.getMessage()}));
            disabled.add(address);
         }
      }

      return new TracingStatus.Status(enabled.build(), disabled.build());
   }

   static class Status {
      final ImmutableMap<InetAddress, UUID> enabled;
      final ImmutableSet<InetAddress> disabled;

      private Status(ImmutableMap<InetAddress, UUID> enabled, ImmutableSet<InetAddress> disabled) {
         this.enabled = enabled;
         this.disabled = disabled;
      }

      UUID traceIdIfCommon() {
         if(this.enabled.isEmpty()) {
            return null;
         } else {
            Iterator<UUID> iter = this.enabled.values().iterator();
            UUID common = (UUID)iter.next();

            do {
               if(!iter.hasNext()) {
                  return common;
               }
            } while(common.equals(iter.next()));

            return null;
         }
      }
   }
}
