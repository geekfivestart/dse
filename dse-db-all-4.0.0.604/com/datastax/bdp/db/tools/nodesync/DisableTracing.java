package com.datastax.bdp.db.tools.nodesync;

import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import io.airlift.airline.Command;
import java.net.InetAddress;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Consumer;

@Command(
   name = "disable",
   description = "Disable NodeSync tracing"
)
public class DisableTracing extends NodeSyncTracingCommand {
   public DisableTracing() {
   }

   protected void execute(Metadata metadata, Session session, NodeProbes probes) {
      disableTracing(this.nodes(metadata), probes, (x$0) -> {
         this.printVerbose(x$0, new Object[0]);
      }, (x$0) -> {
         this.printWarning(x$0, new Object[0]);
      });
   }

   static void disableTracing(Set<InetAddress> nodes, NodeProbes probes, Consumer<String> verboseLogger, Consumer<String> warningLogger) {
      Iterator var4 = nodes.iterator();

      while(var4.hasNext()) {
         InetAddress address = (InetAddress)var4.next();

         try {
            probes.disableNodeSyncTracing(address);
            verboseLogger.accept(String.format("Disabled tracing on %s", new Object[]{address}));
         } catch (Exception var7) {
            warningLogger.accept(String.format("failed to disable tracing on %s: %s", new Object[]{address, var7.getMessage()}));
         }
      }

   }
}
