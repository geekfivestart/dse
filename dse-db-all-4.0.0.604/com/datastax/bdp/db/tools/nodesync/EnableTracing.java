package com.datastax.bdp.db.tools.nodesync;

import com.datastax.bdp.db.nodesync.TracingLevel;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.cassandra.utils.UUIDGen;

@Command(
   name = "enable",
   description = "Enable NodeSync tracing"
)
public class EnableTracing extends NodeSyncTracingCommand {
   @Option(
      name = {"-l", "--level"},
      description = "The tracing level: either 'low' or 'high'. If omitted, the 'low' level is used. Note that the 'high' level is somewhat verbose and should be used with care."
   )
   private String levelStr = "low";
   @Option(
      name = {"--tables"},
      description = "A comma separated list of fully-qualified table names to trace. If omitted, all tables are trace."
   )
   private String tableStr;
   @Option(
      name = {"-f", "--follow"},
      description = "After having enabled tracing, continuously show the trace events,showing new events as they come. Note that this won't exit unless you either manually exit (with Ctrl-c) or use a timeout (--timeout option)"
   )
   private boolean follow;
   @Option(
      name = {"-t", "--timeout"},
      description = "Timeout on the tracing; after that amount of time, tracing will be automatically disabled (and if --follow is used, the command will return). This default in seconds, but a 's', 'm' or 'h' suffix can be used for seconds, minutes or hours respectively."
   )
   private String timeoutStr;
   @Option(
      name = {"-c", "--color"},
      description = "If --follow is used, color each trace event according from which host it originates from"
   )
   private boolean useColors;

   public EnableTracing() {
   }

   private Map<String, String> makeTracingOptions(UUID id, long timeoutSec) {
      Builder<String, String> builder = ImmutableMap.builder();
      builder.put("id", id.toString());
      builder.put("level", TracingLevel.parse(this.levelStr).toString());
      if(this.timeoutStr != null) {
         builder.put("timeout_sec", Long.toString(timeoutSec));
      }

      if(this.tableStr != null) {
         builder.put("tables", this.tableStr);
      }

      return builder.build();
   }

   protected void execute(Metadata metadata, Session session, NodeProbes probes) {
      UUID id = UUIDGen.getTimeUUID();
      long timeoutSec = ShowTracing.parseTimeoutSec(this.timeoutStr);
      Map<String, String> tracingOptions = this.makeTracingOptions(id, timeoutSec);
      Set<InetAddress> nodes = this.nodes(metadata);
      HashSet enabled = new HashSet();

      try {
         Iterator var10 = nodes.iterator();

         while(var10.hasNext()) {
            InetAddress address = (InetAddress)var10.next();

            try {
               probes.enableNodeSyncTracing(address, tracingOptions);
               this.printVerbose("Enabled tracing on %s with id %s", new Object[]{address, id});
               enabled.add(address);
            } catch (IllegalStateException | IllegalArgumentException var13) {
               throw new NodeSyncException(String.format("Unable to enable tracing on %s: %s", new Object[]{address, var13.getMessage()}));
            }
         }
      } catch (RuntimeException var14) {
         if(!enabled.isEmpty()) {
            this.printVerbose("Failed enabling tracing on all nodes, disabling it on just enabled ones", new Object[0]);
            DisableTracing.disableTracing(enabled, probes, (x$0) -> {
               this.printVerbose(x$0, new Object[0]);
            }, (x$0) -> {
               this.printWarning(x$0, new Object[0]);
            });
         }

         throw var14;
      }

      if(this.follow) {
         this.printVerbose("Displaying events for session id %s...", new Object[]{id});
         (new ShowTracing.TraceDisplayer(session, id, true, timeoutSec, this.useColors && nodes.size() > 1, 500L, nodes.size() > 1, () -> {
            if(timeoutSec <= 0L) {
               this.printWarning("Do not forget to stop tracing with 'nodesync tracing disable'.", new Object[0]);
            }

         })).run();
      } else {
         if(timeoutSec <= 0L) {
            this.printWarning("Do not forget to stop tracing with 'nodesync tracing disable'.", new Object[0]);
         }

         System.out.println("Enabled tracing. Session id is " + id);
      }

   }
}
