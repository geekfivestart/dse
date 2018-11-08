package com.datastax.bdp.db.tools.nodesync;

import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.util.concurrent.Uninterruptibles;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.cassandra.utils.units.TimeValue;

@Command(
   name = "show",
   description = "Display the events of a NodeSync tracing session"
)
public class ShowTracing extends NodeSyncTracingCommand {
   @Option(
      name = {"-i", "--id"},
      description = "The trace ID to show. If omitted, this will check if some nodes have tracing enable, and if all node that have it use the same trace ID, it will default to showing that. Otherwise, the command error out."
   )
   public String traceIdStr;
   @Option(
      name = {"-f", "--follow"},
      description = "Continuously show the trace events, showing new events as they come. Note that this won't exit unless you either manually exit (with Ctrl-c) or use a timeout (--timeout option)"
   )
   private boolean follow;
   @Option(
      name = {"-t", "--timeout"},
      description = "When --follow is used, automatically exit after the provided amountof time elapses. This default to seconds, but a 's', 'm' or 'h' suffix can be used for seconds, minutes or hours respectively."
   )
   private String timeoutStr;
   @Option(
      name = {"-c", "--color"},
      description = "Colorize each trace event according from which host it originates from. Has no effect if the trace only come from a single host"
   )
   private boolean useColors;

   public ShowTracing() {
   }

   static long parseTimeoutSec(String timeoutStr) {
      if(timeoutStr == null) {
         return -1L;
      } else {
         String timeout = timeoutStr.trim();
         TimeUnit unit = TimeUnit.SECONDS;
         int last = timeout.length() - 1;
         switch(timeout.toLowerCase().charAt(last)) {
         case 'h':
            unit = TimeUnit.HOURS;
            timeout = timeout.substring(0, last);
            break;
         case 'm':
            unit = TimeUnit.MINUTES;
            timeout = timeout.substring(0, last);
            break;
         case 's':
            timeout = timeout.substring(0, last);
         }

         try {
            return unit.toSeconds(Long.parseLong(timeout.trim()));
         } catch (NumberFormatException var5) {
            throw new NodeSyncException("Invalid timeout value: " + timeout);
         }
      }
   }

   private UUID getTraceId(Set<InetAddress> nodes, NodeProbes probes) {
      if(this.traceIdStr != null) {
         this.traceIdStr = this.traceIdStr.trim();

         try {
            return UUID.fromString(this.traceIdStr);
         } catch (Exception var6) {
            throw new NodeSyncException(String.format("Invalid id provided for the trace session (got '%s' but must be a UUID)", new Object[]{this.traceIdStr}));
         }
      } else {
         TracingStatus.Status status = TracingStatus.checkStatus(nodes, probes, (x$0) -> {
            this.printVerbose(x$0, new Object[0]);
         }, (x$0) -> {
            this.printWarning(x$0, new Object[0]);
         });
         UUID id = status.traceIdIfCommon();
         if(id == null) {
            String reason = status.enabled.isEmpty()?"no node have tracing enabled.":"not all node with tracing enabled use the same tracing ID; use --id to disambiguate.";
            throw new NodeSyncException("Cannot auto-detect the trace ID: " + reason);
         } else {
            return id;
         }
      }
   }

   protected void execute(Metadata metadata, Session session, NodeProbes probes) {
      Set<InetAddress> nodes = this.nodes(metadata);
      UUID traceId = this.getTraceId(nodes, probes);
      (new ShowTracing.TraceDisplayer(session, traceId, this.follow, parseTimeoutSec(this.timeoutStr), this.useColors && nodes.size() > 1, 500L, nodes.size() > 1, () -> {
      })).run();
   }

   static class TraceDisplayer implements Runnable {
      private final Session session;
      private final UUID tracingId;
      private final boolean continuous;
      private final long timeoutSec;
      private final boolean useColors;
      private final long delayBetweenQueryMs;
      private final boolean showHost;
      private final Runnable runOnExit;
      private final BlockingQueue<ShowTracing.TraceDisplayer.TraceEvent> events = new LinkedBlockingQueue();

      TraceDisplayer(Session session, UUID tracingId, boolean continuous, long timeoutSec, boolean useColors, long delayBetweenQueryMs, boolean showHost, Runnable runOnExit) {
         this.session = session;
         this.tracingId = tracingId;
         this.continuous = continuous;
         this.timeoutSec = timeoutSec;
         this.useColors = useColors;
         this.showHost = showHost;
         this.delayBetweenQueryMs = delayBetweenQueryMs;
         this.runOnExit = runOnExit;
      }

      public void run() {
         ShowTracing.TraceDisplayer.TraceFetcher fetcher = new ShowTracing.TraceDisplayer.TraceFetcher();
         fetcher.start();
         ScheduledExecutorService service = null;
         if(this.timeoutSec > 0L) {
            service = Executors.newSingleThreadScheduledExecutor();
            service.schedule(() -> {
               fetcher.stopBlocking();
            }, this.timeoutSec, TimeUnit.SECONDS);
         }

         Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            fetcher.stopBlocking();
            this.runOnExit.run();
         }));
         this.displayEvents(this.events);
         if(service != null) {
            service.shutdown();
         }

         this.runOnExit.run();
      }

      private void displayEvents(BlockingQueue<ShowTracing.TraceDisplayer.TraceEvent> events) {
         ShowTracing.TraceDisplayer.Colorizer colorizer = this.useColors?new ShowTracing.TraceDisplayer.Colorizer():ShowTracing.TraceDisplayer.Colorizer.NOOP;

         ShowTracing.TraceDisplayer.TraceEvent event;
         while((event = (ShowTracing.TraceDisplayer.TraceEvent)Uninterruptibles.takeUninterruptibly(events)) != ShowTracing.TraceDisplayer.TraceEvent.SENTINEL) {
            String msg = this.showHost?String.format("%s: %s (elapsed: %s)", new Object[]{event.source, event.description, event.sourceElapsed}):String.format("%s (elapsed: %s)", new Object[]{event.description, event.sourceElapsed});
            System.out.println(colorizer.color(event.source, msg));
         }

      }

      private class TraceFetcher extends Thread {
         private final PreparedStatement query;
         private volatile boolean stopped;
         private long lastQueryTimestamp;
         private UUID lastEventId;

         private TraceFetcher() {
            this.lastQueryTimestamp = -1L;
            this.lastEventId = UUIDs.startOf(0L);
            this.query = TraceDisplayer.this.session.prepare("SELECT * FROM system_traces.events WHERE session_id=" + TraceDisplayer.this.tracingId + " AND event_id > ?");
         }

         private void stopBlocking() {
            this.stopped = true;
            Uninterruptibles.joinUninterruptibly(this);
         }

         public void run() {
            while(true) {
               label30: {
                  if(!this.stopped) {
                     if(this.lastQueryTimestamp <= 0L) {
                        break label30;
                     }

                     if(TraceDisplayer.this.continuous) {
                        long timeSinceLast = System.currentTimeMillis() - this.lastQueryTimestamp;
                        long toSleep = TraceDisplayer.this.delayBetweenQueryMs - timeSinceLast;
                        Uninterruptibles.sleepUninterruptibly(toSleep, TimeUnit.MILLISECONDS);
                        break label30;
                     }

                     this.stopped = true;
                  }

                  TraceDisplayer.this.events.offer(ShowTracing.TraceDisplayer.TraceEvent.SENTINEL);
                  return;
               }

               this.lastQueryTimestamp = System.currentTimeMillis();

               try {
                  ResultSet result = TraceDisplayer.this.session.execute(this.query.bind(new Object[]{this.lastEventId}));
                  Iterator var2 = result.iterator();

                  while(var2.hasNext()) {
                     Row row = (Row)var2.next();
                     this.lastEventId = row.getUUID("event_id");
                     String description = row.getString("activity");
                     InetAddress source = row.getInet("source");
                     TimeValue sourceElapsed = TimeValue.of((long)row.getInt("source_elapsed"), TimeUnit.MICROSECONDS);
                     TraceDisplayer.this.events.offer(new ShowTracing.TraceDisplayer.TraceEvent(description, source, sourceElapsed));
                  }
               } catch (Exception var7) {
                  System.err.println(String.format("Error reading trace events (%s); will retry", new Object[]{var7.getMessage()}));
               }
            }
         }
      }

      private static class TraceEvent {
         private static final ShowTracing.TraceDisplayer.TraceEvent SENTINEL = new ShowTracing.TraceDisplayer.TraceEvent((String)null, (InetAddress)null, (TimeValue)null);
         private final String description;
         private final InetAddress source;
         private final TimeValue sourceElapsed;

         private TraceEvent(String description, InetAddress source, TimeValue sourceElapsed) {
            this.description = description;
            this.source = source;
            this.sourceElapsed = sourceElapsed;
         }
      }

      private static class Colorizer {
         private static final ShowTracing.TraceDisplayer.Colorizer NOOP = new ShowTracing.TraceDisplayer.Colorizer() {
            String color(InetAddress host, String str) {
               return str;
            }
         };
         private final Map<InetAddress, ShowTracing.TraceDisplayer.Color> colors;
         private int i;

         private Colorizer() {
            this.colors = new HashMap();
         }

         String color(InetAddress host, String str) {
            ShowTracing.TraceDisplayer.Color c = (ShowTracing.TraceDisplayer.Color)this.colors.get(host);
            if(c == null) {
               c = ShowTracing.TraceDisplayer.Color.values()[this.i++ % ShowTracing.TraceDisplayer.Color.values().length];
               this.colors.put(host, c);
            }

            return c.set(str);
         }
      }

      private static enum Color {
         GREEN("\u001b[32m"),
         BLUE("\u001b[34m"),
         YELLOW("\u001b[33m"),
         PURPLE("\u001b[35m"),
         CYAN("\u001b[36m"),
         RED("\u001b[31m");

         private static final String RESET = "\u001b[0m";
         private final String code;

         private Color(String code) {
            this.code = code;
         }

         String set(String str) {
            return this.code + str + "\u001b[0m";
         }
      }
   }
}
