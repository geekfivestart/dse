package org.apache.cassandra.tools;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.JdkSSLOptions;
import com.datastax.driver.core.SSLOptions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Iterator;
import java.util.Set;
import javax.net.ssl.SSLContext;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.io.sstable.SSTableLoader;
import org.apache.cassandra.security.SSLFactory;
import org.apache.cassandra.streaming.ProgressInfo;
import org.apache.cassandra.streaming.SessionInfo;
import org.apache.cassandra.streaming.StreamConnectionFactory;
import org.apache.cassandra.streaming.StreamEvent;
import org.apache.cassandra.streaming.StreamEventHandler;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.NativeSSTableLoaderClient;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class BulkLoader {
   public BulkLoader() {
   }

   public static void main(String[] args) {
      LoaderOptions options = LoaderOptions.builder().parseArgs(args).build();
      if(options.configFile != null) {
         System.setProperty("cassandra.config", options.configFile.toString());
      }

      try {
         load(options);
      } catch (Throwable var3) {
         var3.printStackTrace();
         System.exit(1);
      }

      System.exit(0);
   }

   public static void load(LoaderOptions options) throws BulkLoadException {
      DatabaseDescriptor.toolInitialization();
      OutputHandler handler = new OutputHandler.SystemOutput(options.verbose, options.debug);
      SSTableLoader loader = new SSTableLoader(options.directory.getAbsoluteFile(), new BulkLoader.ExternalClient(options.hosts, options.nativePort, options.authProvider, options.storagePort, options.sslStoragePort, options.serverEncOptions, buildSSLOptions(options.clientEncOptions)), handler, options.connectionsPerHost);
      DatabaseDescriptor.setStreamThroughputOutboundMegabitsPerSec(options.throttle);
      DatabaseDescriptor.setInterDCStreamThroughputOutboundMegabitsPerSec(options.interDcThrottle);
      StreamResultFuture future = null;
      BulkLoader.ProgressIndicator indicator = new BulkLoader.ProgressIndicator();

      try {
         if(options.noProgress) {
            future = loader.stream(options.ignores, new StreamEventHandler[0]);
         } else {
            future = loader.stream(options.ignores, new StreamEventHandler[]{indicator});
         }
      } catch (Exception var7) {
         JVMStabilityInspector.inspectThrowable(var7);
         System.err.println(var7.getMessage());
         if(var7.getCause() != null) {
            System.err.println(var7.getCause());
         }

         var7.printStackTrace(System.err);
         throw new BulkLoadException(var7);
      }

      try {
         future.get();
         if(!options.noProgress) {
            indicator.printSummary(options.connectionsPerHost);
         }

         Thread.sleep(1000L);
      } catch (Exception var6) {
         System.err.println("Streaming to the following hosts failed:");
         System.err.println(loader.getFailedHosts());
         var6.printStackTrace(System.err);
         throw new BulkLoadException(var6);
      }
   }

   private static SSLOptions buildSSLOptions(EncryptionOptions.ClientEncryptionOptions clientEncryptionOptions) {
      if(!clientEncryptionOptions.enabled) {
         return null;
      } else {
         SSLContext sslContext;
         try {
            sslContext = SSLFactory.createSSLContext(clientEncryptionOptions, true);
         } catch (IOException var3) {
            throw new RuntimeException("Could not create SSL Context.", var3);
         }

         return JdkSSLOptions.builder().withSSLContext(sslContext).withCipherSuites(clientEncryptionOptions.cipher_suites).build();
      }
   }

   public static class CmdLineOptions extends Options {
      public CmdLineOptions() {
      }

      public Options addOption(String opt, String longOpt, String argName, String description) {
         Option option = new Option(opt, longOpt, true, description);
         option.setArgName(argName);
         return this.addOption(option);
      }

      public Options addOption(String opt, String longOpt, String description) {
         return this.addOption(new Option(opt, longOpt, false, description));
      }
   }

   static class ExternalClient extends NativeSSTableLoaderClient {
      private final int storagePort;
      private final int sslStoragePort;
      private final EncryptionOptions.ServerEncryptionOptions serverEncOptions;

      public ExternalClient(Set<InetAddress> hosts, int port, AuthProvider authProvider, int storagePort, int sslStoragePort, EncryptionOptions.ServerEncryptionOptions serverEncryptionOptions, SSLOptions sslOptions) {
         super(hosts, port, authProvider, sslOptions);
         this.storagePort = storagePort;
         this.sslStoragePort = sslStoragePort;
         this.serverEncOptions = serverEncryptionOptions;
      }

      public StreamConnectionFactory getConnectionFactory() {
         return new BulkLoadConnectionFactory(this.storagePort, this.sslStoragePort, this.serverEncOptions, false);
      }
   }

   static class ProgressIndicator implements StreamEventHandler {
      private long start;
      private long lastProgress;
      private long lastTime;
      private long peak = 0L;
      private int totalFiles = 0;
      private final Multimap<InetAddress, SessionInfo> sessionsByHost = HashMultimap.create();

      public ProgressIndicator() {
         this.start = this.lastTime = System.nanoTime();
      }

      public void onSuccess(StreamState finalState) {
      }

      public void onFailure(Throwable t) {
      }

      public synchronized void handleStreamEvent(StreamEvent event) {
         if(event.eventType == StreamEvent.Type.STREAM_PREPARED) {
            SessionInfo session = ((StreamEvent.SessionPreparedEvent)event).session;
            this.sessionsByHost.put(session.peer, session);
         } else if(event.eventType == StreamEvent.Type.FILE_PROGRESS || event.eventType == StreamEvent.Type.STREAM_COMPLETE) {
            ProgressInfo progressInfo = null;
            if(event.eventType == StreamEvent.Type.FILE_PROGRESS) {
               progressInfo = ((StreamEvent.ProgressEvent)event).progress;
            }

            long time = System.nanoTime();
            long deltaTime = time - this.lastTime;
            StringBuilder sb = new StringBuilder();
            sb.append("\rprogress: ");
            long totalProgress = 0L;
            long totalSize = 0L;
            boolean updateTotalFiles = this.totalFiles == 0;
            Iterator var13 = this.sessionsByHost.keySet().iterator();

            while(var13.hasNext()) {
               InetAddress peer = (InetAddress)var13.next();
               sb.append("[").append(peer).append("]");
               Iterator var15 = this.sessionsByHost.get(peer).iterator();

               while(var15.hasNext()) {
                  SessionInfo session = (SessionInfo)var15.next();
                  long size = session.getTotalSizeToSend();
                  long current = 0L;
                  int completed = 0;
                  if(progressInfo != null && session.peer.equals(progressInfo.peer) && session.sessionIndex == progressInfo.sessionIndex) {
                     session.updateProgress(progressInfo);
                  }

                  ProgressInfo progress;
                  for(Iterator var22 = session.getSendingFiles().iterator(); var22.hasNext(); current += progress.currentBytes) {
                     progress = (ProgressInfo)var22.next();
                     if(progress.isCompleted()) {
                        ++completed;
                     }
                  }

                  totalProgress += current;
                  totalSize += size;
                  sb.append(session.sessionIndex).append(":");
                  sb.append(completed).append("/").append(session.getTotalFilesToSend());
                  sb.append(" ").append(String.format("%-3d", new Object[]{Long.valueOf(size == 0L?100L:current * 100L / size)})).append("% ");
                  if(updateTotalFiles) {
                     this.totalFiles = (int)((long)this.totalFiles + session.getTotalFilesToSend());
                  }
               }
            }

            this.lastTime = time;
            long deltaProgress = totalProgress - this.lastProgress;
            this.lastProgress = totalProgress;
            sb.append("total: ").append(totalSize == 0L?100L:totalProgress * 100L / totalSize).append("% ");
            sb.append(FBUtilities.prettyPrintMemoryPerSecond(deltaProgress, deltaTime));
            long average = this.bytesPerSecond(totalProgress, time - this.start);
            if(average > this.peak) {
               this.peak = average;
            }

            sb.append(" (avg: ").append(FBUtilities.prettyPrintMemoryPerSecond(totalProgress, time - this.start)).append(")");
            System.out.println(sb.toString());
         }

      }

      private long bytesPerSecond(long bytes, long timeInNano) {
         return timeInNano != 0L?(long)((double)bytes / (double)timeInNano * 1000.0D * 1000.0D * 1000.0D):0L;
      }

      private void printSummary(int connectionsPerHost) {
         long end = System.nanoTime();
         long durationMS = (end - this.start) / 1000000L;
         StringBuilder sb = new StringBuilder();
         sb.append("\nSummary statistics: \n");
         sb.append(String.format("   %-24s: %-10d%n", new Object[]{"Connections per host ", Integer.valueOf(connectionsPerHost)}));
         sb.append(String.format("   %-24s: %-10d%n", new Object[]{"Total files transferred ", Integer.valueOf(this.totalFiles)}));
         sb.append(String.format("   %-24s: %-10s%n", new Object[]{"Total bytes transferred ", FBUtilities.prettyPrintMemory(this.lastProgress)}));
         sb.append(String.format("   %-24s: %-10s%n", new Object[]{"Total duration ", durationMS + " ms"}));
         sb.append(String.format("   %-24s: %-10s%n", new Object[]{"Average transfer rate ", FBUtilities.prettyPrintMemoryPerSecond(this.lastProgress, end - this.start)}));
         sb.append(String.format("   %-24s: %-10s%n", new Object[]{"Peak transfer rate ", FBUtilities.prettyPrintMemoryPerSecond(this.peak)}));
         System.out.println(sb.toString());
      }
   }
}
