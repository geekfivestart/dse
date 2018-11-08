package org.apache.cassandra.tools;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.airlift.airline.Cli;
import io.airlift.airline.Help;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;
import io.airlift.airline.ParseArgumentsMissingException;
import io.airlift.airline.ParseArgumentsUnexpectedException;
import io.airlift.airline.ParseCommandMissingException;
import io.airlift.airline.ParseCommandUnrecognizedException;
import io.airlift.airline.ParseOptionConversionException;
import io.airlift.airline.ParseOptionMissingException;
import io.airlift.airline.ParseOptionMissingValueException;
import io.airlift.airline.Cli.CliBuilder;
import java.io.Console;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOError;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.SortedMap;
import java.util.Map.Entry;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.tools.nodetool.AbortRebuild;
import org.apache.cassandra.tools.nodetool.Assassinate;
import org.apache.cassandra.tools.nodetool.BootstrapResume;
import org.apache.cassandra.tools.nodetool.CfHistograms;
import org.apache.cassandra.tools.nodetool.CfStats;
import org.apache.cassandra.tools.nodetool.Cleanup;
import org.apache.cassandra.tools.nodetool.ClearSnapshot;
import org.apache.cassandra.tools.nodetool.Compact;
import org.apache.cassandra.tools.nodetool.CompactionHistory;
import org.apache.cassandra.tools.nodetool.CompactionStats;
import org.apache.cassandra.tools.nodetool.Decommission;
import org.apache.cassandra.tools.nodetool.DescribeCluster;
import org.apache.cassandra.tools.nodetool.DescribeRing;
import org.apache.cassandra.tools.nodetool.DisableAutoCompaction;
import org.apache.cassandra.tools.nodetool.DisableBackup;
import org.apache.cassandra.tools.nodetool.DisableBinary;
import org.apache.cassandra.tools.nodetool.DisableGossip;
import org.apache.cassandra.tools.nodetool.DisableHandoff;
import org.apache.cassandra.tools.nodetool.DisableHintsForDC;
import org.apache.cassandra.tools.nodetool.Drain;
import org.apache.cassandra.tools.nodetool.EnableAutoCompaction;
import org.apache.cassandra.tools.nodetool.EnableBackup;
import org.apache.cassandra.tools.nodetool.EnableBinary;
import org.apache.cassandra.tools.nodetool.EnableGossip;
import org.apache.cassandra.tools.nodetool.EnableHandoff;
import org.apache.cassandra.tools.nodetool.EnableHintsForDC;
import org.apache.cassandra.tools.nodetool.FailureDetectorInfo;
import org.apache.cassandra.tools.nodetool.Flush;
import org.apache.cassandra.tools.nodetool.GarbageCollect;
import org.apache.cassandra.tools.nodetool.GcStats;
import org.apache.cassandra.tools.nodetool.GetBatchlogReplayTrottle;
import org.apache.cassandra.tools.nodetool.GetCompactionThreshold;
import org.apache.cassandra.tools.nodetool.GetCompactionThroughput;
import org.apache.cassandra.tools.nodetool.GetConcurrentCompactors;
import org.apache.cassandra.tools.nodetool.GetConcurrentViewBuilders;
import org.apache.cassandra.tools.nodetool.GetEndpoints;
import org.apache.cassandra.tools.nodetool.GetInterDCStreamThroughput;
import org.apache.cassandra.tools.nodetool.GetLoggingLevels;
import org.apache.cassandra.tools.nodetool.GetMaxHintWindow;
import org.apache.cassandra.tools.nodetool.GetSSTables;
import org.apache.cassandra.tools.nodetool.GetSeeds;
import org.apache.cassandra.tools.nodetool.GetStreamThroughput;
import org.apache.cassandra.tools.nodetool.GetTimeout;
import org.apache.cassandra.tools.nodetool.GetTraceProbability;
import org.apache.cassandra.tools.nodetool.GossipInfo;
import org.apache.cassandra.tools.nodetool.HandoffWindow;
import org.apache.cassandra.tools.nodetool.InMemoryStatus;
import org.apache.cassandra.tools.nodetool.Info;
import org.apache.cassandra.tools.nodetool.InvalidateCounterCache;
import org.apache.cassandra.tools.nodetool.InvalidateKeyCache;
import org.apache.cassandra.tools.nodetool.InvalidateRowCache;
import org.apache.cassandra.tools.nodetool.Join;
import org.apache.cassandra.tools.nodetool.ListSnapshots;
import org.apache.cassandra.tools.nodetool.MarkUnrepaired;
import org.apache.cassandra.tools.nodetool.Move;
import org.apache.cassandra.tools.nodetool.NetStats;
import org.apache.cassandra.tools.nodetool.PauseHandoff;
import org.apache.cassandra.tools.nodetool.ProxyHistograms;
import org.apache.cassandra.tools.nodetool.RangeKeySample;
import org.apache.cassandra.tools.nodetool.Rebuild;
import org.apache.cassandra.tools.nodetool.RebuildIndex;
import org.apache.cassandra.tools.nodetool.Refresh;
import org.apache.cassandra.tools.nodetool.RefreshSizeEstimates;
import org.apache.cassandra.tools.nodetool.ReloadLocalSchema;
import org.apache.cassandra.tools.nodetool.ReloadSeeds;
import org.apache.cassandra.tools.nodetool.ReloadTriggers;
import org.apache.cassandra.tools.nodetool.RelocateSSTables;
import org.apache.cassandra.tools.nodetool.RemoveNode;
import org.apache.cassandra.tools.nodetool.Repair;
import org.apache.cassandra.tools.nodetool.RepairAdmin;
import org.apache.cassandra.tools.nodetool.ReplayBatchlog;
import org.apache.cassandra.tools.nodetool.ResetLocalSchema;
import org.apache.cassandra.tools.nodetool.ResumeHandoff;
import org.apache.cassandra.tools.nodetool.Ring;
import org.apache.cassandra.tools.nodetool.Scrub;
import org.apache.cassandra.tools.nodetool.Sequence;
import org.apache.cassandra.tools.nodetool.SetBatchlogReplayThrottle;
import org.apache.cassandra.tools.nodetool.SetCacheCapacity;
import org.apache.cassandra.tools.nodetool.SetCacheKeysToSave;
import org.apache.cassandra.tools.nodetool.SetCompactionThreshold;
import org.apache.cassandra.tools.nodetool.SetCompactionThroughput;
import org.apache.cassandra.tools.nodetool.SetConcurrentCompactors;
import org.apache.cassandra.tools.nodetool.SetConcurrentViewBuilders;
import org.apache.cassandra.tools.nodetool.SetHintedHandoffThrottleInKB;
import org.apache.cassandra.tools.nodetool.SetHostStat;
import org.apache.cassandra.tools.nodetool.SetInterDCStreamThroughput;
import org.apache.cassandra.tools.nodetool.SetLoggingLevel;
import org.apache.cassandra.tools.nodetool.SetMaxHintWindow;
import org.apache.cassandra.tools.nodetool.SetStreamThroughput;
import org.apache.cassandra.tools.nodetool.SetTimeout;
import org.apache.cassandra.tools.nodetool.SetTraceProbability;
import org.apache.cassandra.tools.nodetool.Sjk;
import org.apache.cassandra.tools.nodetool.Snapshot;
import org.apache.cassandra.tools.nodetool.Status;
import org.apache.cassandra.tools.nodetool.StatusAutoCompaction;
import org.apache.cassandra.tools.nodetool.StatusBackup;
import org.apache.cassandra.tools.nodetool.StatusBinary;
import org.apache.cassandra.tools.nodetool.StatusGossip;
import org.apache.cassandra.tools.nodetool.StatusHandoff;
import org.apache.cassandra.tools.nodetool.Stop;
import org.apache.cassandra.tools.nodetool.StopDaemon;
import org.apache.cassandra.tools.nodetool.TableHistograms;
import org.apache.cassandra.tools.nodetool.TableStats;
import org.apache.cassandra.tools.nodetool.TopPartitions;
import org.apache.cassandra.tools.nodetool.TpStats;
import org.apache.cassandra.tools.nodetool.TruncateHints;
import org.apache.cassandra.tools.nodetool.UpgradeSSTable;
import org.apache.cassandra.tools.nodetool.Verify;
import org.apache.cassandra.tools.nodetool.Version;
import org.apache.cassandra.tools.nodetool.ViewBuildStatus;
import org.apache.cassandra.tools.nodetool.nodesync.Disable;
import org.apache.cassandra.tools.nodetool.nodesync.Enable;
import org.apache.cassandra.tools.nodetool.nodesync.GetRate;
import org.apache.cassandra.tools.nodetool.nodesync.RateSimulatorCmd;
import org.apache.cassandra.tools.nodetool.nodesync.SetRate;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

public class NodeTool {
   private static final String HISTORYFILE = "nodetool.history";

   public NodeTool() {
   }

   public static void main(String... args) {
      Cli<Runnable> parser = createCli(true);
      byte status = 0;

      try {
         Runnable parse = (Runnable)parser.parse(args);
         printHistory(args);
         parse.run();
      } catch (IllegalStateException | ParseArgumentsMissingException | ParseArgumentsUnexpectedException | ParseOptionConversionException | ParseOptionMissingException | ParseOptionMissingValueException | ParseCommandMissingException | ParseCommandUnrecognizedException | IllegalArgumentException var4) {
         badUse(var4);
         status = 1;
      } catch (Throwable var5) {
         err(Throwables.getRootCause(var5));
         status = 2;
      }

      System.exit(status);
   }

   public static Cli<Runnable> createCli(boolean withSequence) {
      List<Class<? extends Runnable>> commands = Lists.newArrayList(new Class[]{Help.class, Info.class, Ring.class, NetStats.class, CfStats.class, TableStats.class, CfHistograms.class, TableHistograms.class, Cleanup.class, ClearSnapshot.class, Compact.class, Scrub.class, Verify.class, Flush.class, UpgradeSSTable.class, GarbageCollect.class, DisableAutoCompaction.class, EnableAutoCompaction.class, CompactionStats.class, CompactionHistory.class, Decommission.class, DescribeCluster.class, DisableBinary.class, EnableBinary.class, EnableGossip.class, DisableGossip.class, EnableHandoff.class, GcStats.class, GetBatchlogReplayTrottle.class, GetCompactionThreshold.class, GetCompactionThroughput.class, GetTimeout.class, GetStreamThroughput.class, GetTraceProbability.class, GetInterDCStreamThroughput.class, GetEndpoints.class, GetSeeds.class, GetSSTables.class, GetMaxHintWindow.class, GossipInfo.class, InvalidateKeyCache.class, InvalidateRowCache.class, InvalidateCounterCache.class, Join.class, Move.class, PauseHandoff.class, ResumeHandoff.class, ProxyHistograms.class, Rebuild.class, Refresh.class, RemoveNode.class, Assassinate.class, ReloadSeeds.class, Repair.class, RepairAdmin.class, ReplayBatchlog.class, SetCacheCapacity.class, SetHintedHandoffThrottleInKB.class, SetBatchlogReplayThrottle.class, SetCompactionThreshold.class, SetCompactionThroughput.class, GetConcurrentCompactors.class, SetConcurrentCompactors.class, GetConcurrentViewBuilders.class, SetConcurrentViewBuilders.class, SetTimeout.class, SetStreamThroughput.class, SetInterDCStreamThroughput.class, SetTraceProbability.class, SetMaxHintWindow.class, Snapshot.class, ListSnapshots.class, Status.class, StatusBinary.class, StatusGossip.class, StatusBackup.class, StatusHandoff.class, StatusAutoCompaction.class, Stop.class, StopDaemon.class, Version.class, DescribeRing.class, RebuildIndex.class, RangeKeySample.class, EnableBackup.class, DisableBackup.class, ResetLocalSchema.class, ReloadLocalSchema.class, ReloadTriggers.class, SetCacheKeysToSave.class, DisableHandoff.class, Drain.class, TruncateHints.class, TpStats.class, TopPartitions.class, SetLoggingLevel.class, GetLoggingLevels.class, Sjk.class, DisableHintsForDC.class, EnableHintsForDC.class, FailureDetectorInfo.class, RefreshSizeEstimates.class, RelocateSSTables.class, ViewBuildStatus.class, InMemoryStatus.class, MarkUnrepaired.class, HandoffWindow.class, AbortRebuild.class});
      if(withSequence) {
         commands.add(Sequence.class);
      }

      CliBuilder<Runnable> builder = Cli.builder("nodetool");
      builder.withDescription("Manage your Cassandra cluster").withDefaultCommand(Help.class).withCommands(commands);
      builder.withGroup("bootstrap").withDescription("Monitor/manage node's bootstrap process").withDefaultCommand(Help.class).withCommand(BootstrapResume.class);
      builder.withGroup("nodesyncservice").withDescription("Manage the NodeSync service on the connected node").withDefaultCommand(Help.class).withCommand(SetRate.class).withCommand(GetRate.class).withCommand(Enable.class).withCommand(Disable.class).withCommand(org.apache.cassandra.tools.nodetool.nodesync.Status.class).withCommand(RateSimulatorCmd.class);
      return builder.build();
   }

   private static void printHistory(String... args) {
      if(args.length != 0) {
         String cmdLine = Joiner.on(" ").skipNulls().join(args);
         cmdLine = cmdLine.replaceFirst("(?<=(-pw|--password))\\s+\\S+", " <hidden>");

         try {
            FileWriter writer = new FileWriter(new File(FBUtilities.getToolsOutputDirectory(), "nodetool.history"), true);
            Throwable var3 = null;

            try {
               SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
               writer.append(sdf.format(new Date())).append(": ").append(cmdLine).append(System.lineSeparator());
            } catch (Throwable var13) {
               var3 = var13;
               throw var13;
            } finally {
               if(writer != null) {
                  if(var3 != null) {
                     try {
                        writer.close();
                     } catch (Throwable var12) {
                        var3.addSuppressed(var12);
                     }
                  } else {
                     writer.close();
                  }
               }

            }
         } catch (IOError | IOException var15) {
            ;
         }

      }
   }

   private static void badUse(Exception e) {
      System.out.println("nodetool: " + e.getMessage());
      System.out.println("See 'nodetool help' or 'nodetool help <command>'.");
   }

   private static void err(Throwable e) {
      System.err.println("error: " + e.getMessage());
      System.err.println("-- StackTrace --");
      System.err.println(Throwables.getStackTraceAsString(e));
   }

   public static SortedMap<String, SetHostStat> getOwnershipByDc(NodeProbe probe, boolean resolveIp, Map<String, String> tokenToEndpoint, Map<InetAddress, Float> ownerships) {
      SortedMap<String, SetHostStat> ownershipByDc = Maps.newTreeMap();
      EndpointSnitchInfoMBean epSnitchInfo = probe.getEndpointSnitchInfoProxy();

      try {
         Entry tokenAndEndPoint;
         String dc;
         for(Iterator var6 = tokenToEndpoint.entrySet().iterator(); var6.hasNext(); ((SetHostStat)ownershipByDc.get(dc)).add((String)tokenAndEndPoint.getKey(), (String)tokenAndEndPoint.getValue(), ownerships)) {
            tokenAndEndPoint = (Entry)var6.next();
            dc = epSnitchInfo.getDatacenter((String)tokenAndEndPoint.getValue());
            if(!ownershipByDc.containsKey(dc)) {
               ownershipByDc.put(dc, new SetHostStat(resolveIp));
            }
         }

         return ownershipByDc;
      } catch (UnknownHostException var9) {
         throw new RuntimeException(var9);
      }
   }

   public abstract static class NodeToolCmd implements Runnable {
      @Option(
         type = OptionType.GLOBAL,
         name = {"-h", "--host"},
         description = "Node hostname or ip address"
      )
      private String host = "127.0.0.1";
      @Option(
         type = OptionType.GLOBAL,
         name = {"-p", "--port"},
         description = "Remote jmx agent port number"
      )
      private String port = "7199";
      @Option(
         type = OptionType.GLOBAL,
         name = {"-u", "--username"},
         description = "Remote jmx agent username"
      )
      private String username = "";
      @Option(
         type = OptionType.GLOBAL,
         name = {"-pw", "--password"},
         description = "Remote jmx agent password"
      )
      private String password = "";
      @Option(
         type = OptionType.GLOBAL,
         name = {"-pwf", "--password-file"},
         description = "Path to the JMX password file"
      )
      private String passwordFilePath = "";

      public NodeToolCmd() {
      }

      public void applyGeneralArugments(NodeTool.NodeToolCmd source) {
         this.host = source.host;
         this.port = source.port;
         this.username = source.username;
         this.password = source.password;
         this.passwordFilePath = source.passwordFilePath;
      }

      public void sequenceRun(NodeProbe probe) {
         this.execute(probe);
         if(probe.isFailed()) {
            throw new RuntimeException("nodetool failed, check server logs");
         }
      }

      public void run() {
         if(StringUtils.isNotEmpty(this.username)) {
            if(StringUtils.isNotEmpty(this.passwordFilePath)) {
               this.password = this.readUserPasswordFromFile(this.username, this.passwordFilePath);
            }

            if(StringUtils.isEmpty(this.password)) {
               this.password = this.promptAndReadPassword();
            }
         }

         try {
            NodeProbe probe = this.connect();
            Throwable var2 = null;

            try {
               this.execute(probe);
               if(probe.isFailed()) {
                  throw new RuntimeException("nodetool failed, check server logs");
               }
            } catch (Throwable var12) {
               var2 = var12;
               throw var12;
            } finally {
               if(probe != null) {
                  if(var2 != null) {
                     try {
                        probe.close();
                     } catch (Throwable var11) {
                        var2.addSuppressed(var11);
                     }
                  } else {
                     probe.close();
                  }
               }

            }

         } catch (IOException var14) {
            throw new RuntimeException("Error while closing JMX connection", var14);
         }
      }

      private String readUserPasswordFromFile(String username, String passwordFilePath) {
         String password = "";
         File passwordFile = new File(passwordFilePath);

         try {
            Scanner scanner = (new Scanner(passwordFile)).useDelimiter("\\s+");
            Throwable var6 = null;

            try {
               for(; scanner.hasNextLine(); scanner.nextLine()) {
                  if(scanner.hasNext()) {
                     String jmxRole = scanner.next();
                     if(jmxRole.equals(username) && scanner.hasNext()) {
                        password = scanner.next();
                        break;
                     }
                  }
               }
            } catch (Throwable var16) {
               var6 = var16;
               throw var16;
            } finally {
               if(scanner != null) {
                  if(var6 != null) {
                     try {
                        scanner.close();
                     } catch (Throwable var15) {
                        var6.addSuppressed(var15);
                     }
                  } else {
                     scanner.close();
                  }
               }

            }

            return password;
         } catch (FileNotFoundException var18) {
            throw new RuntimeException(var18);
         }
      }

      private String promptAndReadPassword() {
         String password = "";
         Console console = System.console();
         if(console != null) {
            password = String.valueOf(console.readPassword("Password:", new Object[0]));
         }

         return password;
      }

      protected abstract void execute(NodeProbe var1);

      private NodeProbe connect() {
         NodeProbe nodeClient = null;

         try {
            if(this.username.isEmpty()) {
               nodeClient = new NodeProbe(this.host, Integer.parseInt(this.port));
            } else {
               nodeClient = new NodeProbe(this.host, Integer.parseInt(this.port), this.username, this.password);
            }
         } catch (SecurityException | IOException var4) {
            Throwable rootCause = Throwables.getRootCause(var4);
            System.err.println(String.format("nodetool: Failed to connect to '%s:%s' - %s: '%s'.", new Object[]{this.host, this.port, rootCause.getClass().getSimpleName(), rootCause.getMessage()}));
            System.exit(1);
         }

         return nodeClient;
      }

      protected List<String> parseOptionalKeyspace(List<String> cmdArgs, NodeProbe nodeProbe) {
         return this.parseOptionalKeyspace(cmdArgs, nodeProbe, NodeTool.NodeToolCmd.KeyspaceSet.ALL);
      }

      protected List<String> parseOptionalKeyspace(List<String> cmdArgs, NodeProbe nodeProbe, NodeTool.NodeToolCmd.KeyspaceSet defaultKeyspaceSet) {
         List<String> keyspaces = new ArrayList();
         if(cmdArgs != null && !cmdArgs.isEmpty()) {
            ((List)keyspaces).add(cmdArgs.get(0));
         } else if(defaultKeyspaceSet == NodeTool.NodeToolCmd.KeyspaceSet.NON_LOCAL_STRATEGY) {
            ((List)keyspaces).addAll((Collection)(keyspaces = nodeProbe.getNonLocalStrategyKeyspaces()));
         } else if(defaultKeyspaceSet == NodeTool.NodeToolCmd.KeyspaceSet.NON_SYSTEM) {
            ((List)keyspaces).addAll((Collection)(keyspaces = nodeProbe.getNonSystemKeyspaces()));
         } else {
            ((List)keyspaces).addAll(nodeProbe.getKeyspaces());
         }

         Iterator var5 = ((List)keyspaces).iterator();

         String keyspace;
         do {
            if(!var5.hasNext()) {
               return Collections.unmodifiableList((List)keyspaces);
            }

            keyspace = (String)var5.next();
         } while(nodeProbe.getKeyspaces().contains(keyspace));

         throw new IllegalArgumentException("Keyspace [" + keyspace + "] does not exist.");
      }

      protected String[] parseOptionalTables(List<String> cmdArgs) {
         return cmdArgs.size() <= 1?ArrayUtils.EMPTY_STRING_ARRAY:(String[])Iterables.toArray(cmdArgs.subList(1, cmdArgs.size()), String.class);
      }

      protected static enum KeyspaceSet {
         ALL,
         NON_SYSTEM,
         NON_LOCAL_STRATEGY;

         private KeyspaceSet() {
         }
      }
   }
}
