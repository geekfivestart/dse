package com.datastax.bdp.tools;

import com.datastax.bdp.cassandra.crypto.KmipSystemKey;
import com.datastax.bdp.cassandra.crypto.LocalSystemKey;
import com.datastax.bdp.cassandra.crypto.SystemKey;
import com.datastax.bdp.cassandra.crypto.kmip.KmipHost;
import com.datastax.bdp.cassandra.crypto.kmip.KmipHosts;
import com.datastax.bdp.cassandra.crypto.kmip.cli.CliHelp;
import com.datastax.bdp.cassandra.crypto.kmip.cli.KmipCommand;
import com.datastax.bdp.cassandra.crypto.kmip.cli.KmipCommands;
import com.datastax.bdp.cassandra.db.tiered.TieredStorageConfig;
import com.datastax.bdp.cassandra.db.tiered.TieredTableStatsMXBean;
import com.datastax.bdp.config.DseConfig;
import com.datastax.bdp.jmx.JMX;
import com.datastax.bdp.leasemanager.LeaseMXBean;
import com.datastax.bdp.leasemanager.LeaseMonitorCore;
import com.datastax.bdp.plugin.CqlSlowLogMXBean;
import com.datastax.bdp.plugin.PerformanceObjectsController;
import com.datastax.bdp.plugin.bean.AsyncSnapshotPluginMXBean;
import com.datastax.bdp.plugin.bean.HistogramDataTablesBean;
import com.datastax.bdp.plugin.bean.UserLatencyTrackingBean;
import com.datastax.bdp.server.DseDaemonMXBean;
import com.datastax.bdp.snitch.EndpointStateTrackerMXBean;
import com.datastax.bdp.transport.common.DseReloadableTrustManagerMXBean;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import java.beans.PropertyVetoException;
import java.io.Console;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.math.BigInteger;
import java.security.InvalidParameterException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.Map.Entry;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.crypto.NoSuchPaddingException;
import javax.management.InstanceNotFoundException;
import javax.management.MalformedObjectNameException;
import org.apache.cassandra.db.mos.MemoryOnlyStatusMXBean;
import org.apache.cassandra.db.mos.MemoryOnlyStatusMXBean.TableInfo;
import org.apache.cassandra.db.mos.MemoryOnlyStatusMXBean.TotalInfo;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.ParseException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig.Feature;

public class DseToolCommands implements ProxySource {
   public DseToolCommands() {
   }

   public void makeProxies(NodeJmxProxyPool probe) throws MalformedObjectNameException {
      probe.makeProxy(JMX.Type.CORE, "DseDaemon", DseDaemonMXBean.class);
      probe.makeProxy(JMX.Type.CORE, "EndpointStateTracker", EndpointStateTrackerMXBean.class);
      probe.makeProxy(JMX.Type.CORE, "Leases", LeaseMXBean.class);
      probe.makeProxy(JMX.Type.CORE, "DseServerReloadableTrustManager", DseReloadableTrustManagerMXBean.class);
      probe.makeProxy(JMX.Type.CORE, "DseClientReloadableTrustManager", DseReloadableTrustManagerMXBean.class);
      probe.makeProxy(JMX.Type.CORE, "TieredTableStats", TieredTableStatsMXBean.class);
      probe.makePerfProxy(PerformanceObjectsController.SparkClusterInfoBean.class, AsyncSnapshotPluginMXBean.class);
      probe.makePerfProxy(PerformanceObjectsController.CqlSlowLogBean.class, CqlSlowLogMXBean.class);
      probe.makePerfProxy(PerformanceObjectsController.CqlSystemInfoBean.class, AsyncSnapshotPluginMXBean.class);
      probe.makePerfProxy(PerformanceObjectsController.ClusterSummaryStatsBean.class, AsyncSnapshotPluginMXBean.class);
      probe.makePerfProxy(PerformanceObjectsController.DbSummaryStatsBean.class, AsyncSnapshotPluginMXBean.class);
      probe.makePerfProxy(HistogramDataTablesBean.class, AsyncSnapshotPluginMXBean.class);
      probe.makePerfProxy(PerformanceObjectsController.ResourceLatencyTrackingBean.class, AsyncSnapshotPluginMXBean.class);
      probe.makePerfProxy(UserLatencyTrackingBean.class, AsyncSnapshotPluginMXBean.class);
   }

   public static class CleanupLeases extends DseTool.Plugin {
      public CleanupLeases() {
      }

      public String getName() {
         return "cleanup_leases";
      }

      public boolean isJMX() {
         return true;
      }

      public String getHelp() {
         return "Cleans up unneeded leases.  Leases require active maintenance, so this reduces CPU and memory usage.";
      }

      public void executeJMX(NodeProbe np, NodeJmxProxyPool dnp, String[] args) throws Exception {
         if(args.length > 0) {
            throw new IllegalArgumentException("cleanup_leases does not take arguments");
         } else {
            this.cleanupLeases((LeaseMXBean)dnp.getProxy(LeaseMXBean.class));
         }
      }

      @VisibleForTesting
      public void cleanupLeases(LeaseMXBean leasePlugin) throws Exception {
         Set<LeaseMonitorCore.LeaseId> oldLeases = leasePlugin.getLeasesOfNonexistentDatacenters();
         if(!oldLeases.isEmpty()) {
            System.out.println("Leases for old datacenters that no longer exist: " + oldLeases);
            Iterator var3 = oldLeases.iterator();

            while(var3.hasNext()) {
               LeaseMonitorCore.LeaseId lease = (LeaseMonitorCore.LeaseId)var3.next();
               leasePlugin.cleanupDeadLease(lease.getName(), lease.getDc());
               System.out.println("Removed " + lease + ".");
            }
         }

         Set<LeaseMonitorCore.LeaseId> sparkMastersOfNonAnalyticsDcs = leasePlugin.getSparkMastersOfNonAnalyticsDcs();
         if(!sparkMastersOfNonAnalyticsDcs.isEmpty()) {
            System.out.println("Spark masters for datacenters that no longer have nodes with an analytics workload: " + sparkMastersOfNonAnalyticsDcs);
            Iterator var7 = sparkMastersOfNonAnalyticsDcs.iterator();

            while(var7.hasNext()) {
               LeaseMonitorCore.LeaseId lease = (LeaseMonitorCore.LeaseId)var7.next();
               if(leasePlugin.clientPing(lease.getName(), lease.getDc()).isHeld()) {
                  System.err.println(lease + " is held somehow; skipping.  This really shouldn't happen; ifyou try this command again in a few minutes and this message doesn't goaway, contact DSE support.");
               } else if(!leasePlugin.disableLease(lease.getName(), lease.getDc())) {
                  System.err.println("Failed to disable old spark master " + lease + ".  Try running cleanup_leases again in a few minutes.");
               } else if(!leasePlugin.deleteLease(lease.getName(), lease.getDc())) {
                  System.err.println("Failed to delete old spark master " + lease + ".  Try running cleanup_leases again in a few minutes.");
               } else {
                  System.out.println("Removed " + lease + ".");
               }
            }
         }

         if(oldLeases.isEmpty() && sparkMastersOfNonAnalyticsDcs.isEmpty()) {
            System.out.println("Lease table clean; nothing to do!");
         }

      }
   }

   public static class ServerId extends DseTool.Plugin {
      public ServerId() {
      }

      public String getName() {
         return "server_id";
      }

      public String getHelp() {
         return "Returns the server ID for the node. Usage: server_id [IP ADDRESS]";
      }

      public void executeJMX(NodeProbe np, NodeJmxProxyPool dnp, String[] args) throws Exception {
         if(args.length > 1) {
            throw new IllegalArgumentException("server_id takes either no arguments or a single argument.");
         } else {
            EndpointStateTrackerMXBean endpointStateTracker = (EndpointStateTrackerMXBean)dnp.getProxy(EndpointStateTrackerMXBean.class);
            String id = "";
            if(args.length == 0) {
               id = endpointStateTracker.getServerId();
            } else {
               id = endpointStateTracker.getServerId(args[0]);
            }

            System.out.println("Server ID: " + id);
         }
      }
   }

   public static class ReloadTruststore extends DseTool.Plugin {
      public ReloadTruststore() {
      }

      private static void reloadServerTruststore(DseReloadableTrustManagerMXBean serverTrustManager) throws Exception {
         try {
            serverTrustManager.reloadTrustManager();
         } catch (InstanceNotFoundException var2) {
            System.out.println("Server truststore not loaded. Is internode_encryption enabled?");
            return;
         }

         System.out.println("Server truststore reloaded.");
      }

      private static void reloadClientTruststore(DseReloadableTrustManagerMXBean clientTrustStore) throws Exception {
         try {
            clientTrustStore.reloadTrustManager();
         } catch (InstanceNotFoundException var2) {
            System.out.println("Client truststore not loaded. Is require_client_auth enabled in the client encryption options section?");
            return;
         }

         System.out.println("Client truststore reloaded.");
      }

      public String getName() {
         return "tsreload";
      }

      public boolean isJMX() {
         return true;
      }

      public String getHelp() {
         return "Reloads, without needing to restart, the node's SSL certificate truststores. Usage: tsreload <client|server>";
      }

      public String getOptionsHelp() {
         return "client: reloads the client truststore. This truststore is used for encrypted client-to-node SSL communications.\nserver: reloads the server truststore. This truststore is used for encrypted node-to-node SSL communications.";
      }

      public void executeJMX(NodeProbe np, NodeJmxProxyPool dnp, String[] args) throws Exception {
         if(args.length != 1) {
            throw new IllegalArgumentException("tsreload takes a single argument: client or server");
         } else if(args[0].equalsIgnoreCase("server")) {
            reloadServerTruststore((DseReloadableTrustManagerMXBean)dnp.getProxy("DseServerReloadableTrustManager"));
         } else if(args[0].equalsIgnoreCase("client")) {
            reloadClientTruststore((DseReloadableTrustManagerMXBean)dnp.getProxy("DseClientReloadableTrustManager"));
         } else {
            throw new IllegalArgumentException("tsreload takes client or server");
         }
      }
   }

   public static class GetNodeHealth extends DseTool.Plugin {
      private static final org.apache.commons.cli.Options options = new org.apache.commons.cli.Options();

      public GetNodeHealth() {
      }

      public String getName() {
         return "node_health [-all]";
      }

      public boolean isJMX() {
         return true;
      }

      public String getHelp() {
         return "Returns a health score between 0 and 1 for the node.";
      }

      public void executeJMX(NodeProbe np, NodeJmxProxyPool dnp, String[] args) throws Exception {
         CommandLineParser parser = new GnuParser();
         CommandLine cl = parser.parse(options, args);
         if(cl.hasOption("all")) {
            this.getNodeHealthForRing(np, dnp, System.out);
         } else {
            if(args.length > 0) {
               throw new IllegalArgumentException(String.format("Unknown argument '%s'. Only argument allowed is --all", new Object[]{args[0]}));
            }

            this.getNodeHealth(dnp, System.out);
         }

      }

      private static double getNodeHealth(NodeJmxProxyPool dnp) {
         return ((EndpointStateTrackerMXBean)dnp.getProxy(EndpointStateTrackerMXBean.class)).getNodeHealth(dnp.getDsehost()).doubleValue();
      }

      private void getNodeHealth(NodeJmxProxyPool dnp, PrintStream out) {
         out.println("Node Health [0,1]: " + getNodeHealth(dnp));
      }

      public void getNodeHealthForRing(NodeProbe np, NodeJmxProxyPool dnp, PrintStream out) {
         Set<String> endpoints = new HashSet(np.getTokenToEndpointMap().values());
         Collection<String> deadNodes = np.getUnreachableNodes();
         EndpointStateTrackerMXBean endpointStateProxy = (EndpointStateTrackerMXBean)dnp.getProxy(EndpointStateTrackerMXBean.class);
         Map<String, Double> nodeHealthForRing = new HashMap();
         Iterator var8 = endpoints.iterator();

         while(var8.hasNext()) {
            String primaryEndpoint = (String)var8.next();
            if(!deadNodes.contains(primaryEndpoint)) {
               double health = endpointStateProxy.getNodeHealth(primaryEndpoint).doubleValue();
               nodeHealthForRing.put(primaryEndpoint, Double.valueOf(health));
            } else {
               nodeHealthForRing.put(primaryEndpoint, Double.valueOf(0.0D));
            }
         }

         out.print("Address \t Health [0,1]\n");
         var8 = nodeHealthForRing.entrySet().iterator();

         while(var8.hasNext()) {
            Entry<String, Double> e = (Entry)var8.next();
            out.print(String.format("%s \t %s\n", new Object[]{e.getKey(), e.getValue()}));
         }

      }

      static {
         options.addOption("all", "all", false, "Returns the node health of all endpoints from the ring");
      }
   }

   public static class TieredTableStats extends DseTool.Plugin {
      private static final org.apache.commons.cli.Options options = new org.apache.commons.cli.Options();

      public TieredTableStats() {
         options.addOption("v", "verbose", false, "prints per-sstable stats");
      }

      public String getName() {
         return "tieredtablestats ";
      }

      public String getOptionsHelp() {
         return "[<keyspace.table> ...] [-v]";
      }

      public String getHelp() {
         return "Prints detailed info about tiered storage tiers. -v flag prints individual sstable info";
      }

      private Map<String, Map<String, Map<String, Map<String, Map<String, String>>>>> getInfo(NodeJmxProxyPool dnp, List<String> tables, boolean verbose) throws Exception {
         TieredTableStatsMXBean proxy = (TieredTableStatsMXBean)dnp.getProxy(TieredTableStatsMXBean.class);
         Object info;
         if(tables.isEmpty()) {
            info = proxy.tierInfo(verbose);
         } else {
            info = new HashMap();

            String keyspace;
            String table;
            for(Iterator var6 = tables.iterator(); var6.hasNext(); ((Map)((Map)info).get(keyspace)).put(table, proxy.tierInfo(keyspace, table, verbose))) {
               String ksTbl = (String)var6.next();
               String[] tblArray = ksTbl.split("\\.");
               if(tblArray.length != 2) {
                  System.out.println(String.format("Unable to parse '%s'", new Object[]{ksTbl}));
                  System.out.println("  Use the format <keyspace>.<table>");
                  System.exit(-1);
               }

               keyspace = tblArray[0];
               table = tblArray[1];
               if(!((Map)info).containsKey(keyspace)) {
                  ((Map)info).put(keyspace, new HashMap());
               }
            }
         }

         return (Map)info;
      }

      private List<String> keys(Set<String> keySet) {
         return (List)keySet.stream().sorted().collect(Collectors.toList());
      }

      private void printLevel(Map<String, Map<String, Map<String, String>>> info, int level, boolean verbose) {
         String levelKey = level < 0?"orphan":Integer.toString(level);
         if(info.containsKey(levelKey)) {
            Map<String, Map<String, String>> levelInfo = (Map)info.get(levelKey);
            if(level < 0) {
               System.out.println("  Orphan Tables:");
            } else {
               System.out.println("  Tier " + levelKey + ":");
            }

            System.out.println("    Summary:");
            Iterator var6 = this.keys(((Map)levelInfo.get("_summary")).keySet()).iterator();

            String sstable;
            while(var6.hasNext()) {
               sstable = (String)var6.next();
               System.out.println("      " + sstable + ": " + (String)((Map)levelInfo.get("_summary")).get(sstable));
            }

            if(verbose) {
               System.out.println("    SSTables:");
               var6 = this.keys(levelInfo.keySet()).iterator();

               while(true) {
                  do {
                     if(!var6.hasNext()) {
                        return;
                     }

                     sstable = (String)var6.next();
                  } while(sstable.equals("_summary"));

                  System.out.println("      " + sstable + ":");
                  Iterator var8 = this.keys(((Map)levelInfo.get(sstable)).keySet()).iterator();

                  while(var8.hasNext()) {
                     String key = (String)var8.next();
                     System.out.println("        " + key + ": " + (String)((Map)levelInfo.get(sstable)).get(key));
                  }
               }
            }
         }
      }

      private int maxConfiguredTiers() {
         int max = 0;

         TieredStorageConfig config;
         for(Iterator var2 = DseConfig.getTieredStorageOptions().values().iterator(); var2.hasNext(); max = Math.max(max, config.tiers.size())) {
            config = (TieredStorageConfig)var2.next();
         }

         return max;
      }

      private void printInfo(Map<String, Map<String, Map<String, Map<String, Map<String, String>>>>> info, boolean verbose) {
         List<String> keyspaces = this.keys(info.keySet());
         Iterator var4 = keyspaces.iterator();

         while(var4.hasNext()) {
            String keyspace = (String)var4.next();
            List<String> tables = this.keys(((Map)info.get(keyspace)).keySet());
            Iterator var7 = tables.iterator();

            while(var7.hasNext()) {
               String table = (String)var7.next();
               System.out.println(String.format("%s.%s", new Object[]{keyspace, table}));
               Map<String, Map<String, Map<String, String>>> tableInfo = (Map)((Map)info.get(keyspace)).get(table);
               if(tableInfo.isEmpty()) {
                  System.out.println("  (No SSTables)");
               }

               int maxTiers = this.maxConfiguredTiers();

               for(int i = 0; i < maxTiers; ++i) {
                  this.printLevel(tableInfo, i, verbose);
               }

               this.printLevel(tableInfo, -1, verbose);
            }
         }

      }

      public void executeJMX(NodeProbe np, NodeJmxProxyPool dnp, String[] args) throws Exception {
         CommandLineParser parser = new GnuParser();
         CommandLine commandLine = parser.parse(options, args);
         List<String> tables = commandLine.getArgList();
         boolean verbose = commandLine.hasOption("verbose");
         this.printInfo(this.getInfo(dnp, tables, verbose), verbose);
      }
   }

   public static class ManageKmip extends DseTool.Plugin {
      public ManageKmip() {
      }

      public String getName() {
         return "managekmip";
      }

      public String getHelp() {
         return "Tool for managing keys on a KMIP server.";
      }

      public String getOptionsHelp() {
         return "<command> <kmip_groupname>\tRun managekmip help for more info";
      }

      public boolean isJMX() {
         return false;
      }

      void printHelp() {
         System.out.println("Tool for managing keys on a KMIP server. Available commands are:\n");
         String[] cmds = new String[]{"list", "expirekey", "revoke", "destroy"};
         String[] var2 = cmds;
         int var3 = cmds.length;

         for(int var4 = 0; var4 < var3; ++var4) {
            String cmd = var2[var4];
            System.out.println(cmd);
            String help = KmipCommands.getCommand(cmd).help();
            String[] var7 = help.split("\n");
            int var8 = var7.length;

            for(int var9 = 0; var9 < var8; ++var9) {
               String line = var7[var9];
               System.out.println("  " + line);
            }

            System.out.println();
         }

      }

      public void execute(String[] args) throws Exception {
         if(args.length > 0 && args[0].equals("help")) {
            this.printHelp();
            System.exit(0);
         }

         if(args.length < 2) {
            System.out.println("Command and kmip group name required");
            System.out.print(this.getHelp());
            System.exit(-1);
         }

         String cmdName = args[0];
         String hostName = args[1];
         String[] subArgs = new String[args.length - 2];

         for(int i = 0; i < subArgs.length; ++i) {
            subArgs[i] = args[i + 2];
         }

         KmipCommand command = KmipCommands.getCommand(cmdName);
         if(command == null) {
            System.out.println("Invalid command: " + cmdName);
            System.out.print(this.getHelp());
            System.exit(-1);
         }

         if(command instanceof CliHelp) {
            command.execute((KmipHost)null, (String[])null);
         } else {
            KmipHosts.init();
            KmipHost host = KmipHosts.getHost(hostName);
            if(host == null) {
               System.out.println("Unknown KMIP host: " + hostName);
               System.exit(-1);
            }

            if(!command.execute(host, subArgs)) {
               System.exit(-1);
            }

         }
      }
   }

   public static class EncryptConfigValue extends DseTool.Plugin {
      public EncryptConfigValue() {
      }

      public String getName() {
         return "encryptconfigvalue";
      }

      public String getHelp() {
         return "encrypts sensitive configuration information. This command takes no arguments; you will be prompted for the value to encrypt.";
      }

      public boolean isJMX() {
         return false;
      }

      public void execute(String[] args) {
         SystemKey systemKey = null;

         try {
            systemKey = SystemKey.getSystemKey(DseConfig.getConfigEncryptionKeyName());
         } catch (IOException var10) {
            File keyFile = new File(DseConfig.getSystemKeyDirectory(), DseConfig.getConfigEncryptionKeyName());
            System.out.println("Error accessing key " + keyFile);
            System.out.println(var10.getMessage());
            System.exit(1);
         }

         System.out.println("Using system key " + systemKey.getName() + "\n");
         Console console = System.console();
         if(console == null) {
            System.out.println("Couldn't get system console. Exiting...");
            System.exit(1);
         }

         char[] valArray1 = console.readPassword("Enter value to encrypt:", new Object[0]);
         char[] valArray2 = console.readPassword("Enter again to confirm:", new Object[0]);
         String val1 = new String(valArray1);
         String val2 = new String(valArray2);
         if(!val1.equals(val2)) {
            System.out.println("\nInput didn't match. Exiting...");
            System.exit(0);
         }

         try {
            String encrypted = systemKey.encrypt(val1);
            System.out.println("\nYour encrypted value is:\n");
            System.out.println(encrypted + "\n");
         } catch (IOException var9) {
            System.out.println("Unable to encrypt value");
            System.exit(1);
         }

      }
   }

   public static class CreateSystemKey extends DseTool.Plugin {
      private static final org.apache.commons.cli.Options options = new org.apache.commons.cli.Options();

      public CreateSystemKey() {
      }

      public String getName() {
         return "createsystemkey";
      }

      public String getHelp() {
         return "Creates a system key for sstable encryption";
      }

      public String getOptionsHelp() {
         return "<algorithm[/mode/padding]>\n<key strength>\n[<file>]\n[-k|-kmip=<kmip_groupname>]\tuse the given kmip connection info to create a remote system key\n[-t|-kmip-template=<kmip template>]\tUses a kmip server side key template to create key. Only applicable when -kmip is set.\n[-n|-kmip-namespace=<namespace>]\tnamespace to create key with. Only applicable when -kmip is set. Key strength not required for Hmac algorithms. <file> will be appended to the directory defined in system_key_directory for non-kmip keys.";
      }

      public boolean isJMX() {
         return false;
      }

      public void execute(String[] args) throws Exception {
         CommandLineParser parser = new GnuParser();
         CommandLine cl = parser.parse(options, args);
         args = cl.getArgs();
         if(args.length < 2) {
            throw new ParseException("Usage: dsetool createsystemkey <algorithm> <key strength> [<file>] [-kmiphost=<kmip_groupname>]");
         } else {
            String cipherName = args[0];
            int keyStrength = cipherName.startsWith("Hmac")?0:Integer.parseInt(args[1]);
            String keyLocation = null;

            try {
               Object key;
               if(cl.hasOption("kmip")) {
                  String kmipHost = cl.getOptionValue("kmip");
                  keyLocation = String.format("KMIP host: %s", new Object[]{kmipHost});
                  KmipHost.Options kmipOptions = new KmipHost.Options(cl.getOptionValue("kmip-template"), cl.getOptionValue("kmip-namespace"));
                  key = KmipSystemKey.createKey(kmipHost, cipherName, keyStrength, kmipOptions);
               } else {
                  if(cl.getOptionValue("kmip-template") != null) {
                     throw new ParseException("'kmip-template' option cannot be used with local system keys");
                  }

                  if(cl.getOptionValue("kmip-namespace") != null) {
                     throw new ParseException("'kmip-namespace' option cannot be used with local system keys");
                  }

                  File directory = null;
                  String dirString = cl.getOptionValue("directory");
                  if(dirString != null) {
                     directory = new File(dirString);
                  }

                  keyLocation = args.length > 2?args[2]:"system_key";
                  File keyFile = LocalSystemKey.createKey(directory, keyLocation, cipherName, keyStrength);
                  key = new LocalSystemKey(keyFile);
               }

               System.out.println(String.format("Successfully created key %s", new Object[]{((SystemKey)key).getName()}));
            } catch (NoSuchAlgorithmException var11) {
               System.out.println(String.format("System key (%s %s) was not created at %s", new Object[]{cipherName, Integer.valueOf(keyStrength), keyLocation}));
               System.out.println(var11.getMessage());
               System.out.println("Available algorithms are: AES, ARCFOUR, Blowfish, DES, DESede, HmacMD5, HmacSHA1, HmacSHA256, HmacSHA384, HmacSHA512, & RC2");
            } catch (NoSuchPaddingException | InvalidParameterException var12) {
               System.out.println(String.format("System key (%s %s) was not created at %s", new Object[]{cipherName, Integer.valueOf(keyStrength), keyLocation}));
               System.out.println(var12.getMessage());
            }

         }
      }

      static {
         options.addOption("k", "kmip", true, "configured kmip host name to create the key with");
         options.addOption("t", "kmip-template", true, "kmip server template to create key with");
         options.addOption("n", "kmip-namespace", true, "kmip namespace to create key with");
         options.addOption("d", "directory", true, "directory to put file with generated key");
      }
   }

   public static class PerfCommand extends DseTool.Plugin {
      private final Map<String, PerfSubcommand> subcommands;

      public PerfCommand() {
         this(DseTool.findPlugins(PerfSubcommand.class));
      }

      public PerfCommand(Map<String, PerfSubcommand> subcommands) {
         this.subcommands = subcommands;
      }

      public String getName() {
         return "perf";
      }

      public String getHelp() {
         return "Modify performance objects settings.";
      }

      public String getOptionsHelp() {
         StringBuilder sb = new StringBuilder();
         Iterator var2 = this.subcommands.entrySet().iterator();

         while(var2.hasNext()) {
            Entry<String, PerfSubcommand> entry = (Entry)var2.next();
            sb.append((String)entry.getKey()).append('\t').append(((PerfSubcommand)entry.getValue()).getHelp()).append('\n');
         }

         return sb.toString();
      }

      public void executeJMX(NodeProbe np, NodeJmxProxyPool dnp, DseToolArgumentParser parser) throws Exception {
         String[] arguments = parser.getArguments();
         PerfSubcommand perfSubcommand = (PerfSubcommand)this.subcommands.get(parser.getPerfCommand());
         if(perfSubcommand == null) {
            throw new IllegalArgumentException("Unknown subcommand: " + parser.getPerfCommand());
         } else {
            perfSubcommand.execute(dnp, arguments);
         }
      }
   }

   public static class CqlSlowLogCommand extends PerfSubcommand {
      public CqlSlowLogCommand() {
         super("cqlslowlog", PerformanceObjectsController.CqlSlowLogBean.class);
      }

      public void execute(NodeJmxProxyPool dnp, String[] arguments) throws PropertyVetoException {
         CqlSlowLogMXBean slowLogBean = (CqlSlowLogMXBean)this.getProxy(dnp);
         if(arguments.length == 2 && DseToolCommands.CqlSlowLogCommand.Options.recent_slowest_queries.name().equalsIgnoreCase(arguments[1])) {
            printSlowestQueries(slowLogBean);
         } else if(arguments.length == 3 && DseToolCommands.CqlSlowLogCommand.Options.set_num_slowest_queries.name().equalsIgnoreCase(arguments[1])) {
            slowLogBean.setNumSlowestQueries(parse(arguments[2]));
         } else if(arguments.length == 2 && DseToolCommands.CqlSlowLogCommand.Options.skip_writing_to_db.name().equalsIgnoreCase(arguments[1])) {
            slowLogBean.setSkipWritingToDB(true);
         } else if(arguments.length == 2 && DseToolCommands.CqlSlowLogCommand.Options.write_to_db.name().equalsIgnoreCase(arguments[1])) {
            slowLogBean.setSkipWritingToDB(false);
         } else if(arguments.length == 2 && arguments[1].matches("[0-9\\.]+")) {
            slowLogBean.setThreshold(Double.parseDouble(arguments[1]));
         } else {
            super.execute(dnp, arguments);
         }

      }

      public boolean isEnabled(String[] arguments) {
         if(arguments.length == 2) {
            String arg = arguments[1];
            if(DseToolCommands.CqlSlowLogCommand.Options.enable.name().equalsIgnoreCase(arg)) {
               return true;
            }

            if(DseToolCommands.CqlSlowLogCommand.Options.disable.name().equalsIgnoreCase(arg)) {
               return false;
            }

            if(DseToolCommands.CqlSlowLogCommand.Options.set_num_slowest_queries.name().equalsIgnoreCase(arg)) {
               throw new IllegalArgumentException(String.format("Missing integer value. Example usage: %s 3", new Object[]{DseToolCommands.CqlSlowLogCommand.Options.set_num_slowest_queries.name()}));
            }
         } else if(arguments.length >= 3) {
            throw new IllegalArgumentException("Wrong number of arguments");
         }

         throw new IllegalArgumentException(String.format("Expecting '%s' / '%s' / '%s N' / '%s' / %s / %s", new Object[]{DseToolCommands.CqlSlowLogCommand.Options.enable.name(), DseToolCommands.CqlSlowLogCommand.Options.disable.name(), DseToolCommands.CqlSlowLogCommand.Options.set_num_slowest_queries.name(), DseToolCommands.CqlSlowLogCommand.Options.recent_slowest_queries.name(), DseToolCommands.CqlSlowLogCommand.Options.skip_writing_to_db, DseToolCommands.CqlSlowLogCommand.Options.write_to_db}));
      }

      public String getHelp() {
         StringBuilder sb = new StringBuilder();
         sb.append(DseToolCommands.CqlSlowLogCommand.Options.enable.name()).append("|").append(DseToolCommands.CqlSlowLogCommand.Options.disable.name()).append("   Toggle CQL slow log\n");
         sb.append("\tthreshold        Set the CQL slow log threshold\n");
         sb.append("\t").append(DseToolCommands.CqlSlowLogCommand.Options.recent_slowest_queries.name());
         sb.append("  Shows the N most recent slowest CQL queries\n");
         sb.append("\t").append(DseToolCommands.CqlSlowLogCommand.Options.set_num_slowest_queries.name()).append("  Changes the number of slowest queries saved to be N\n");
         sb.append("\t").append(DseToolCommands.CqlSlowLogCommand.Options.skip_writing_to_db.name()).append("  Keeps slow CQL queries in-memory only\n");
         sb.append("\t").append(DseToolCommands.CqlSlowLogCommand.Options.write_to_db.name()).append("  Keeps slow CQL queries in-memory and also writes them to the DB\n");
         return sb.toString();
      }

      private static int parse(String number) {
         try {
            return Integer.parseInt(number);
         } catch (NumberFormatException var2) {
            throw new IllegalArgumentException("Expecting an integer value");
         }
      }

      private static void printSlowestQueries(CqlSlowLogMXBean slowLogBean) {
         List<CqlSlowLogMXBean.SlowCqlQuery> queries = slowLogBean.retrieveRecentSlowestCqlQueries();
         if(queries.isEmpty()) {
            System.out.println("There were no recent slowest CQL queries to retrieve.");
         } else {
            System.out.println(String.format("Showing N=%s most recent CQL queries slower than %s ms", new Object[]{Integer.valueOf(slowLogBean.getNumSlowestQueries()), Double.valueOf(slowLogBean.getThreshold())}));
            System.out.println("---------------------------------------------");

            try {
               System.out.println((new ObjectMapper()).enable(new Feature[]{Feature.INDENT_OUTPUT}).writerWithDefaultPrettyPrinter().writeValueAsString(queries));
            } catch (IOException var3) {
               System.out.println("Something went wrong: " + var3.getMessage());
               System.exit(1);
            }

         }
      }

      private static enum Options {
         recent_slowest_queries,
         set_num_slowest_queries,
         enable,
         disable,
         skip_writing_to_db,
         write_to_db;

         private Options() {
         }
      }
   }

   public static class UserLatencyCommand extends PerfSubcommand {
      public UserLatencyCommand() {
         super("userlatencytracking", UserLatencyTrackingBean.class);
      }

      public String getHelp() {
         return "enable|disable   Toggle user latency tracking";
      }
   }

   public static class ResourceLatencyCommand extends PerfSubcommand {
      public ResourceLatencyCommand() {
         super("resourcelatencytracking", PerformanceObjectsController.ResourceLatencyTrackingBean.class);
      }

      public String getHelp() {
         return "enable|disable   Toggle resource latency tracking";
      }
   }

   public static class DbSummaryCommand extends PerfSubcommand {
      public DbSummaryCommand() {
         super("dbsummary", PerformanceObjectsController.DbSummaryStatsBean.class);
      }

      public String getHelp() {
         return "enable|disable   Toggle database summary stats";
      }
   }

   public static class CqlSystemInfoCommand extends PerfSubcommand {
      public CqlSystemInfoCommand() {
         super("cqlsysteminfo", PerformanceObjectsController.CqlSystemInfoBean.class);
      }

      public String getHelp() {
         return "enable|disable   Toggle CQL system information statistics";
      }
   }

   public static class ClusterSummaryCommand extends PerfSubcommand {
      public ClusterSummaryCommand() {
         super("clustersummary", PerformanceObjectsController.ClusterSummaryStatsBean.class);
      }

      public String getHelp() {
         return "enable|disable   Toggle cluster summary statistics";
      }
   }

   public static class HistoryPerfCommand extends PerfSubcommand {
      public HistoryPerfCommand() {
         super("histograms", HistogramDataTablesBean.class);
      }

      public String getHelp() {
         return "enable|disable   Toggle column family histograms";
      }
   }

   public static class InMemoryStatus extends DseTool.Plugin {
      private static final String WARN_MSG = "\n\nWarning: This command is deprecated. Please use 'nodetool inmemorystatus' instead.";

      public InMemoryStatus() {
      }

      public String getName() {
         return "inmemorystatus";
      }

      public String getHelp() {
         return "Returns a list of the in-memory tables for this node and the amount of memory each table is using, or information about a single table if the keyspace and columnfamily are given.\n\nWarning: This command is deprecated. Please use 'nodetool inmemorystatus' instead.";
      }

      public void executeJMX(NodeProbe np, NodeJmxProxyPool dnp, String[] args) throws Exception {
         try {
            MemoryOnlyStatusMXBean proxy = np.getMemoryOnlyStatusProxy();
            if(args.length == 0) {
               this.printInmemoryInfo(proxy);
            } else {
               if(args.length != 2) {
                  throw new IllegalArgumentException("inmemorystatus can be called with either no arguments (print all tables) or a keyspace/columnfamily pair.");
               }

               this.printInmemoryInfo(proxy, args[0], args[1]);
            }
         } finally {
            System.err.println("\n\nWarning: This command is deprecated. Please use 'nodetool inmemorystatus' instead.");
         }

      }

      private void printInmemoryInfo(MemoryOnlyStatusMXBean proxy) {
         this.printInmemoryInfo(proxy.getMemoryOnlyTableInformation(), proxy.getMemoryOnlyTotals());
      }

      private void printInmemoryInfo(MemoryOnlyStatusMXBean proxy, String ks, String cf) {
         this.printInmemoryInfo(Collections.singletonList(proxy.getMemoryOnlyTableInformation(ks, cf)), proxy.getMemoryOnlyTotals());
      }

      private void printInmemoryInfo(List<TableInfo> infos, TotalInfo totals) {
         System.out.format("Max Memory to Lock:                    %10dMB\n", new Object[]{Long.valueOf(totals.getMaxMemoryToLock() / 1048576L)});
         System.out.format("Current Total Memory Locked:           %10dMB\n", new Object[]{Long.valueOf(totals.getUsed() / 1048576L)});
         System.out.format("Current Total Memory Not Able To Lock: %10dMB\n", new Object[]{Long.valueOf(totals.getNotAbleToLock() / 1048576L)});
         if(infos.size() > 0) {
            System.out.format("%-30s %-30s %12s %17s %7s\n", new Object[]{"Keyspace", "ColumnFamily", "Size", "Couldn't Lock", "Usage"});
            Iterator var3 = infos.iterator();

            while(var3.hasNext()) {
               TableInfo mi = (TableInfo)var3.next();
               System.out.format("%-30s %-30s %10dMB %15dMB %6.0f%%\n", new Object[]{mi.getKs(), mi.getCf(), Long.valueOf(mi.getUsed() / 1048576L), Long.valueOf(mi.getNotAbleToLock() / 1048576L), Double.valueOf(100.0D * (double)mi.getUsed() / (double)mi.getMaxMemoryToLock())});
            }
         } else {
            System.out.format("No MemoryOnlyStrategy tables found.\n", new Object[0]);
         }

      }
   }

   public static class Partitioner extends DseTool.Plugin {
      public Partitioner() {
      }

      public String getName() {
         return "partitioner";
      }

      public String getHelp() {
         return "Returns the fully qualified classname of the IPartitioner in use by the cluster.";
      }

      public void executeJMX(NodeProbe np, NodeJmxProxyPool dnp, String[] args) throws Exception {
         System.out.println(np.getPartitioner());
      }
   }

   public static class ListSubranges extends DseTool.Plugin {
      public ListSubranges() {
      }

      public String getName() {
         return "list_subranges";
      }

      public String getHelp() {
         return "Divide a token range for a given Keyspace/ColumnFamily into a number of smaller subranges of approx. keys_per_range. To be useful, the specified range should be contained by the target node's primary range.";
      }

      public String getOptionsHelp() {
         return "<keyspace> <cf-name> <keys_per_range> <start_token>, <end_token>\t";
      }

      public void executeJMX(NodeProbe np, NodeJmxProxyPool dnp, String[] args) throws Exception {
         if(args.length < 5) {
            throw new IllegalArgumentException("list_subranges command requires keyspace, columnfamily, range size, start token and end token args.");
         } else {
            this.listSubranges((DseDaemonMXBean)dnp.getProxy(DseDaemonMXBean.class), args[0], args[1], args[2], args[3], args[4], System.out);
         }
      }

      private void listSubranges(DseDaemonMXBean proxy, String ks, String cf, String rangeSize, String startToken, String endToken, PrintStream out) {
         Iterator var8 = proxy.getSplits(ks, cf, Integer.parseInt(rangeSize), startToken, endToken).iterator();

         while(var8.hasNext()) {
            List<String> split = (List)var8.next();
            out.printf("%-44s %-44s %-20s\n", new Object[]{split.get(0), split.get(1), split.get(2)});
         }

      }
   }

   public static class RebuildIndexes extends DseTool.Plugin {
      public RebuildIndexes() {
      }

      public String getName() {
         return "rebuild_indexes";
      }

      public String getHelp() {
         return "Rebuild specified secondary indexes for given Keyspace/ColumnFamily.";
      }

      public String getOptionsHelp() {
         return "<keyspace> <cf-name>\n[<idx1>,<idx2>,...]\tRebuild only specified indexes. Omit to re-build all indexes.";
      }

      public void executeJMX(NodeProbe np, NodeJmxProxyPool dnp, String[] args) throws Exception {
         if(args.length < 2) {
            throw new IllegalArgumentException("rebuild_indexes command requires keyspace and columnfamily args.");
         } else {
            if(args.length >= 3) {
               this.rebuildSecondaryIndexes(dnp, args[0], args[1], args[2].split(","));
            } else {
               this.rebuildSecondaryIndexes(dnp, args[0], args[1]);
            }

         }
      }

      private void rebuildSecondaryIndexes(NodeJmxProxyPool dnp, String keyspace, String columnFamily) throws Exception {
         this.rebuildSecondaryIndexes(dnp, keyspace, columnFamily, (String[])null);
      }

      private void rebuildSecondaryIndexes(NodeJmxProxyPool dnp, String keyspace, String columnFamily, String[] indexes) throws Exception {
         ((DseDaemonMXBean)dnp.getProxy(DseDaemonMXBean.class)).rebuildSecondaryIndexes(keyspace, columnFamily, indexes == null?Collections.emptyList():Arrays.asList(indexes));
      }
   }

   public static class Status extends DseToolCommands.Ring {
      public Status() {
      }

      public String getHelp() {
         return "List the nodes in the ring.";
      }

      public String getName() {
         return "status";
      }

      public void executeJMX(NodeProbe np, NodeJmxProxyPool dnp, String[] args) throws Exception {
         this.ring(np, dnp, System.out, args.length > 0?args[0]:null);
      }

      protected void ring(PrintStream out, Collection<EndpointStateTrackerMXBean.NodeStatus> ring, Set<LeaseMonitorCore.LeaseRow> leases, String partitioner, boolean vNodesEnabled, boolean printServerIds, boolean effective) {
         ring.stream().map((ns) -> {
            return ns.getDc();
         }).distinct().sorted().forEach((dc) -> {
            List<EndpointStateTrackerMXBean.NodeStatus> dcRing = (List)ring.stream().filter((ns) -> {
               return ns.getDc().equals(dc);
            }).collect(Collectors.toList());
            String dcHeader = String.format("DC: %-15s Workload: %-15s Graph: %-6s %s\n", new Object[]{dc, this.getDcUniqueValueOrUnknown(dcRing, dc, "Workload:", "Mixed"), this.getDcUniqueValueOrUnknown(dcRing, dc, "Graph", "Mixed"), dcRing.stream().filter((ns) -> {
               return ns.getWorkload().toLowerCase().contains("analytics");
            }).findAny().isPresent()?"Analytics Master: " + this.getLeaseLeader(dc, leases):""});
            out.print(dcHeader);

            for(int i = 0; i < dcHeader.trim().length() - 1; ++i) {
               out.print('=');
            }

            out.println("");
            System.out.println("Status=Up/Down");
            System.out.println("|/ State=Normal/Leaving/Joining/Moving");
            this.printRing(out, partitioner, vNodesEnabled, dcRing, new String[]{"--", printServerIds?"Server ID":"", "Address", "Load", effective?"Effective-Ownership":"Owns", vNodesEnabled?"VNodes":"Token", "Rack", "Health [0,1]"});
            out.println("");
         });
      }
   }

   public static class Ring extends DseTool.Plugin {
      public static final String SERVER_ID = "Server ID";
      public static final String ADDRESS = "Address";
      public static final String DC = "DC";
      public static final String RACK = "Rack";
      public static final String RING_WORKLOAD = "Workload";
      public static final String STATUS_WORKLOAD = "Workload:";
      public static final String GRAPH = "Graph";
      public static final String STATUS = "Status";
      public static final String STATE = "State";
      public static final String CASS_STATUS = "--";
      public static final String LOAD = "Load";
      public static final String EFFECTIVE_OWNERSHIP = "Effective-Ownership";
      public static final String OWNS = "Owns";
      public static final String VNODES = "VNodes";
      public static final String TOKEN = "Token";
      public static final String HEALTH = "Health [0,1]";
      public final ImmutableMap<String, Pair<String, Function<EndpointStateTrackerMXBean.NodeStatus, String>>> NODESTATUS_FORMATTING = (new Builder()).put("Server ID", Pair.create("%-18s", (ns) -> {
         return ns.getServerId();
      })).put("Address", Pair.create("%-16s", (ns) -> {
         return ns.getAddress();
      })).put("DC", Pair.create("%-20s", (ns) -> {
         return ns.getDc();
      })).put("Rack", Pair.create("%-12s", (ns) -> {
         return ns.getRack();
      })).put("Workload", Pair.create("%-20s", (ns) -> {
         return ns.getWorkload() + ns.getAnalyticsLabel();
      })).put("Workload:", Pair.create("%-20s", (ns) -> {
         return ns.getWorkload();
      })).put("Graph", Pair.create("%-6s", (ns) -> {
         return ns.getIsGraphNode()?"yes":"no";
      })).put("Status", Pair.create("%-7s", (ns) -> {
         return ns.getStatus();
      })).put("State", Pair.create("%-8s", (ns) -> {
         return ns.getState();
      })).put("--", Pair.create("%-4s", (ns) -> {
         return ns.getStatus().substring(0, 1) + ns.getState().substring(0, 1);
      })).put("Load", Pair.create("%-16s", (ns) -> {
         return ns.getLoad();
      })).put("Effective-Ownership", Pair.create("%-20s", (ns) -> {
         return ns.getOwnership();
      })).put("Owns", Pair.create("%-20s", (ns) -> {
         return "?";
      })).put("VNodes", Pair.create("%-44s", (ns) -> {
         return ns.getToken();
      })).put("Token", Pair.create("%-44s", (ns) -> {
         return ns.getToken();
      })).put("Health [0,1]", Pair.create("%-12s", (ns) -> {
         return String.format("%.2f", new Object[]{Double.valueOf(ns.getHealth())});
      })).build();

      public Ring() {
      }

      public String getHelp() {
         return "List the nodes in the ring (for more readable output, use 'dsetool status').";
      }

      public String getName() {
         return "ring";
      }

      public void executeJMX(NodeProbe np, NodeJmxProxyPool dnp, String[] args) throws Exception {
         this.ring(np, dnp, System.out, args.length > 0?args[0]:null);
      }

      public void formatNodeStatus(PrintStream out, BiFunction<String, Function<EndpointStateTrackerMXBean.NodeStatus, String>, String> formatter, String... fields) {
         assert fields.length > 0;

         Arrays.stream(fields).filter((field) -> {
            return !field.isEmpty();
         }).forEach((field) -> {
            out.printf((String)((Pair)this.NODESTATUS_FORMATTING.get(field)).left + " ", new Object[]{formatter.apply(field, ((Pair)this.NODESTATUS_FORMATTING.get(field)).right)});
         });
         out.println("");
      }

      public void printRing(PrintStream out, String partitioner, boolean vNodesEnabled, Collection<EndpointStateTrackerMXBean.NodeStatus> ring, String... fields) {
         Comparator<String> order = this.isRandomizingPartitioner(partitioner)?(token1, token2) -> {
            return (new BigInteger(token1)).compareTo(new BigInteger(token2));
         }:(token1, token2) -> {
            return token1.compareTo(token2);
         };
         List<EndpointStateTrackerMXBean.NodeStatus> sortedRing = (List)ring.stream().sorted((ns1, ns2) -> {
            return order.compare(ns1.getToken(), ns2.getToken());
         }).collect(Collectors.toList());
         this.formatNodeStatus(out, (field, accessor) -> {
            return field;
         }, fields);
         if(ring.size() > 1 && !vNodesEnabled) {
            String lastToken = ((EndpointStateTrackerMXBean.NodeStatus)sortedRing.get(ring.size() - 1)).token;
            this.formatNodeStatus(out, (field, accessor) -> {
               return !field.equals("Token") && !field.equals("VNodes")?"":lastToken;
            }, fields);
         }

         sortedRing.stream().forEach((ns) -> {
            this.formatNodeStatus(out, (field, accessor) -> {
               return (String)accessor.apply(ns);
            }, fields);
         });
      }

      public List<String> getDcUniqueValues(Collection<EndpointStateTrackerMXBean.NodeStatus> ring, String dc, String field) {
         return (List)ring.stream().filter((ns) -> {
            return ns.getDc().equals(dc);
         }).map((Function)((Pair)this.NODESTATUS_FORMATTING.get(field)).right).distinct().collect(Collectors.toList());
      }

      public String getDcUniqueValueOrUnknown(Collection<EndpointStateTrackerMXBean.NodeStatus> ring, String dc, String field, String unknown) {
         List<String> uniqueValues = this.getDcUniqueValues(ring, dc, field);
         return uniqueValues.size() == 1?(String)uniqueValues.get(0):unknown;
      }

      public String getLeaseLeader(String dc, Set<LeaseMonitorCore.LeaseRow> leases) {
         return (String)leases.stream().filter((lease) -> {
            return lease.getDc().equals(dc) && lease.getName().equals("Leader/master/6.0");
         }).map((lease) -> {
            return lease.getHolder() == null?"None":lease.getHolder();
         }).findFirst().orElse("None");
      }

      public void maybePrintRingWarnings(PrintStream out, String partitioner, Collection<EndpointStateTrackerMXBean.NodeStatus> ring, Map<LeaseMonitorCore.LeaseId, Map<String, Boolean>> allLeasesStatus) {
         if(ring.size() > 0 && !((EndpointStateTrackerMXBean.NodeStatus)ring.iterator().next()).effective) {
            out.printf("Note: you must specify a keyspace to get ownership information.\n", new Object[0]);
         }

         Map<String, Pair<String, Float>> dcMaxOwnership = new HashMap();
         Map<String, Pair<String, Float>> dcMinOwnership = new HashMap();
         Iterator var7 = ring.iterator();

         while(true) {
            EndpointStateTrackerMXBean.NodeStatus ns;
            float ownership;
            do {
               do {
                  if(!var7.hasNext()) {
                     if(this.isRandomizingPartitioner(partitioner)) {
                        var7 = dcMaxOwnership.keySet().iterator();

                        while(var7.hasNext()) {
                           String dc = (String)var7.next();
                           Pair<String, Float> maxNode = (Pair)dcMaxOwnership.get(dc);
                           Pair<String, Float> minNode = (Pair)dcMinOwnership.get(dc);
                           float ratio = ((Float)maxNode.right).floatValue() / ((Float)minNode.right).floatValue();
                           if((double)ratio > 1.1D) {
                              out.printf("Warning:  Node %s is serving %.2f times the token space of node %s, which means it will be using %.2f times more disk space and network bandwidth. If this is unintentional, check out http://wiki.apache.org/cassandra/Operations#Ring_management\n", new Object[]{maxNode.left, Float.valueOf(ratio), minNode.left, Float.valueOf(ratio)});
                           }
                        }
                     }

                     ring.stream().filter((status) -> {
                        return status.getWorkload() != null && status.getWorkload().contains("Analytics");
                     }).map((status) -> {
                        return status.getDc();
                     }).distinct().forEach((dc) -> {
                        List<String> dcWorkloads = this.getDcUniqueValues(ring, dc, "Workload:");
                        if(dcWorkloads.size() > 1) {
                           out.format("Warning: datacenter '%s' has heterogeneous workloads %s, which may lead to degradation of workload specific features.\n", new Object[]{dc, Arrays.toString(dcWorkloads.toArray())});
                        }

                        this.getDcUniqueValues(ring, dc, "Graph");
                        if(this.getDcUniqueValues(ring, dc, "Graph").size() > 1) {
                           out.format("Warning: datacenter '%s' only has graph enabled on some nodes, which may lead to degradation of graph specific features.\n", new Object[]{dc});
                        }

                        Map<String, Boolean> replicas = (Map)allLeasesStatus.get(new LeaseMonitorCore.LeaseId("Leader/master/6.0", dc));
                        if(replicas == null) {
                           out.printf("Warning: dse_leases has no replicas in datacenter '%s'.  SparkMaster nodes will not be elected until the replication factor is increased by running the ALTER KEYSPACE command.\n", new Object[]{dc});
                        } else {
                           long liveNodes = replicas.values().stream().filter((up) -> {
                              return up.booleanValue();
                           }).count();
                           if(liveNodes <= (long)(replicas.size() / 2)) {
                              out.printf("Warning: only %d of the %d replicas for the '%s.%s' lease are alive.  Sparkmaster nodes will not be elected until a quorum of live nodes is achieved.%s\nNode Status: %s\n", new Object[]{Long.valueOf(liveNodes), Integer.valueOf(replicas.size()), "Leader/master/6.0", dc, replicas.size() < 3?" Increasing the replication factor of the dse_leases keyspace for datacenter '" + dc + "' to 3 will increase reliability.":"", replicas.entrySet().stream().collect(Collectors.toMap((entry) -> {
                                 return (String)entry.getKey();
                              }, (entry) -> {
                                 return ((Boolean)entry.getValue()).booleanValue()?"UP":"DOWN";
                              }))});
                           } else if(replicas.size() < 3 && ring.stream().filter((status) -> {
                              return Objects.equals(status.getDc(), dc);
                           }).count() >= 3L) {
                              out.printf("Warning: dse_leases has only %d replicas in the datacenter '%s'; a single lease replica failure will prevent the SparkMaster from being elected. You should probably increase the replication factor by running the ALTER KEYSPACE command.\n", new Object[]{Integer.valueOf(replicas.size()), dc});
                           }
                        }

                     });
                     return;
                  }

                  ns = (EndpointStateTrackerMXBean.NodeStatus)var7.next();
               } while(!ns.status.equals("Normal"));

               ownership = Float.valueOf(ns.ownership).floatValue();
               if(!dcMaxOwnership.containsKey(ns.dc) || ownership > ((Float)((Pair)dcMaxOwnership.get(ns.dc)).right).floatValue()) {
                  dcMaxOwnership.put(ns.dc, Pair.create(ns.address, Float.valueOf(ownership)));
               }
            } while(dcMinOwnership.containsKey(ns.dc) && ownership >= ((Float)((Pair)dcMinOwnership.get(ns.dc)).right).floatValue());

            dcMinOwnership.put(ns.dc, Pair.create(ns.address, Float.valueOf(ownership)));
         }
      }

      private boolean isRandomizingPartitioner(String partitioner) {
         return partitioner.equals("org.apache.cassandra.dht.RandomPartitioner") || partitioner.equals("org.apache.cassandra.dht.Murmur3Partitioner");
      }

      protected void ring(NodeProbe np, NodeJmxProxyPool dnp, PrintStream out, String keyspace) throws Exception {
         EndpointStateTrackerMXBean endpointStateTracker = (EndpointStateTrackerMXBean)dnp.getProxy(EndpointStateTrackerMXBean.class);
         LeaseMXBean leasePlugin = (LeaseMXBean)dnp.getProxy(LeaseMXBean.class);
         SortedMap<String, EndpointStateTrackerMXBean.NodeStatus> ring = endpointStateTracker.getRing(keyspace);
         String partitioner = np.getPartitioner();

         Set allLeases;
         Map allLeasesStatus;
         try {
            allLeases = leasePlugin.getAllLeases();
            allLeasesStatus = leasePlugin.getAllLeasesStatus();
         } catch (Exception var12) {
            allLeases = Collections.emptySet();
            allLeasesStatus = Collections.emptyMap();
            out.print("Warning: Not able to read Analytics Master data, SparkMaster data may not be correct.\n\n");
         }

         this.ring(out, ring.values(), allLeases, partitioner, endpointStateTracker.vnodesEnabled(), (long)ring.values().size() != ring.values().stream().map((ns) -> {
            return ns.getServerId();
         }).distinct().count(), ((EndpointStateTrackerMXBean.NodeStatus)ring.get(ring.firstKey())).effective);
         this.maybePrintRingWarnings(out, partitioner, ring.values(), allLeasesStatus);
      }

      protected void ring(PrintStream out, Collection<EndpointStateTrackerMXBean.NodeStatus> ring, Set<LeaseMonitorCore.LeaseRow> leases, String partitioner, boolean vNodesEnabled, boolean printServerIds, boolean effective) {
         this.printRing(out, partitioner, vNodesEnabled, ring, new String[]{printServerIds?"Server ID":"", "Address", "DC", "Rack", "Workload", "Graph", "Status", "State", "Load", effective?"Effective-Ownership":"Owns", vNodesEnabled?"VNodes":"Token", "Health [0,1]"});
      }
   }
}
