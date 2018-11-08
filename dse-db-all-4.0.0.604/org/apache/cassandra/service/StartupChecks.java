package org.apache.cassandra.service;

import com.datastax.bdp.db.util.CGroups;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.netty.channel.epoll.Aio;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.unix.FileDescriptor;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaKeyspace;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.JavaUtils;
import org.apache.cassandra.utils.NativeLibrary;
import org.apache.cassandra.utils.SigarLibrary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StartupChecks {
   private final List<StartupCheck> preFlightChecks = new ArrayList();
   private final List<StartupCheck> DEFAULT_TESTS;
   public static final StartupCheck checkJemalloc = new StartupCheck() {
      public void execute(Logger logger) {
         if(!FBUtilities.isWindows) {
            String jemalloc = System.getProperty("cassandra.libjemalloc");
            if(jemalloc == null) {
               logger.warn("jemalloc shared library could not be preloaded to speed up memory allocations");
            } else if("-".equals(jemalloc)) {
               logger.info("jemalloc preload explicitly disabled");
            } else {
               logger.info("jemalloc seems to be preloaded from {}", jemalloc);
            }

         }
      }
   };
   public static final StartupCheck checkValidLaunchDate = new StartupCheck() {
      private static final long EARLIEST_LAUNCH_DATE = 1215820800000L;

      public void execute(Logger logger) throws StartupException {
         long now = System.currentTimeMillis();
         if(now < 1215820800000L) {
            throw new StartupException(1, String.format("current machine time is %s, but that is seemingly incorrect. exiting now.", new Object[]{(new Date(now)).toString()}));
         }
      }
   };
   public static final StartupCheck checkInvalidJmxProperty = new StartupCheck() {
      public void execute(Logger logger) throws StartupException {
         if(System.getProperty("com.sun.management.jmxremote.port") != null) {
            throw new StartupException(100, "The JVM property 'com.sun.management.jmxremote.port' is not allowed. Please use cassandra.jmx.remote.port instead and refer to cassandra-env.(sh|ps1) for JMX configuration info.");
         }
      }
   };
   public static final StartupCheck inspectJvmOptions = new StartupCheck() {
      public void execute(Logger logger) {
         if(!DatabaseDescriptor.hasLargeAddressSpace()) {
            logger.warn("32bit JVM detected.  It is recommended to run Cassandra on a 64bit JVM for better performance.");
         }

         String javaVmName = System.getProperty("java.vm.name");
         if(!javaVmName.contains("HotSpot") && !javaVmName.contains("OpenJDK")) {
            logger.warn("Non-Oracle JVM detected.  Some features, such as immediate unmap of compacted SSTables, may not work as intended");
         } else {
            this.checkOutOfMemoryHandling(logger);
         }

      }

      private void checkOutOfMemoryHandling(Logger logger) {
         if(JavaUtils.supportExitOnOutOfMemory(System.getProperty("java.version"))) {
            if(!this.jvmOptionsContainsOneOf(new String[]{"-XX:OnOutOfMemoryError=", "-XX:+ExitOnOutOfMemoryError", "-XX:+CrashOnOutOfMemoryError"})) {
               logger.warn("The JVM is not configured to stop on OutOfMemoryError which can cause data corruption. Use one of the following JVM options to configure the behavior on OutOfMemoryError:  -XX:+ExitOnOutOfMemoryError, -XX:+CrashOnOutOfMemoryError, or -XX:OnOutOfMemoryError=\"<cmd args>;<cmd args>\"");
            }
         } else if(!this.jvmOptionsContainsOneOf(new String[]{"-XX:OnOutOfMemoryError="})) {
            logger.warn("The JVM is not configured to stop on OutOfMemoryError which can cause data corruption. Either upgrade your JRE to a version greater or equal to 8u92 and use -XX:+ExitOnOutOfMemoryError/-XX:+CrashOnOutOfMemoryError or use -XX:OnOutOfMemoryError=\"<cmd args>;<cmd args>\" on your current JRE.");
         }

      }

      private boolean jvmOptionsContainsOneOf(String... optionNames) {
         RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
         List<String> inputArguments = runtimeMxBean.getInputArguments();
         Iterator var4 = inputArguments.iterator();

         while(var4.hasNext()) {
            String argument = (String)var4.next();
            String[] var6 = optionNames;
            int var7 = optionNames.length;

            for(int var8 = 0; var8 < var7; ++var8) {
               String optionName = var6[var8];
               if(argument.startsWith(optionName)) {
                  return true;
               }
            }
         }

         return false;
      }
   };
   public static final StartupCheck checkNativeLibraryInitialization = new StartupCheck() {
      public void execute(Logger logger) throws StartupException {
         if(!NativeLibrary.isAvailable()) {
            throw new StartupException(1, "The native library could not be initialized properly. ");
         }
      }
   };
   public static final StartupCheck initSigarLibrary = new StartupCheck() {
      public void execute(Logger logger) {
         SigarLibrary.instance.warnIfRunningInDegradedMode();
      }
   };
   public static final StartupCheck checkMaxMapCount = new StartupCheck() {
      private final long EXPECTED_MAX_MAP_COUNT = 1048575L;
      private final String MAX_MAP_COUNT_PATH = "/proc/sys/vm/max_map_count";

      private long getMaxMapCount(Logger logger) {
         Path path = Paths.get("/proc/sys/vm/max_map_count", new String[0]);

         try {
            BufferedReader bufferedReader = Files.newBufferedReader(path);
            Throwable var4 = null;

            long var6;
            try {
               String data = bufferedReader.readLine();
               if(data == null) {
                  return -1L;
               }

               try {
                  var6 = Long.parseLong(data);
               } catch (NumberFormatException var19) {
                  logger.warn("Unable to parse {}.", path, var19);
                  return -1L;
               }
            } catch (Throwable var20) {
               var4 = var20;
               throw var20;
            } finally {
               if(bufferedReader != null) {
                  if(var4 != null) {
                     try {
                        bufferedReader.close();
                     } catch (Throwable var18) {
                        var4.addSuppressed(var18);
                     }
                  } else {
                     bufferedReader.close();
                  }
               }

            }

            return var6;
         } catch (IOException var22) {
            logger.warn("IO exception while reading file {}.", path, var22);
            return -1L;
         }
      }

      public void execute(Logger logger) {
         if(FBUtilities.isLinux) {
            if(DatabaseDescriptor.getDiskAccessMode() != Config.AccessMode.standard || DatabaseDescriptor.getIndexAccessMode() != Config.AccessMode.standard) {
               long maxMapCount = this.getMaxMapCount(logger);
               if(maxMapCount < 1048575L) {
                  logger.warn("Maximum number of memory map areas per process (vm.max_map_count) {} is too low, recommended value: {}, you can change it with sysctl.", Long.valueOf(maxMapCount), Long.valueOf(1048575L));
               }

            }
         }
      }
   };
   public static final StartupCheck checkDataDirs = (logger) -> {
      Iterable<String> dirs = Iterables.concat(Arrays.asList(DatabaseDescriptor.getAllDataFileLocations()), Arrays.asList(new String[]{DatabaseDescriptor.getCommitLogLocation(), DatabaseDescriptor.getSavedCachesLocation(), DatabaseDescriptor.getHintsDirectory().getAbsolutePath()}));
      Iterator var2 = dirs.iterator();

      String dataDir;
      File dir;
      do {
         if(!var2.hasNext()) {
            return;
         }

         dataDir = (String)var2.next();
         logger.debug("Checking directory {}", dataDir);
         dir = new File(dataDir);
         if(!dir.exists()) {
            logger.warn("Directory {} doesn't exist", dataDir);
            if(!dir.mkdirs()) {
               throw new StartupException(3, "Has no permission to create directory " + dataDir);
            }
         }
      } while(Directories.verifyFullPermissions(dir, dataDir));

      throw new StartupException(3, "Insufficient permissions on directory " + dataDir);
   };
   public static final StartupCheck checkSSTablesFormat = new StartupCheck() {
      private final Set<String> IGNORED_DIRECTORIES = ImmutableSet.of("lost+found");

      public void execute(Logger logger) throws StartupException {
         final Set<String> invalid = new HashSet();
         final Set<String> nonSSTablePaths = new HashSet();
         nonSSTablePaths.add(FileUtils.getCanonicalPath(DatabaseDescriptor.getCommitLogLocation()));
         nonSSTablePaths.add(FileUtils.getCanonicalPath(DatabaseDescriptor.getSavedCachesLocation()));
         nonSSTablePaths.add(FileUtils.getCanonicalPath(DatabaseDescriptor.getHintsDirectory()));
         FileVisitor<Path> sstableVisitor = new SimpleFileVisitor<Path>() {
            public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) {
               File file = path.toFile();
               if(!Descriptor.isValidFile(file)) {
                  return FileVisitResult.CONTINUE;
               } else {
                  try {
                     if(!Descriptor.fromFilename(file).isCompatible()) {
                        invalid.add(file.toString());
                     }
                  } catch (Exception var5) {
                     invalid.add(file.toString());
                  }

                  return FileVisitResult.CONTINUE;
               }
            }

            public FileVisitResult visitFileFailed(Path path, IOException e) throws IOException {
               String directoryName = path.toFile().getCanonicalFile().getName();
               return IGNORED_DIRECTORIES.contains(directoryName)?FileVisitResult.SKIP_SUBTREE:super.visitFileFailed(path, e);
            }

            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
               String name = dir.getFileName().toString();
               return !name.equals("snapshots") && !name.equals("backups") && !nonSSTablePaths.contains(dir.toFile().getCanonicalPath())?FileVisitResult.CONTINUE:FileVisitResult.SKIP_SUBTREE;
            }
         };
         String[] var5 = DatabaseDescriptor.getAllDataFileLocations();
         int var6 = var5.length;

         for(int var7 = 0; var7 < var6; ++var7) {
            String dataDir = var5[var7];

            try {
               Files.walkFileTree(Paths.get(dataDir, new String[0]), sstableVisitor);
            } catch (IOException var10) {
               throw new StartupException(3, "Unable to verify sstable files on disk", var10);
            }
         }

         if(!invalid.isEmpty()) {
            throw new StartupException(3, String.format("Detected unreadable sstables %s, please check NEWS.txt and ensure that you have upgraded through all required intermediate versions, running upgradesstables", new Object[]{Joiner.on(",").join(invalid)}));
         }
      }
   };
   public static final StartupCheck checkOutdatedTables = new StartupCheck() {
      public void execute(Logger logger) throws StartupException {
         SchemaKeyspace.validateNonCompact();
      }
   };
   public static final StartupCheck checkSystemKeyspaceState = new StartupCheck() {
      public void execute(Logger logger) throws StartupException {
         Iterator var2 = Schema.instance.getTablesAndViews("system").iterator();

         while(var2.hasNext()) {
            TableMetadata cfm = (TableMetadata)var2.next();
            ColumnFamilyStore.scrubDataDirectories(cfm);
         }

         try {
            TPCUtils.blockingAwait(SystemKeyspace.checkHealth());
         } catch (ConfigurationException var4) {
            throw new StartupException(100, "Fatal exception during initialization", var4);
         }
      }
   };
   public static final StartupCheck checkDatacenter = new StartupCheck() {
      public void execute(Logger logger) throws StartupException {
         if(!Boolean.getBoolean("cassandra.ignore_dc")) {
            String storedDc = (String)TPCUtils.blockingGet(SystemKeyspace.getDatacenter());
            if(storedDc != null) {
               String currentDc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddress());
               if(!storedDc.equals(currentDc)) {
                  String formatMessage = "Cannot start node if snitch's data center (%s) differs from previous data center (%s). Please fix the snitch configuration, decommission and rebootstrap this node or use the flag -Dcassandra.ignore_dc=true.";
                  throw new StartupException(100, String.format(formatMessage, new Object[]{currentDc, storedDc}));
               }
            }
         }

      }
   };
   public static final StartupCheck checkRack = new StartupCheck() {
      public void execute(Logger logger) throws StartupException {
         if(!Boolean.getBoolean("cassandra.ignore_rack")) {
            String storedRack = (String)TPCUtils.blockingGet(SystemKeyspace.getRack());
            if(storedRack != null) {
               String currentRack = DatabaseDescriptor.getLocalRack();
               if(!storedRack.equals(currentRack)) {
                  String formatMessage = "Cannot start node if snitch's rack (%s) differs from previous rack (%s). Please fix the snitch configuration, decommission and rebootstrap this node or use the flag -Dcassandra.ignore_rack=true.";
                  throw new StartupException(100, String.format(formatMessage, new Object[]{currentRack, storedRack}));
               }
            }
         }

      }
   };
   public static final StartupCheck checkLegacyAuthTables = (logger) -> {
      List<String> existingTables = getExistingAuthTablesFrom(SchemaConstants.LEGACY_AUTH_TABLES);
      if(!existingTables.isEmpty()) {
         String msg = String.format("Legacy auth tables %s in keyspace %s still exist and have not been properly migrated.", new Object[]{Joiner.on(", ").join(existingTables), "system_auth"});
         throw new StartupException(100, msg);
      }
   };
   public static final StartupCheck checkObsoleteAuthTables = (logger) -> {
      List<String> existingTables = getExistingAuthTablesFrom(SchemaConstants.OBSOLETE_AUTH_TABLES);
      if(!existingTables.isEmpty()) {
         logger.warn("Auth tables {} in keyspace {} exist but can safely be dropped.", Joiner.on(", ").join(existingTables), "system_auth");
      }

   };
   private static final StartupCheck warnOnUnsupportedPlatform = (logger) -> {
      if(!FBUtilities.isLinux) {
         String url = "(see http://docs.datastax.com/en/landing_page/doc/landing_page/supportedPlatforms.html for details)";
         String warning = "this could result in instabilities, degraded performance and/or unsupported features.";
         if(FBUtilities.isWindows) {
            logger.warn("Please note that Microsoft Windows is not officially supported by DataStax {}; {}", url, warning);
         } else if(FBUtilities.isMacOSX) {
            logger.warn("Please note that Mac OS X is only supported by DataStax for development, not production {}", url);
         } else {
            logger.warn("Please note that you operating system ({}) does not seem to be officially supported by DataStax {}; {}", new Object[]{FBUtilities.OPERATING_SYSTEM, url, warning});
         }

      }
   };
   static String[] LIBAIO_INSTALLED_CMD = new String[]{"/bin/sh", "-c", "ldconfig -p | grep libaio | wc -l"};
   private static final StartupCheck warnOnLackOfAIO = (logger) -> {
      if(FBUtilities.isLinux) {
         if(!TPC.USE_EPOLL) {
            if(!Boolean.parseBoolean(System.getProperty("cassandra.native.epoll.enabled", "true"))) {
               logger.warn("EPoll has been manually disabled (through the 'cassandra.native.epoll.enabled' system property). This may result in subpar performance.");
            } else {
               warnOnEpollUnavailable(logger);
            }
         } else if(DatabaseDescriptor.assumeDataDirectoriesOnSSD()) {
            if(TPC.USE_AIO) {
               warnOnDataDirNotSupportingODirect(logger);
            } else if(!Boolean.parseBoolean(System.getProperty("dse.io.aio.enabled", "true"))) {
               logger.warn("Asynchronous I/O has been manually disabled (through the 'dse.io.aio.enabled' system property). This may result in subpar performance.");
            } else {
               warnOnAIOUnavailable(logger);
            }
         } else if(TPC.USE_AIO) {
            logger.warn("Forcing Asynchronous I/O as requested with the 'dse.io.aio.force' system property  despite not using SSDs; please note that this is not the recommended configuration.");
         }

      }
   };
   public static final StartupCheck checkPCID = (logger) -> {
      if(FBUtilities.isLinux) {
         Set<String> flags = cpuinfoFlags(logger);
         if(!flags.contains("pcid")) {
            logger.warn("CPU does not have PCID (enabled). This will cause an unnecessary performance regression with a kernel having kernel-page-tables-isolation enabled, which should be the case to since CVE-2017-5754 (\"Meltdown\").");
         }

      }
   };
   public static final StartupCheck checkClockSource = (logger) -> {
      if(FBUtilities.isLinux) {
         File fClockSource = new File("/sys/devices/system/clocksource/clocksource0/current_clocksource");
         if(!fClockSource.exists()) {
            logger.warn("Could not find {}", fClockSource);
         } else {
            List<String> clocksource = FileUtils.readLines(fClockSource);
            if(clocksource.size() != 1) {
               logger.warn("Unknown content in {}: {}", fClockSource, clocksource);
            } else {
               Set<String> flags = cpuinfoFlags(logger);
               String cs = (String)clocksource.get(0);
               byte var6 = -1;
               switch(cs.hashCode()) {
               case -1605454792:
                  if(cs.equals("jiffies")) {
                     var6 = 4;
                  }
                  break;
               case -1294188829:
                  if(cs.equals("kvm-clock")) {
                     var6 = 1;
                  }
                  break;
               case -1165512927:
                  if(cs.equals("acpi_pm")) {
                     var6 = 3;
                  }
                  break;
               case 115140:
                  if(cs.equals("tsc")) {
                     var6 = 0;
                  }
                  break;
               case 3209143:
                  if(cs.equals("hpet")) {
                     var6 = 2;
                  }
               }

               switch(var6) {
               case 0:
                  if(flags.contains("constant_tsc") && flags.contains("nonstop_tsc")) {
                     logger.info("Detected TSC clocksource (constant_tsc={}, nonstop_tsc={}).", Boolean.valueOf(flags.contains("constant_tsc")), Boolean.valueOf(flags.contains("nonstop_tsc")));
                  } else {
                     logger.warn("Detected TSC clocksource may perform suboptimal: constant_tsc={}, nonstop_tsc={}", Boolean.valueOf(flags.contains("constant_tsc")), Boolean.valueOf(flags.contains("nonstop_tsc")));
                  }
                  break;
               case 1:
                  logger.info("Detected KVM clocksource (constant_tsc={}, nonstop_tsc={}).", Boolean.valueOf(flags.contains("constant_tsc")), Boolean.valueOf(flags.contains("nonstop_tsc")));
                  break;
               case 2:
                  logger.warn("Detected HPET clocksource. Consider using TSC as the clocksource (constant_tsc={}, nonstop_tsc={}).", Boolean.valueOf(flags.contains("constant_tsc")), Boolean.valueOf(flags.contains("nonstop_tsc")));
                  break;
               case 3:
                  logger.warn("Detected ACPI-power-management clocksource, which is known to cause severe performance issues. Stongly consider configuring TSC or HPET as the clocksource (constant_tsc={}, nonstop_tsc={}).", Boolean.valueOf(flags.contains("constant_tsc")), Boolean.valueOf(flags.contains("nonstop_tsc")));
                  break;
               case 4:
                  logger.warn("Detected jiffies clocksource. Consider configuring TSC or HPET as the clocksource.");
                  break;
               default:
                  logger.warn("Detected unknown clocksource '{}'.", cs);
               }
            }
         }

      }
   };
   public static final StartupCheck checkVirtualization = (logger) -> {
      if(FBUtilities.isLinux) {
         String virutalization = FileUtils.detectVirtualization();
         if(virutalization == null) {
            logger.info("No virtualization/hypervisor detected");
         } else {
            logger.info("Virtualization/hypervisor '{}' detected. Make sure the disk_optimization_strategy settings reflect the actual hardware. Be aware that certain startup checks may return wrong results due to virtualization/hypervisors. Be also aware that running on virtualized environments can lead to serious performance penalties.", virutalization);
         }

      }
   };
   public static final StartupCheck checkCgroupCpuSets = (logger) -> {
      if(FBUtilities.isLinux) {
         FBUtilities.CpuInfo cpuInfo = FBUtilities.CpuInfo.load();
         if(CGroups.blkioThrottled()) {
            logger.warn("Block I/O is throttled for this process via Linux cgroups");
         }

         Integer cpus = CGroups.countCpus();
         if(cpus != null && cpus.intValue() != cpuInfo.cpuCount()) {
            logger.warn("Some CPUs are not usable because their usage has been restricted via Linux cgroups. Can only use {} of {} CPUs.", CGroups.countCpus(), Integer.valueOf(cpuInfo.cpuCount()));
         }

         if(CGroups.memoryLimit() != CGroups.MEM_UNLIMITED) {
            logger.warn("Not all memory is accessable by this process as it has been restricted via Linux cgroups. Can only use {}.", FileUtils.stringifyFileSize((double)CGroups.memoryLimit()));
         }

      }
   };
   public static final StartupCheck checkCpu = (logger) -> {
      if(FBUtilities.isLinux) {
         try {
            verifyCpu(logger, FBUtilities.CpuInfo::load);
         } catch (Exception var2) {
            logger.debug("Could not read /proc/cpuinfo", var2);
            logger.warn("Could not read /proc/cpuinfo");
         }

      }
   };
   public static final StartupCheck checkZoneReclaimMode = (logger) -> {
      if(FBUtilities.isLinux) {
         try {
            String reclaimMode = FileUtils.readLine(new File("/proc/sys/vm/zone_reclaim_mode"));
            verifyZoneReclaimMode(logger, reclaimMode);
         } catch (Exception var2) {
            logger.debug("Unable to read /sys/kernel/mm/transparent_hugepage/defrag", var2);
            logger.warn("Unable to read /sys/kernel/mm/transparent_hugepage/defrag");
         }

      }
   };
   public static final StartupCheck checkUlimits = (logger) -> {
      if(FBUtilities.isLinux) {
         try {
            List<String> limits = FileUtils.readLines(new File("/proc/self/limits"));
            verifyLimits(logger, limits);
         } catch (Exception var2) {
            logger.debug("Unable to read /proc/self/limits", var2);
            logger.warn("Unable to read /proc/self/limits");
         }

      }
   };
   public static final StartupCheck checkThpDefrag = (logger) -> {
      if(FBUtilities.isLinux) {
         try {
            String defrag = FileUtils.readLine(new File("/sys/kernel/mm/transparent_hugepage/defrag"));
            verifyThpDefrag(logger, defrag);
         } catch (Exception var2) {
            logger.debug("Unable to read /sys/kernel/mm/transparent_hugepage/defrag", var2);
            logger.warn("Unable to read /sys/kernel/mm/transparent_hugepage/defrag");
         }

      }
   };
   public static final StartupCheck checkFilesystems = (logger) -> {
      if(FBUtilities.isLinux) {
         if(!FileUtils.MountPoint.hasMountPoints()) {
            throw new StartupException(3, "Could not detect disk partitions on Linux");
         } else {
            try {
               checkMountpoint(logger, "saved caches", DatabaseDescriptor.getSavedCachesLocation());
               checkMountpoint(logger, "commitlog", DatabaseDescriptor.getCommitLogLocation());
               String[] var1 = DatabaseDescriptor.getAllDataFileLocations();
               int var2 = var1.length;

               for(int var3 = 0; var3 < var2; ++var3) {
                  String dataDirectory = var1[var3];
                  checkMountpoint(logger, "data", dataDirectory);
               }
            } catch (Exception var5) {
               if(var5 instanceof StartupException) {
                  throw var5;
               }

               logger.debug("Unable to inspect mounted partitions", var5);
               logger.warn("Unable to inspect mounted partitions");
            }

         }
      }
   };

   public StartupChecks() {
      this.DEFAULT_TESTS = ImmutableList.of(checkJemalloc, checkValidLaunchDate, checkInvalidJmxProperty, inspectJvmOptions, checkNativeLibraryInitialization, initSigarLibrary, checkMaxMapCount, checkDataDirs, checkSSTablesFormat, checkOutdatedTables, checkSystemKeyspaceState, checkDatacenter, new StartupCheck[]{checkRack, checkLegacyAuthTables, checkObsoleteAuthTables, warnOnUnsupportedPlatform, warnOnLackOfAIO, checkVirtualization, checkClockSource, checkCgroupCpuSets, checkCpu, checkPCID, checkZoneReclaimMode, checkUlimits, checkThpDefrag, checkFilesystems});
   }

   public StartupChecks withDefaultTests() {
      this.preFlightChecks.addAll(this.DEFAULT_TESTS);
      return this;
   }

   public StartupChecks withTest(StartupCheck test) {
      this.preFlightChecks.add(test);
      return this;
   }

   public void verify() throws StartupException {
      Logger logger = LoggerFactory.getLogger(StartupChecks.class);
      Iterator var2 = this.preFlightChecks.iterator();

      while(var2.hasNext()) {
         StartupCheck test = (StartupCheck)var2.next();

         try {
            test.execute(logger);
         } catch (StartupException var5) {
            throw var5;
         } catch (Exception var6) {
            logger.warn("Failed to execute a startup check", var6);
         }
      }

   }

   private static void warnOnAIOUnavailable(Logger logger) {
      assert !Aio.isAvailable();

      warnOnNettyComponentUnavailable("Asynchronous I/O", Aio.unavailabilityCause(), logger);
   }

   private static void warnOnEpollUnavailable(Logger logger) {
      assert !Epoll.isAvailable();

      warnOnNettyComponentUnavailable("Epoll", Epoll.unavailabilityCause(), logger);
   }

   private static void warnOnNettyComponentUnavailable(String component, Throwable cause, Logger logger) {
      Throwable cause = cause == null?new UnknownError(String.format("%s unavailable due to unknown cause", new Object[]{component})):Throwables.getRootCause(cause);
      boolean libaioInstalled = libaioIsInstalled(logger);
      logger.warn("{} doesn't seem to be available: this may result in subpar performance. Libaio {} to be installed.", new Object[]{component, libaioInstalled?"appears":"doesn't appear", cause});
   }

   private static boolean libaioIsInstalled(Logger logger) {
      if(!FBUtilities.isLinux) {
         return false;
      } else {
         try {
            return Integer.parseInt(FBUtilities.execBlocking(LIBAIO_INSTALLED_CMD, 1, TimeUnit.SECONDS).trim()) > 0;
         } catch (Throwable var2) {
            JVMStabilityInspector.inspectThrowable(var2);
            logger.warn("Could not determine if libaio is installed: {}/{}", var2.getClass().getName(), var2.getMessage());
            return false;
         }
      }
   }

   private static void warnOnDataDirNotSupportingODirect(Logger logger) {
      Set<String> locationsWithoutODirect = new HashSet();
      String[] var2 = DatabaseDescriptor.getAllDataFileLocations();
      int var3 = var2.length;

      for(int var4 = 0; var4 < var3; ++var4) {
         String dataDir = var2[var4];
         File dir = new File(dataDir);

         try {
            File tmp = File.createTempFile("dse-db", (String)null, dir);

            try {
               FileDescriptor.from(tmp, 16384).close();
            } catch (IOException var13) {
               if(!var13.getMessage().contains("Invalid argument")) {
                  throw var13;
               }

               locationsWithoutODirect.add(dataDir);
            } finally {
               if(!tmp.delete()) {
                  logger.warn("Wasn't able to delete empty temporary file {} for an unknown reason; while this shouldn't happen, this is of no consequence outside of the fact that you will need to delete this (empty and now unused) file manually.", tmp);
               }

            }
         } catch (IOException var15) {
            logger.debug("Unexpected error while trying to read empty file for O_DIRECT check", var15);
            locationsWithoutODirect.add(dataDir);
         }
      }

      if(!locationsWithoutODirect.isEmpty()) {
         boolean noODirect = locationsWithoutODirect.size() == DatabaseDescriptor.getAllDataFileLocations().length;
         logger.warn("Asynchronous I/O is available/enabled but {} of the configured data directories{} are on file systems supporting O_DIRECT; This will result in subpar performance{}.", new Object[]{noODirect?"none":"some", noODirect?"":String.format(" (%s)", new Object[]{locationsWithoutODirect}), noODirect?"":" for operations involving the aforementioned data directories"});
      }
   }

   private static List<String> getExistingAuthTablesFrom(List<String> tables) {
      return (List)tables.stream().filter((table) -> {
         String cql = String.format("SELECT table_name FROM %s.%s WHERE keyspace_name='%s' AND table_name='%s'", new Object[]{"system_schema", "tables", "system_auth", table});
         UntypedResultSet result = (UntypedResultSet)TPCUtils.blockingGet(QueryProcessor.executeOnceInternal(cql, new Object[0]));
         return result != null && !result.isEmpty();
      }).collect(Collectors.toList());
   }

   private static Set<String> cpuinfoFlags(Logger logger) {
      Set flags = Collections.emptySet();

      try {
         FBUtilities.CpuInfo cpuInfo = FBUtilities.CpuInfo.load();
         flags = ((FBUtilities.CpuInfo.PhysicalProcessor)cpuInfo.getProcessors().get(0)).getFlags();
      } catch (Exception var3) {
         logger.warn("Could not read /proc/cpuinfo");
      }

      return flags;
   }

   static void verifyCpu(Logger logger, Supplier<FBUtilities.CpuInfo> cpuInfoSupplier) {
      FBUtilities.CpuInfo cpuInfo = (FBUtilities.CpuInfo)cpuInfoSupplier.get();
      if(cpuInfo != null && !cpuInfo.getProcessors().isEmpty()) {
         logger.info("CPU information: {} physical processors: {}", Integer.valueOf(cpuInfo.getProcessors().size()), cpuInfo.getProcessors().stream().map((p) -> {
            return p.getName() + " (" + p.getCores() + " cores, " + p.getThreadsPerCore() + " threads-per-core, " + p.getCacheSize() + " cache)";
         }).collect(Collectors.joining(", ")));
         Stream var10000 = cpuInfo.getProcessors().stream().flatMap((p) -> {
            return p.cpuIds().boxed();
         });
         cpuInfo.getClass();
         Map<String, List<Integer>> governors = (Map)var10000.collect(Collectors.groupingBy(cpuInfo::fetchCpuScalingGovernor));
         Map<String, List<Integer>> governors = new TreeMap(governors);
         logger.info("CPU scaling governors: {}", governors.entrySet().stream().map((e) -> {
            return "CPUs " + FBUtilities.CpuInfo.niceCpuIdList((List)e.getValue()) + ": " + (String)e.getKey();
         }).collect(Collectors.joining(", ")));
         if(governors.size() == 1) {
            String governor = (String)governors.keySet().iterator().next();
            byte var6 = -1;
            switch(governor.hashCode()) {
            case -1480388560:
               if(governor.equals("performance")) {
                  var6 = 0;
               }
               break;
            case -841873492:
               if(governor.equals("no_scaling_governor")) {
                  var6 = 2;
               }
               break;
            case -284840886:
               if(governor.equals("unknown")) {
                  var6 = 3;
               }
               break;
            case 143845282:
               if(governor.equals("no_cpufreq")) {
                  var6 = 1;
               }
            }

            switch(var6) {
            case 0:
               break;
            case 1:
            case 2:
               logger.warn("CPU scaling governors could not be inquired, as /sys/devices/system/cpu/cpu*/cpufreq is not readable.");
               break;
            case 3:
            default:
               logger.warn("CPU scaling governors are all set to {}, but 'performance' is recommended (see above)", governor);
            }
         } else {
            logger.warn(governors.containsKey("performance")?"Not all CPU scaling governors not set to 'performance' (see above)":"None of the CPU scaling governors are set to 'performance' (see above)");
         }

      } else {
         logger.warn("Could not get any CPU information from /proc/cpuinfo");
      }
   }

   static void verifyZoneReclaimMode(Logger logger, String reclaimMode) {
      if("0".equals(reclaimMode)) {
         logger.info("Linux NUMA zone-reclaim-mode is set to '0'");
      } else {
         logger.warn("Linux NUMA zone-reclaim-mode is set to '{}', but should be '0'", reclaimMode);
      }

   }

   private static boolean verifyLimit(Logger logger, Map<String, List<String>> limitsMap, String name, String shortName, int limit) {
      List<String> lst = (List)limitsMap.get(name);
      String value = (String)lst.get(0);
      int intValue = "unlimited".equals(value)?2147483647:Integer.parseInt(value);
      if(intValue < limit) {
         logger.warn("Limit for '{}' ({}) is recommended to be '{}', but is '{}'", new Object[]{shortName, name, limit == 2147483647?"unlimited":Integer.toString(limit), "soft:" + (String)lst.get(0) + ", hard:" + (String)lst.get(1) + " [" + (String)lst.get(2) + ']'});
         return false;
      } else {
         return true;
      }
   }

   static void verifyLimits(Logger logger, List<String> limits) {
      Pattern twoSpaces = Pattern.compile("[ ][ ]+");
      Stream var10000 = limits.stream().skip(1L);
      twoSpaces.getClass();
      Map<String, List<String>> limitsMap = (Map)var10000.map(twoSpaces::split).map((arr) -> {
         return (List)Arrays.stream(arr).map(String::trim).collect(Collectors.toList());
      }).collect(Collectors.toMap((l) -> {
         return (String)l.get(0);
      }, (l) -> {
         return l.subList(1, l.size());
      }));
      boolean ok = verifyLimit(logger, limitsMap, "Max locked memory", "memlock", 2147483647);
      ok &= verifyLimit(logger, limitsMap, "Max open files", "nofile", 100000);
      ok &= verifyLimit(logger, limitsMap, "Max processes", "nproc", '耀');
      ok &= verifyLimit(logger, limitsMap, "Max address space", "as", 2147483647);
      if(ok) {
         logger.info("Limits configured according to recommended production settings");
      }

   }

   static void verifyThpDefrag(Logger logger, String defrag) {
      Matcher m = Pattern.compile(".*\\[(.+)].*").matcher(defrag);
      if(m.matches()) {
         defrag = m.group(1);
         byte var4 = -1;
         switch(defrag.hashCode()) {
         case 104712844:
            if(defrag.equals("never")) {
               var4 = 0;
            }
         default:
            switch(var4) {
            case 0:
               logger.info("Linux THP defrag is set to 'never'");
               break;
            default:
               logger.warn("Linux THP defrag is set to '{}', but should be 'never'", defrag);
            }
         }
      } else {
         logger.warn("Unable to parse content '{}' of  /sys/kernel/mm/transparent_hugepage/defrag", defrag);
      }

   }

   private static void checkMountpoint(Logger logger, String type, String directory) throws StartupException {
      FileUtils.MountPoint mountpoint = FileUtils.MountPoint.mountPointForDirectory(directory);
      if(mountpoint == FileUtils.MountPoint.DEFAULT) {
         logger.warn("Could not detect mountpoint for directory {} {}", type, directory);
      } else if(Integer.bitCount(mountpoint.sectorSize) != 1) {
         throw new StartupException(3, String.format("Sector size for %s is not a power of 2 (%d)", new Object[]{mountpoint.device, Integer.valueOf(mountpoint.sectorSize)}));
      } else {
         String ssd = mountpoint.onSSD?"SSD":"rotational";
         String var5 = mountpoint.fstype;
         byte var6 = -1;
         switch(var5.hashCode()) {
         case 118597:
            if(var5.equals("xfs")) {
               var6 = 1;
            }
            break;
         case 3127859:
            if(var5.equals("ext4")) {
               var6 = 0;
            }
         }

         switch(var6) {
         case 0:
         case 1:
            logger.info("{} directory {} is on device {} (appears to be {} with logical sector size {}), formatted using {} file system", new Object[]{type, directory, mountpoint.device, ssd, Integer.valueOf(mountpoint.sectorSize), mountpoint.fstype});
            break;
         default:
            logger.warn("{} directory {} on device {} (appears to be {} with logical sector size {}) uses a not recommended file system type '{}', (mounted as {})", new Object[]{type, directory, mountpoint.device, ssd, Integer.valueOf(mountpoint.sectorSize), mountpoint.fstype, mountpoint.mountpoint});
         }

      }
   }
}
