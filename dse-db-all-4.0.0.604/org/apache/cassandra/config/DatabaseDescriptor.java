package org.apache.cassandra.config;

import com.datastax.bdp.db.audit.AuditLoggingOptions;
import com.datastax.bdp.db.audit.IAuditLogger;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.sun.management.OperatingSystemMXBean;
import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Constructor;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.file.FileStore;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.cassandra.auth.AllowAllInternodeAuthenticator;
import org.apache.cassandra.auth.AuthConfig;
import org.apache.cassandra.auth.AuthManager;
import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.auth.IAuthorizer;
import org.apache.cassandra.auth.IInternodeAuthenticator;
import org.apache.cassandra.auth.IRoleManager;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.DiskOptimizationStrategy;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.DynamicEndpointSnitch;
import org.apache.cassandra.locator.EndpointSnitchInfo;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.SeedProvider;
import org.apache.cassandra.net.BackPressureStrategy;
import org.apache.cassandra.net.RateBasedBackPressure;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.LineNumberInference;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabaseDescriptor {
   private static final Logger logger = LoggerFactory.getLogger(DatabaseDescriptor.class);
   private static final int MAX_NUM_TOKENS = 1536;
   private static Config conf;
   static final long LOWEST_ACCEPTED_TIMEOUT = 10L;
   private static IEndpointSnitch snitch;
   private static InetAddress listenAddress;
   private static InetAddress broadcastAddress;
   private static InetAddress nativeTransportAddress;
   private static InetAddress broadcastNativeTransportAddress;
   private static SeedProvider seedProvider;
   private static IInternodeAuthenticator internodeAuthenticator = new AllowAllInternodeAuthenticator();
   private static boolean jmxLocalOnly;
   private static Integer jmxPort;
   private static IPartitioner partitioner;
   private static String paritionerName;
   private static Config.AccessMode diskAccessMode;
   private static Config.AccessMode indexAccessMode;
   private static Config.AccessMode commitlogAccessMode;
   private static IAuthenticator authenticator;
   private static AuthManager authManager;
   private static long preparedStatementsCacheSizeInMB;
   private static long keyCacheSizeInMB;
   private static long counterCacheSizeInMB;
   private static long indexSummaryCapacityInMB;
   private static Comparator<InetAddress> localComparator;
   private static EncryptionContext encryptionContext;
   private static boolean hasLoggedConfig;
   private static BackPressureStrategy backPressureStrategy;
   private static DiskOptimizationStrategy diskOptimizationStrategy;
   private static boolean clientInitialized;
   private static boolean toolInitialized;
   private static boolean daemonInitialized;
   private static final AtomicBoolean tpcInitialized = new AtomicBoolean();
   private static final int searchConcurrencyFactor = Integer.parseInt(System.getProperty("cassandra.search_concurrency_factor", "1"));
   private static final boolean disableSTCSInL0 = Boolean.getBoolean("cassandra.disable_stcs_in_l0");
   private static final boolean unsafeSystem = Boolean.getBoolean("cassandra.unsafesystem");
   private static IAuditLogger auditLogger;

   public DatabaseDescriptor() {
   }

   @VisibleForTesting
   public static void daemonInitialization(boolean initializeTPC) throws ConfigurationException {
      if(toolInitialized) {
         throw new AssertionError("toolInitialization() already called");
      } else if(clientInitialized) {
         throw new AssertionError("clientInitialization() already called");
      } else {
         LineNumberInference.init();
         if(!daemonInitialized) {
            daemonInitialized = true;
            setConfig(loadConfig());
            applyAll();
            AuthConfig.applyAuth();
            applyAuditLoggerConfig();
            if(initializeTPC) {
               TPC.ensureInitialized(true);
            }

         }
      }
   }

   public static void daemonInitialization() throws ConfigurationException {
      daemonInitialization(true);
   }

   public static void toolInitialization() {
      toolInitialization(true);
   }

   public static void toolInitialization(boolean failIfDaemonOrClient) {
      if(failIfDaemonOrClient || !daemonInitialized && !clientInitialized) {
         if(daemonInitialized) {
            throw new AssertionError("daemonInitialization() already called");
         } else if(clientInitialized) {
            throw new AssertionError("clientInitialization() already called");
         } else if(!toolInitialized) {
            toolInitialized = true;
            setConfig(loadConfig());
            applySimpleConfig();
            applyPartitioner();
            applySnitch();
            applyEncryptionContext();
            TPC.ensureInitialized(true);
         }
      }
   }

   public static void clientInitialization() {
      clientInitialization(true);
   }

   public static void clientInitialization(boolean failIfDaemonOrTool) {
      clientInitialization(failIfDaemonOrTool, true, new Config());
   }

   public static void clientInitialization(boolean failIfDaemonOrTool, boolean initializeTPC, Config config) {
      if(failIfDaemonOrTool || !daemonInitialized && !toolInitialized) {
         if(daemonInitialized) {
            throw new AssertionError("daemonInitialization() already called");
         } else if(toolInitialized) {
            throw new AssertionError("toolInitialization() already called");
         } else if(!clientInitialized) {
            clientInitialized = true;
            Config.setClientMode(true);
            conf = config;
            diskOptimizationStrategy = DiskOptimizationStrategy.create(conf);
            if(initializeTPC) {
               TPC.ensureInitialized(false);
            }

         }
      }
   }

   public static boolean isClientInitialized() {
      return clientInitialized;
   }

   public static boolean isToolInitialized() {
      return toolInitialized;
   }

   public static boolean isClientOrToolInitialized() {
      return clientInitialized || toolInitialized;
   }

   public static boolean isDaemonInitialized() {
      return daemonInitialized;
   }

   public static boolean isTPCInitialized() {
      return tpcInitialized.get();
   }

   public static boolean setTPCInitialized() {
      return tpcInitialized.compareAndSet(false, true);
   }

   public static Config getRawConfig() {
      return conf;
   }

   @VisibleForTesting
   public static Config loadConfig() throws ConfigurationException {
      ConfigurationLoader loader = ConfigurationLoader.create();
      Config config = loader.loadConfig();
      if(!hasLoggedConfig) {
         hasLoggedConfig = true;
         Config.log(config);
      }

      return config;
   }

   private static InetAddress getNetworkInterfaceAddress(String intf, String configName, boolean preferIPv6) throws ConfigurationException {
      try {
         NetworkInterface ni = NetworkInterface.getByName(intf);
         if(ni == null) {
            throw new ConfigurationException("Configured " + configName + " \"" + intf + "\" could not be found", false);
         } else {
            Enumeration<InetAddress> addrs = ni.getInetAddresses();
            if(!addrs.hasMoreElements()) {
               throw new ConfigurationException("Configured " + configName + " \"" + intf + "\" was found, but had no addresses", false);
            } else {
               InetAddress retval = null;

               while(addrs.hasMoreElements()) {
                  InetAddress temp = (InetAddress)addrs.nextElement();
                  if(preferIPv6 && temp instanceof Inet6Address) {
                     return temp;
                  }

                  if(!preferIPv6 && temp instanceof Inet4Address) {
                     return temp;
                  }

                  if(retval == null) {
                     retval = temp;
                  }
               }

               return retval;
            }
         }
      } catch (SocketException var7) {
         throw new ConfigurationException("Configured " + configName + " \"" + intf + "\" caused an exception", var7);
      }
   }

   @VisibleForTesting
   public static void setConfig(Config config) {
      conf = config;
   }

   private static void applyAll() throws ConfigurationException {
      applySimpleConfig();
      applyPartitioner();
      applyAddressConfig();
      applySnitch();
      applyInitialTokens();
      applySeedProvider();
      applyEncryptionContext();
      conf.nodesync.validate();
   }

   private static void applyAuditLoggerConfig() {
      auditLogger = IAuditLogger.fromConfiguration(getRawConfig());
   }

   private static void applySimpleConfig() {
      if(conf.commitlog_sync == null) {
         throw new ConfigurationException("Missing required directive CommitLogSync", false);
      } else {
         if(conf.commitlog_sync == Config.CommitLogSync.batch) {
            if(conf.commitlog_sync_period_in_ms != 0) {
               throw new ConfigurationException("Batch sync specified, but commitlog_sync_period_in_ms found. Only specify commitlog_sync_batch_window_in_ms when using batch sync", false);
            }

            logger.debug("Syncing log with batch mode");
         } else if(conf.commitlog_sync == Config.CommitLogSync.group) {
            if(Double.isNaN(conf.commitlog_sync_group_window_in_ms) || conf.commitlog_sync_group_window_in_ms <= 0.0D) {
               throw new ConfigurationException("Missing value for commitlog_sync_group_window_in_ms: positive double value expected.", false);
            }

            if(conf.commitlog_sync_period_in_ms != 0) {
               throw new ConfigurationException("Group sync specified, but commitlog_sync_period_in_ms found. Only specify commitlog_sync_group_window_in_ms when using group sync", false);
            }

            logger.debug("Syncing log with a group window of {}", Integer.valueOf(conf.commitlog_sync_period_in_ms));
         } else {
            if(conf.commitlog_sync_period_in_ms <= 0) {
               throw new ConfigurationException("Missing value for commitlog_sync_period_in_ms: positive integer expected", false);
            }

            if(!Double.isNaN(conf.commitlog_sync_batch_window_in_ms)) {
               throw new ConfigurationException("commitlog_sync_period_in_ms specified, but commitlog_sync_batch_window_in_ms found.  Only specify commitlog_sync_period_in_ms when using periodic sync.", false);
            }

            logger.debug("Syncing log with a period of {}", Integer.valueOf(conf.commitlog_sync_period_in_ms));
         }

         diskAccessMode = conf.disk_access_mode.data;
         indexAccessMode = conf.disk_access_mode.index;
         commitlogAccessMode = conf.disk_access_mode.commitlog;
         logger.info("DiskAccessMode is {}, indexAccessMode is {}, commitlogAccessMode is {}", new Object[]{diskAccessMode, indexAccessMode, commitlogAccessMode});
         if(diskAccessMode == Config.AccessMode.mmap || indexAccessMode == Config.AccessMode.mmap || commitlogAccessMode == Config.AccessMode.mmap) {
            logger.info("Memory-mapped access mode is selected. Because page faults block and thus cause thread-per-core architectures to be inefficient, this should only be used if all data is either locked to memory or if the underlying storage is non-volatile memory.");
         }

         if(conf.gc_warn_threshold_in_ms < 0) {
            throw new ConfigurationException("gc_warn_threshold_in_ms must be a positive integer");
         } else if(conf.phi_convict_threshold >= 5.0D && conf.phi_convict_threshold <= 16.0D) {
            if(conf.file_cache_size_in_mb == null) {
               RuntimeMXBean bean = ManagementFactory.getRuntimeMXBean();
               String directMemSizeParam = "-XX:MaxDirectMemorySize=";
               Iterator var2 = bean.getInputArguments().iterator();

               while(var2.hasNext()) {
                  String s = (String)var2.next();
                  if(s.contains(directMemSizeParam)) {
                     String arg = s.replace(directMemSizeParam, "").trim().toLowerCase();
                     int multiplier;
                     if(arg.endsWith("k")) {
                        multiplier = 1024;
                     } else if(arg.endsWith("m")) {
                        multiplier = 1048576;
                     } else {
                        if(!arg.endsWith("g")) {
                           throw new ConfigurationException("Can't parse direct memory param: '" + arg + "'");
                        }

                        multiplier = 1073741824;
                     }

                     int memSize = Integer.parseInt(arg.replaceAll("\\D+", ""));
                     conf.file_cache_size_in_mb = Integer.valueOf((int)(0.5D * (double)memSize * (double)multiplier / 1024.0D / 1024.0D));
                     logger.info("Using file cache of {}MB", conf.file_cache_size_in_mb);
                     break;
                  }
               }

               if(conf.file_cache_size_in_mb == null) {
                  OperatingSystemMXBean osBean = (OperatingSystemMXBean)ManagementFactory.getOperatingSystemMXBean();
                  conf.file_cache_size_in_mb = Integer.valueOf((int)(osBean.getTotalPhysicalMemorySize() / 3145728L));
                  if(!toolInitialized) {
                     logger.warn("Falling back to default file_cache_size_in_mb calculation. file_cache_size_in_mb is set to {}. Set `-DMaxDirectMemorySize` for a more precise calculation", conf.file_cache_size_in_mb);
                  }
               }
            }

            if(conf.file_cache_round_up == null) {
               conf.file_cache_round_up = Boolean.valueOf(conf.disk_optimization_strategy == Config.DiskOptimizationStrategy.spinning);
            }

            if(conf.memtable_offheap_space_in_mb == null) {
               conf.memtable_offheap_space_in_mb = Integer.valueOf((int)(Runtime.getRuntime().maxMemory() / 4194304L));
            }

            if(conf.memtable_offheap_space_in_mb.intValue() < 0) {
               throw new ConfigurationException("memtable_offheap_space_in_mb must be positive, but was " + conf.memtable_offheap_space_in_mb, false);
            } else {
               if(conf.memtable_heap_space_in_mb == null) {
                  conf.memtable_heap_space_in_mb = Integer.valueOf((int)(Runtime.getRuntime().maxMemory() / 4194304L));
               }

               if(conf.memtable_heap_space_in_mb.intValue() <= 0) {
                  throw new ConfigurationException("memtable_heap_space_in_mb must be positive, but was " + conf.memtable_heap_space_in_mb, false);
               } else {
                  logger.info("Global memtable on-heap threshold is enabled at {} MiB", conf.memtable_heap_space_in_mb);
                  if(conf.memtable_offheap_space_in_mb.intValue() == 0) {
                     logger.info("Global memtable off-heap threshold is disabled, HeapAllocator will be used instead");
                  } else {
                     logger.info("Global memtable off-heap threshold is enabled at {} MiB", conf.memtable_offheap_space_in_mb);
                  }

                  checkForLowestAcceptedTimeouts(conf);
                  if(conf.native_transport_max_frame_size_in_mb <= 0) {
                     throw new ConfigurationException("native_transport_max_frame_size_in_mb must be positive, but was " + conf.native_transport_max_frame_size_in_mb, false);
                  } else if(conf.native_transport_max_frame_size_in_mb >= 2048) {
                     throw new ConfigurationException("native_transport_max_frame_size_in_mb must be smaller than 2048, but was " + conf.native_transport_max_frame_size_in_mb, false);
                  } else {
                     if(conf.commitlog_directory == null) {
                        conf.commitlog_directory = storagedirFor("commitlog");
                     }

                     conf.commitlog_directory = resolveAndCheckDirectory("commitlog", conf.commitlog_directory);
                     if(conf.hints_directory == null) {
                        conf.hints_directory = storagedirFor("hints");
                     }

                     conf.hints_directory = resolveAndCheckDirectory("hints", conf.hints_directory);
                     if(conf.cdc_raw_directory == null) {
                        conf.cdc_raw_directory = storagedirFor("cdc_raw");
                     }

                     conf.cdc_raw_directory = resolveAndCheckDirectory("cdc-raw", conf.cdc_raw_directory);
                     short preferredSize;
                     boolean var17;
                     int minSize;
                     if(conf.commitlog_total_space_in_mb == null) {
                        preferredSize = 8192;
                        var17 = false;

                        try {
                           minSize = Ints.saturatedCast(guessFileStore(conf.commitlog_directory).getTotalSpace() / 1048576L / 4L);
                        } catch (IOException var9) {
                           logger.debug("Error checking disk space", var9);
                           throw new ConfigurationException(String.format("Unable to check disk space available to %s. Perhaps the Cassandra user does not have the necessary permissions", new Object[]{conf.commitlog_directory}), var9);
                        }

                        if(minSize < preferredSize) {
                           logger.warn("Small commitlog volume detected at {}; setting commitlog_total_space_in_mb to {}.  You can override this in cassandra.yaml", conf.commitlog_directory, Integer.valueOf(minSize));
                           conf.commitlog_total_space_in_mb = Integer.valueOf(minSize);
                        } else {
                           conf.commitlog_total_space_in_mb = Integer.valueOf(preferredSize);
                        }
                     }

                     if(conf.cdc_total_space_in_mb == 0) {
                        preferredSize = 4096;
                        var17 = false;

                        try {
                           minSize = Ints.saturatedCast(guessFileStore(conf.cdc_raw_directory).getTotalSpace() / 1048576L / 8L);
                        } catch (IOException var8) {
                           logger.debug("Error checking disk space", var8);
                           throw new ConfigurationException(String.format("Unable to check disk space available to %s. Perhaps the Cassandra user does not have the necessary permissions", new Object[]{conf.cdc_raw_directory}), var8);
                        }

                        if(minSize < preferredSize) {
                           logger.warn("Small cdc volume detected at {}; setting cdc_total_space_in_mb to {}.  You can override this in cassandra.yaml", conf.cdc_raw_directory, Integer.valueOf(minSize));
                           conf.cdc_total_space_in_mb = minSize;
                        } else {
                           conf.cdc_total_space_in_mb = preferredSize;
                        }
                     }

                     if(conf.cdc_enabled) {
                        logger.info("cdc_enabled is true. Starting casssandra node with Change-Data-Capture enabled.");
                     }

                     if(conf.saved_caches_directory == null) {
                        conf.saved_caches_directory = storagedirFor("saved_caches");
                     }

                     conf.saved_caches_directory = resolveAndCheckDirectory("saved-caches", conf.saved_caches_directory);
                     if(conf.data_file_directories == null || conf.data_file_directories.length == 0) {
                        conf.data_file_directories = new String[]{storagedir("data_file_directories") + File.separator + "data"};
                     }

                     for(int i = 0; i < conf.data_file_directories.length; ++i) {
                        conf.data_file_directories[i] = resolveAndCheckDirectory("data", conf.data_file_directories[i]);
                     }

                     long dataFreeBytes = 0L;
                     String[] var21 = conf.data_file_directories;
                     int var23 = var21.length;

                     for(int var25 = 0; var25 < var23; ++var25) {
                        String datadir = var21[var25];
                        if(datadir == null) {
                           throw new ConfigurationException("data_file_directories must not contain empty entry", false);
                        }

                        if(datadir.equals(conf.commitlog_directory)) {
                           throw new ConfigurationException("commitlog_directory must not be the same as any data_file_directories", false);
                        }

                        if(datadir.equals(conf.hints_directory)) {
                           throw new ConfigurationException("hints_directory must not be the same as any data_file_directories", false);
                        }

                        if(datadir.equals(conf.saved_caches_directory)) {
                           throw new ConfigurationException("saved_caches_directory must not be the same as any data_file_directories", false);
                        }

                        try {
                           dataFreeBytes = saturatedSum(dataFreeBytes, guessFileStore(datadir).getUnallocatedSpace());
                        } catch (IOException var7) {
                           logger.debug("Error checking disk space", var7);
                           throw new ConfigurationException(String.format("Unable to check disk space available to %s. Perhaps the Cassandra user does not have the necessary permissions", new Object[]{datadir}), var7);
                        }
                     }

                     if(dataFreeBytes < 68719476736L) {
                        logger.warn("Only {} free across all data volumes. Consider adding more capacity to your cluster or removing obsolete snapshots", FBUtilities.prettyPrintMemory(dataFreeBytes));
                     }

                     if(conf.commitlog_directory.equals(conf.saved_caches_directory)) {
                        throw new ConfigurationException("saved_caches_directory must not be the same as the commitlog_directory", false);
                     } else if(conf.commitlog_directory.equals(conf.hints_directory)) {
                        throw new ConfigurationException("hints_directory must not be the same as the commitlog_directory", false);
                     } else if(conf.hints_directory.equals(conf.saved_caches_directory)) {
                        throw new ConfigurationException("saved_caches_directory must not be the same as the hints_directory", false);
                     } else {
                        if(conf.memtable_flush_writers == 0) {
                           conf.memtable_flush_writers = 4;
                        }

                        if(conf.memtable_flush_writers < 1) {
                           throw new ConfigurationException("memtable_flush_writers must be at least 1, but was " + conf.memtable_flush_writers, false);
                        } else {
                           if(conf.memtable_cleanup_threshold == null) {
                              conf.memtable_cleanup_threshold = Double.valueOf(1.0D / (double)(1 + conf.memtable_flush_writers));
                           }

                           if(conf.memtable_cleanup_threshold.doubleValue() < 0.009999999776482582D) {
                              throw new ConfigurationException("memtable_cleanup_threshold must be >= 0.01, but was " + conf.memtable_cleanup_threshold, false);
                           } else if(conf.memtable_cleanup_threshold.doubleValue() > 0.9900000095367432D) {
                              throw new ConfigurationException("memtable_cleanup_threshold must be <= 0.99, but was " + conf.memtable_cleanup_threshold, false);
                           } else {
                              if(conf.memtable_cleanup_threshold.doubleValue() < 0.10000000149011612D) {
                                 logger.warn("memtable_cleanup_threshold is set very low [{}], which may cause performance degradation", conf.memtable_cleanup_threshold);
                              }

                              logger.info("Using {} memtable flush writers with global cleanup threshold of {}", Integer.valueOf(conf.memtable_flush_writers), conf.memtable_cleanup_threshold);
                              if(conf.concurrent_compactors == null) {
                                 conf.concurrent_compactors = Integer.valueOf(Math.min(8, Math.max(2, Math.min(FBUtilities.getAvailableProcessors(), conf.data_file_directories.length))));
                              }

                              if(conf.concurrent_validations < 1) {
                                 conf.concurrent_validations = 2147483647;
                              }

                              if(conf.concurrent_compactors.intValue() <= 0) {
                                 throw new ConfigurationException("concurrent_compactors should be strictly greater than 0, but was " + conf.concurrent_compactors, false);
                              } else if(conf.concurrent_materialized_view_builders <= 0) {
                                 throw new ConfigurationException("concurrent_materialized_view_builders should be strictly greater than 0, but was " + conf.concurrent_materialized_view_builders, false);
                              } else if(conf.seed_gossip_probability >= 0.01D && conf.seed_gossip_probability <= 1.0D) {
                                 if(conf.sstable_preemptive_open_interval_in_mb > 0 && conf.sstable_preemptive_open_interval_in_mb < 4) {
                                    logger.warn("Setting sstable_preemptive_open_interval_in_mb to a very low value ({}) will increase GC pressure significantly during compactions, and will likely have an adverse effect on performance.", Integer.valueOf(conf.sstable_preemptive_open_interval_in_mb));
                                 }

                                 if(conf.num_tokens > 1536) {
                                    throw new ConfigurationException(String.format("A maximum number of %d tokens per node is supported", new Object[]{Integer.valueOf(1536)}), false);
                                 } else {
                                    try {
                                       preparedStatementsCacheSizeInMB = conf.prepared_statements_cache_size_mb == null?(long)Math.max(10, (int)(Runtime.getRuntime().maxMemory() / 1024L / 1024L / 256L)):conf.prepared_statements_cache_size_mb.longValue();
                                       if(preparedStatementsCacheSizeInMB <= 0L) {
                                          throw new NumberFormatException();
                                       }
                                    } catch (NumberFormatException var14) {
                                       throw new ConfigurationException("prepared_statements_cache_size_mb option was set incorrectly to '" + conf.prepared_statements_cache_size_mb + "', supported values are <integer> >= 0.", false);
                                    }

                                    try {
                                       keyCacheSizeInMB = conf.key_cache_size_in_mb == null?(long)Math.min(Math.max(1, (int)((double)Runtime.getRuntime().totalMemory() * 0.05D / 1024.0D / 1024.0D)), 100):conf.key_cache_size_in_mb.longValue();
                                       if(keyCacheSizeInMB < 0L) {
                                          throw new NumberFormatException();
                                       }
                                    } catch (NumberFormatException var13) {
                                       throw new ConfigurationException("key_cache_size_in_mb option was set incorrectly to '" + conf.key_cache_size_in_mb + "', supported values are <integer> >= 0.", false);
                                    }

                                    try {
                                       counterCacheSizeInMB = conf.counter_cache_size_in_mb == null?(long)Math.min(Math.max(1, (int)((double)Runtime.getRuntime().totalMemory() * 0.025D / 1024.0D / 1024.0D)), 50):conf.counter_cache_size_in_mb.longValue();
                                       if(counterCacheSizeInMB < 0L) {
                                          throw new NumberFormatException();
                                       }
                                    } catch (NumberFormatException var12) {
                                       throw new ConfigurationException("counter_cache_size_in_mb option was set incorrectly to '" + conf.counter_cache_size_in_mb + "', supported values are <integer> >= 0.", false);
                                    }

                                    if(conf.encryption_options != null) {
                                       logger.warn("Please rename encryption_options as server_encryption_options in the yaml");
                                       conf.server_encryption_options = conf.encryption_options;
                                    }

                                    if(conf.user_defined_function_fail_timeout < 0L) {
                                       throw new ConfigurationException("user_defined_function_fail_timeout must not be negative", false);
                                    } else if(conf.user_defined_function_warn_timeout < 0L) {
                                       throw new ConfigurationException("user_defined_function_warn_timeout must not be negative", false);
                                    } else if(conf.user_defined_function_fail_timeout < conf.user_defined_function_warn_timeout) {
                                       throw new ConfigurationException("user_defined_function_warn_timeout must less than user_defined_function_fail_timeout", false);
                                    } else if(conf.commitlog_segment_size_in_mb <= 0) {
                                       throw new ConfigurationException("commitlog_segment_size_in_mb must be positive, but was " + conf.commitlog_segment_size_in_mb, false);
                                    } else if(conf.commitlog_segment_size_in_mb >= 2048) {
                                       throw new ConfigurationException("commitlog_segment_size_in_mb must be smaller than 2048, but was " + conf.commitlog_segment_size_in_mb, false);
                                    } else {
                                       if(conf.max_mutation_size_in_kb == null) {
                                          conf.max_mutation_size_in_kb = Integer.valueOf(conf.commitlog_segment_size_in_mb * 1024 / 2);
                                       } else if(conf.commitlog_segment_size_in_mb * 1024 < 2 * conf.max_mutation_size_in_kb.intValue()) {
                                          throw new ConfigurationException("commitlog_segment_size_in_mb must be at least twice the size of max_mutation_size_in_kb / 1024", false);
                                       }

                                       if(conf.native_transport_port_ssl != null && conf.native_transport_port_ssl.intValue() != conf.native_transport_port && !conf.client_encryption_options.enabled) {
                                          throw new ConfigurationException("Encryption must be enabled in client_encryption_options for native_transport_port_ssl", false);
                                       } else if(conf.max_value_size_in_mb <= 0) {
                                          throw new ConfigurationException("max_value_size_in_mb must be positive", false);
                                       } else if(conf.max_value_size_in_mb >= 2048) {
                                          throw new ConfigurationException("max_value_size_in_mb must be smaller than 2048, but was " + conf.max_value_size_in_mb, false);
                                       } else {
                                          diskOptimizationStrategy = DiskOptimizationStrategy.create(conf);
                                          logger.info("Assuming {} based on disk_optimization_strategy YAML property. Please update this property if this is incorrect as a number of optimizations depend on it and an incorrect value may result in degraded performances.", diskOptimizationStrategy.diskType());
                                          if(conf.max_memory_to_lock_mb < 0) {
                                             throw new ConfigurationException("max_memory_to_lock_mb must be be >= 0");
                                          } else if(conf.max_memory_to_lock_fraction >= 0.0D && conf.max_memory_to_lock_fraction <= 1.0D) {
                                             try {
                                                ParameterizedClass strategy = conf.back_pressure_strategy != null?conf.back_pressure_strategy:RateBasedBackPressure.withDefaultParams();
                                                Class<?> clazz = Class.forName(strategy.class_name);
                                                if(!BackPressureStrategy.class.isAssignableFrom(clazz)) {
                                                   throw new ConfigurationException(strategy + " is not an instance of " + BackPressureStrategy.class.getCanonicalName(), false);
                                                }

                                                Constructor<?> ctor = clazz.getConstructor(new Class[]{Map.class});
                                                BackPressureStrategy instance = (BackPressureStrategy)ctor.newInstance(new Object[]{strategy.parameters});
                                                logger.info("Back-pressure is {} with strategy {}.", backPressureEnabled()?"enabled":"disabled", conf.back_pressure_strategy);
                                                backPressureStrategy = instance;
                                             } catch (ConfigurationException var10) {
                                                throw var10;
                                             } catch (Exception var11) {
                                                throw new ConfigurationException("Error configuring back-pressure strategy: " + conf.back_pressure_strategy, var11);
                                             }

                                             if(conf.otc_coalescing_enough_coalesced_messages > 128) {
                                                throw new ConfigurationException("otc_coalescing_enough_coalesced_messages must be smaller than 128", false);
                                             } else if(conf.otc_coalescing_enough_coalesced_messages <= 0) {
                                                throw new ConfigurationException("otc_coalescing_enough_coalesced_messages must be positive", false);
                                             }
                                          } else {
                                             throw new ConfigurationException("max_memory_to_lock_fraction must be 0.0 <= max_memory_to_lock_fraction <= 1.0");
                                          }
                                       }
                                    }
                                 }
                              } else {
                                 throw new ConfigurationException("seed_gossip_probability must be between 0.01 and 1.0", false);
                              }
                           }
                        }
                     }
                  }
               }
            }
         } else {
            throw new ConfigurationException("phi_convict_threshold must be between 5 and 16, but was " + conf.phi_convict_threshold, false);
         }
      }
   }

   public static String resolveAndCheckDirectory(String type, String path) {
      try {
         File dir = new File(path);
         if(!dir.isAbsolute()) {
            dir = dir.getAbsoluteFile();
            logger.warn("{} directory '{}' is a relative path that has been resolved to '{}'. Specify absolute path names in the configuration to prevent this warning.", new Object[]{type, path, dir.getPath()});
            path = dir.getPath();
         }

         for(File checkDir = dir; checkDir != null; checkDir = checkDir.getParentFile()) {
            if(checkDir.exists()) {
               if(checkDir.canWrite() && checkDir.canRead()) {
                  break;
               }

               throw new ConfigurationException(String.format("%s directory '%s' or, if it does not already exist, an existing parent directory of it, is not readable and writable for the DSE. Check file system and configuration.", new Object[]{type, path}));
            }
         }

         FileUtils.MountPoint mountPoint = FileUtils.MountPoint.mountPointForDirectory(path);
         if(!mountPoint.blockDevice) {
            logger.warn("{} directory '{}' is not on a block device - check the I/O parameters: {}", new Object[]{type, path, mountPoint});
         }

         return path;
      } catch (ConfigurationException var4) {
         throw var4;
      } catch (Exception var5) {
         throw new ConfigurationException(String.format("Path for %s directory '%s' cannot be resolved (%s). Check configuration.", new Object[]{type, path, var5.toString()}));
      }
   }

   private static String storagedirFor(String type) {
      return storagedir(type + "_directory") + File.separator + type;
   }

   private static String storagedir(String errMsgType) {
      String storagedir = System.getProperty("cassandra.storagedir", (String)null);
      if(storagedir == null) {
         throw new ConfigurationException(errMsgType + " is missing and -Dcassandra.storagedir is not set", false);
      } else {
         return storagedir;
      }
   }

   public static void applyAddressConfig() throws ConfigurationException {
      applyAddressConfig(conf);
   }

   public static void applyAddressConfig(Config config) throws ConfigurationException {
      listenAddress = null;
      nativeTransportAddress = null;
      broadcastAddress = null;
      broadcastNativeTransportAddress = null;
      boolean isRpcAddressSet = config.rpc_address != null;
      boolean isRpcInterfaceSet = config.rpc_interface != null;
      boolean isNativeTransportAddressSet = config.native_transport_address != null;
      boolean isNativeTransportInterfaceSet = config.native_transport_interface != null;
      String propertyName;
      String propertyValue;
      if(!isRpcAddressSet && !isNativeTransportAddressSet || !isRpcInterfaceSet && !isNativeTransportInterfaceSet) {
         if(isRpcAddressSet) {
            if(config.native_transport_address == null) {
               logger.warn("The 'rpc_address' property is deprecated and will be removed on the next major release. Please update your yaml to use 'native_transport_address' instead.");
            } else {
               logger.warn("Both 'rpc_address={}' and 'native_transport_address={}' are specified. Using deprecated property 'rpc_address={}'. Please update your yaml to specify only 'native_transport_address'.", new Object[]{config.rpc_address, config.native_transport_address, config.rpc_address});
            }

            config.native_transport_address = config.rpc_address;
         }

         if(isRpcInterfaceSet) {
            if(config.native_transport_interface == null) {
               logger.warn("The 'rpc_interface' property is deprecated and will be removed on the next major release. Please update your yaml to use 'native_transport_interface' instead.");
            } else {
               logger.warn("Both 'rpc_interface={}' and 'native_transport_interface={}' are specified. Using deprecated property 'rpc_interface={}'. Please update your yaml to specify only 'native_transport_interface'.", new Object[]{config.rpc_interface, config.native_transport_interface, config.rpc_interface});
            }

            config.native_transport_interface = config.rpc_interface;
         }

         if(config.rpc_interface_prefer_ipv6 != null) {
            if(config.native_transport_interface_prefer_ipv6 == null) {
               logger.warn("The 'rpc_interface_prefer_ipv6' property is deprecated and will be removed on the next major release. Please update your yaml to use 'native_transport_interface_prefer_ipv6' instead.");
            } else {
               logger.warn("Both 'rpc_interface_prefer_ipv6={}' and 'rpc_interface_prefer_ipv6={}' are specified. Using deprecated property 'rpc_interface={}'. Please update your yaml to specify only 'native_transport_interface_prefer_ipv6'.", new Object[]{config.rpc_interface_prefer_ipv6, config.native_transport_interface_prefer_ipv6, config.rpc_interface_prefer_ipv6});
            }

            config.native_transport_interface_prefer_ipv6 = config.rpc_interface_prefer_ipv6;
         }

         if(config.broadcast_rpc_address != null) {
            if(config.native_transport_broadcast_address == null) {
               logger.warn("The 'broadcast_rpc_address' property is deprecated and will be removed on the next major release. Please update your yaml to use 'native_transport_broadcast_address' instead.");
            } else {
               logger.warn("Both 'broadcast_rpc_address={}' and 'native_transport_broadcast_address={}' are specified. Using deprecated property 'broadcast_rpc_address={}'. Please update your yaml to specify only 'native_transport_broadcast_address'.", new Object[]{config.broadcast_rpc_address, config.native_transport_broadcast_address, config.broadcast_rpc_address});
            }

            config.native_transport_broadcast_address = config.broadcast_rpc_address;
         }

         if(config.rpc_keepalive != null) {
            if(config.native_transport_keepalive == null) {
               logger.warn("The 'rpc_keepalive' property is deprecated and will be removed on the next major release. Please update your yaml to use 'native_transport_keepalive' instead.");
            } else {
               logger.warn("Both 'rpc_keepalive={}' and 'native_transport_keepalive={}' are specified. Using deprecated property 'rpc_keepalive={}'. Please update your yaml to specify only 'native_transport_keepalive'.", new Object[]{config.rpc_keepalive, config.native_transport_keepalive, config.rpc_keepalive});
            }

            config.native_transport_keepalive = config.rpc_keepalive;
         }

         if(config.native_transport_keepalive == null) {
            config.native_transport_keepalive = Boolean.valueOf(true);
         }

         if(config.native_transport_interface_prefer_ipv6 == null) {
            config.native_transport_interface_prefer_ipv6 = Boolean.valueOf(false);
         }

         if(config.listen_address != null && config.listen_interface != null) {
            throw new ConfigurationException("Set listen_address OR listen_interface, not both", false);
         } else {
            if(config.listen_address != null) {
               try {
                  listenAddress = InetAddress.getByName(config.listen_address);
               } catch (UnknownHostException var12) {
                  throw new ConfigurationException("Unknown listen_address '" + config.listen_address + "'", false);
               }

               if(listenAddress.isAnyLocalAddress()) {
                  throw new ConfigurationException("listen_address cannot be a wildcard address (" + config.listen_address + ")!", false);
               }
            } else if(config.listen_interface != null) {
               listenAddress = getNetworkInterfaceAddress(config.listen_interface, "listen_interface", config.listen_interface_prefer_ipv6);
            }

            if(config.broadcast_address != null) {
               try {
                  broadcastAddress = InetAddress.getByName(config.broadcast_address);
               } catch (UnknownHostException var11) {
                  throw new ConfigurationException("Unknown broadcast_address '" + config.broadcast_address + "'", false);
               }

               if(broadcastAddress.isAnyLocalAddress()) {
                  throw new ConfigurationException("broadcast_address cannot be a wildcard address (" + config.broadcast_address + ")!", false);
               }
            }

            if(config.native_transport_address != null) {
               try {
                  nativeTransportAddress = InetAddress.getByName(config.native_transport_address);
               } catch (UnknownHostException var10) {
                  throw new ConfigurationException("Unknown host in native_transport_address " + config.native_transport_address, false);
               }
            } else if(config.native_transport_interface != null) {
               nativeTransportAddress = getNetworkInterfaceAddress(config.native_transport_interface, "native_transport_interface", config.native_transport_interface_prefer_ipv6.booleanValue());
            } else {
               nativeTransportAddress = FBUtilities.getLocalAddress();
            }

            if(config.native_transport_broadcast_address != null) {
               try {
                  broadcastNativeTransportAddress = InetAddress.getByName(config.native_transport_broadcast_address);
               } catch (UnknownHostException var9) {
                  throw new ConfigurationException("Unknown broadcast_native_transport_address '" + config.native_transport_broadcast_address + "'", false);
               }

               if(broadcastNativeTransportAddress.isAnyLocalAddress()) {
                  throw new ConfigurationException("broadcast_native_transport_address cannot be a wildcard address (" + config.native_transport_broadcast_address + ")!", false);
               }
            } else if(nativeTransportAddress.isAnyLocalAddress()) {
               throw new ConfigurationException("If native_transport_address is set to a wildcard address (" + config.native_transport_address + "), then you must set broadcast_native_transport_address to a value other than " + config.native_transport_address, false);
            }

            propertyName = "cassandra.jmx.remote.port";
            propertyValue = System.getProperty(propertyName);
            if(propertyValue != null) {
               jmxLocalOnly = false;
               logger.info("JMX is enabled to receive remote connections on port: {}", jmxPort);
            } else {
               logger.warn("JMX is not enabled to receive remote connections. Please see cassandra-env.sh for more info.");
               propertyName = "cassandra.jmx.local.port";
               propertyValue = System.getProperty(propertyName);
               if(propertyValue == null) {
                  logger.error(propertyName + " missing from cassandra-env.sh, unable to start local JMX service.");
               } else {
                  jmxLocalOnly = true;
               }
            }

            if(propertyValue != null) {
               try {
                  jmxPort = Integer.valueOf(Integer.parseInt(propertyValue));
               } catch (NumberFormatException var8) {
                  throw new ConfigurationException("Unparseable JMX port at property'" + propertyName + "': " + propertyValue, false);
               }
            }

         }
      } else {
         propertyName = isRpcAddressSet?"rpc_address (deprecated)":"native_transport_address";
         propertyValue = isRpcInterfaceSet?"rpc_interface (deprecated)":"native_transport_interface";
         throw new ConfigurationException(String.format("Set %s OR %s, not both", new Object[]{propertyName, propertyValue}), false);
      }
   }

   public static void applyEncryptionContext() {
      encryptionContext = new EncryptionContext(conf.transparent_data_encryption_options);
      if(conf.system_info_encryption.enabled) {
         if(conf.system_info_encryption.isKmipKeyProvider()) {
            if(conf.system_info_encryption.kmip_host == null || conf.system_info_encryption.kmip_host.isEmpty()) {
               throw new ConfigurationException("system_info_encryption.kmip_host must be specified for KMIP key provider");
            }
         } else {
            if(conf.system_key_directory == null) {
               conf.system_key_directory = storagedirFor("system_key");
            }

            File systemKeyDir = new File(conf.system_key_directory);
            if(!systemKeyDir.isDirectory() || !systemKeyDir.canRead() || !systemKeyDir.canWrite()) {
               throw new ConfigurationException(String.format("system_key_directory '%s' must be a directory , readable and writeable by the DSE process.", new Object[]{conf.system_key_directory}));
            }
         }

         if(conf.system_info_encryption.key_name == null || conf.system_info_encryption.key_name.trim().isEmpty()) {
            throw new ConfigurationException("system_info_encryption.key_name must not be empty");
         }

         if(conf.system_info_encryption.chunk_length_kb <= 0) {
            throw new ConfigurationException("system_info_encryption.chunk_length_kb must be greater than 0");
         }

         if(conf.system_info_encryption.secret_key_strength <= 0) {
            throw new ConfigurationException("system_info_encryption.secret_key_strength must be greater than 0");
         }
      }

   }

   public static void applySeedProvider() {
      if(conf.seed_provider == null) {
         throw new ConfigurationException("seeds configuration is missing; a minimum of one seed is required.", false);
      } else {
         try {
            Class<?> seedProviderClass = Class.forName(conf.seed_provider.class_name);
            seedProvider = (SeedProvider)seedProviderClass.getConstructor(new Class[]{Map.class}).newInstance(new Object[]{conf.seed_provider.parameters});
         } catch (Exception var1) {
            throw new ConfigurationException(var1.getMessage() + "\nFatal configuration error; unable to start server.  See log for stacktrace.", true);
         }

         if(seedProvider.getSeeds().size() == 0) {
            throw new ConfigurationException("The seed provider lists no seeds.", false);
         }
      }
   }

   @VisibleForTesting
   static void checkForLowestAcceptedTimeouts(Config conf) {
      if(conf.read_request_timeout_in_ms < 10L) {
         logInfo("read_request_timeout_in_ms", conf.read_request_timeout_in_ms, 10L);
         conf.read_request_timeout_in_ms = 10L;
      }

      if(conf.range_request_timeout_in_ms < 10L) {
         logInfo("range_request_timeout_in_ms", conf.range_request_timeout_in_ms, 10L);
         conf.range_request_timeout_in_ms = 10L;
      }

      if(conf.request_timeout_in_ms < 10L) {
         logInfo("request_timeout_in_ms", conf.request_timeout_in_ms, 10L);
         conf.request_timeout_in_ms = 10L;
      }

      if(conf.write_request_timeout_in_ms < 10L) {
         logInfo("write_request_timeout_in_ms", conf.write_request_timeout_in_ms, 10L);
         conf.write_request_timeout_in_ms = 10L;
      }

      if(conf.cas_contention_timeout_in_ms < 10L) {
         logInfo("cas_contention_timeout_in_ms", conf.cas_contention_timeout_in_ms, 10L);
         conf.cas_contention_timeout_in_ms = 10L;
      }

      if(conf.counter_write_request_timeout_in_ms < 10L) {
         logInfo("counter_write_request_timeout_in_ms", conf.counter_write_request_timeout_in_ms, 10L);
         conf.counter_write_request_timeout_in_ms = 10L;
      }

      if(conf.truncate_request_timeout_in_ms < 10L) {
         logInfo("truncate_request_timeout_in_ms", conf.truncate_request_timeout_in_ms, 10L);
         conf.truncate_request_timeout_in_ms = 10L;
      }

   }

   private static void logInfo(String property, long actualValue, long lowestAcceptedValue) {
      logger.info("found {}::{} less than lowest acceptable value {}, continuing with {}", new Object[]{property, Long.valueOf(actualValue), Long.valueOf(lowestAcceptedValue), Long.valueOf(lowestAcceptedValue)});
   }

   public static void applyInitialTokens() {
      if(conf.initial_token != null) {
         Collection<String> tokens = tokensFromString(conf.initial_token);
         if(tokens.size() != conf.num_tokens) {
            throw new ConfigurationException("The number of initial tokens (by initial_token) specified is different from num_tokens value", false);
         }

         Iterator var1 = tokens.iterator();

         while(var1.hasNext()) {
            String token = (String)var1.next();
            partitioner.getTokenFactory().validate(token);
         }
      }

   }

   public static void applySnitch() {
      if(snitch == null) {
         if(conf.endpoint_snitch == null) {
            throw new ConfigurationException("Missing endpoint_snitch directive", false);
         }

         snitch = createEndpointSnitch(conf.dynamic_snitch, conf.endpoint_snitch);
      }

      EndpointSnitchInfo.create();
      final String localDC = snitch.getLocalDatacenter();
      localComparator = new Comparator<InetAddress>() {
         public int compare(InetAddress endpoint1, InetAddress endpoint2) {
            boolean local1 = localDC.equals(DatabaseDescriptor.snitch.getDatacenter(endpoint1));
            boolean local2 = localDC.equals(DatabaseDescriptor.snitch.getDatacenter(endpoint2));
            return local1 && !local2?-1:(local2 && !local1?1:0);
         }
      };
      if(conf.cross_dc_rtt_in_ms < 0L) {
         throw new ConfigurationException("cross_dc_rtt_in_ms must be non-negative, use 0 to disable or positive value to enable.", false);
      }
   }

   public static void applyPartitioner() {
      if(partitioner == null) {
         if(conf.partitioner == null) {
            throw new ConfigurationException("Missing directive: partitioner", false);
         }

         try {
            partitioner = FBUtilities.newPartitioner(System.getProperty("cassandra.partitioner", conf.partitioner));
         } catch (Exception var1) {
            throw new ConfigurationException("Invalid partitioner class " + conf.partitioner, false);
         }
      }

      paritionerName = partitioner.getClass().getCanonicalName();
   }

   private static long saturatedSum(long left, long right) {
      assert left >= 0L && right >= 0L;

      long sum = left + right;
      return sum < 0L?9223372036854775807L:sum;
   }

   static FileStore guessFileStore(String dir) throws IOException {
      Path path = Paths.get(dir, new String[0]);

      while(true) {
         try {
            return FileUtils.getFileStore(path);
         } catch (IOException var4) {
            if(!(var4 instanceof NoSuchFileException)) {
               throw var4;
            }

            Path parent = path.getParent();
            if(parent == null) {
               throw new ConfigurationException(String.format("Cannot resolve probably relative directory '%s' as it does not exist.", new Object[]{dir}));
            }

            path = parent;
         }
      }
   }

   public static IEndpointSnitch createEndpointSnitch(boolean dynamic, String snitchClassName) throws ConfigurationException {
      if(!snitchClassName.contains(".")) {
         snitchClassName = "org.apache.cassandra.locator." + snitchClassName;
      }

      IEndpointSnitch snitch = (IEndpointSnitch)FBUtilities.construct(snitchClassName, "snitch");
      return (IEndpointSnitch)(dynamic?new DynamicEndpointSnitch(snitch):snitch);
   }

   public static String getSystemKeyDirectory() {
      return conf.system_key_directory;
   }

   public static SystemTableEncryptionOptions getSystemTableEncryptionOptions() {
      return conf.system_info_encryption;
   }

   public static IAuthenticator getAuthenticator() {
      return authenticator;
   }

   public static void setAuthenticator(IAuthenticator authenticator) {
      authenticator = authenticator;
   }

   public static void setAuthManager(AuthManager authManager) {
      authManager = authManager;
   }

   public static AuthManager getAuthManager() {
      return authManager;
   }

   public static IAuthorizer getAuthorizer() {
      return authManager.getAuthorizer();
   }

   public static IRoleManager getRoleManager() {
      return authManager.getRoleManager();
   }

   public static boolean isSystemKeyspaceFilteringEnabled() {
      return conf.system_keyspaces_filtering;
   }

   public static void enableSystemKeyspaceFiltering() {
      conf.system_keyspaces_filtering = true;
   }

   public static int getPermissionsValidity() {
      return conf.permissions_validity_in_ms;
   }

   public static void setPermissionsValidity(int timeout) {
      conf.permissions_validity_in_ms = timeout;
   }

   public static int getPermissionsUpdateInterval() {
      return conf.permissions_update_interval_in_ms == -1?conf.permissions_validity_in_ms:conf.permissions_update_interval_in_ms;
   }

   public static void setPermissionsUpdateInterval(int updateInterval) {
      conf.permissions_update_interval_in_ms = updateInterval;
   }

   public static int getPermissionsCacheMaxEntries() {
      return conf.permissions_cache_max_entries;
   }

   public static int setPermissionsCacheMaxEntries(int maxEntries) {
      return conf.permissions_cache_max_entries = maxEntries;
   }

   public static int getPermissionsCacheInitialCapacity() {
      return conf.permissions_cache_initial_capacity;
   }

   public static int setPermissionsCacheInitialCapacity(int maxEntries) {
      return conf.permissions_cache_initial_capacity = maxEntries;
   }

   public static int getRolesValidity() {
      return conf.roles_validity_in_ms;
   }

   public static void setRolesValidity(int validity) {
      conf.roles_validity_in_ms = validity;
   }

   public static int getRolesUpdateInterval() {
      return conf.roles_update_interval_in_ms == -1?conf.roles_validity_in_ms:conf.roles_update_interval_in_ms;
   }

   public static void setRolesUpdateInterval(int interval) {
      conf.roles_update_interval_in_ms = interval;
   }

   public static int getRolesCacheMaxEntries() {
      return conf.roles_cache_max_entries;
   }

   public static int setRolesCacheMaxEntries(int maxEntries) {
      return conf.roles_cache_max_entries = maxEntries;
   }

   public static int getRolesCacheInitialCapacity() {
      return conf.roles_cache_initial_capacity;
   }

   public static int setRolesCacheInitialCapacity(int maxEntries) {
      return conf.roles_cache_initial_capacity = maxEntries;
   }

   public static int getMaxValueSize() {
      return conf.max_value_size_in_mb * 1024 * 1024;
   }

   public static void setMaxValueSize(int maxValueSizeInBytes) {
      conf.max_value_size_in_mb = maxValueSizeInBytes / 1024 / 1024;
   }

   public static void createAllDirectories() {
      try {
         if(conf.data_file_directories.length == 0) {
            throw new ConfigurationException("At least one DataFileDirectory must be specified", false);
         } else {
            String[] var0 = conf.data_file_directories;
            int var1 = var0.length;

            for(int var2 = 0; var2 < var1; ++var2) {
               String dataFileDirectory = var0[var2];
               FileUtils.createDirectory(dataFileDirectory);
            }

            if(conf.commitlog_directory == null) {
               throw new ConfigurationException("commitlog_directory must be specified", false);
            } else {
               FileUtils.createDirectory(conf.commitlog_directory);
               if(conf.hints_directory == null) {
                  throw new ConfigurationException("hints_directory must be specified", false);
               } else {
                  FileUtils.createDirectory(conf.hints_directory);
                  if(conf.saved_caches_directory == null) {
                     throw new ConfigurationException("saved_caches_directory must be specified", false);
                  } else {
                     FileUtils.createDirectory(conf.saved_caches_directory);
                     if(conf.cdc_enabled) {
                        if(conf.cdc_raw_directory == null) {
                           throw new ConfigurationException("cdc_raw_directory must be specified", false);
                        }

                        FileUtils.createDirectory(conf.cdc_raw_directory);
                     }

                  }
               }
            }
         }
      } catch (ConfigurationException var4) {
         throw new IllegalArgumentException("Bad configuration; unable to start server: " + var4.getMessage());
      } catch (FSWriteError var5) {
         throw new IllegalStateException(var5.getCause().getMessage() + "; unable to start server");
      }
   }

   public static IPartitioner getPartitioner() {
      return partitioner;
   }

   public static String getPartitionerName() {
      return paritionerName;
   }

   public static IPartitioner setPartitionerUnsafe(IPartitioner newPartitioner) {
      IPartitioner old = partitioner;
      partitioner = newPartitioner;
      return old;
   }

   public static IEndpointSnitch getEndpointSnitch() {
      return snitch;
   }

   public static void setEndpointSnitch(IEndpointSnitch eps) {
      snitch = eps;
   }

   public static int getColumnIndexSize() {
      return conf.column_index_size_in_kb * 1024;
   }

   @VisibleForTesting
   public static void setColumnIndexSizeInKB(int val) {
      conf.column_index_size_in_kb = val;
   }

   public static int getColumnIndexCacheSize() {
      return conf.column_index_cache_size_in_kb * 1024;
   }

   @VisibleForTesting
   public static void setColumnIndexCacheSize(int val) {
      conf.column_index_cache_size_in_kb = val;
   }

   public static int getBatchSizeWarnThreshold() {
      return conf.batch_size_warn_threshold_in_kb * 1024;
   }

   public static int getBatchSizeWarnThresholdInKB() {
      return conf.batch_size_warn_threshold_in_kb;
   }

   public static long getBatchSizeFailThreshold() {
      return (long)conf.batch_size_fail_threshold_in_kb * 1024L;
   }

   public static int getBatchSizeFailThresholdInKB() {
      return conf.batch_size_fail_threshold_in_kb;
   }

   public static int getUnloggedBatchAcrossPartitionsWarnThreshold() {
      return conf.unlogged_batch_across_partitions_warn_threshold.intValue();
   }

   public static void setBatchSizeWarnThresholdInKB(int threshold) {
      conf.batch_size_warn_threshold_in_kb = threshold;
   }

   public static void setBatchSizeFailThresholdInKB(int threshold) {
      conf.batch_size_fail_threshold_in_kb = threshold;
   }

   public static Collection<String> getInitialTokens() {
      return tokensFromString(System.getProperty("cassandra.initial_token", conf.initial_token));
   }

   public static String getAllocateTokensForKeyspace() {
      return System.getProperty("cassandra.allocate_tokens_for_keyspace", conf.allocate_tokens_for_keyspace);
   }

   public static Integer getAllocateTokensForLocalReplicationFactor() {
      String propValue = System.getProperty("cassandra.allocate_tokens_for_local_replication_factor");
      return propValue != null?Integer.valueOf(Integer.parseInt(propValue)):conf.allocate_tokens_for_local_replication_factor;
   }

   public static Collection<String> tokensFromString(String tokenString) {
      List<String> tokens = new ArrayList();
      if(tokenString != null) {
         String[] var2 = StringUtils.split(tokenString, ',');
         int var3 = var2.length;

         for(int var4 = 0; var4 < var3; ++var4) {
            String token = var2[var4];
            tokens.add(token.trim());
         }
      }

      return tokens;
   }

   public static int getNumTokens() {
      return conf.num_tokens;
   }

   public static InetAddress getReplaceAddress() {
      try {
         return System.getProperty("cassandra.replace_address", (String)null) != null?InetAddress.getByName(System.getProperty("cassandra.replace_address", (String)null)):(System.getProperty("cassandra.replace_address_first_boot", (String)null) != null?InetAddress.getByName(System.getProperty("cassandra.replace_address_first_boot", (String)null)):null);
      } catch (UnknownHostException var1) {
         throw new RuntimeException("Replacement host name could not be resolved or scope_id was specified for a global IPv6 address", var1);
      }
   }

   public static Collection<String> getReplaceTokens() {
      return tokensFromString(System.getProperty("cassandra.replace_token", (String)null));
   }

   public static UUID getReplaceNode() {
      try {
         return UUID.fromString(System.getProperty("cassandra.replace_node", (String)null));
      } catch (NullPointerException var1) {
         return null;
      }
   }

   public static String getClusterName() {
      return conf.cluster_name;
   }

   public static int getStoragePort() {
      return Integer.parseInt(System.getProperty("cassandra.storage_port", Integer.toString(conf.storage_port)));
   }

   public static int getSSLStoragePort() {
      return Integer.parseInt(System.getProperty("cassandra.ssl_storage_port", Integer.toString(conf.ssl_storage_port)));
   }

   public static long getRpcTimeout() {
      return conf.request_timeout_in_ms;
   }

   public static void setRpcTimeout(long timeOutInMillis) {
      conf.request_timeout_in_ms = timeOutInMillis;
   }

   public static long getReadRpcTimeout() {
      return conf.read_request_timeout_in_ms;
   }

   public static void setReadRpcTimeout(long timeOutInMillis) {
      conf.read_request_timeout_in_ms = timeOutInMillis;
   }

   public static long getRangeRpcTimeout() {
      return conf.range_request_timeout_in_ms;
   }

   public static void setRangeRpcTimeout(long timeOutInMillis) {
      conf.range_request_timeout_in_ms = timeOutInMillis;
   }

   public static long getAggregatedQueryTimeout() {
      return conf.aggregated_request_timeout_in_ms;
   }

   public static void setAggregatedQueryTimeout(long timeOutInMillis) {
      conf.aggregated_request_timeout_in_ms = timeOutInMillis;
   }

   public static long getWriteRpcTimeout() {
      return conf.write_request_timeout_in_ms;
   }

   public static void setWriteRpcTimeout(long timeOutInMillis) {
      conf.write_request_timeout_in_ms = timeOutInMillis;
   }

   public static long getCrossDCRttLatency() {
      return conf.cross_dc_rtt_in_ms;
   }

   public static void setCrossDCRttLatency(long timeOutInMillis) {
      conf.cross_dc_rtt_in_ms = timeOutInMillis;
   }

   public static long getCounterWriteRpcTimeout() {
      return conf.counter_write_request_timeout_in_ms;
   }

   public static void setCounterWriteRpcTimeout(long timeOutInMillis) {
      conf.counter_write_request_timeout_in_ms = timeOutInMillis;
   }

   public static long getCasContentionTimeout() {
      return conf.cas_contention_timeout_in_ms;
   }

   public static void setCasContentionTimeout(long timeOutInMillis) {
      conf.cas_contention_timeout_in_ms = timeOutInMillis;
   }

   public static long getTruncateRpcTimeout() {
      return conf.truncate_request_timeout_in_ms;
   }

   public static void setTruncateRpcTimeout(long timeOutInMillis) {
      conf.truncate_request_timeout_in_ms = timeOutInMillis;
   }

   public static boolean hasCrossNodeTimeout() {
      return conf.cross_node_timeout;
   }

   public static long getSlowQueryTimeout() {
      return conf.slow_query_log_timeout_in_ms;
   }

   public static long getMinRpcTimeout() {
      return Longs.min(new long[]{getRpcTimeout(), getReadRpcTimeout(), getRangeRpcTimeout(), getWriteRpcTimeout(), getCounterWriteRpcTimeout(), getTruncateRpcTimeout()});
   }

   public static long getMaxRpcTimeout() {
      return Longs.max(new long[]{getRpcTimeout(), getReadRpcTimeout(), getRangeRpcTimeout(), getWriteRpcTimeout(), getCounterWriteRpcTimeout(), getTruncateRpcTimeout()});
   }

   public static double getPhiConvictThreshold() {
      return conf.phi_convict_threshold;
   }

   public static void setPhiConvictThreshold(double phiConvictThreshold) {
      conf.phi_convict_threshold = phiConvictThreshold;
   }

   public static int getFlushWriters() {
      return conf.memtable_flush_writers;
   }

   public static int getConcurrentCompactors() {
      return conf.concurrent_compactors.intValue();
   }

   public static void setConcurrentCompactors(int value) {
      conf.concurrent_compactors = Integer.valueOf(value);
   }

   public static int getCompactionThroughputMbPerSec() {
      return conf.compaction_throughput_mb_per_sec;
   }

   public static void setCompactionThroughputMbPerSec(int value) {
      conf.compaction_throughput_mb_per_sec = value;
   }

   public static long getCompactionLargePartitionWarningThreshold() {
      return (long)conf.compaction_large_partition_warning_threshold_mb * 1024L * 1024L;
   }

   public static int getConcurrentValidations() {
      return conf.concurrent_validations;
   }

   public static void setConcurrentValidations(int value) {
      value = value > 0?value:2147483647;
      conf.concurrent_validations = value;
   }

   public static int getConcurrentViewBuilders() {
      return conf.concurrent_materialized_view_builders;
   }

   public static void setConcurrentViewBuilders(int value) {
      conf.concurrent_materialized_view_builders = value;
   }

   public static int getTPCConcurrentRequestsLimit() {
      return conf.tpc_concurrent_requests_limit;
   }

   public static int getTPCPendingRequestsLimit() {
      return conf.tpc_pending_requests_limit;
   }

   public static long getMinFreeSpacePerDriveInBytes() {
      return (long)conf.min_free_space_per_drive_in_mb * 1024L * 1024L;
   }

   public static boolean getDisableSTCSInL0() {
      return disableSTCSInL0;
   }

   public static int getStreamThroughputOutboundMegabitsPerSec() {
      return conf.stream_throughput_outbound_megabits_per_sec;
   }

   public static void setStreamThroughputOutboundMegabitsPerSec(int value) {
      conf.stream_throughput_outbound_megabits_per_sec = value;
   }

   public static int getInterDCStreamThroughputOutboundMegabitsPerSec() {
      return conf.inter_dc_stream_throughput_outbound_megabits_per_sec;
   }

   public static void setInterDCStreamThroughputOutboundMegabitsPerSec(int value) {
      conf.inter_dc_stream_throughput_outbound_megabits_per_sec = value;
   }

   public static String[] getAllDataFileLocations() {
      return conf.data_file_directories;
   }

   public static String getCommitLogLocation() {
      return conf.commitlog_directory;
   }

   @VisibleForTesting
   public static void setCommitLogLocation(String value) {
      conf.commitlog_directory = value;
   }

   public static ParameterizedClass getCommitLogCompression() {
      return conf.commitlog_compression;
   }

   public static void setCommitLogCompression(ParameterizedClass compressor) {
      conf.commitlog_compression = compressor;
   }

   public static int getCommitLogMaxCompressionBuffersInPool() {
      return conf.commitlog_max_compression_buffers_in_pool;
   }

   public static void setCommitLogMaxCompressionBuffersPerPool(int buffers) {
      conf.commitlog_max_compression_buffers_in_pool = buffers;
   }

   public static int getMaxMutationSize() {
      return conf.max_mutation_size_in_kb.intValue() * 1024;
   }

   public static int getTombstoneWarnThreshold() {
      return conf.tombstone_warn_threshold;
   }

   public static void setTombstoneWarnThreshold(int threshold) {
      conf.tombstone_warn_threshold = threshold;
   }

   public static int getTombstoneFailureThreshold() {
      return conf.tombstone_failure_threshold;
   }

   public static void setTombstoneFailureThreshold(int threshold) {
      conf.tombstone_failure_threshold = threshold;
   }

   public static int getCommitLogSegmentSize() {
      return conf.commitlog_segment_size_in_mb * 1024 * 1024;
   }

   public static void setCommitLogSegmentSize(int sizeMegabytes) {
      conf.commitlog_segment_size_in_mb = sizeMegabytes;
   }

   public static String getSavedCachesLocation() {
      return conf.saved_caches_directory;
   }

   public static Set<InetAddress> getSeeds() {
      return ImmutableSet.<InetAddress>builder().addAll(seedProvider.getSeeds()).build();
   }

   public static SeedProvider getSeedProvider() {
      return seedProvider;
   }

   public static void setSeedProvider(SeedProvider newSeedProvider) {
      seedProvider = newSeedProvider;
   }

   public static InetAddress getListenAddress() {
      return listenAddress;
   }

   public static InetAddress getBroadcastAddress() {
      return broadcastAddress;
   }

   public static boolean shouldListenOnBroadcastAddress() {
      return conf.listen_on_broadcast_address;
   }

   public static IInternodeAuthenticator getInternodeAuthenticator() {
      return internodeAuthenticator;
   }

   public static void setInternodeAuthenticator(IInternodeAuthenticator internodeAuthenticator) {
      Objects.requireNonNull(internodeAuthenticator);
      internodeAuthenticator = internodeAuthenticator;
   }

   public static void setBroadcastAddress(InetAddress broadcastAdd) {
      broadcastAddress = broadcastAdd;
   }

   public static InetAddress getNativeTransportAddress() {
      return nativeTransportAddress;
   }

   public static void setBroadcastNativeTransportAddress(InetAddress broadcastRPCAddr) {
      broadcastNativeTransportAddress = broadcastRPCAddr;
   }

   public static InetAddress getBroadcastNativeTransportAddress() {
      return broadcastNativeTransportAddress;
   }

   public static boolean getNativeTransportKeepAlive() {
      return conf.native_transport_keepalive.booleanValue();
   }

   public static int getInternodeSendBufferSize() {
      return conf.internode_send_buff_size_in_bytes;
   }

   public static int getInternodeRecvBufferSize() {
      return conf.internode_recv_buff_size_in_bytes;
   }

   public static boolean startNativeTransport() {
      return conf.start_native_transport;
   }

   public static int getNativeTransportPort() {
      return Integer.parseInt(System.getProperty("cassandra.native_transport_port", Integer.toString(conf.native_transport_port)));
   }

   @VisibleForTesting
   public static void setNativeTransportPort(int port) {
      conf.native_transport_port = port;
   }

   public static int getNativeTransportPortSSL() {
      return conf.native_transport_port_ssl == null?getNativeTransportPort():conf.native_transport_port_ssl.intValue();
   }

   @VisibleForTesting
   public static void setNativeTransportPortSSL(Integer port) {
      conf.native_transport_port_ssl = port;
   }

   public static int getNativeTransportMaxThreads() {
      return conf.native_transport_max_threads;
   }

   public static int getNativeTransportMaxFrameSize() {
      return conf.native_transport_max_frame_size_in_mb * 1024 * 1024;
   }

   public static long getNativeTransportMaxConcurrentConnections() {
      return conf.native_transport_max_concurrent_connections;
   }

   public static void setNativeTransportMaxConcurrentConnections(long nativeTransportMaxConcurrentConnections) {
      conf.native_transport_max_concurrent_connections = nativeTransportMaxConcurrentConnections;
   }

   public static long getNativeTransportMaxConcurrentConnectionsPerIp() {
      return conf.native_transport_max_concurrent_connections_per_ip;
   }

   public static void setNativeTransportMaxConcurrentConnectionsPerIp(long native_transport_max_concurrent_connections_per_ip) {
      conf.native_transport_max_concurrent_connections_per_ip = native_transport_max_concurrent_connections_per_ip;
   }

   public static double getCommitLogSyncGroupWindow() {
      return conf.commitlog_sync_group_window_in_ms;
   }

   public static void setCommitLogSyncGroupWindow(double windowMillis) {
      conf.commitlog_sync_group_window_in_ms = windowMillis;
   }

   public static int getCommitLogSyncPeriod() {
      return conf.commitlog_sync_period_in_ms;
   }

   public static void setCommitLogSyncPeriod(int periodMillis) {
      conf.commitlog_sync_period_in_ms = periodMillis;
   }

   public static Config.CommitLogSync getCommitLogSync() {
      return conf.commitlog_sync;
   }

   public static void setCommitLogSync(Config.CommitLogSync sync) {
      conf.commitlog_sync = sync;
   }

   public static Config.AccessMode getDiskAccessMode() {
      return diskAccessMode;
   }

   @VisibleForTesting
   public static void setDiskAccessMode(Config.AccessMode mode) {
      diskAccessMode = mode;
   }

   public static Config.AccessMode getCommitlogAccessMode() {
      return commitlogAccessMode;
   }

   @VisibleForTesting
   public static void setCommitlogAccessMode(Config.AccessMode mode) {
      commitlogAccessMode = mode;
   }

   public static Config.AccessMode getIndexAccessMode() {
      return indexAccessMode;
   }

   @VisibleForTesting
   public static void setIndexAccessMode(Config.AccessMode mode) {
      indexAccessMode = mode;
   }

   public static void setDiskFailurePolicy(Config.DiskFailurePolicy policy) {
      conf.disk_failure_policy = policy;
   }

   public static Config.DiskFailurePolicy getDiskFailurePolicy() {
      return conf.disk_failure_policy;
   }

   public static void setCommitFailurePolicy(Config.CommitFailurePolicy policy) {
      conf.commit_failure_policy = policy;
   }

   public static Config.CommitFailurePolicy getCommitFailurePolicy() {
      return conf.commit_failure_policy;
   }

   public static boolean isSnapshotBeforeCompaction() {
      return conf.snapshot_before_compaction;
   }

   public static boolean isAutoSnapshot() {
      return conf.auto_snapshot;
   }

   @VisibleForTesting
   public static void setAutoSnapshot(boolean autoSnapshot) {
      conf.auto_snapshot = autoSnapshot;
   }

   @VisibleForTesting
   public static boolean getAutoSnapshot() {
      return conf.auto_snapshot;
   }

   public static boolean isAutoBootstrap() {
      return Boolean.parseBoolean(System.getProperty("cassandra.auto_bootstrap", Boolean.toString(conf.auto_bootstrap)));
   }

   public static void setHintedHandoffEnabled(boolean hintedHandoffEnabled) {
      conf.hinted_handoff_enabled = hintedHandoffEnabled;
   }

   public static boolean hintedHandoffEnabled() {
      return conf.hinted_handoff_enabled;
   }

   public static Set<String> hintedHandoffDisabledDCs() {
      return conf.hinted_handoff_disabled_datacenters;
   }

   public static void enableHintsForDC(String dc) {
      conf.hinted_handoff_disabled_datacenters.remove(dc);
   }

   public static void disableHintsForDC(String dc) {
      conf.hinted_handoff_disabled_datacenters.add(dc);
   }

   public static void setMaxHintWindow(int ms) {
      conf.max_hint_window_in_ms = ms;
   }

   public static int getMaxHintWindow() {
      return conf.max_hint_window_in_ms;
   }

   public static File getHintsDirectory() {
      return new File(conf.hints_directory);
   }

   public static File getSerializedCachePath(CacheService.CacheType cacheType, String version, String extension) {
      String name = cacheType.toString() + (version == null?"":"-" + version + "." + extension);
      return new File(conf.saved_caches_directory, name);
   }

   public static int getDynamicUpdateInterval() {
      return conf.dynamic_snitch_update_interval_in_ms;
   }

   public static void setDynamicUpdateInterval(int dynamicUpdateInterval) {
      conf.dynamic_snitch_update_interval_in_ms = dynamicUpdateInterval;
   }

   public static int getDynamicResetInterval() {
      return conf.dynamic_snitch_reset_interval_in_ms;
   }

   public static void setDynamicResetInterval(int dynamicResetInterval) {
      conf.dynamic_snitch_reset_interval_in_ms = dynamicResetInterval;
   }

   public static double getDynamicBadnessThreshold() {
      return conf.dynamic_snitch_badness_threshold;
   }

   public static void setDynamicBadnessThreshold(double dynamicBadnessThreshold) {
      conf.dynamic_snitch_badness_threshold = dynamicBadnessThreshold;
   }

   public static EncryptionOptions.ServerEncryptionOptions getServerEncryptionOptions() {
      return conf.server_encryption_options;
   }

   public static EncryptionOptions.ClientEncryptionOptions getClientEncryptionOptions() {
      return conf.client_encryption_options;
   }

   public static int getHintedHandoffThrottleInKB() {
      return conf.hinted_handoff_throttle_in_kb;
   }

   public static void setHintedHandoffThrottleInKB(int throttleInKB) {
      conf.hinted_handoff_throttle_in_kb = throttleInKB;
   }

   public static int getBatchlogReplayThrottleInKB() {
      return conf.batchlog_replay_throttle_in_kb;
   }

   public static void setBatchlogReplayThrottleInKB(int throttleInKB) {
      conf.batchlog_replay_throttle_in_kb = throttleInKB;
   }

   public static boolean isDynamicEndpointSnitch() {
      return snitch instanceof DynamicEndpointSnitch;
   }

   public static Config.BatchlogEndpointStrategy getBatchlogEndpointStrategy() {
      return conf.batchlog_endpoint_strategy;
   }

   public static void setBatchlogEndpointStrategy(Config.BatchlogEndpointStrategy batchlogEndpointStrategy) {
      conf.batchlog_endpoint_strategy = batchlogEndpointStrategy;
   }

   public static int getMaxHintsDeliveryThreads() {
      return conf.max_hints_delivery_threads;
   }

   public static int getHintsFlushPeriodInMS() {
      return conf.hints_flush_period_in_ms;
   }

   public static long getMaxHintsFileSize() {
      return (long)conf.max_hints_file_size_in_mb * 1024L * 1024L;
   }

   public static ParameterizedClass getHintsCompression() {
      return conf.hints_compression;
   }

   public static void setHintsCompression(ParameterizedClass parameterizedClass) {
      conf.hints_compression = parameterizedClass;
   }

   public static boolean isIncrementalBackupsEnabled() {
      return conf.incremental_backups;
   }

   public static void setIncrementalBackupsEnabled(boolean value) {
      conf.incremental_backups = value;
   }

   public static int getFileCacheSizeInMB() {
      if(conf.file_cache_size_in_mb == null) {
         assert isClientInitialized();

         return 0;
      } else {
         return conf.file_cache_size_in_mb.intValue();
      }
   }

   public static boolean getFileCacheRoundUp() {
      if(conf.file_cache_round_up == null) {
         assert isClientInitialized();

         return false;
      } else {
         return conf.file_cache_round_up.booleanValue();
      }
   }

   public static DiskOptimizationStrategy getDiskOptimizationStrategy() {
      return diskOptimizationStrategy;
   }

   public static boolean assumeDataDirectoriesOnSSD() {
      return conf.disk_optimization_strategy == Config.DiskOptimizationStrategy.ssd;
   }

   public static double getDiskOptimizationEstimatePercentile() {
      return conf.disk_optimization_estimate_percentile;
   }

   public static long getTotalCommitlogSpaceInMB() {
      return (long)conf.commitlog_total_space_in_mb.intValue();
   }

   public static int getSSTablePreempiveOpenIntervalInMB() {
      return FBUtilities.isWindows?-1:conf.sstable_preemptive_open_interval_in_mb;
   }

   public static void setSSTablePreempiveOpenIntervalInMB(int mb) {
      conf.sstable_preemptive_open_interval_in_mb = mb;
   }

   public static boolean getTrickleFsync() {
      return conf.trickle_fsync;
   }

   public static int getTrickleFsyncIntervalInKb() {
      return conf.trickle_fsync_interval_in_kb;
   }

   public static long getKeyCacheSizeInMB() {
      return keyCacheSizeInMB;
   }

   public static long getIndexSummaryCapacityInMB() {
      return indexSummaryCapacityInMB;
   }

   public static int getKeyCacheSavePeriod() {
      return conf.key_cache_save_period;
   }

   public static void setKeyCacheSavePeriod(int keyCacheSavePeriod) {
      conf.key_cache_save_period = keyCacheSavePeriod;
   }

   public static int getKeyCacheKeysToSave() {
      return conf.key_cache_keys_to_save;
   }

   public static void setKeyCacheKeysToSave(int keyCacheKeysToSave) {
      conf.key_cache_keys_to_save = keyCacheKeysToSave;
   }

   public static String getRowCacheClassName() {
      return conf.row_cache_class_name;
   }

   public static long getRowCacheSizeInMB() {
      return conf.row_cache_size_in_mb;
   }

   @VisibleForTesting
   public static void setRowCacheSizeInMB(long val) {
      conf.row_cache_size_in_mb = val;
   }

   public static int getRowCacheSavePeriod() {
      return conf.row_cache_save_period;
   }

   public static void setRowCacheSavePeriod(int rowCacheSavePeriod) {
      conf.row_cache_save_period = rowCacheSavePeriod;
   }

   public static int getRowCacheKeysToSave() {
      return conf.row_cache_keys_to_save;
   }

   public static long getCounterCacheSizeInMB() {
      return counterCacheSizeInMB;
   }

   public static void setRowCacheKeysToSave(int rowCacheKeysToSave) {
      conf.row_cache_keys_to_save = rowCacheKeysToSave;
   }

   public static int getCounterCacheSavePeriod() {
      return conf.counter_cache_save_period;
   }

   public static void setCounterCacheSavePeriod(int counterCacheSavePeriod) {
      conf.counter_cache_save_period = counterCacheSavePeriod;
   }

   public static int getCounterCacheKeysToSave() {
      return conf.counter_cache_keys_to_save;
   }

   public static void setCounterCacheKeysToSave(int counterCacheKeysToSave) {
      conf.counter_cache_keys_to_save = counterCacheKeysToSave;
   }

   public static int getStreamingKeepAlivePeriod() {
      return conf.streaming_keep_alive_period_in_secs.intValue();
   }

   public static int getStreamingConnectionsPerHost() {
      return conf.streaming_connections_per_host.intValue();
   }

   public static void setStreamingConnectionsPerHost(int streamingConnectionsPerHost) {
      conf.streaming_connections_per_host = Integer.valueOf(streamingConnectionsPerHost);
   }

   public static String getLocalDataCenter() {
      return snitch.getLocalDatacenter();
   }

   public static String getLocalRack() {
      return snitch.getLocalRack();
   }

   public static Comparator<InetAddress> getLocalComparator() {
      return localComparator;
   }

   public static Config.InternodeCompression internodeCompression() {
      return conf.internode_compression;
   }

   public static boolean getInterDCTcpNoDelay() {
      return conf.inter_dc_tcp_nodelay;
   }

   public static long getMemtableHeapSpaceInMb() {
      return (long)conf.memtable_heap_space_in_mb.intValue();
   }

   public static long getMemtableOffheapSpaceInMb() {
      return (long)conf.memtable_offheap_space_in_mb.intValue();
   }

   public static Config.MemtableAllocationType getMemtableAllocationType() {
      return conf.memtable_allocation_type;
   }

   public static double getMemtableCleanupThreshold() {
      return conf.memtable_cleanup_threshold.doubleValue();
   }

   public static boolean hasLargeAddressSpace() {
      String datamodel = System.getProperty("sun.arch.data.model");
      if(datamodel != null) {
         byte var2 = -1;
         switch(datamodel.hashCode()) {
         case 1631:
            if(datamodel.equals("32")) {
               var2 = 1;
            }
            break;
         case 1726:
            if(datamodel.equals("64")) {
               var2 = 0;
            }
         }

         switch(var2) {
         case 0:
            return true;
         case 1:
            return false;
         }
      }

      String arch = System.getProperty("os.arch");
      return arch.contains("64") || arch.contains("sparcv9");
   }

   public static int getTracetypeRepairTTL() {
      return conf.tracetype_repair_ttl;
   }

   public static int getTracetypeNodeSyncTTL() {
      return (int)conf.nodesync.traceTTL(TimeUnit.SECONDS);
   }

   public static int getTracetypeQueryTTL() {
      return conf.tracetype_query_ttl;
   }

   public static String getOtcCoalescingStrategy() {
      return conf.otc_coalescing_strategy;
   }

   public static int getOtcCoalescingWindow() {
      return conf.otc_coalescing_window_us;
   }

   public static int getOtcCoalescingEnoughCoalescedMessages() {
      return conf.otc_coalescing_enough_coalesced_messages;
   }

   public static void setOtcCoalescingEnoughCoalescedMessages(int otc_coalescing_enough_coalesced_messages) {
      conf.otc_coalescing_enough_coalesced_messages = otc_coalescing_enough_coalesced_messages;
   }

   public static int getWindowsTimerInterval() {
      return conf.windows_timer_interval;
   }

   public static long getPreparedStatementsCacheSizeMB() {
      return preparedStatementsCacheSizeInMB;
   }

   public static boolean enableUserDefinedFunctions() {
      return conf.enable_user_defined_functions;
   }

   public static boolean enableScriptedUserDefinedFunctions() {
      return conf.enable_scripted_user_defined_functions;
   }

   public static void enableScriptedUserDefinedFunctions(boolean enableScriptedUserDefinedFunctions) {
      conf.enable_scripted_user_defined_functions = enableScriptedUserDefinedFunctions;
   }

   public static boolean enableUserDefinedFunctionsThreads() {
      return conf.enable_user_defined_functions_threads;
   }

   public static void enableUserDefinedFunctionsThreads(boolean enableUserDefinedFunctionsThreads) {
      conf.enable_user_defined_functions_threads = enableUserDefinedFunctionsThreads;
   }

   public static long getUserDefinedFunctionWarnTimeout() {
      return conf.user_defined_function_warn_timeout;
   }

   public static void setUserDefinedFunctionWarnTimeout(long userDefinedFunctionWarnTimeout) {
      conf.user_defined_function_warn_timeout = userDefinedFunctionWarnTimeout;
   }

   public static long getUserDefinedFunctionFailTimeout() {
      return conf.user_defined_function_fail_timeout;
   }

   public static void setUserDefinedFunctionFailTimeout(long userDefinedFunctionFailTimeout) {
      conf.user_defined_function_fail_timeout = userDefinedFunctionFailTimeout;
   }

   public static Config.UserFunctionTimeoutPolicy getUserFunctionTimeoutPolicy() {
      return conf.user_function_timeout_policy;
   }

   public static void setUserFunctionTimeoutPolicy(Config.UserFunctionTimeoutPolicy userFunctionTimeoutPolicy) {
      conf.user_function_timeout_policy = userFunctionTimeoutPolicy;
   }

   public static long getGCLogThreshold() {
      return (long)conf.gc_log_threshold_in_ms;
   }

   public static EncryptionContext getEncryptionContext() {
      return encryptionContext;
   }

   public static long getGCWarnThreshold() {
      return (long)conf.gc_warn_threshold_in_ms;
   }

   public static double getSeedGossipProbability() {
      return conf.seed_gossip_probability;
   }

   public static void setSeedGossipProbability(double probability) {
      conf.seed_gossip_probability = probability;
   }

   public static boolean isCDCEnabled() {
      return conf.cdc_enabled;
   }

   public static void setCDCEnabled(boolean cdc_enabled) {
      conf.cdc_enabled = cdc_enabled;
   }

   public static String getCDCLogLocation() {
      return conf.cdc_raw_directory;
   }

   public static int getCDCSpaceInMB() {
      return conf.cdc_total_space_in_mb;
   }

   @VisibleForTesting
   public static void setCDCSpaceInMB(int input) {
      conf.cdc_total_space_in_mb = input;
   }

   public static int getCDCDiskCheckInterval() {
      return conf.cdc_free_space_check_interval_ms;
   }

   @VisibleForTesting
   public static void setEncryptionContext(EncryptionContext ec) {
      encryptionContext = ec;
   }

   public static int searchConcurrencyFactor() {
      return searchConcurrencyFactor;
   }

   public static boolean isUnsafeSystem() {
      return unsafeSystem;
   }

   public static void setBackPressureEnabled(boolean backPressureEnabled) {
      conf.back_pressure_enabled = backPressureEnabled;
   }

   public static boolean backPressureEnabled() {
      return conf.back_pressure_enabled;
   }

   @VisibleForTesting
   public static void setBackPressureStrategy(BackPressureStrategy strategy) {
      backPressureStrategy = strategy;
   }

   public static BackPressureStrategy getBackPressureStrategy() {
      return backPressureStrategy;
   }

   public static ContinuousPagingConfig getContinuousPaging() {
      return conf.continuous_paging;
   }

   public static ConsistencyLevel getIdealConsistencyLevel() {
      return conf.ideal_consistency_level;
   }

   public static void setIdealConsistencyLevel(ConsistencyLevel cl) {
      conf.ideal_consistency_level = cl;
   }

   public static long getMaxMemoryToLockBytes() {
      if(conf.max_memory_to_lock_mb > 0) {
         return (long)conf.max_memory_to_lock_mb * 1024L * 1024L;
      } else {
         long system_memory_in_mb = Long.getLong("dse.system_memory_in_mb", 2048L).longValue();
         return (long)(conf.max_memory_to_lock_fraction * (double)(system_memory_in_mb * 1024L * 1024L));
      }
   }

   public static int getMetricsHistogramUpdateTimeMillis() {
      return conf.metrics_histogram_update_interval_millis;
   }

   @VisibleForTesting
   public static void setMetricsHistogramUpdateTimeMillis(int interval) {
      conf.metrics_histogram_update_interval_millis = interval;
   }

   public static int getTPCCores() {
      int confCores = conf != null && conf.tpc_cores != null?conf.tpc_cores.intValue():FBUtilities.getAvailableProcessors() - 1;
      int cores = Integer.getInteger("cassandra.tpc_cores", confCores).intValue();
      return Math.max(cores, 1);
   }

   public static int getTPCIOCores() {
      return Math.min(getTPCCores(), conf != null && conf.tpc_io_cores != null?conf.tpc_io_cores.intValue():(int)Math.ceil((double)getIOGlobalQueueDepth() / 4.0D));
   }

   public static int getIOGlobalQueueDepth() {
      return conf != null && conf.io_global_queue_depth != null?conf.io_global_queue_depth.intValue():FileUtils.getIOGlobalQueueDepth();
   }

   public static NodeSyncConfig getNodeSyncConfig() {
      return conf.nodesync;
   }

   public static int getRepairCommandPoolSize() {
      return conf.repair_command_pool_size;
   }

   public static Config.RepairCommandPoolFullStrategy getRepairCommandPoolFullStrategy() {
      return conf.repair_command_pool_full_strategy;
   }

   public static Optional<Integer> getJMXPort() {
      return Optional.ofNullable(jmxPort);
   }

   public static boolean isJMXLocalOnly() {
      return jmxLocalOnly;
   }

   public static int getMaxBackgroundIOThreads() {
      return Integer.getInteger("cassandra.io.background.max_pool_size", 256).intValue();
   }

   public static IAuditLogger setAuditLoggerUnsafe(IAuditLogger logger) {
      IAuditLogger old = auditLogger;
      auditLogger = logger;
      return old;
   }

   public static IAuditLogger getAuditLogger() {
      return auditLogger;
   }

   public static AuditLoggingOptions getAuditLoggingOptions() {
      return conf.audit_logging_options;
   }

   public static boolean isPickLevelOnStreaming() {
      return conf.pick_level_on_streaming;
   }

   public static void setPickLevelOnStreaming(boolean pickLevelOnStreaming) {
      conf.pick_level_on_streaming = pickLevelOnStreaming;
   }
}
