package com.datastax.bdp.util;

import com.datastax.bdp.snitch.DseDelegateSnitch;
import com.datastax.bdp.snitch.EndpointStateTracker;
import com.datastax.bdp.snitch.Workload;
import com.datastax.bdp.tools.VanillaCpuLayout;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.reactivex.Completable;
import io.reactivex.schedulers.Schedulers;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Predicate;
import javax.validation.constraints.NotNull;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCUtils.WouldBlockException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.DynamicEndpointSnitch;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DseUtil {
   private static Logger logger = LoggerFactory.getLogger(DseDelegateSnitch.class);

   public DseUtil() {
   }

   public static String getDatacenterWithWorkloads(InetAddress address) {
      return getDatacenterWithWorkloads(DatabaseDescriptor.getEndpointSnitch(), address);
   }

   private static String getDatacenterWithWorkloads(IEndpointSnitch snitch, InetAddress address) {
      String datacenter;
      if(snitch instanceof DynamicEndpointSnitch) {
         datacenter = getDatacenterWithWorkloads(((DynamicEndpointSnitch)snitch).subsnitch, address);
      } else if(snitch instanceof DseDelegateSnitch) {
         datacenter = ((DseDelegateSnitch)snitch).getDatacenterWithWorkloads(address);
      } else {
         datacenter = snitch.getDatacenter(address);
      }

      return datacenter;
   }

   public static Set<Workload> getWorkloads() {
      return getWorkloads(Addresses.Internode.getBroadcastAddress());
   }

   public static Set<Workload> getWorkloads(InetAddress address) {
      return getWorkloads(DatabaseDescriptor.getEndpointSnitch(), address);
   }

   private static Set<Workload> getWorkloads(IEndpointSnitch snitch, InetAddress address) {
      if(snitch instanceof DynamicEndpointSnitch) {
         logger.debug("DynamicEndpointSnitch detected.");
         return getWorkloads(((DynamicEndpointSnitch)snitch).subsnitch, address);
      } else if(snitch instanceof DseDelegateSnitch) {
         logger.debug("DseDelegateSnitch detected.");
         return ((DseDelegateSnitch)snitch).getEndpointWorkloads(address);
      } else {
         if(snitch != null) {
            logger.debug("Snitch {} detected. Selecting 'Unknown' workload", snitch.getClass().getName());
         } else {
            logger.debug("No snitch detected. Selecting 'Unknown' workload");
         }

         return ImmutableSet.of(Workload.Unknown);
      }
   }

   public static String getDatacenter() {
      return getDatacenter(Addresses.Internode.getBroadcastAddress());
   }

   public static String getDatacenter(InetAddress address) {
      return getDatacenter(DatabaseDescriptor.getEndpointSnitch(), address);
   }

   private static String getDatacenter(IEndpointSnitch snitch, InetAddress address) {
      String dc = null;
      if(snitch instanceof DynamicEndpointSnitch) {
         logger.debug("DynamicEndpointSnitch detected.");
         dc = getDatacenter(((DynamicEndpointSnitch)snitch).subsnitch, address);
      } else if(snitch instanceof DseDelegateSnitch) {
         logger.debug("DseDelegateSnitch detected.");
         dc = ((DseDelegateSnitch)snitch).getDatacenter(address);
      }

      return dc;
   }

   public static String getRack() {
      return getRack(Addresses.Internode.getBroadcastAddress());
   }

   public static String getRack(InetAddress address) {
      return getRack(DatabaseDescriptor.getEndpointSnitch(), address);
   }

   private static String getRack(IEndpointSnitch snitch, InetAddress address) {
      String rack = null;
      if(snitch instanceof DynamicEndpointSnitch) {
         logger.debug("DynamicEndpointSnitch detected.");
         rack = getRack(((DynamicEndpointSnitch)snitch).subsnitch, address);
      } else if(snitch instanceof DseDelegateSnitch) {
         logger.debug("DseDelegateSnitch detected.");
         rack = snitch.getRack(address);
      }

      return rack;
   }

   public static Set<String> getLiveNodes() {
      Set<String> liveNodes = Sets.newHashSet();
      Iterator var1 = StorageService.instance.getLiveRingMembers(true).iterator();

      while(var1.hasNext()) {
         InetAddress address = (InetAddress)var1.next();
         liveNodes.add(address.getHostName());
      }

      return liveNodes;
   }

   public static IEndpointSnitch getInnermostSnitch() {
      IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
      if(snitch instanceof DynamicEndpointSnitch) {
         snitch = ((DynamicEndpointSnitch)snitch).subsnitch;
      }

      if(snitch instanceof DseDelegateSnitch) {
         snitch = ((DseDelegateSnitch)snitch).getDelegatedSnitch();
      }

      return snitch;
   }

   public static String makeAbsolute(String path) {
      File file = new File(path);
      String dseHome = System.getenv("DSE_HOME");
      if(dseHome != null && !file.isAbsolute()) {
         file = new File(dseHome, path);
      }

      return file.getAbsolutePath();
   }

   public static String getHadoopConfigDir(HadoopVersion hadoopConfDirLayout) {
      String hadoopConfDir = System.getenv(hadoopConfDirLayout.getEnvVar());
      if(hadoopConfDir != null) {
         File dir = new File(hadoopConfDir);
         if(!dir.exists()) {
            logger.warn(hadoopConfDirLayout.getEnvVar() + " is set but directory does not exist: " + dir);
         }

         return hadoopConfDir;
      } else {
         String[] var2 = hadoopConfDirLayout.getAllDirNames();
         int var3 = var2.length;

         for(int var4 = 0; var4 < var3; ++var4) {
            String dir = var2[var4];
            String interpolated = interpolatePath(dir, System.getenv());
            if(interpolated != null) {
               File file = new File(interpolated);
               if(file.exists()) {
                  return file.getAbsolutePath();
               }
            }
         }

         return null;
      }
   }

   public static String interpolatePath(String path, Map<String, String> environment) {
      if(path == null) {
         return null;
      } else {
         String[] elements = path.split(File.separator);
         List<String> interpolated = Lists.newArrayListWithCapacity(elements.length);
         String[] var4 = elements;
         int var5 = elements.length;

         for(int var6 = 0; var6 < var5; ++var6) {
            String e = var4[var6];
            String value = e.startsWith("$")?(String)environment.get(e.substring(1)):e;
            if(value == null) {
               return null;
            }

            interpolated.add(value);
         }

         return StringUtils.join(interpolated, File.separator);
      }
   }

   public static List<InetAddress> getHostsInTheSameDC() {
      List<InetAddress> addresses = new ArrayList();
      String localDC = EndpointStateTracker.instance.getDatacenter(Addresses.Internode.getBroadcastAddress());
      Iterator var2 = Gossiper.instance.getEndpointStates().iterator();

      while(var2.hasNext()) {
         Entry<InetAddress, EndpointState> es = (Entry)var2.next();
         InetAddress address = (InetAddress)es.getKey();
         if(localDC.equals(EndpointStateTracker.instance.getDatacenter(address))) {
            addresses.add(address);
         }
      }

      return addresses;
   }

   public static Map<InetAddress, String> getCassandraVersions() {
      Map<InetAddress, String> result = Maps.newHashMap();
      Iterator var1 = Gossiper.instance.getEndpointStates().iterator();

      while(var1.hasNext()) {
         Entry<InetAddress, EndpointState> e = (Entry)var1.next();
         InetAddress address = (InetAddress)e.getKey();
         String version = ((EndpointState)e.getValue()).getApplicationState(ApplicationState.RELEASE_VERSION).value;
         result.put(address, version);
      }

      return result;
   }

   public static boolean areAllNodesSameCassandraVersion() {
      return (new HashSet(getCassandraVersions().values())).size() == 1;
   }

   public static boolean isCausedByUnavailability(Throwable throwable) {
      return isCausedBy(throwable, new Class[]{UnavailableException.class});
   }

   public static boolean isCausedBy(Throwable throwable, Class... exceptionClasses) {
      return Arrays.stream(exceptionClasses).anyMatch((exceptionClass) -> {
         return findCause(throwable, exceptionClass).isPresent();
      });
   }

   public static Optional<Throwable> findCause(Throwable throwable, Class<? extends Throwable> exceptionClass) {
      Throwable t;
      for(t = throwable; t != null && !exceptionClass.isAssignableFrom(t.getClass()); t = t.getCause()) {
         ;
      }

      return Optional.ofNullable(t);
   }

   public static void createPrivateDirSafely(Path path) {
      if(Files.notExists(path, new LinkOption[0])) {
         try {
            Set<PosixFilePermission> permissions = PosixFilePermissions.fromString("rwx------");
            Files.createDirectories(path, new FileAttribute[]{PosixFilePermissions.asFileAttribute(permissions)});
         } catch (IOException var2) {
            throw new RuntimeException("Couldn't create directory " + path.toString(), var2);
         }
      }

   }

   public static OpenOption[] fileWriteOpts(boolean forceOverwrite) {
      return forceOverwrite?new OpenOption[]{StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE, StandardOpenOption.WRITE}:new OpenOption[]{StandardOpenOption.CREATE_NEW};
   }

   public static InetAddress getByName(String str) {
      try {
         return InetAddress.getByName(str);
      } catch (UnknownHostException var2) {
         throw new RuntimeException(var2);
      }
   }

   public static <V, T extends V> Optional<T> firstInstanceOf(V[] elems, Class<T> klass) {
      return Arrays.stream(elems).filter((x) -> {
         return klass.isAssignableFrom(x.getClass());
      }).map((x) -> {
         return x;
      }).findFirst();
   }

   public static void ignoreInterrupts(DseUtil.RunnableMayThrowInterruptedException runnable) {
      try {
         runnable.run();
      } catch (InterruptedException var2) {
         logger.debug("Ignoring InterruptedException", var2);
      }

   }

   @NotNull
   public static Throwable getRootCause(Throwable throwable) {
      Throwable t;
      for(t = throwable.getCause(); t.getCause() != null; t = t.getCause()) {
         ;
      }

      return t;
   }

   public static <T> T getWithRetry(RetrySetup retrySetup, Callable<T> supplier, Predicate<T> stopCondition) throws TimeoutException {
      for(RetrySetup.RetrySchedule retrying = retrySetup.fromNow(); retrying.hasMoreTries(); retrying.waitForNextTry()) {
         try {
            T result = supplier.call();
            if(stopCondition.test(result)) {
               return result;
            }
         } catch (Exception var6) {
            logger.trace("Failed to obtain or test a value", var6);
         }
      }

      throw new TimeoutException("Could not obtain a satisfactory value");
   }

   public static int calculateThreadsPerCore() {
      try {
         return calculateThreadsPerCore(VanillaCpuLayout.fromCpuInfo());
      } catch (AssertionError var1) {
         logger.warn("/proc/cpuinfo is incomplete, defaulting to 1 thread per CPU core...", var1);
         return 1;
      } catch (IOException var2) {
         logger.info("/proc/cpuinfo is not available, defaulting to 1 thread per CPU core...");
         return 1;
      }
   }

   @VisibleForTesting
   public static int calculateThreadsPerCore(VanillaCpuLayout layout) {
      int threadsPerCore = layout.threadsPerCore();
      if(threadsPerCore > 2) {
         logger.warn("/proc/cpuinfo is reporting {} threads per CPU core, but DSE will assume a value of 2 for auto-tuning.", Integer.valueOf(threadsPerCore));
         threadsPerCore = 2;
      }

      return threadsPerCore;
   }

   public static Completable retryingRunnable(Runnable runnable) {
      return Completable.fromRunnable(runnable).onErrorResumeNext((error) -> {
         return Throwables.getRootCause(error) instanceof WouldBlockException?ioRunnable(runnable):Completable.error(error);
      });
   }

   public static Completable ioRunnable(Runnable runnable) {
      return Completable.fromRunnable(runnable).subscribeOn(Schedulers.io()).observeOn(TPC.bestTPCScheduler());
   }

   public static Completable ioCallable(Callable callable) {
      return Completable.fromCallable(callable).subscribeOn(Schedulers.io()).observeOn(TPC.bestTPCScheduler());
   }

   public static boolean majorMinorVersionEquals(CassandraVersion cv1, CassandraVersion cv2) {
      return cv1.major == cv2.major && cv1.minor == cv2.minor;
   }

   public interface RunnableMayThrowInterruptedException {
      void run() throws InterruptedException;
   }
}
