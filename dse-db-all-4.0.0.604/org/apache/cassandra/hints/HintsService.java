package org.apache.cassandra.hints;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ImmutableMap.Builder;
import java.io.File;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.metrics.HintedHandoffMetrics;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class HintsService implements HintsServiceMBean {
   private static final Logger logger = LoggerFactory.getLogger(HintsService.class);
   public static HintsService instance = new HintsService();
   private static final String MBEAN_NAME = "org.apache.cassandra.hints:type=HintsService";
   private static final int MIN_BUFFER_SIZE = 33554432;
   static final ImmutableMap<String, Object> EMPTY_PARAMS = ImmutableMap.of();
   private final HintsCatalog catalog;
   private final HintsWriteExecutor writeExecutor;
   private final HintsBufferPool bufferPool;
   private final HintsDispatchExecutor dispatchExecutor;
   private final AtomicBoolean isDispatchPaused;
   private volatile boolean isShutDown;
   private final ScheduledFuture triggerFlushingFuture;
   private volatile ScheduledFuture triggerDispatchFuture;
   public final HintedHandoffMetrics metrics;

   private HintsService() {
      this(FailureDetector.instance);
   }

   @VisibleForTesting
   HintsService(IFailureDetector failureDetector) {
      this.isShutDown = false;
      File hintsDirectory = DatabaseDescriptor.getHintsDirectory();
      int maxDeliveryThreads = DatabaseDescriptor.getMaxHintsDeliveryThreads();
      this.catalog = HintsCatalog.load(hintsDirectory, createDescriptorParams());
      this.writeExecutor = new HintsWriteExecutor(this.catalog);
      int bufferSize = Math.max(DatabaseDescriptor.getMaxMutationSize() * 2, 33554432);
      HintsWriteExecutor var10004 = this.writeExecutor;
      this.writeExecutor.getClass();
      this.bufferPool = new HintsBufferPool(bufferSize, var10004::flushBuffer);
      this.isDispatchPaused = new AtomicBoolean(true);
      AtomicBoolean var10005 = this.isDispatchPaused;
      failureDetector.getClass();
      this.dispatchExecutor = new HintsDispatchExecutor(hintsDirectory, maxDeliveryThreads, var10005, failureDetector::isAlive);
      int flushPeriod = DatabaseDescriptor.getHintsFlushPeriodInMS();
      this.triggerFlushingFuture = ScheduledExecutors.optionalTasks.scheduleWithFixedDelay(() -> {
         this.writeExecutor.flushBufferPool(this.bufferPool);
      }, (long)flushPeriod, (long)flushPeriod, TimeUnit.MILLISECONDS);
      this.metrics = new HintedHandoffMetrics();
   }

   private static ImmutableMap<String, Object> createDescriptorParams() {
      Builder<String, Object> builder = ImmutableMap.builder();
      ParameterizedClass compressionConfig = DatabaseDescriptor.getHintsCompression();
      if(compressionConfig != null) {
         Builder<String, Object> compressorParams = ImmutableMap.builder();
         compressorParams.put("class_name", compressionConfig.class_name);
         if(compressionConfig.parameters != null) {
            compressorParams.put("parameters", compressionConfig.parameters);
         }

         builder.put("compression", compressorParams.build());
      }

      return builder.build();
   }

   public void registerMBean() {
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

      try {
         mbs.registerMBean(this, new ObjectName("org.apache.cassandra.hints:type=HintsService"));
      } catch (Exception var3) {
         throw new RuntimeException(var3);
      }
   }

   public void write(Iterable<UUID> hostIds, Hint hint) {
      if(this.isShutDown) {
         throw new IllegalStateException("HintsService is shut down and can't accept new hints");
      } else {
         this.catalog.maybeLoadStores(hostIds);
         this.bufferPool.write(hostIds, hint);
         StorageMetrics.totalHints.inc((long)Iterables.size(hostIds));
      }
   }

   public void write(UUID hostId, Hint hint) {
      this.write((Iterable)Collections.singleton(hostId), hint);
   }

   void writeForAllReplicas(Hint hint) {
      String keyspaceName = hint.mutation.getKeyspaceName();
      Token token = hint.mutation.key().getToken();
      Iterable var10000 = Iterables.filter(StorageService.instance.getNaturalAndPendingEndpoints(keyspaceName, token), StorageProxy::shouldHint);
      StorageService var10001 = StorageService.instance;
      StorageService.instance.getClass();
      Iterable<UUID> hostIds = Iterables.transform(var10000, var10001::getHostIdForEndpoint);
      this.write(hostIds, hint);
   }

   public void flushAndFsyncBlockingly(Iterable<UUID> hostIds) {
      HintsCatalog var10001 = this.catalog;
      this.catalog.getClass();
      Iterable<HintsStore> stores = Iterables.transform(hostIds, var10001::get);
      this.writeExecutor.flushBufferPool(this.bufferPool, stores);
      this.writeExecutor.fsyncWritersBlockingly(stores);
   }

   public synchronized void startDispatch() {
      if(this.isShutDown) {
         throw new IllegalStateException("HintsService is shut down and cannot be restarted");
      } else {
         this.isDispatchPaused.set(false);
         HintsDispatchTrigger trigger = new HintsDispatchTrigger(this.catalog, this.writeExecutor, this.dispatchExecutor, this.isDispatchPaused);
         this.triggerDispatchFuture = ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(trigger, 10L, 10L, TimeUnit.SECONDS);
      }
   }

   public void pauseDispatch() {
      logger.info("Paused hints dispatch");
      this.isDispatchPaused.set(true);
   }

   public void resumeDispatch() {
      logger.info("Resumed hints dispatch");
      this.isDispatchPaused.set(false);
   }

   public synchronized void shutdownBlocking() throws ExecutionException, InterruptedException {
      if(this.isShutDown) {
         throw new IllegalStateException("HintsService has already been shut down");
      } else {
         this.isShutDown = true;
         if(this.triggerDispatchFuture != null) {
            this.triggerDispatchFuture.cancel(false);
         }

         this.pauseDispatch();
         this.triggerFlushingFuture.cancel(false);
         this.writeExecutor.flushBufferPool(this.bufferPool).get();
         this.writeExecutor.closeAllWriters().get();
         this.dispatchExecutor.shutdownBlocking();
         this.writeExecutor.shutdownBlocking();
      }
   }

   public void deleteAllHints() {
      this.catalog.deleteAllHints();
   }

   public void deleteAllHintsForEndpoint(String address) {
      InetAddress target;
      try {
         target = InetAddress.getByName(address);
      } catch (UnknownHostException var4) {
         throw new IllegalArgumentException(var4);
      }

      this.deleteAllHintsForEndpoint(target);
   }

   public void deleteAllHintsForEndpoint(InetAddress target) {
      UUID hostId = StorageService.instance.getHostIdForEndpoint(target);
      if(hostId == null) {
         throw new IllegalArgumentException("Can't delete hints for unknown address " + target);
      } else {
         this.catalog.deleteAllHints(hostId);
      }
   }

   public void excise(UUID hostId) {
      HintsStore store = this.catalog.getNullable(hostId);
      if(store != null) {
         Future flushFuture = this.writeExecutor.flushBufferPool(this.bufferPool, Collections.singleton(store));
         Future closeFuture = this.writeExecutor.closeWriter(store);

         try {
            flushFuture.get();
            closeFuture.get();
         } catch (ExecutionException | InterruptedException var6) {
            throw new RuntimeException(var6);
         }

         this.dispatchExecutor.interruptDispatch(store.hostId);
         this.catalog.exciseStore(hostId);
      }
   }


   public Future transferHints(Supplier<UUID> hostIdSupplier) {
      Future<?> flushFuture = this.writeExecutor.flushBufferPool(this.bufferPool);
      Future<?> closeFuture = this.writeExecutor.closeAllWriters();
      try {
         flushFuture.get();
         closeFuture.get();
      }
      catch (InterruptedException | ExecutionException e) {
         throw new RuntimeException(e);
      }
      this.resumeDispatch();
      this.catalog.stores().forEach(this.dispatchExecutor::completeDispatchBlockingly);
      return this.dispatchExecutor.transfer(this.catalog, hostIdSupplier);
   }

   HintsCatalog getCatalog() {
      return this.catalog;
   }

   public boolean isShutDown() {
      return this.isShutDown;
   }
}
