package org.apache.cassandra.hints;

import com.google.common.util.concurrent.RateLimiter;
import java.io.File;
import java.net.InetAddress;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MessagingVersion;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class HintsDispatchExecutor {
   private static final Logger logger = LoggerFactory.getLogger(HintsDispatchExecutor.class);
   private final File hintsDirectory;
   private final ExecutorService executor;
   private final AtomicBoolean isPaused;
   private final Predicate<InetAddress> isAlive;
   private final Map<UUID, Future> scheduledDispatches;

   HintsDispatchExecutor(File hintsDirectory, int maxThreads, AtomicBoolean isPaused, Predicate<InetAddress> isAlive) {
      this.hintsDirectory = hintsDirectory;
      this.isPaused = isPaused;
      this.isAlive = isAlive;
      this.scheduledDispatches = new ConcurrentHashMap();
      this.executor = new JMXEnabledThreadPoolExecutor(maxThreads, 1L, TimeUnit.MINUTES, new LinkedBlockingQueue(), new NamedThreadFactory("HintsDispatcher", 1), "internal");
   }

   void shutdownBlocking() {
      this.scheduledDispatches.clear();
      this.executor.shutdownNow();

      try {
         this.executor.awaitTermination(1L, TimeUnit.MINUTES);
      } catch (InterruptedException var2) {
         throw new AssertionError(var2);
      }
   }

   boolean isScheduled(HintsStore store) {
      return this.scheduledDispatches.containsKey(store.hostId);
   }

   Future dispatch(HintsStore store) {
      return this.dispatch(store, store.hostId);
   }

   Future dispatch(HintsStore store, UUID hostId) {
      return (Future)this.scheduledDispatches.computeIfAbsent(hostId, (uuid) -> {
         return this.executor.submit(new HintsDispatchExecutor.DispatchHintsTask(store, hostId));
      });
   }

   Future transfer(HintsCatalog catalog, Supplier<UUID> hostIdSupplier) {
      return this.executor.submit(new HintsDispatchExecutor.TransferHintsTask(catalog, hostIdSupplier));
   }

   void completeDispatchBlockingly(HintsStore store) {
      Future future = (Future)this.scheduledDispatches.get(store.hostId);

      try {
         if(future != null) {
            future.get();
         }

      } catch (InterruptedException | ExecutionException var4) {
         throw new RuntimeException(var4);
      }
   }

   void interruptDispatch(UUID hostId) {
      Future future = (Future)this.scheduledDispatches.remove(hostId);
      if(null != future) {
         future.cancel(true);
      }

   }

   private final class DispatchHintsTask implements Runnable {
      private final HintsStore store;
      private final UUID hostId;
      private final RateLimiter rateLimiter;

      DispatchHintsTask(HintsStore store, UUID hostId) {
         this.store = store;
         this.hostId = hostId;
         int nodesCount = Math.max(1, StorageService.instance.getTokenMetadata().getSizeOfAllEndpoints() - 1);
         int throttleInKB = DatabaseDescriptor.getHintedHandoffThrottleInKB() / nodesCount;
         this.rateLimiter = RateLimiter.create(throttleInKB == 0?1.7976931348623157E308D:(double)(throttleInKB * 1024));
      }

      public void run() {
         try {
            this.dispatch();
         } finally {
            HintsDispatchExecutor.this.scheduledDispatches.remove(this.hostId);
         }

      }

      private void dispatch() {
         while(true) {
            if(!HintsDispatchExecutor.this.isPaused.get()) {
               HintsDescriptor descriptor = this.store.poll();
               if(descriptor != null) {
                  try {
                     if(this.dispatch(descriptor)) {
                        continue;
                     }
                  } catch (FSReadError var3) {
                     HintsDispatchExecutor.logger.error("Failed to dispatch hints file {}: file is corrupted ({})", descriptor.fileName(), var3.getMessage());
                     this.store.cleanUp(descriptor);
                     this.store.blacklist(descriptor);
                     throw var3;
                  }
               }
            }

            return;
         }
      }

      private boolean dispatch(HintsDescriptor descriptor) {
         HintsDispatchExecutor.logger.trace("Dispatching hints file {}", descriptor.fileName());
         InetAddress address = StorageService.instance.getEndpointForHostId(this.hostId);
         if(address != null) {
            return this.deliver(descriptor, address);
         } else {
            this.convert(descriptor);
            return true;
         }
      }

      private boolean deliver(HintsDescriptor descriptor, InetAddress address) {
         File file = new File(HintsDispatchExecutor.this.hintsDirectory, descriptor.fileName());
         InputPosition offset = this.store.getDispatchOffset(descriptor);
         BooleanSupplier shouldAbort = () -> {
            return !HintsDispatchExecutor.this.isAlive.test(address) || HintsDispatchExecutor.this.isPaused.get();
         };
         Optional<MessagingVersion> optVersion = MessagingService.instance().getVersion(address);
         if(!optVersion.isPresent()) {
            HintsDispatchExecutor.logger.debug("Cannot deliver handoff to endpoint {}: its version is unknown. This should be temporary.", address);
            return false;
         } else {
            HintsVerbs.HintsVersion version = (HintsVerbs.HintsVersion)((MessagingVersion)optVersion.get()).groupVersion(Verbs.Group.HINTS);
            HintsDispatcher dispatcher = HintsDispatcher.create(file, this.rateLimiter, address, version, descriptor.hostId, shouldAbort);
            Throwable var9 = null;

            boolean var10;
            try {
               if(offset != null) {
                  dispatcher.seek(offset);
               }

               if(!dispatcher.dispatch()) {
                  this.store.recordDispatchFailure();
                  this.store.markDispatchOffset(descriptor, dispatcher.dispatchPosition());
                  this.store.offerFirst(descriptor);
                  HintsDispatchExecutor.logger.info("Finished hinted handoff of file {} to endpoint {}: {}, partially", new Object[]{descriptor.fileName(), address, this.hostId});
                  var10 = false;
                  return var10;
               }

               this.store.recordDispatchSuccess();
               this.store.delete(descriptor);
               this.store.cleanUp(descriptor);
               HintsDispatchExecutor.logger.info("Finished hinted handoff of file {} to endpoint {}: {}", new Object[]{descriptor.fileName(), address, this.hostId});
               var10 = true;
            } catch (Throwable var20) {
               var9 = var20;
               throw var20;
            } finally {
               if(dispatcher != null) {
                  if(var9 != null) {
                     try {
                        dispatcher.close();
                     } catch (Throwable var19) {
                        var9.addSuppressed(var19);
                     }
                  } else {
                     dispatcher.close();
                  }
               }

            }

            return var10;
         }
      }

      private void convert(HintsDescriptor descriptor) {
         File file = new File(HintsDispatchExecutor.this.hintsDirectory, descriptor.fileName());
         HintsReader reader = HintsReader.open(file, this.rateLimiter);
         Throwable var4 = null;

         try {
            reader.forEach((page) -> {
               Iterator var10000 = page.hintsIterator();
               HintsService var10001 = HintsService.instance;
               HintsService.instance.getClass();
               var10000.forEachRemaining(var10001::writeForAllReplicas);
            });
            this.store.delete(descriptor);
            this.store.cleanUp(descriptor);
            HintsDispatchExecutor.logger.info("Finished converting hints file {}", descriptor.fileName());
         } catch (Throwable var13) {
            var4 = var13;
            throw var13;
         } finally {
            if(reader != null) {
               if(var4 != null) {
                  try {
                     reader.close();
                  } catch (Throwable var12) {
                     var4.addSuppressed(var12);
                  }
               } else {
                  reader.close();
               }
            }

         }

      }
   }

   private final class TransferHintsTask implements Runnable {
      private final HintsCatalog catalog;
      private final Supplier<UUID> hostIdSupplier;

      private TransferHintsTask(HintsCatalog var1, Supplier<UUID> catalog) {
         this.catalog = catalog;
         this.hostIdSupplier = hostIdSupplier;
      }

      public void run() {
         UUID hostId = (UUID)this.hostIdSupplier.get();
         InetAddress address = StorageService.instance.getEndpointForHostId(hostId);
         HintsDispatchExecutor.logger.info("Transferring all hints to {}: {}", address, hostId);
         if(!this.transfer(hostId)) {
            HintsDispatchExecutor.logger.warn("Failed to transfer all hints to {}: {}; will retry in {} seconds", new Object[]{address, hostId, Integer.valueOf(10)});

            try {
               TimeUnit.SECONDS.sleep(10L);
            } catch (InterruptedException var4) {
               throw new RuntimeException(var4);
            }

            hostId = (UUID)this.hostIdSupplier.get();
            HintsDispatchExecutor.logger.info("Transferring all hints to {}: {}", address, hostId);
            if(!this.transfer(hostId)) {
               HintsDispatchExecutor.logger.error("Failed to transfer all hints to {}: {}", address, hostId);
               throw new RuntimeException("Failed to transfer all hints to " + hostId);
            }
         }
      }

      private boolean transfer(UUID hostId) {
         this.catalog.stores().map((store) -> {
            return HintsDispatchExecutor.this.new DispatchHintsTask(store, hostId);
         }).forEach(Runnable::run);
         return !this.catalog.hasFiles();
      }
   }
}
