package org.apache.cassandra.hints;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.ToLongFunction;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.SyncUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class HintsStore {
   private static final Logger logger = LoggerFactory.getLogger(HintsStore.class);
   private static final long MIN_DISPATCH_BACKOFF_DURATION = DatabaseDescriptor.getWriteRpcTimeout();
   private static final long MAX_DISPATCH_BACKOFF_DURATION;
   public final UUID hostId;
   private final File hintsDirectory;
   private final ImmutableMap<String, Object> writerParams;
   private final Map<HintsDescriptor, InputPosition> dispatchPositions;
   private final Deque<HintsDescriptor> dispatchDequeue;
   private final Queue<HintsDescriptor> blacklistedFiles;
   private volatile long dispatchBackoffDuration;
   private volatile long lastDispatchFailureTimestamp;
   private volatile long lastUsedTimestamp;
   private volatile HintsWriter hintsWriter;

   private HintsStore(UUID hostId, File hintsDirectory, ImmutableMap<String, Object> writerParams, List<HintsDescriptor> descriptors) {
      this.dispatchBackoffDuration = MIN_DISPATCH_BACKOFF_DURATION;
      this.hostId = hostId;
      this.hintsDirectory = hintsDirectory;
      this.writerParams = writerParams;
      this.dispatchPositions = new ConcurrentHashMap();
      this.dispatchDequeue = new ConcurrentLinkedDeque(descriptors);
      this.blacklistedFiles = new ConcurrentLinkedQueue();
      this.lastUsedTimestamp = descriptors.stream().mapToLong((d) -> {
         return d.timestamp;
      }).max().orElse(0L);
   }

   static HintsStore create(UUID hostId, File hintsDirectory, ImmutableMap<String, Object> writerParams, List<HintsDescriptor> descriptors) {
      descriptors.sort((d1, d2) -> {
         return Long.compare(d1.timestamp, d2.timestamp);
      });
      return new HintsStore(hostId, hintsDirectory, writerParams, descriptors);
   }

   @VisibleForTesting
   int getDispatchQueueSize() {
      return this.dispatchDequeue.size();
   }

   InetAddress address() {
      return StorageService.instance.getEndpointForHostId(this.hostId);
   }

   boolean isReadyToDispatchHints() {
      InetAddress address = this.address();
      long now = System.currentTimeMillis();
      return address != null && FailureDetector.instance.isAlive(address) && this.lastDispatchFailureTimestamp < now - this.dispatchBackoffDuration;
   }

   HintsDescriptor poll() {
      return (HintsDescriptor)this.dispatchDequeue.poll();
   }

   void offerFirst(HintsDescriptor descriptor) {
      this.dispatchDequeue.offerFirst(descriptor);
   }

   void offerLast(HintsDescriptor descriptor) {
      this.dispatchDequeue.offerLast(descriptor);
   }

   void deleteAllHints() {
      HintsDescriptor descriptor;
      while((descriptor = this.poll()) != null) {
         this.cleanUp(descriptor);
         this.delete(descriptor);
      }

      while((descriptor = (HintsDescriptor)this.blacklistedFiles.poll()) != null) {
         this.cleanUp(descriptor);
         this.delete(descriptor);
      }

   }

   void delete(HintsDescriptor descriptor) {
      File hintsFile = new File(this.hintsDirectory, descriptor.fileName());
      if(hintsFile.delete()) {
         StorageMetrics.hintsOnDisk.dec(descriptor.statistics().totalCount());
         logger.info("Deleted hint file {}", descriptor.fileName());
      } else {
         logger.error("Failed to delete hint file {}", descriptor.fileName());
      }

      (new File(this.hintsDirectory, descriptor.checksumFileName())).delete();
      (new File(this.hintsDirectory, descriptor.statisticsFileName())).delete();
   }

   boolean hasFiles() {
      return !this.dispatchDequeue.isEmpty();
   }

   InputPosition getDispatchOffset(HintsDescriptor descriptor) {
      return (InputPosition)this.dispatchPositions.get(descriptor);
   }

   void markDispatchOffset(HintsDescriptor descriptor, InputPosition inputPosition) {
      this.dispatchPositions.put(descriptor, inputPosition);
   }

   void cleanUp(HintsDescriptor descriptor) {
      this.dispatchPositions.remove(descriptor);
   }

   void blacklist(HintsDescriptor descriptor) {
      this.blacklistedFiles.add(descriptor);
   }

   boolean isWriting() {
      return this.hintsWriter != null;
   }

   HintsWriter getOrOpenWriter() {
      if(this.hintsWriter == null) {
         this.hintsWriter = this.openWriter();
      }

      return this.hintsWriter;
   }

   HintsWriter getWriter() {
      return this.hintsWriter;
   }

   private HintsWriter openWriter() {
      this.lastUsedTimestamp = Math.max(System.currentTimeMillis(), this.lastUsedTimestamp + 1L);
      HintsDescriptor descriptor = new HintsDescriptor(this.hostId, this.lastUsedTimestamp, this.writerParams);

      try {
         return HintsWriter.create(this.hintsDirectory, descriptor);
      } catch (IOException var3) {
         throw new FSWriteError(var3, descriptor.fileName());
      }
   }

   void closeWriter() {
      if(this.hintsWriter != null) {
         this.hintsWriter.close();
         this.offerLast(this.hintsWriter.descriptor());
         this.hintsWriter = null;
         SyncUtil.trySyncDir(this.hintsDirectory);
      }

   }

   void fsyncWriter() {
      if(this.hintsWriter != null) {
         this.hintsWriter.fsync();
      }

   }

   void recordDispatchFailure() {
      this.lastDispatchFailureTimestamp = System.currentTimeMillis();
      this.dispatchBackoffDuration = Math.min(this.dispatchBackoffDuration * 2L, MAX_DISPATCH_BACKOFF_DURATION);
   }

   void recordDispatchSuccess() {
      this.dispatchBackoffDuration = MIN_DISPATCH_BACKOFF_DURATION;
   }

   static {
      MAX_DISPATCH_BACKOFF_DURATION = TimeUnit.MINUTES.toMillis(10L);
   }
}
