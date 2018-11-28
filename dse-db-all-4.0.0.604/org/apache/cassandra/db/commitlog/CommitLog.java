package org.apache.cassandra.db.commitlog;

import com.google.common.annotations.VisibleForTesting;
import io.reactivex.Completable;
import io.reactivex.Single;
import io.reactivex.functions.Function;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.zip.CRC32;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.util.BufferedDataOutputStreamPlus;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.CommitLogMetrics;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ErrorHandler;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.SystemTimeSource;
import org.apache.cassandra.utils.TimeSource;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommitLog implements CommitLogMBean {
   private static final Logger logger = LoggerFactory.getLogger(CommitLog.class);
   public static final CommitLog instance = construct();
   static final long MAX_MUTATION_SIZE = (long)DatabaseDescriptor.getMaxMutationSize();
   public final AbstractCommitLogSegmentManager segmentManager;
   public final CommitLogArchiver archiver;
   final CommitLogMetrics metrics;
   final AbstractCommitLogService executor;
   volatile CommitLog.Configuration configuration;
   @VisibleForTesting
   final TimeSource timeSource;

   private static CommitLog construct() {
      CommitLog log = new CommitLog(CommitLogArchiver.construct());
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

      try {
         mbs.registerMBean(log, new ObjectName("org.apache.cassandra.db:type=Commitlog"));
      } catch (Exception var3) {
         throw new RuntimeException(var3);
      }

      return log.start();
   }

   @VisibleForTesting
   CommitLog(CommitLogArchiver archiver) {
      try {
         this.timeSource = (TimeSource)Class.forName(System.getProperty("dse.commitlog.timesource", SystemTimeSource.class.getCanonicalName())).newInstance();
         this.configuration = new Configuration(DatabaseDescriptor.getCommitLogCompression(), DatabaseDescriptor.getEncryptionContext());
         DatabaseDescriptor.createAllDirectories();
         this.archiver = archiver;
         this.metrics = new CommitLogMetrics();
         switch (DatabaseDescriptor.getCommitLogSync()) {
            case periodic: {
               this.executor = new PeriodicCommitLogService(this, this.timeSource);
               break;
            }
            case batch: {
               this.executor = new BatchCommitLogService(this, this.timeSource);
               break;
            }
            case group: {
               this.executor = new GroupCommitLogService(this, this.timeSource);
               break;
            }
            default: {
               throw new IllegalArgumentException("Unknown commitlog service type: " + (Object)((Object)DatabaseDescriptor.getCommitLogSync()));
            }
         }
         this.segmentManager = DatabaseDescriptor.isCDCEnabled() ? new CommitLogSegmentManagerCDC(this, DatabaseDescriptor.getCommitLogLocation()) : new CommitLogSegmentManagerStandard(this, DatabaseDescriptor.getCommitLogLocation());
         this.metrics.attach(this.executor, this.segmentManager);
      }
      catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   CommitLog start() {
      this.segmentManager.start();
      this.executor.start();
      return this;
   }

   public int recoverSegmentsOnDisk() throws IOException {
      FilenameFilter unmanagedFilesFilter = (dir, name) -> {
         return CommitLogDescriptor.isValid(name) && CommitLogSegment.shouldReplay(name);
      };
      File[] files = (new File(this.segmentManager.storageDirectory)).listFiles(unmanagedFilesFilter);
      int replayed = files.length;

      for(int var4 = 0; var4 < replayed; ++var4) {
         File file = files[var4];
         this.archiver.maybeArchive(file.getPath(), file.getName());
         this.archiver.maybeWaitForArchiving(file.getName());
      }

      assert this.archiver.archivePending.isEmpty() : "Not all commit log archive tasks were completed before restore";

      this.archiver.maybeRestoreArchive();
      files = (new File(this.segmentManager.storageDirectory)).listFiles(unmanagedFilesFilter);
      replayed = 0;
      if(files.length == 0) {
         logger.info("No commitlog files found; skipping replay");
      } else {
         Arrays.sort(files, new CommitLogSegment.CommitLogSegmentFileComparator());
         logger.info("Replaying {}", StringUtils.join(files, ", "));
         replayed = this.recoverFiles(files);
         logger.info("Log replay complete, {} replayed mutations", Integer.valueOf(replayed));
         File[] var8 = files;
         int var9 = files.length;

         for(int var6 = 0; var6 < var9; ++var6) {
            File f = var8[var6];
            this.segmentManager.handleReplayedSegment(f);
         }
      }

      return replayed;
   }

   public int recoverFiles(File... clogs) throws IOException {
      CommitLogReplayer replayer = CommitLogReplayer.construct(this);
      replayer.replayFiles(clogs);
      return replayer.blockForWrites();
   }

   public void recoverPath(String path) throws IOException {
      CommitLogReplayer replayer = CommitLogReplayer.construct(this);
      replayer.replayPath(new File(path), false);
      replayer.blockForWrites();
   }

   public void recover(String path) throws IOException {
      this.recoverPath(path);
   }

   public CommitLogPosition getCurrentPosition() {
      return this.segmentManager.getCurrentPosition();
   }

   public void forceRecycleAllSegments(Iterable<TableId> droppedTables) {
      this.segmentManager.forceRecycleAll(droppedTables);
   }

   public void forceRecycleAllSegments() {
      this.segmentManager.forceRecycleAll(Collections.emptyList());
   }

   public void sync() throws IOException {
      this.segmentManager.sync();
   }

   public void requestExtraSync() {
      this.executor.requestExtraSync();
   }

   public static boolean isOversizedMutation(long mutationSize) {
      return mutationSize + 12L > MAX_MUTATION_SIZE;
   }

   public Single<CommitLogPosition> add(Mutation mutation) throws WriteTimeoutException {
      assert mutation != null;

      ByteBuffer serializedMutation = ((Mutation.MutationSerializer)Mutation.rawSerializers.get(CommitLogDescriptor.current_version.encodingVersion)).serializedBuffer(mutation);
      int size = serializedMutation.remaining();
      int totalSize = size + 12;
      return isOversizedMutation((long)size)?Single.error(new IllegalArgumentException(String.format("Mutation of %s is too large for the maximum size of %s", new Object[]{FBUtilities.prettyPrintMemory((long)totalSize), FBUtilities.prettyPrintMemory(MAX_MUTATION_SIZE)}))):this.segmentManager.allocate(mutation, totalSize).flatMap((alloc) -> {
         CRC32 checksum = new CRC32();
         ByteBuffer buffer = alloc.getBuffer();

         label137: {
            Single var8;
            try {
               BufferedDataOutputStreamPlus dos = new DataOutputBufferFixed(buffer);
               Throwable var30 = null;

               try {
                  dos.writeInt(size);
                  FBUtilities.updateChecksumInt(checksum, size);
                  buffer.putInt((int)checksum.getValue());
                  dos.write(serializedMutation);
                  FBUtilities.updateChecksum(checksum, buffer, buffer.position() - size, size);
                  buffer.putInt((int)checksum.getValue());
                  break label137;
               } catch (Throwable var26) {
                  var30 = var26;
                  throw var26;
               } finally {
                  if(dos != null) {
                     if(var30 != null) {
                        try {
                           dos.close();
                        } catch (Throwable var25) {
                           var30.addSuppressed(var25);
                        }
                     } else {
                        dos.close();
                     }
                  }

               }
            } catch (Exception var28) {
               var8 = Single.error(new FSWriteError(var28, alloc.getSegment().getPath()));
            } finally {
               alloc.markWritten();
            }

            return var8;
         }

         Completable var10000 = this.executor.finishWriteFor(alloc, mutation.getScheduler());
         alloc.getClass();
         return var10000.toSingle(alloc::getCommitLogPosition);
      });
   }

   public void discardCompletedSegments(TableId id, CommitLogPosition lowerBound, CommitLogPosition upperBound) {
      logger.trace("discard completed log segments for {}-{}, table {}", new Object[]{lowerBound, upperBound, id});
      Iterator iter = this.segmentManager.getActiveSegments().iterator();

      while(iter.hasNext()) {
         CommitLogSegment segment = (CommitLogSegment)iter.next();
         segment.markClean(id, lowerBound, upperBound);
         if(segment.isUnused()) {
            logger.debug("Commit log segment {} is unused", segment);
            this.segmentManager.archiveAndDiscard(segment);
         } else if(logger.isTraceEnabled()) {
            logger.trace("Not safe to delete{} commit log segment {}; dirty is {}", new Object[]{iter.hasNext()?"":" active", segment, segment.dirtyString()});
         }

         if(segment.contains(upperBound)) {
            break;
         }
      }

   }

   public String getArchiveCommand() {
      return this.archiver.archiveCommand;
   }

   public String getRestoreCommand() {
      return this.archiver.restoreCommand;
   }

   public String getRestoreDirectories() {
      return this.archiver.restoreDirectories;
   }

   public long getRestorePointInTime() {
      return this.archiver.restorePointInTime;
   }

   public String getRestorePrecision() {
      return this.archiver.precision.toString();
   }

   public List<String> getActiveSegmentNames() {
      Collection<CommitLogSegment> segments = this.segmentManager.getActiveSegments();
      List<String> segmentNames = new ArrayList(segments.size());
      Iterator var3 = segments.iterator();

      while(var3.hasNext()) {
         CommitLogSegment seg = (CommitLogSegment)var3.next();
         segmentNames.add(seg.getName());
      }

      return segmentNames;
   }

   public List<String> getArchivingSegmentNames() {
      return new ArrayList(this.archiver.archivePending.keySet());
   }

   public long getActiveContentSize() {
      long size = 0L;

      CommitLogSegment seg;
      for(Iterator var3 = this.segmentManager.getActiveSegments().iterator(); var3.hasNext(); size += seg.contentSize()) {
         seg = (CommitLogSegment)var3.next();
      }

      return size;
   }

   public long getActiveOnDiskSize() {
      return this.segmentManager.onDiskSize();
   }

   public Map<String, Double> getActiveSegmentCompressionRatios() {
      Map<String, Double> segmentRatios = new TreeMap();
      Iterator var2 = this.segmentManager.getActiveSegments().iterator();

      while(var2.hasNext()) {
         CommitLogSegment seg = (CommitLogSegment)var2.next();
         segmentRatios.put(seg.getName(), Double.valueOf(1.0D * (double)seg.onDiskSize() / (double)seg.contentSize()));
      }

      return segmentRatios;
   }

   public void shutdownBlocking() throws InterruptedException {
      this.executor.shutdown();
      this.executor.awaitTermination();
      this.segmentManager.shutdown();
      this.segmentManager.awaitTermination();
   }

   public int resetUnsafe(boolean deleteSegments) throws IOException {
      this.stopUnsafe(deleteSegments);
      this.resetConfiguration();
      return this.restartUnsafe();
   }

   public void resetConfiguration() {
      this.configuration = new CommitLog.Configuration(DatabaseDescriptor.getCommitLogCompression(), DatabaseDescriptor.getEncryptionContext());
   }

   public void stopUnsafe(boolean deleteSegments) {
      this.executor.shutdown();

      try {
         this.executor.awaitTermination();
      } catch (InterruptedException var6) {
         throw new RuntimeException(var6);
      }

      this.segmentManager.stopUnsafe(deleteSegments);
      CommitLogSegment.resetReplayLimit();
      if(DatabaseDescriptor.isCDCEnabled() && deleteSegments) {
         File[] var2 = (new File(DatabaseDescriptor.getCDCLogLocation())).listFiles();
         int var3 = var2.length;

         for(int var4 = 0; var4 < var3; ++var4) {
            File f = var2[var4];
            FileUtils.deleteWithConfirm(f);
         }
      }

   }

   public int restartUnsafe() throws IOException {
      return this.start().recoverSegmentsOnDisk();
   }

   @VisibleForTesting
   public static boolean handleCommitError(final String message, final Throwable t) {
      class Handler implements ErrorHandler {
         private boolean ignored;

         Handler() {
         }

         public void handleError(Throwable error) {
            Config.CommitFailurePolicy policy = DatabaseDescriptor.getCommitFailurePolicy();
            switch (policy) {
               case die: {
                  JVMStabilityInspector.killJVM(t, false);
               }
               case stop: {
                  StorageService.instance.stopTransportsAsync();
               }
               case stop_commit: {
                  logger.error(String.format("%s. Commit disk failure policy is %s; terminating thread", new Object[]{message, policy}), t);
                  this.ignored = false;
                  break;
               }
               case ignore: {
                  logger.error(String.format("%s. Commit disk failure policy is %s; ignoring", new Object[]{message, policy}), t);
                  this.ignored = true;
                  break;
               }
               default: {
                  throw new AssertionError((Object)policy);
               }
            }
         }
      }

      Handler handler = new Handler();
      JVMStabilityInspector.inspectThrowable(t, (ErrorHandler)handler);
      return handler.ignored;
   }

   public static final class Configuration {
      private final ParameterizedClass compressorClass;
      private final ICompressor compressor;
      private EncryptionContext encryptionContext;

      public Configuration(ParameterizedClass compressorClass, EncryptionContext encryptionContext) {
         this.compressorClass = compressorClass;
         this.compressor = compressorClass != null?CompressionParams.createCompressor(compressorClass):null;
         this.encryptionContext = encryptionContext;
      }

      public boolean useCompression() {
         return this.compressor != null;
      }

      public boolean useEncryption() {
         return this.encryptionContext.isEnabled();
      }

      public ICompressor getCompressor() {
         return this.compressor;
      }

      public ParameterizedClass getCompressorClass() {
         return this.compressorClass;
      }

      public String getCompressorName() {
         return this.useCompression()?this.compressor.getClass().getSimpleName():"none";
      }

      public EncryptionContext getEncryptionContext() {
         return this.encryptionContext;
      }
   }
}
