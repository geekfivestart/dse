package org.apache.cassandra.db.commitlog;

import com.google.common.annotations.VisibleForTesting;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.RebufferingInputStream;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommitLogReader {
   private static final Logger logger = LoggerFactory.getLogger(CommitLogReader.class);
   private static final int LEGACY_END_OF_SEGMENT_MARKER = 0;
   @VisibleForTesting
   public static final int ALL_MUTATIONS = -1;
   private final CRC32 checksum = new CRC32();
   private final Map<TableId, AtomicInteger> invalidMutations = new HashMap();
   private byte[] buffer = new byte[4096];

   public CommitLogReader() {
   }

   public Set<Entry<TableId, AtomicInteger>> getInvalidMutations() {
      return this.invalidMutations.entrySet();
   }

   public void readAllFiles(CommitLogReadHandler handler, File[] files) throws IOException {
      this.readAllFiles(handler, files, CommitLogPosition.NONE);
   }

   private static boolean shouldSkip(File file) throws IOException, ConfigurationException {
      RandomAccessReader reader = RandomAccessReader.open(file);
      Throwable var2 = null;

      boolean var7;
      try {
         CommitLogDescriptor.readHeader(reader, DatabaseDescriptor.getEncryptionContext());
         int end = reader.readInt();
         long filecrc = (long)reader.readInt() & 4294967295L;
         int mutationSize = reader.readInt();
         var7 = end == 0 && filecrc == 0L && mutationSize == 0;
      } catch (Throwable var16) {
         var2 = var16;
         throw var16;
      } finally {
         if(reader != null) {
            if(var2 != null) {
               try {
                  reader.close();
               } catch (Throwable var15) {
                  var2.addSuppressed(var15);
               }
            } else {
               reader.close();
            }
         }

      }

      return var7;
   }

   static List<File> filterCommitLogFiles(File[] toFilter) {
      List<File> filtered = new ArrayList(toFilter.length);
      File[] var2 = toFilter;
      int var3 = toFilter.length;

      for(int var4 = 0; var4 < var3; ++var4) {
         File file = var2[var4];

         try {
            if(shouldSkip(file)) {
               logger.info("Skipping playback of empty log: {}", file.getName());
            } else {
               filtered.add(file);
            }
         } catch (Exception var7) {
            filtered.add(file);
         }
      }

      return filtered;
   }

   public void readAllFiles(CommitLogReadHandler handler, File[] files, CommitLogPosition minPosition) throws IOException {
      List<File> filteredLogs = filterCommitLogFiles(files);
      int i = 0;
      Iterator var6 = filteredLogs.iterator();

      while(var6.hasNext()) {
         File file = (File)var6.next();
         ++i;
         this.readCommitLogSegment(handler, file, minPosition, -1, i == filteredLogs.size());
      }

   }

   public void readCommitLogSegment(CommitLogReadHandler handler, File file, boolean tolerateTruncation) throws IOException {
      this.readCommitLogSegment(handler, file, CommitLogPosition.NONE, -1, tolerateTruncation);
   }

   @VisibleForTesting
   public void readCommitLogSegment(CommitLogReadHandler handler, File file, int mutationLimit, boolean tolerateTruncation) throws IOException {
      this.readCommitLogSegment(handler, file, CommitLogPosition.NONE, mutationLimit, tolerateTruncation);
   }

   public void readCommitLogSegment(CommitLogReadHandler handler, File file, CommitLogPosition minPosition, int mutationLimit, boolean tolerateTruncation) throws IOException {
      CommitLogDescriptor desc = CommitLogDescriptor.fromFileName(file.getName());
      RandomAccessReader reader = RandomAccessReader.open(file);
      Throwable var8 = null;

      try {
         long segmentIdFromFilename = desc.id;

         try {
            desc = CommitLogDescriptor.readHeader(reader, DatabaseDescriptor.getEncryptionContext());
         } catch (Exception var29) {
            desc = null;
         }

         if(desc != null) {
            if(segmentIdFromFilename != desc.id && handler.shouldSkipSegmentOnError(new CommitLogReadHandler.CommitLogReadException(String.format("Segment id mismatch (filename %d, descriptor %d) in file %s", new Object[]{Long.valueOf(segmentIdFromFilename), Long.valueOf(desc.id), file}), CommitLogReadHandler.CommitLogReadErrorReason.RECOVERABLE_DESCRIPTOR_ERROR, false))) {
               return;
            }

            CommitLogSegmentReader segmentReader;
            try {
               if(this.shouldSkipSegmentId(file, desc, minPosition)) {
                  return;
               }

               segmentReader = new CommitLogSegmentReader(handler, desc, reader, tolerateTruncation);
            } catch (Exception var30) {
               handler.handleUnrecoverableError(new CommitLogReadHandler.CommitLogReadException(String.format("Unable to create segment reader for commit log file: %s", new Object[]{var30}), CommitLogReadHandler.CommitLogReadErrorReason.UNRECOVERABLE_UNKNOWN_ERROR, tolerateTruncation));
               return;
            }

            try {
               CommitLogReader.ReadStatusTracker statusTracker = new CommitLogReader.ReadStatusTracker(mutationLimit, tolerateTruncation);
               Iterator var13 = segmentReader.iterator();

               label241:
               do {
                  CommitLogSegmentReader.SyncSegment syncSegment;
                  do {
                     if(!var13.hasNext()) {
                        break label241;
                     }

                     syncSegment = (CommitLogSegmentReader.SyncSegment)var13.next();
                     statusTracker.tolerateErrorsInSection = tolerateTruncation & syncSegment.toleratesErrorsInSection;
                  } while(desc.id == minPosition.segmentId && syncSegment.endPosition < minPosition.position);

                  statusTracker.errorContext = String.format("Next section at %d in %s", new Object[]{Integer.valueOf(syncSegment.fileStartPosition), desc.fileName()});
                  this.readSection(handler, syncSegment.input, minPosition, syncSegment.endPosition, statusTracker, desc);
               } while(statusTracker.shouldContinue());
            } catch (RuntimeException var31) {
               if(var31.getCause() instanceof IOException) {
                  throw (IOException)var31.getCause();
               }

               throw var31;
            }

            logger.debug("Finished reading {}", file);
            return;
         }

         handler.handleUnrecoverableError(new CommitLogReadHandler.CommitLogReadException(String.format("Could not read commit log descriptor in file %s", new Object[]{file}), CommitLogReadHandler.CommitLogReadErrorReason.UNRECOVERABLE_DESCRIPTOR_ERROR, tolerateTruncation));
      } catch (Throwable var32) {
         var8 = var32;
         throw var32;
      } finally {
         if(reader != null) {
            if(var8 != null) {
               try {
                  reader.close();
               } catch (Throwable var28) {
                  var8.addSuppressed(var28);
               }
            } else {
               reader.close();
            }
         }

      }

   }

   private boolean shouldSkipSegmentId(File file, CommitLogDescriptor desc, CommitLogPosition minPosition) {
      logger.debug("Reading {} (CL version {}, compression {})", new Object[]{file.getPath(), desc.version, desc.compression});
      if(minPosition.segmentId > desc.id) {
         logger.trace("Skipping read of fully-flushed {}", file);
         return true;
      } else {
         return false;
      }
   }

   private void readSection(CommitLogReadHandler handler, FileDataInput reader, CommitLogPosition minPosition, int end, CommitLogReader.ReadStatusTracker statusTracker, CommitLogDescriptor desc) throws IOException {
      if(desc.id == minPosition.segmentId && reader.getFilePointer() < (long)minPosition.position) {
         reader.seek((long)minPosition.position);
      }

      while(statusTracker.shouldContinue() && reader.getFilePointer() < (long)end && !reader.isEOF()) {
         long mutationStart = reader.getFilePointer();
         if(logger.isTraceEnabled()) {
            logger.trace("Reading mutation at {}", Long.valueOf(mutationStart));
         }

         long claimedCRC32;
         int serializedSize;
         long mutationPosition;
         try {
            if((long)end - reader.getFilePointer() < 4L) {
               logger.trace("Not enough bytes left for another mutation in this CommitLog section, continuing");
               statusTracker.requestTermination();
               return;
            }

            serializedSize = reader.readInt();
            if(serializedSize == 0) {
               logger.trace("Encountered end of segment marker at {}", Long.valueOf(reader.getFilePointer()));
               statusTracker.requestTermination();
               return;
            }

            if(serializedSize < 10) {
               if(handler.shouldSkipSegmentOnError(new CommitLogReadHandler.CommitLogReadException(String.format("Invalid mutation size %d at %d in %s", new Object[]{Integer.valueOf(serializedSize), Long.valueOf(mutationStart), statusTracker.errorContext}), CommitLogReadHandler.CommitLogReadErrorReason.MUTATION_ERROR, statusTracker.tolerateErrorsInSection))) {
                  statusTracker.requestTermination();
               }

               return;
            }

            mutationPosition = CommitLogReader.CommitLogFormat.calculateClaimedChecksum(reader);
            this.checksum.reset();
            FBUtilities.updateChecksumInt(this.checksum, serializedSize);
            if(this.checksum.getValue() != mutationPosition) {
               if(handler.shouldSkipSegmentOnError(new CommitLogReadHandler.CommitLogReadException(String.format("Mutation size checksum failure at %d in %s", new Object[]{Long.valueOf(mutationStart), statusTracker.errorContext}), CommitLogReadHandler.CommitLogReadErrorReason.MUTATION_ERROR, statusTracker.tolerateErrorsInSection))) {
                  statusTracker.requestTermination();
               }

               return;
            }

            if(serializedSize > this.buffer.length) {
               this.buffer = new byte[(int)(1.2D * (double)serializedSize)];
            }

            reader.readFully(this.buffer, 0, serializedSize);
            claimedCRC32 = CommitLogReader.CommitLogFormat.calculateClaimedCRC32(reader);
         } catch (EOFException var14) {
            if(handler.shouldSkipSegmentOnError(new CommitLogReadHandler.CommitLogReadException(String.format("Unexpected end of segment at %d in %s", new Object[]{Long.valueOf(mutationStart), statusTracker.errorContext}), CommitLogReadHandler.CommitLogReadErrorReason.EOF, statusTracker.tolerateErrorsInSection))) {
               statusTracker.requestTermination();
            }

            return;
         }

         this.checksum.update(this.buffer, 0, serializedSize);
         if(claimedCRC32 != this.checksum.getValue()) {
            if(handler.shouldSkipSegmentOnError(new CommitLogReadHandler.CommitLogReadException(String.format("Mutation checksum failure at %d in %s", new Object[]{Long.valueOf(mutationStart), statusTracker.errorContext}), CommitLogReadHandler.CommitLogReadErrorReason.MUTATION_ERROR, statusTracker.tolerateErrorsInSection))) {
               statusTracker.requestTermination();
            }
         } else {
            mutationPosition = reader.getFilePointer();
            this.readMutation(handler, this.buffer, serializedSize, minPosition, (int)mutationPosition, desc);
            if(mutationPosition >= (long)minPosition.position) {
               statusTracker.addProcessedMutation();
            }
         }
      }

   }

   @VisibleForTesting
   protected void readMutation(CommitLogReadHandler handler, byte[] inputBuffer, int size, CommitLogPosition minPosition, int entryLocation, CommitLogDescriptor desc) throws IOException {
      boolean shouldReplay = entryLocation > minPosition.position;

      Mutation mutation;
      try {
         RebufferingInputStream bufIn = new DataInputBuffer(inputBuffer, 0, size);
         Throwable var45 = null;

         try {
            mutation = ((Mutation.MutationSerializer)Mutation.rawSerializers.get(desc.version.encodingVersion)).deserialize(bufIn, SerializationHelper.Flag.LOCAL);
            Iterator var46 = mutation.getPartitionUpdates().iterator();

            while(var46.hasNext()) {
               PartitionUpdate upd = (PartitionUpdate)var46.next();
               upd.validate();
            }
         } catch (Throwable var40) {
            var45 = var40;
            throw var40;
         } finally {
            if(bufIn != null) {
               if(var45 != null) {
                  try {
                     bufIn.close();
                  } catch (Throwable var38) {
                     var45.addSuppressed(var38);
                  }
               } else {
                  bufIn.close();
               }
            }

         }
      } catch (UnknownTableException var42) {
         if(var42.id == null) {
            return;
         }

         AtomicInteger i = (AtomicInteger)this.invalidMutations.get(var42.id);
         if(i == null) {
            i = new AtomicInteger(1);
            this.invalidMutations.put(var42.id, i);
         } else {
            i.incrementAndGet();
         }

         return;
      } catch (Throwable var43) {
         JVMStabilityInspector.inspectThrowable(var43);
         Path p = Files.createTempFile("mutation", "dat", new FileAttribute[0]);
         DataOutputStream out = new DataOutputStream(Files.newOutputStream(p, new OpenOption[0]));
         Throwable var12 = null;

         try {
            out.write(inputBuffer, 0, size);
         } catch (Throwable var37) {
            var12 = var37;
            throw var37;
         } finally {
            if(out != null) {
               if(var12 != null) {
                  try {
                     out.close();
                  } catch (Throwable var36) {
                     var12.addSuppressed(var36);
                  }
               } else {
                  out.close();
               }
            }

         }

         handler.handleUnrecoverableError(new CommitLogReadHandler.CommitLogReadException(String.format("Unexpected error deserializing mutation; saved to %s.  This may be caused by replaying a mutation against a table with the same name but incompatible schema.  Exception follows: %s", new Object[]{p.toString(), var43}), CommitLogReadHandler.CommitLogReadErrorReason.MUTATION_ERROR, false));
         return;
      }

      if(logger.isTraceEnabled()) {
         logger.trace("Read mutation for {}.{}: {}", new Object[]{mutation.getKeyspaceName(), mutation.key(), "{" + StringUtils.join(mutation.getPartitionUpdates().iterator(), ", ") + "}"});
      }

      if(shouldReplay) {
         handler.handleMutation(mutation, size, entryLocation, desc);
      }

   }

   private static class ReadStatusTracker {
      private int mutationsLeft;
      public String errorContext = "";
      public boolean tolerateErrorsInSection;
      private boolean error;

      public ReadStatusTracker(int mutationLimit, boolean tolerateErrorsInSection) {
         this.mutationsLeft = mutationLimit;
         this.tolerateErrorsInSection = tolerateErrorsInSection;
      }

      public void addProcessedMutation() {
         if(this.mutationsLeft != -1) {
            --this.mutationsLeft;
         }
      }

      public boolean shouldContinue() {
         return !this.error && (this.mutationsLeft != 0 || this.mutationsLeft == -1);
      }

      public void requestTermination() {
         this.error = true;
      }
   }

   private static class CommitLogFormat {
      private CommitLogFormat() {
      }

      static long calculateClaimedChecksum(FileDataInput input) throws IOException {
         return (long)input.readInt() & 4294967295L;
      }

      static void updateChecksum(CRC32 checksum, int serializedSize) {
         FBUtilities.updateChecksumInt(checksum, serializedSize);
      }

      static long calculateClaimedCRC32(FileDataInput input) throws IOException {
         return (long)input.readInt() & 4294967295L;
      }
   }
}
