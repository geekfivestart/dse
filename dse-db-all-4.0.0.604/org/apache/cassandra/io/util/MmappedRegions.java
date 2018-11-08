package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.cassandra.db.mos.MemoryLockedBuffer;
import org.apache.cassandra.db.mos.MemoryOnlyStatus;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.RefCounted;
import org.apache.cassandra.utils.concurrent.SharedCloseableImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MmappedRegions extends SharedCloseableImpl {
   private static final Logger logger = LoggerFactory.getLogger(MmappedRegions.class);
   public static int MAX_SEGMENT_SIZE = 2147483647;
   static final int REGION_ALLOC_SIZE = 15;
   private final MmappedRegions.State state;
   private volatile MmappedRegions.State copy;

   private MmappedRegions(MmappedRegions.State state, CompressionMetadata metadata) {
      super((RefCounted.Tidy)(new MmappedRegions.Tidier(state)));
      this.state = state;
      this.updateState(metadata);
      this.copy = new MmappedRegions.State(state);
   }

   private MmappedRegions(MmappedRegions.State state, long length, int chunkSize) {
      super((RefCounted.Tidy)(new MmappedRegions.Tidier(state)));
      this.state = state;
      if(length > 0L) {
         this.updateState(length, chunkSize);
      }

      this.copy = new MmappedRegions.State(state);
   }

   private MmappedRegions(MmappedRegions original) {
      super((SharedCloseableImpl)original);
      this.state = original.copy;
   }

   public static MmappedRegions empty(ChannelProxy channel) {
      return new MmappedRegions(new MmappedRegions.State(channel), 0L, 0);
   }

   public static MmappedRegions map(ChannelProxy channel, CompressionMetadata metadata) {
      if(metadata == null) {
         throw new IllegalArgumentException("metadata cannot be null");
      } else {
         return new MmappedRegions(new MmappedRegions.State(channel), metadata);
      }
   }

   public static MmappedRegions map(ChannelProxy channel, long length, int chunkSize) {
      if(length <= 0L) {
         throw new IllegalArgumentException("Length must be positive");
      } else {
         return new MmappedRegions(new MmappedRegions.State(channel), length, chunkSize);
      }
   }

   public MmappedRegions sharedCopy() {
      return new MmappedRegions(this);
   }

   private boolean isCopy() {
      return this.copy == null;
   }

   public void extend(long length, int chunkSize) {
      if(length < 0L) {
         throw new IllegalArgumentException("Length must not be negative");
      } else {
         assert !this.isCopy() : "Copies cannot be extended";

         if(length > this.state.length) {
            this.updateState(length, chunkSize);
            this.copy = new MmappedRegions.State(this.state);
         }
      }
   }

   private void updateState(long length, int chunkSize) {
      long maxSize = (long)(MAX_SEGMENT_SIZE / chunkSize * chunkSize);
      this.state.length = length;

      long size;
      for(long pos = this.state.getPosition(); pos < length; pos += size) {
         size = Math.min(maxSize, length - pos);
         this.state.add(pos, size);
      }

   }

   private void updateState(CompressionMetadata metadata) {
      long offset = 0L;
      long lastSegmentOffset = 0L;

      long segmentSize;
      for(segmentSize = 0L; offset < metadata.dataLength; offset += (long)metadata.chunkLength()) {
         CompressionMetadata.Chunk chunk = metadata.chunkFor(offset);
         if(segmentSize + (long)chunk.length + 4L > (long)MAX_SEGMENT_SIZE && segmentSize > 0L) {
            this.state.add(lastSegmentOffset, segmentSize);
            lastSegmentOffset += segmentSize;
            segmentSize = 0L;
         }

         segmentSize += (long)(chunk.length + 4);
      }

      if(segmentSize > 0L) {
         this.state.add(lastSegmentOffset, segmentSize);
      }

      this.state.length = lastSegmentOffset + segmentSize;
   }

   public boolean isValid(ChannelProxy channel) {
      return this.state.isValid(channel);
   }

   public boolean isEmpty() {
      return this.state.isEmpty();
   }

   public MmappedRegions.Region floor(long position) {
      assert !this.isCleanedUp() : "Attempted to use closed region";

      return this.state.floor(position);
   }

   public void lock(MemoryOnlyStatus memoryOnlyStatus) {
      this.state.lock(memoryOnlyStatus);
   }

   public void unlock(MemoryOnlyStatus memoryOnlyStatus) {
      this.state.unlock(memoryOnlyStatus);
   }

   public List<MemoryLockedBuffer> getLockedMemory() {
      return this.state.getLockedMemory();
   }

   public void closeQuietly() {
      Throwable err = this.close((Throwable)null);
      if(err != null) {
         JVMStabilityInspector.inspectThrowable(err);
         LoggerFactory.getLogger(this.getClass()).error("Error while closing mmapped regions", err);
      }

   }

   public static final class Tidier implements RefCounted.Tidy {
      final MmappedRegions.State state;

      Tidier(MmappedRegions.State state) {
         this.state = state;
      }

      public String name() {
         return this.state.channel.filePath();
      }

      public void tidy() {
         try {
            Throwables.maybeFail(this.state.close((Throwable)null));
         } catch (Exception var2) {
            throw new FSReadError(var2, this.state.channel.filePath());
         }
      }
   }

   private static final class State {
      private final ChannelProxy channel;
      private MappedByteBuffer[] buffers;
      private long[] offsets;
      private long length;
      private int last;
      @Nullable
      private List<MemoryLockedBuffer> lockedBuffers;

      private State(ChannelProxy channel) {
         this.channel = channel.sharedCopy();
         this.buffers = new MappedByteBuffer[15];
         this.offsets = new long[15];
         this.length = 0L;
         this.last = -1;
      }

      private State(MmappedRegions.State original) {
         this.channel = original.channel;
         this.buffers = original.buffers;
         this.offsets = original.offsets;
         this.length = original.length;
         this.last = original.last;
         this.lockedBuffers = original.lockedBuffers;
      }

      private boolean isEmpty() {
         return this.last < 0;
      }

      private boolean isValid(ChannelProxy channel) {
         return this.channel.filePath().equals(channel.filePath());
      }

      private MmappedRegions.Region floor(long position) {
         assert 0L <= position && position <= this.length : String.format("%d > %d", new Object[]{Long.valueOf(position), Long.valueOf(this.length)});

         int idx = Arrays.binarySearch(this.offsets, 0, this.last + 1, position);

         assert idx != -1 : String.format("Bad position %d for regions %s, last %d in %s", new Object[]{Long.valueOf(position), Arrays.toString(this.offsets), Integer.valueOf(this.last), this.channel});

         if(idx < 0) {
            idx = -(idx + 2);
         }

         return new MmappedRegions.Region(this.offsets[idx], this.buffers[idx]);
      }

      private long getPosition() {
         return this.last < 0?0L:this.offsets[this.last] + (long)this.buffers[this.last].capacity();
      }

      private void add(long pos, long size) {
         MappedByteBuffer buffer = this.channel.map(MapMode.READ_ONLY, pos, size);
         ++this.last;
         if(this.last == this.offsets.length) {
            this.offsets = Arrays.copyOf(this.offsets, this.offsets.length + 15);
            this.buffers = (MappedByteBuffer[])Arrays.copyOf(this.buffers, this.buffers.length + 15);
         }

         this.offsets[this.last] = pos;
         this.buffers[this.last] = buffer;
      }

      public void lock(MemoryOnlyStatus memoryOnlyStatus) {
         if(this.lockedBuffers != null) {
            throw new IllegalStateException(String.format("Attempted to lock memory for %s twice", new Object[]{this.channel.filePath()}));
         } else {
            MmappedRegions.logger.debug("Locking file {} in RAM", this.channel.filePath());
            Stream var10001 = Arrays.stream(this.buffers).filter(Objects::nonNull);
            memoryOnlyStatus.getClass();
            this.lockedBuffers = (List)var10001.map(memoryOnlyStatus::lock).collect(Collectors.toList());
         }
      }

      public void unlock(MemoryOnlyStatus memoryOnlyStatus) {
         if(this.lockedBuffers == null) {
            throw new IllegalStateException(String.format("Attempted to unlock memory for %s without any previous locking", new Object[]{this.channel.filePath()}));
         } else {
            MmappedRegions.logger.debug("Unlocking file {} from RAM", this.channel.filePath());
            this.lockedBuffers.stream().forEach(memoryOnlyStatus::unlock);
            this.lockedBuffers = null;
         }
      }

      public List<MemoryLockedBuffer> getLockedMemory() {
         return this.lockedBuffers == null?Collections.emptyList():this.lockedBuffers;
      }

      private Throwable close(Throwable accumulate) {
         accumulate = this.channel.close(accumulate);
         return !FileUtils.isCleanerAvailable?accumulate:Throwables.perform(accumulate, this.channel.filePath(), Throwables.FileOpType.READ, Stream.of(this.buffers).map((buffer) -> {
            return () -> {
               if(buffer != null) {
                  FileUtils.clean(buffer);
               }

            };
         }));
      }
   }

   public static final class Region implements Rebufferer.BufferHolder {
      public final long offset;
      public final ByteBuffer buffer;

      public Region(long offset, ByteBuffer buffer) {
         this.offset = offset;
         this.buffer = buffer;
      }

      public ByteBuffer buffer() {
         return this.buffer.duplicate();
      }

      public long offset() {
         return this.offset;
      }

      public long end() {
         return this.offset + (long)this.buffer.capacity();
      }

      public void release() {
      }
   }
}
