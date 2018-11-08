package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;
import java.util.Objects;
import org.apache.cassandra.io.compress.BufferType;

public class SequentialWriterOption {
   public static final SequentialWriterOption DEFAULT = newBuilder().build();
   private final int bufferSize;
   private final BufferType bufferType;
   private final boolean trickleFsync;
   private final int trickleFsyncByteInterval;
   private final boolean finishOnClose;

   private SequentialWriterOption(int bufferSize, BufferType bufferType, boolean trickleFsync, int trickleFsyncByteInterval, boolean finishOnClose) {
      this.bufferSize = bufferSize;
      this.bufferType = bufferType;
      this.trickleFsync = trickleFsync;
      this.trickleFsyncByteInterval = trickleFsyncByteInterval;
      this.finishOnClose = finishOnClose;
   }

   public static SequentialWriterOption.Builder newBuilder() {
      return new SequentialWriterOption.Builder();
   }

   public int bufferSize() {
      return this.bufferSize;
   }

   public BufferType bufferType() {
      return this.bufferType;
   }

   public boolean trickleFsync() {
      return this.trickleFsync;
   }

   public int trickleFsyncByteInterval() {
      return this.trickleFsyncByteInterval;
   }

   public boolean finishOnClose() {
      return this.finishOnClose;
   }

   public ByteBuffer allocateBuffer() {
      return this.bufferType.allocate(this.bufferSize);
   }

   public static class Builder {
      private int bufferSize;
      private BufferType bufferType;
      private boolean trickleFsync;
      private int trickleFsyncByteInterval;
      private boolean finishOnClose;

      private Builder() {
         this.bufferSize = 65536;
         this.bufferType = BufferType.ON_HEAP;
         this.trickleFsync = false;
         this.trickleFsyncByteInterval = 10485760;
         this.finishOnClose = false;
      }

      public SequentialWriterOption build() {
         return new SequentialWriterOption(this.bufferSize, this.bufferType, this.trickleFsync, this.trickleFsyncByteInterval, this.finishOnClose);
      }

      public SequentialWriterOption.Builder bufferSize(int bufferSize) {
         this.bufferSize = bufferSize;
         return this;
      }

      public SequentialWriterOption.Builder bufferType(BufferType bufferType) {
         this.bufferType = (BufferType)Objects.requireNonNull(bufferType);
         return this;
      }

      public SequentialWriterOption.Builder trickleFsync(boolean trickleFsync) {
         this.trickleFsync = trickleFsync;
         return this;
      }

      public SequentialWriterOption.Builder trickleFsyncByteInterval(int trickleFsyncByteInterval) {
         this.trickleFsyncByteInterval = trickleFsyncByteInterval;
         return this;
      }

      public SequentialWriterOption.Builder finishOnClose(boolean finishOnClose) {
         this.finishOnClose = finishOnClose;
         return this;
      }
   }
}
