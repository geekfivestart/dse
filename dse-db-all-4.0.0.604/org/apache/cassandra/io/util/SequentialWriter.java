package org.apache.cassandra.io.util;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.utils.SyncUtil;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Transactional;

public class SequentialWriter extends BufferedDataOutputStreamPlus implements Transactional {
   private final String filePath;
   protected long bufferOffset;
   protected final FileChannel fchannel;
   private final SequentialWriterOption option;
   private int bytesSinceTrickleFsync;
   protected long lastFlushOffset;
   protected Runnable runOnFileSync;
   private boolean requestSyncOnNextFlush;
   private final SequentialWriter.TransactionalProxy txnProxy;

   private static FileChannel openChannel(File file) {
      try {
         if(file.exists()) {
            return FileChannel.open(file.toPath(), new OpenOption[]{StandardOpenOption.READ, StandardOpenOption.WRITE});
         } else {
            FileChannel channel = FileChannel.open(file.toPath(), new OpenOption[]{StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW});

            try {
               SyncUtil.trySyncDir(file.getParentFile());
            } catch (Throwable var5) {
               try {
                  channel.close();
               } catch (Throwable var4) {
                  var5.addSuppressed(var4);
               }
            }

            return channel;
         }
      } catch (IOException var6) {
         throw new RuntimeException(var6);
      }
   }

   public SequentialWriter(File file) {
      this(file, SequentialWriterOption.DEFAULT);
   }

   public SequentialWriter(File file, SequentialWriterOption option) {
      super(openChannel(file), option.allocateBuffer());
      this.bytesSinceTrickleFsync = 0;
      this.txnProxy = this.txnProxy();
      this.strictFlushing = true;
      this.fchannel = (FileChannel)this.channel;
      this.filePath = file.getAbsolutePath();
      this.option = option;
   }

   public void skipBytes(int numBytes) throws IOException {
      this.flush();
      this.fchannel.position(this.fchannel.position() + (long)numBytes);
      this.bufferOffset = this.fchannel.position();
   }

   public void sync() {
      this.syncInternal();
   }

   private void syncDataOnlyInternal() {
      try {
         SyncUtil.forceAlways(this.fchannel, false);
         if(this.runOnFileSync != null) {
            this.runOnFileSync.run();
         }

      } catch (IOException var2) {
         throw new FSWriteError(var2, this.getPath());
      }
   }

   protected void syncInternal() {
      this.doFlush(true);
   }

   public void requestSyncOnNextFlush() {
      this.requestSyncOnNextFlush = true;
   }

   protected void doFlush(int count) {
      if(this.requestSyncOnNextFlush) {
         this.doFlush(true);
         this.requestSyncOnNextFlush = false;
      } else {
         this.doFlush(false);
      }

   }

   private void doFlush(boolean forceSync) {
      this.flushData();
      boolean synced = false;
      if(this.option.trickleFsync()) {
         this.bytesSinceTrickleFsync += this.buffer.position();
         if(this.bytesSinceTrickleFsync >= this.option.trickleFsyncByteInterval()) {
            this.syncDataOnlyInternal();
            synced = true;
            this.bytesSinceTrickleFsync = 0;
         }
      }

      if(!synced && forceSync) {
         this.syncDataOnlyInternal();
      }

      this.resetBuffer();
   }

   public void setFileSyncListener(Runnable runOnFileSync) {
      assert this.runOnFileSync == null;

      this.runOnFileSync = runOnFileSync;
   }

   protected void flushData() {
      try {
         this.buffer.flip();
         this.channel.write(this.buffer);
         this.lastFlushOffset += (long)this.buffer.position();
      } catch (IOException var2) {
         throw new FSWriteError(var2, this.getPath());
      }
   }

   public boolean hasPosition() {
      return true;
   }

   public long position() {
      return this.current();
   }

   public long getOnDiskFilePointer() {
      return this.position();
   }

   public long getEstimatedOnDiskBytesWritten() {
      return this.getOnDiskFilePointer();
   }

   public long length() {
      try {
         return Math.max(this.current(), this.fchannel.size());
      } catch (IOException var2) {
         throw new FSReadError(var2, this.getPath());
      }
   }

   public String getPath() {
      return this.filePath;
   }

   protected void resetBuffer() {
      this.bufferOffset = this.current();
      this.buffer.clear();
   }

   protected long current() {
      return this.bufferOffset + (long)(this.buffer == null?0:this.buffer.position());
   }

   public DataPosition mark() {
      return new SequentialWriter.BufferedFileWriterMark(this.current());
   }

   public void resetAndTruncate(DataPosition mark) {
      assert mark instanceof SequentialWriter.BufferedFileWriterMark;

      long previous = this.current();
      long truncateTarget = ((SequentialWriter.BufferedFileWriterMark)mark).pointer;
      if(previous - truncateTarget <= (long)this.buffer.position()) {
         this.buffer.position(this.buffer.position() - (int)(previous - truncateTarget));
      } else {
         this.syncInternal();
         this.truncate(truncateTarget);

         try {
            this.fchannel.position(truncateTarget);
         } catch (IOException var7) {
            throw new FSReadError(var7, this.getPath());
         }

         this.bufferOffset = truncateTarget;
         this.resetBuffer();
      }
   }

   public long getLastFlushOffset() {
      return this.lastFlushOffset;
   }

   public void truncate(long toSize) {
      try {
         this.fchannel.truncate(toSize);
         this.lastFlushOffset = toSize;
      } catch (IOException var4) {
         throw new FSWriteError(var4, this.getPath());
      }
   }

   public boolean isOpen() {
      return this.channel.isOpen();
   }

   public final void prepareToCommit() {
      this.txnProxy.prepareToCommit();
   }

   public final Throwable commit(Throwable accumulate) {
      return this.txnProxy.commit(accumulate);
   }

   public final Throwable abort(Throwable accumulate) {
      return this.txnProxy.abort(accumulate);
   }

   public final void close() {
      if(this.option.finishOnClose()) {
         this.txnProxy.finish();
      } else {
         this.txnProxy.close();
      }

   }

   public final void finish() {
      this.txnProxy.finish();
   }

   protected SequentialWriter.TransactionalProxy txnProxy() {
      return new SequentialWriter.TransactionalProxy();
   }

   protected static class BufferedFileWriterMark implements DataPosition {
      final long pointer;

      public BufferedFileWriterMark(long pointer) {
         this.pointer = pointer;
      }
   }

   protected class TransactionalProxy extends Transactional.AbstractTransactional {
      protected TransactionalProxy() {
      }

      protected Throwable doPreCleanup(Throwable accumulate) {
         try {
            SequentialWriter.this.channel.close();
         } catch (Throwable var4) {
            accumulate = Throwables.merge(accumulate, var4);
         }

         if(SequentialWriter.this.buffer != null) {
            try {
               FileUtils.clean(SequentialWriter.this.buffer);
            } catch (Throwable var3) {
               accumulate = Throwables.merge(accumulate, var3);
            }

            SequentialWriter.this.buffer = null;
         }

         return accumulate;
      }

      protected void doPrepare() {
         SequentialWriter.this.syncInternal();
      }

      protected Throwable doCommit(Throwable accumulate) {
         return accumulate;
      }

      protected Throwable doAbort(Throwable accumulate) {
         return accumulate;
      }
   }
}
