package org.apache.cassandra.hints;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.FSWriteError;

final class HintsWriteExecutor {
   static final int WRITE_BUFFER_SIZE = 262144;
   private final HintsCatalog catalog;
   private final ByteBuffer writeBuffer;
   private final ExecutorService executor;

   HintsWriteExecutor(HintsCatalog catalog) {
      this.catalog = catalog;
      this.writeBuffer = ByteBuffer.allocateDirect(262144);
      this.executor = DebuggableThreadPoolExecutor.createWithFixedPoolSize((String)"HintsWriteExecutor", 1);
   }

   void shutdownBlocking() {
      this.executor.shutdown();

      try {
         this.executor.awaitTermination(1L, TimeUnit.MINUTES);
      } catch (InterruptedException var2) {
         throw new AssertionError(var2);
      }
   }

   Future<?> flushBuffer(HintsBuffer buffer, HintsBufferPool bufferPool) {
      return this.executor.submit(new HintsWriteExecutor.FlushBufferTask(buffer, bufferPool));
   }

   Future<?> flushBufferPool(HintsBufferPool bufferPool) {
      return this.executor.submit(new HintsWriteExecutor.FlushBufferPoolTask(bufferPool));
   }

   Future<?> flushBufferPool(HintsBufferPool bufferPool, Iterable<HintsStore> stores) {
      return this.executor.submit(new HintsWriteExecutor.PartiallyFlushBufferPoolTask(bufferPool, stores));
   }

   void fsyncWritersBlockingly(Iterable<HintsStore> stores) {
      try {
         this.executor.submit(new HintsWriteExecutor.FsyncWritersTask(stores)).get();
      } catch (ExecutionException | InterruptedException var3) {
         throw new RuntimeException(var3);
      }
   }

   Future<?> closeWriter(HintsStore store) {
      ExecutorService var10000 = this.executor;
      store.getClass();
      return var10000.submit(store::closeWriter);
   }

   Future<?> closeAllWriters() {
      return this.executor.submit(() -> {
         this.catalog.stores().forEach(HintsStore::closeWriter);
      });
   }

   private void flush(HintsBuffer buffer) {
      buffer.hostIds().forEach((hostId) -> {
         this.flush(buffer.consumingHintsIterator(hostId), this.catalog.get(hostId));
      });
   }

   private void flush(Iterator<ByteBuffer> iterator, HintsStore store) {
      while(true) {
         if(iterator.hasNext()) {
            this.flushInternal(iterator, store);
         }

         if(!iterator.hasNext()) {
            return;
         }

         store.closeWriter();
      }
   }

   private void flushInternal(Iterator<ByteBuffer> iterator, HintsStore store) {
      long maxHintsFileSize = DatabaseDescriptor.getMaxHintsFileSize();
      HintsWriter writer = store.getOrOpenWriter();

      try {
         HintsWriter.Session session = writer.newSession(this.writeBuffer);
         Throwable var7 = null;

         try {
            while(iterator.hasNext()) {
               session.append((ByteBuffer)iterator.next());
               if(session.position() >= maxHintsFileSize) {
                  break;
               }
            }
         } catch (Throwable var17) {
            var7 = var17;
            throw var17;
         } finally {
            if(session != null) {
               if(var7 != null) {
                  try {
                     session.close();
                  } catch (Throwable var16) {
                     var7.addSuppressed(var16);
                  }
               } else {
                  session.close();
               }
            }

         }

      } catch (IOException var19) {
         throw new FSWriteError(var19, writer.descriptor().fileName());
      }
   }

   private final class FsyncWritersTask implements Runnable {
      private final Iterable<HintsStore> stores;

      FsyncWritersTask(Iterable<HintsStore> var1) {
         this.stores = stores;
      }

      public void run() {
         this.stores.forEach(HintsStore::fsyncWriter);
         HintsWriteExecutor.this.catalog.fsyncDirectory();
      }
   }

   private final class PartiallyFlushBufferPoolTask implements Runnable {
      private final HintsBufferPool bufferPool;
      private final Iterable<HintsStore> stores;

      PartiallyFlushBufferPoolTask(HintsBufferPool var1, Iterable<HintsStore> bufferPool) {
         this.bufferPool = bufferPool;
         this.stores = stores;
      }

      public void run() {
         HintsBuffer buffer = this.bufferPool.currentBuffer();
         buffer.waitForModifications();
         this.stores.forEach((store) -> {
            HintsWriteExecutor.this.flush(buffer.consumingHintsIterator(store.hostId), store);
         });
      }
   }

   private final class FlushBufferPoolTask implements Runnable {
      private final HintsBufferPool bufferPool;

      FlushBufferPoolTask(HintsBufferPool bufferPool) {
         this.bufferPool = bufferPool;
      }

      public void run() {
         HintsBuffer buffer = this.bufferPool.currentBuffer();
         buffer.waitForModifications();
         HintsWriteExecutor.this.flush(buffer);
      }
   }

   private final class FlushBufferTask implements Runnable {
      private final HintsBuffer buffer;
      private final HintsBufferPool bufferPool;

      FlushBufferTask(HintsBuffer buffer, HintsBufferPool bufferPool) {
         this.buffer = buffer;
         this.bufferPool = bufferPool;
      }

      public void run() {
         this.buffer.waitForModifications();
         boolean var5 = false;

         try {
            var5 = true;
            HintsWriteExecutor.this.flush(this.buffer);
            var5 = false;
         } finally {
            if(var5) {
               HintsBuffer recycledBuffer = this.buffer.recycle();
               this.bufferPool.offer(recycledBuffer);
            }
         }

         HintsBuffer recycledBufferx = this.buffer.recycle();
         this.bufferPool.offer(recycledBufferx);
      }
   }
}
