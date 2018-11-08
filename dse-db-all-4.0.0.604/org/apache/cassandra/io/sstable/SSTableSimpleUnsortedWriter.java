package org.apache.cassandra.io.sstable;

import com.google.common.base.Throwables;
import io.netty.util.concurrent.FastThreadLocalThread;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredSerializer;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.JVMStabilityInspector;

class SSTableSimpleUnsortedWriter extends AbstractSSTableSimpleWriter {
   private static final SSTableSimpleUnsortedWriter.Buffer SENTINEL = new SSTableSimpleUnsortedWriter.Buffer();
   private SSTableSimpleUnsortedWriter.Buffer buffer = new SSTableSimpleUnsortedWriter.Buffer();
   private final long bufferSize;
   private long currentSize;
   private final SerializationHeader header;
   private final BlockingQueue<SSTableSimpleUnsortedWriter.Buffer> writeQueue = new SynchronousQueue();
   private final SSTableSimpleUnsortedWriter.DiskWriter diskWriter = new SSTableSimpleUnsortedWriter.DiskWriter(null);

   SSTableSimpleUnsortedWriter(File directory, TableMetadataRef metadata, RegularAndStaticColumns columns, long bufferSizeInMB) {
      super(directory, metadata, columns);
      this.bufferSize = bufferSizeInMB * 1024L * 1024L;
      this.header = new SerializationHeader(true, metadata.get(), columns, EncodingStats.NO_STATS);
      this.diskWriter.start();
   }

   PartitionUpdate getUpdateFor(DecoratedKey key) {
      assert key != null;

      PartitionUpdate previous = (PartitionUpdate)this.buffer.get(key);
      if(previous == null) {
         previous = this.createPartitionUpdate(key);
         this.currentSize += ((PartitionUpdate.PartitionUpdateSerializer)PartitionUpdate.serializers.get(this.formatType.info.getLatestVersion().encodingVersion())).serializedSize(previous);
         previous.allowNewUpdates();
         this.buffer.put(key, previous);
      }

      return previous;
   }

   private void countRow(Row row) {
      this.currentSize += ((UnfilteredSerializer)UnfilteredSerializer.serializers.get(this.formatType.info.getLatestVersion().encodingVersion())).serializedSize((Unfiltered)row, this.header, 0L);
   }

   private void maybeSync() throws SSTableSimpleUnsortedWriter.SyncException {
      try {
         if(this.currentSize > this.bufferSize) {
            this.sync();
         }

      } catch (IOException var2) {
         throw new SSTableSimpleUnsortedWriter.SyncException(var2);
      }
   }

   private PartitionUpdate createPartitionUpdate(DecoratedKey key) {
      return new PartitionUpdate(this.metadata.get(), key, this.columns, 4) {
         public void add(Row row) {
            super.add(row);
            SSTableSimpleUnsortedWriter.this.countRow(row);
            SSTableSimpleUnsortedWriter.this.maybeSync();
         }
      };
   }

   public void close() throws IOException {
      this.sync();
      this.put(SENTINEL);

      try {
         this.diskWriter.join();
         this.checkForWriterException();
      } catch (Throwable var2) {
         throw new RuntimeException(var2);
      }

      this.checkForWriterException();
   }

   protected void sync() throws IOException {
      if(!this.buffer.isEmpty()) {
         this.put(this.buffer);
         this.buffer = new SSTableSimpleUnsortedWriter.Buffer();
         this.currentSize = 0L;
      }
   }

   private void put(SSTableSimpleUnsortedWriter.Buffer buffer) throws IOException {
      while(true) {
         this.checkForWriterException();

         try {
            if(this.writeQueue.offer(buffer, 1L, TimeUnit.SECONDS)) {
               return;
            }
         } catch (InterruptedException var3) {
            throw new RuntimeException(var3);
         }
      }
   }

   private void checkForWriterException() throws IOException {
      if(this.diskWriter.exception != null) {
         if(this.diskWriter.exception instanceof IOException) {
            throw (IOException)this.diskWriter.exception;
         } else {
            throw Throwables.propagate(this.diskWriter.exception);
         }
      }
   }

   private class DiskWriter extends FastThreadLocalThread {
      volatile Throwable exception;

      private DiskWriter() {
         this.exception = null;
      }

      public void run() {
         while(true) {
            try {
               SSTableSimpleUnsortedWriter.Buffer b = (SSTableSimpleUnsortedWriter.Buffer)SSTableSimpleUnsortedWriter.this.writeQueue.take();
               if(b == SSTableSimpleUnsortedWriter.SENTINEL) {
                  return;
               }

               SSTableTxnWriter writer = SSTableSimpleUnsortedWriter.this.createWriter();
               Throwable var3 = null;

               try {
                  Iterator var4 = b.entrySet().iterator();

                  while(var4.hasNext()) {
                     Entry<DecoratedKey, PartitionUpdate> entry = (Entry)var4.next();
                     writer.append(((PartitionUpdate)entry.getValue()).unfilteredIterator());
                  }

                  writer.finish(false);
               } catch (Throwable var14) {
                  var3 = var14;
                  throw var14;
               } finally {
                  if(writer != null) {
                     if(var3 != null) {
                        try {
                           writer.close();
                        } catch (Throwable var13) {
                           var3.addSuppressed(var13);
                        }
                     } else {
                        writer.close();
                     }
                  }

               }
            } catch (Throwable var16) {
               JVMStabilityInspector.inspectThrowable(var16);
               if(this.exception == null) {
                  this.exception = var16;
               }
            }
         }
      }
   }

   static class Buffer extends TreeMap<DecoratedKey, PartitionUpdate> {
      Buffer() {
      }
   }

   static class SyncException extends RuntimeException {
      SyncException(IOException ioe) {
         super(ioe);
      }
   }
}
