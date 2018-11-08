package org.apache.cassandra.streaming;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.streaming.messages.OutgoingFileMessage;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Ref;

public class StreamTransferTask extends StreamTask {
   private static final ScheduledExecutorService timeoutExecutor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("StreamingTransferTaskTimeouts"));
   private final AtomicInteger sequenceNumber = new AtomicInteger(0);
   private boolean aborted = false;
   @VisibleForTesting
   protected final Map<Integer, OutgoingFileMessage> files = new HashMap();
   private final Map<Integer, ScheduledFuture> timeoutTasks = new HashMap();
   private long totalSize;

   public StreamTransferTask(StreamSession session, TableId tableId) {
      super(session, tableId);
   }

   public synchronized void addTransferFile(Ref<SSTableReader> ref, long estimatedKeys, List<Pair<Long, Long>> sections) {
      assert ref.get() != null && this.tableId.equals(((SSTableReader)ref.get()).metadata().id);

      OutgoingFileMessage message = new OutgoingFileMessage(ref, this.sequenceNumber.getAndIncrement(), estimatedKeys, sections, this.session.keepSSTableLevel());
      message = StreamHook.instance.reportOutgoingFile(this.session, (SSTableReader)ref.get(), message);
      this.files.put(Integer.valueOf(message.header.sequenceNumber), message);
      this.totalSize += message.header.size();
   }

   public void complete(int sequenceNumber) {
      boolean signalComplete;
      synchronized(this) {
         ScheduledFuture timeout = (ScheduledFuture)this.timeoutTasks.remove(Integer.valueOf(sequenceNumber));
         if(timeout != null) {
            timeout.cancel(false);
         }

         OutgoingFileMessage file = (OutgoingFileMessage)this.files.remove(Integer.valueOf(sequenceNumber));
         if(file != null) {
            file.complete();
         }

         signalComplete = this.files.isEmpty();
      }

      if(signalComplete) {
         this.session.taskCompleted(this);
      }

   }

   public synchronized void abort() {
      if(!this.aborted) {
         this.aborted = true;
         Iterator var1 = this.timeoutTasks.values().iterator();

         while(var1.hasNext()) {
            ScheduledFuture future = (ScheduledFuture)var1.next();
            future.cancel(false);
         }

         this.timeoutTasks.clear();
         Throwable fail = null;
         Iterator var7 = this.files.values().iterator();

         while(var7.hasNext()) {
            OutgoingFileMessage file = (OutgoingFileMessage)var7.next();

            try {
               file.complete();
            } catch (Throwable var5) {
               if(fail == null) {
                  fail = var5;
               } else {
                  fail.addSuppressed(var5);
               }
            }
         }

         this.files.clear();
         if(fail != null) {
            throw Throwables.propagate(fail);
         }
      }
   }

   public synchronized int getTotalNumberOfFiles() {
      return this.files.size();
   }

   public long getTotalSize() {
      return this.totalSize;
   }

   public synchronized Collection<OutgoingFileMessage> getFileMessages() {
      return new ArrayList(this.files.values());
   }

   public synchronized OutgoingFileMessage createMessageForRetry(int sequenceNumber) {
      ScheduledFuture future = (ScheduledFuture)this.timeoutTasks.remove(Integer.valueOf(sequenceNumber));
      if(future != null) {
         future.cancel(false);
      }

      return (OutgoingFileMessage)this.files.get(Integer.valueOf(sequenceNumber));
   }

   public synchronized ScheduledFuture scheduleTimeout(final int sequenceNumber, long time, TimeUnit unit) {
      if(!this.files.containsKey(Integer.valueOf(sequenceNumber))) {
         return null;
      } else {
         ScheduledFuture future = timeoutExecutor.schedule(new Runnable() {
            public void run() {
               StreamTransferTask var1 = StreamTransferTask.this;
               synchronized(StreamTransferTask.this) {
                  StreamTransferTask.this.timeoutTasks.remove(Integer.valueOf(sequenceNumber));
                  StreamTransferTask.this.complete(sequenceNumber);
               }
            }
         }, time, unit);
         ScheduledFuture prev = (ScheduledFuture)this.timeoutTasks.put(Integer.valueOf(sequenceNumber), future);

         assert prev == null;

         return future;
      }
   }
}
