package org.apache.cassandra.db.mos;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.CompactionStrategyManager;
import org.apache.cassandra.db.compaction.MemoryOnlyStrategy;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NativeLibrary;
import org.apache.cassandra.utils.UnsafeByteBufferAccess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryOnlyStatus implements MemoryOnlyStatusMXBean {
   private static final Logger logger = LoggerFactory.getLogger(MemoryOnlyStatus.class);
   public static final MemoryOnlyStatus instance = new MemoryOnlyStatus();
   private final long maxAvailableBytes;
   private final AtomicLong totalLockedBytes;
   private final AtomicLong totalFailedBytes;

   private MemoryOnlyStatus() {
      this(DatabaseDescriptor.getMaxMemoryToLockBytes());
   }

   @VisibleForTesting
   MemoryOnlyStatus(long maxAvailableBytes) {
      this.totalLockedBytes = new AtomicLong(0L);
      this.totalFailedBytes = new AtomicLong(0L);
      this.maxAvailableBytes = maxAvailableBytes;
      logger.debug("Max available memory to lock in RAM: {} kbytes", Long.valueOf(maxAvailableBytes >> 10));
   }

   public long getMaxAvailableBytes() {
      return this.maxAvailableBytes;
   }

   public MemoryLockedBuffer lock(MappedByteBuffer buffer) {
      long address = UnsafeByteBufferAccess.getAddress(buffer);
      long length = (long)buffer.capacity();
      long lengthRoundedTo4k = FBUtilities.align(length, 4096);
      long newSize = this.totalLockedBytes.addAndGet(lengthRoundedTo4k);
      if(newSize <= this.maxAvailableBytes) {
         logger.debug("Lock buffer address: {} length: {}", Long.valueOf(address), Long.valueOf(length));
         if(NativeLibrary.tryMlock(address, length)) {
            buffer.load();
            return MemoryLockedBuffer.succeeded(address, lengthRoundedTo4k);
         }
      } else {
         logger.warn("Buffer address: {} length: {} could not be locked.  Sizelimit ({}) reached. After locking size would be: {}", new Object[]{Long.valueOf(address), Long.valueOf(length), Long.valueOf(this.maxAvailableBytes), Long.valueOf(newSize)});
      }

      this.totalLockedBytes.addAndGet(-lengthRoundedTo4k);
      this.totalFailedBytes.addAndGet(lengthRoundedTo4k);
      return MemoryLockedBuffer.failed(address, lengthRoundedTo4k);
   }

   public void unlock(MemoryLockedBuffer buffer) {
      if(buffer.succeeded) {
         this.totalLockedBytes.addAndGet(-buffer.amount);
      } else {
         this.totalFailedBytes.addAndGet(-buffer.amount);
      }

   }

   public void reportFailedAttemptedLocking(long sizeBytes) {
      this.totalFailedBytes.addAndGet(sizeBytes);
   }

   public void clearFailedAttemptedLocking(long sizeBytes) {
      this.totalFailedBytes.addAndGet(-sizeBytes);
   }

   private static boolean isMemoryOnly(ColumnFamilyStore cfs) {
      List<List<AbstractCompactionStrategy>> strategies = cfs.getCompactionStrategyManager().getStrategies();
      return strategies.size() > 0 && ((List)strategies.get(0)).size() > 0 && ((List)strategies.get(0)).get(0) instanceof MemoryOnlyStrategy;
   }

   @Nullable
   private MemoryOnlyStatusMXBean.TableInfo maybeCreateTableInfo(ColumnFamilyStore cfs) {
      if(!isMemoryOnly(cfs)) {
         return null;
      } else {
         List<MemoryLockedBuffer> lockedBuffers = new ArrayList(cfs.getLiveSSTables().size());
         CompactionStrategyManager csm = cfs.getCompactionStrategyManager();
         Iterator var4 = csm.getStrategies().iterator();

         label35:
         while(var4.hasNext()) {
            List<AbstractCompactionStrategy> strategyList = (List)var4.next();
            Iterator var6 = strategyList.iterator();

            while(true) {
               AbstractCompactionStrategy strategy;
               do {
                  if(!var6.hasNext()) {
                     continue label35;
                  }

                  strategy = (AbstractCompactionStrategy)var6.next();
               } while(!(strategy instanceof MemoryOnlyStrategy));

               Iterator var8 = ((MemoryOnlyStrategy)strategy).getSSTables().iterator();

               while(var8.hasNext()) {
                  SSTableReader ssTableReader = (SSTableReader)var8.next();
                  Iterables.addAll(lockedBuffers, ssTableReader.getLockedMemory());
               }
            }
         }

         return createTableInfo(cfs.keyspace.getName(), cfs.getTableName(), lockedBuffers, this.maxAvailableBytes);
      }
   }

   public List<MemoryOnlyStatusMXBean.TableInfo> getMemoryOnlyTableInformation() {
      List<MemoryOnlyStatusMXBean.TableInfo> tableInfos = new ArrayList();
      Iterator var2 = ColumnFamilyStore.all().iterator();

      while(var2.hasNext()) {
         ColumnFamilyStore cfs = (ColumnFamilyStore)var2.next();
         MemoryOnlyStatusMXBean.TableInfo ti = this.maybeCreateTableInfo(cfs);
         if(ti != null) {
            tableInfos.add(ti);
         }
      }

      return tableInfos;
   }

   public MemoryOnlyStatusMXBean.TableInfo getMemoryOnlyTableInformation(String ks, String cf) {
      Keyspace keyspace = Schema.instance.getKeyspaceInstance(ks);
      if(keyspace == null) {
         throw new IllegalArgumentException(String.format("Keyspace %s does not exist.", new Object[]{ks}));
      } else {
         ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cf);
         MemoryOnlyStatusMXBean.TableInfo ti = this.maybeCreateTableInfo(cfs);
         if(ti == null) {
            throw new IllegalArgumentException(String.format("Keyspace %s Table %s is not using MemoryOnlyStrategy.", new Object[]{ks, cf}));
         } else {
            return ti;
         }
      }
   }

   public MemoryOnlyStatusMXBean.TotalInfo getMemoryOnlyTotals() {
      return new MemoryOnlyStatusMXBean.TotalInfo(this.totalLockedBytes.get(), this.totalFailedBytes.get(), this.maxAvailableBytes);
   }

   public double getMemoryOnlyPercentUsed() {
      return this.maxAvailableBytes > 0L?(double)this.totalLockedBytes.get() / (double)this.maxAvailableBytes:0.0D;
   }

   private static MemoryOnlyStatusMXBean.TableInfo createTableInfo(String ks, String cf, List<MemoryLockedBuffer> buffers, long maxMemoryToLock) {
      return new MemoryOnlyStatusMXBean.TableInfo(ks, cf, ((Long)buffers.stream().map(MemoryLockedBuffer::locked).reduce(Long.valueOf(0L), Long::sum)).longValue(), ((Long)buffers.stream().map(MemoryLockedBuffer::notLocked).reduce(Long.valueOf(0L), Long::sum)).longValue(), maxMemoryToLock);
   }
}
