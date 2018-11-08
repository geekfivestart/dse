package org.apache.cassandra.index.sasi;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Uninterruptibles;
import io.netty.util.concurrent.FastThreadLocal;
import java.io.Closeable;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.index.sasi.disk.OnDiskIndexBuilder;
import org.apache.cassandra.index.sasi.disk.Token;
import org.apache.cassandra.index.sasi.plan.Expression;
import org.apache.cassandra.index.sasi.utils.RangeIterator;
import org.apache.cassandra.index.sasi.utils.RangeUnionIterator;
import org.apache.cassandra.io.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TermIterator extends RangeIterator<Long, Token> {
   private static final Logger logger = LoggerFactory.getLogger(TermIterator.class);
   private static final FastThreadLocal<ExecutorService> SEARCH_EXECUTOR = new FastThreadLocal<ExecutorService>() {
      public ExecutorService initialValue() {
         final String currentThread = Thread.currentThread().getName();
         int concurrencyFactor = DatabaseDescriptor.searchConcurrencyFactor();
         TermIterator.logger.info("Search Concurrency Factor is set to {} for {}", Integer.valueOf(concurrencyFactor), currentThread);
         return (ExecutorService)(concurrencyFactor <= 1?MoreExecutors.newDirectExecutorService():Executors.newFixedThreadPool(concurrencyFactor, new ThreadFactory() {
            public final AtomicInteger count = new AtomicInteger();

            public Thread newThread(Runnable task) {
               return NamedThreadFactory.createThread(task, currentThread + "-SEARCH-" + this.count.incrementAndGet(), true);
            }
         }));
      }
   };
   private final Expression expression;
   private final RangeIterator<Long, Token> union;
   private final Set<SSTableIndex> referencedIndexes;

   private TermIterator(Expression e, RangeIterator<Long, Token> union, Set<SSTableIndex> referencedIndexes) {
      super(union.getMinimum(), union.getMaximum(), union.getCount());
      this.expression = e;
      this.union = union;
      this.referencedIndexes = referencedIndexes;
   }

   public static TermIterator build(Expression e, Set<SSTableIndex> perSSTableIndexes) {
      List<RangeIterator<Long, Token>> tokens = new CopyOnWriteArrayList();
      AtomicLong tokenCount = new AtomicLong(0L);
      RangeIterator<Long, Token> memtableIterator = e.index.searchMemtable(e);
      if(memtableIterator != null) {
         tokens.add(memtableIterator);
         tokenCount.addAndGet(memtableIterator.getCount());
      }

      CopyOnWriteArraySet referencedIndexes = new CopyOnWriteArraySet();

      try {
         CountDownLatch latch = new CountDownLatch(perSSTableIndexes.size());
         ExecutorService searchExecutor = (ExecutorService)SEARCH_EXECUTOR.get();
         Iterator var8 = perSSTableIndexes.iterator();

         while(var8.hasNext()) {
            SSTableIndex index = (SSTableIndex)var8.next();
            if(e.getOp() == Expression.Op.PREFIX && index.mode() == OnDiskIndexBuilder.Mode.CONTAINS && !index.hasMarkedPartials()) {
               throw new UnsupportedOperationException(String.format("The index %s has not yet been upgraded to support prefix queries in CONTAINS mode. Wait for compaction or rebuild the index.", new Object[]{index.getPath()}));
            }

            if(!index.reference()) {
               latch.countDown();
            } else {
               referencedIndexes.add(index);
               searchExecutor.submit(() -> {
                  try {
                     e.checkpoint();
                     RangeIterator<Long, Token> keyIterator = index.search(e);
                     if(keyIterator == null) {
                        releaseIndex(referencedIndexes, index);
                        return;
                     }

                     tokens.add(keyIterator);
                     tokenCount.getAndAdd(keyIterator.getCount());
                  } catch (Throwable var10) {
                     releaseIndex(referencedIndexes, index);
                     if(logger.isDebugEnabled()) {
                        logger.debug(String.format("Failed search an index %s, skipping.", new Object[]{index.getPath()}), var10);
                     }
                  } finally {
                     latch.countDown();
                  }

               });
            }
         }

         Uninterruptibles.awaitUninterruptibly(latch);
         e.checkpoint();
         RangeIterator<Long, Token> ranges = RangeUnionIterator.build(tokens);
         return new TermIterator(e, ranges, referencedIndexes);
      } catch (Throwable var10) {
         referencedIndexes.forEach(TermIterator::releaseQuietly);
         throw var10;
      }
   }

   protected Token computeNext() {
      Token var1;
      try {
         var1 = this.union.hasNext()?(Token)this.union.next():(Token)this.endOfData();
      } finally {
         this.expression.checkpoint();
      }

      return var1;
   }

   protected void performSkipTo(Long nextToken) {
      try {
         this.union.skipTo(nextToken);
      } finally {
         this.expression.checkpoint();
      }

   }

   public void close() {
      FileUtils.closeQuietly((Closeable)this.union);
      this.referencedIndexes.forEach(TermIterator::releaseQuietly);
      this.referencedIndexes.clear();
   }

   private static void releaseIndex(Set<SSTableIndex> indexes, SSTableIndex index) {
      indexes.remove(index);
      releaseQuietly(index);
   }

   private static void releaseQuietly(SSTableIndex index) {
      try {
         index.release();
      } catch (Throwable var2) {
         logger.error(String.format("Failed to release index %s", new Object[]{index.getPath()}), var2);
      }

   }
}
