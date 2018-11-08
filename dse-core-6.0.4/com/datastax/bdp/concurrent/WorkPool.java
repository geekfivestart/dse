package com.datastax.bdp.concurrent;

import com.codahale.metrics.Gauge;
import com.datastax.bdp.concurrent.metrics.HdrSlidingTimeStats;
import com.datastax.bdp.concurrent.metrics.SlidingTimeRate;
import com.datastax.bdp.concurrent.metrics.SlidingTimeStats;
import com.datastax.bdp.jmx.JMX;
import com.datastax.bdp.system.TimeSource;
import com.datastax.bdp.util.MapBuilder;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.LongStream;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.CassandraMetricsRegistry.MetricName;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkPool implements WorkPoolMXBean {
   public static final int SLEEP_UNIT_NANOS = 1000000;
   private static final String INDEX_POOL_INCOMING_RATE_GAUGE_NAME = "Incoming Rate";
   private static final String INDEX_POOL_QUEUE_SIZE_GAUGE_NAME = "Queue Size";
   private static final String INDEX_POOL_TASK_PROCESSING_TIME_NANOS_GAUGE_NAME = "Task Processing Time Nanos";
   private static final String INDEX_POOL_PROCESSED_TASKS_GAUGE_NAME = "Processed Tasks";
   private static final String INDEX_POOL_BACKPRESSURE_PAUSE_NANOS_GAUGE_NAME = "Backpressure Pause Nanos";
   private static final String INDEX_POOL_OUTGOING_RATE_GAUGE_NAME = "Outgoing Rate";
   private static final String INDEX_POOL_THROUGHPUT_GAUGE_NAME = "Throughput";
   private static final String INDEX_POOL_METRIC_TYPE_NAME = "IndexPool";
   private static final HashFunction hashing = Hashing.sipHash24();
   private static final Logger logger = LoggerFactory.getLogger(WorkPool.class);
   private final ExecutorService executor;
   private final ScheduledExecutorService scheduler;
   private final TimeSource timeSource;
   private final BlockingQueue<Task>[] queues;
   private final Worker[] workers;
   private final ReadWriteLock workLock;
   private final List<WorkPoolListener> listeners;
   private final String poolName;
   private volatile String flushErrorMessage = "";
   private volatile int flushMaxTimeMillis;
   private volatile int concurrency;
   private boolean scaling;
   private CountDownLatch scalingLatch;
   private long flushEpoch = 0L;
   private final Lock flushLock;
   private final Queue<Task> flushQueue;
   private final SlidingTimeRate incomingRate;
   private final SlidingTimeRate outgoingRate;
   private final SlidingTimeStats averageBackPressurePause;
   private final SlidingTimeRate throughput;
   private volatile int backPressureThreshold;
   private volatile boolean shutdown;

   public WorkPool(TimeSource timeSource, int maxConcurrency, int backPressureThreshold, int flushMaxTimeMillis, String poolName) {
      this.poolName = poolName;
      this.executor = Executors.newFixedThreadPool(maxConcurrency, (new ThreadFactoryBuilder()).setDaemon(true).setNameFormat(poolName + " WorkPool work thread-%d").build());
      this.scheduler = Executors.newScheduledThreadPool(1, (new ThreadFactoryBuilder()).setDaemon(true).setNameFormat(poolName + " WorkPool scheduler thread-%d").build());
      this.timeSource = timeSource;
      this.queues = new BlockingQueue[maxConcurrency];
      this.workers = new Worker[maxConcurrency];

      for(int i = 0; i < maxConcurrency; ++i) {
         this.queues[i] = new LinkedBlockingQueue();
         this.workers[i] = new Worker(timeSource, this.queues[i], new WorkPool.CompletionListener());
         this.executor.submit(this.workers[i]);
      }

      this.workLock = new ReentrantReadWriteLock();
      this.flushLock = new ReentrantLock();
      this.flushQueue = new ConcurrentLinkedQueue();
      this.concurrency = maxConcurrency;
      this.listeners = new LinkedList();
      this.backPressureThreshold = backPressureThreshold;
      this.flushMaxTimeMillis = flushMaxTimeMillis;
      this.incomingRate = new SlidingTimeRate(60, 10, TimeUnit.SECONDS);
      this.outgoingRate = new SlidingTimeRate(60, 10, TimeUnit.SECONDS);
      this.averageBackPressurePause = new HdrSlidingTimeStats(timeSource, 10000L, 60000L, 9223372036854775807L, TimeUnit.MILLISECONDS, 3);
      this.throughput = new SlidingTimeRate(60, 10, TimeUnit.SECONDS);
      this.scheduler.scheduleAtFixedRate(new WorkPool.BackPressureTask(), 1L, 1L, TimeUnit.SECONDS);
   }

   public void addListener(WorkPoolListener listener) {
      this.listeners.add(listener);
   }

   public void submit(RoutableTask task) {
      Worker.Context workerContext = Worker.getCurrentWorkerContext();

      try {
         if(!workerContext.isWorkerThread) {
            this.workLock.readLock().lock();
            if(this.shutdown) {
               throw new IllegalStateException(String.format("Work pool %s has been shutdown!", new Object[]{this.poolName}));
            }

            if(this.scaling) {
               this.scalingLatch.await();
            }

            task.setEpoch(this.flushEpoch);
         } else {
            task.setEpoch(workerContext.currentTaskEpoch);
         }

         int worker = 0;
         if(this.concurrency > 1) {
            int key = hashing.hashUnencodedChars(task.getKey()).asInt() & 2147483647;
            worker = key % this.concurrency;
         }

         this.doSubmit(workerContext, task, worker);
      } catch (InterruptedException var8) {
         Thread.currentThread().interrupt();
         throw new RuntimeException(var8);
      } finally {
         if(!workerContext.isWorkerThread) {
            this.workLock.readLock().unlock();
         }

      }

   }

   public void setBackPressureThreshold(int threshold) {
      this.backPressureThreshold = threshold;
   }

   public int getBackPressureThreshold() {
      return this.backPressureThreshold;
   }

   public void setFlushMaxTime(int flushMaxTime) {
      this.flushMaxTimeMillis = flushMaxTime;
   }

   public int getFlushMaxTime() {
      return this.flushMaxTimeMillis;
   }

   public void setConcurrency(int concurrency) throws InterruptedException, TimeoutException {
      if(concurrency > 1 && concurrency <= this.queues.length) {
         if(Worker.getCurrentWorkerContext().isWorkerThread) {
            throw new IllegalStateException("Cannot set concurrency from a running worker thread!");
         } else {
            this.flushLock.lock();

            try {
               this.workLock.writeLock().lock();

               try {
                  this.scaling = true;
                  this.scalingLatch = new CountDownLatch(1);
               } finally {
                  this.workLock.writeLock().unlock();
               }

               if(this.shutdown) {
                  throw new IllegalStateException(String.format("Work pool %s has been shutdown!", new Object[]{this.poolName}));
               }

               this.doFlush(false, true);
            } catch (Throwable var27) {
               concurrency = this.concurrency;
               throw var27;
            } finally {
               this.workLock.writeLock().lock();

               try {
                  this.concurrency = concurrency;
                  this.scaling = false;
                  this.scalingLatch.countDown();
               } finally {
                  this.workLock.writeLock().unlock();
               }

               this.flushLock.unlock();
            }

         }
      } else {
         throw new IllegalArgumentException("Concurrency must be higher than 1 and less than or equal to: " + this.queues.length);
      }
   }

   public int getConcurrency() {
      return this.concurrency;
   }

   public int getMaxConcurrency() {
      return this.queues.length;
   }

   public long[] getQueueSize() {
      long[] result = new long[this.queues.length];

      for(int i = 0; i < this.queues.length; ++i) {
         result[i] = (long)this.queues[i].size();
      }

      return result;
   }

   public double getQueueSizeStdDev() {
      long[] longSizes = this.getQueueSize();
      double[] doubleSizes = new double[longSizes.length];

      for(int i = 0; i < longSizes.length; ++i) {
         doubleSizes[i] = (double)longSizes[i];
      }

      return (new StandardDeviation()).evaluate(doubleSizes);
   }

   public long getTotalQueueSize() {
      return this.computeTotalQueueSize();
   }

   public long[] getTaskProcessingTimeNanos() {
      long[] result = new long[this.workers.length];

      for(int i = 0; i < this.workers.length; ++i) {
         result[i] = this.workers[i].getTaskProcessingTimeNanos();
      }

      return result;
   }

   public long[] getProcessedTasks() {
      long[] result = new long[this.workers.length];

      for(int i = 0; i < this.workers.length; ++i) {
         result[i] = this.workers[i].getProcessedTasks();
      }

      return result;
   }

   public double getBackPressurePauseNanos() {
      return (double)TimeUnit.NANOSECONDS.convert((long)this.averageBackPressurePause.computeAverage(), TimeUnit.MICROSECONDS);
   }

   public double getIncomingRate() {
      return this.incomingRate.get(TimeUnit.SECONDS);
   }

   public double getOutgoingRate() {
      return this.outgoingRate.get(TimeUnit.SECONDS);
   }

   public long getThroughput() {
      return (long)this.throughput.get(TimeUnit.SECONDS);
   }

   public void setFlushErrorMessage(String flushErrorMessage) {
      this.flushErrorMessage = flushErrorMessage;
   }

   public void flush(boolean allowTimeout) throws InterruptedException, TimeoutException {
      if(Worker.getCurrentWorkerContext().isWorkerThread) {
         throw new IllegalStateException("Cannot flush from a running worker thread!");
      } else {
         this.flushLock.lock();

         try {
            if(!this.shutdown) {
               this.doFlush(false, allowTimeout);
            } else {
               logger.info("Work pool {} has been shutdown!", this.poolName);
            }
         } finally {
            this.flushLock.unlock();
         }

      }
   }

   public void shutdown() throws InterruptedException, TimeoutException {
      if(Worker.getCurrentWorkerContext().isWorkerThread) {
         throw new IllegalStateException("Cannot shutdown from a running worker thread!");
      } else {
         this.flushLock.lock();

         try {
            if(!this.shutdown) {
               this.shutdown = true;
               this.doFlush(false, true);
               this.doFlush(true, true);
            } else {
               logger.info("Work pool {} has been shutdown!", this.poolName);
            }
         } finally {
            this.executor.shutdown();
            this.scheduler.shutdown();
            this.flushLock.unlock();
         }

      }
   }

   public void addToCassandraMetricsRegistry(String index) {
      CassandraMetricsRegistry.Metrics.register(this.buildCMRName("Queue Size", "IndexPool", index), () -> {
         return Long.valueOf(this.getTotalQueueSize());
      });
      CassandraMetricsRegistry.Metrics.register(this.buildCMRName("Task Processing Time Nanos", "IndexPool", index), () -> {
         return Long.valueOf(LongStream.of(this.getTaskProcessingTimeNanos()).sum());
      });
      CassandraMetricsRegistry.Metrics.register(this.buildCMRName("Processed Tasks", "IndexPool", index), () -> {
         return Long.valueOf(LongStream.of(this.getProcessedTasks()).sum());
      });
      CassandraMetricsRegistry.Metrics.register(this.buildCMRName("Backpressure Pause Nanos", "IndexPool", index), () -> {
         return Double.valueOf(this.getBackPressurePauseNanos());
      });
      CassandraMetricsRegistry.Metrics.register(this.buildCMRName("Incoming Rate", "IndexPool", index), () -> {
         return Double.valueOf(this.getIncomingRate());
      });
      CassandraMetricsRegistry.Metrics.register(this.buildCMRName("Outgoing Rate", "IndexPool", index), () -> {
         return Double.valueOf(this.getOutgoingRate());
      });
      CassandraMetricsRegistry.Metrics.register(this.buildCMRName("Throughput", "IndexPool", index), () -> {
         return Long.valueOf(this.getThroughput());
      });
   }

   public void removeFromCassandraMetricsRegistry(String index) {
      CassandraMetricsRegistry.Metrics.remove(this.buildCMRName("Queue Size", "IndexPool", index));
      CassandraMetricsRegistry.Metrics.remove(this.buildCMRName("Task Processing Time Nanos", "IndexPool", index));
      CassandraMetricsRegistry.Metrics.remove(this.buildCMRName("Processed Tasks", "IndexPool", index));
      CassandraMetricsRegistry.Metrics.remove(this.buildCMRName("Backpressure Pause Nanos", "IndexPool", index));
      CassandraMetricsRegistry.Metrics.remove(this.buildCMRName("Incoming Rate", "IndexPool", index));
      CassandraMetricsRegistry.Metrics.remove(this.buildCMRName("Outgoing Rate", "IndexPool", index));
      CassandraMetricsRegistry.Metrics.remove(this.buildCMRName("Throughput", "IndexPool", index));
   }

   private void doSubmit(Worker.Context workerContext, RoutableTask task, int index) {
      BlockingQueue queue = this.queues[index];

      try {
         if(this.computeTotalQueueSize() > (long)this.backPressureThreshold && !workerContext.isWorkerThread) {
            long pause = 9223372036854775807L;
            long remaining = 9223372036854775807L;
            long unit = 1000000L;

            try {
               do {
                  this.timeSource.sleepUninterruptibly(Math.min(remaining, unit), TimeUnit.NANOSECONDS);
                  remaining -= unit;
               } while(remaining > 0L && this.computeTotalQueueSize() >= (long)this.backPressureThreshold);
            } finally {
               this.averageBackPressurePause.update(pause - Math.max(0L, remaining), TimeUnit.NANOSECONDS);
            }
         }
      } finally {
         this.incomingRate.update(1);
         queue.offer(task);
         if(task.getEpoch() < this.flushEpoch) {
            this.flushQueue.add(task);
         }

      }

   }

   private void doFlush(boolean shutdown, boolean allowTimeout) throws InterruptedException, TimeoutException {
      long currentTimeout = (long)this.flushMaxTimeMillis;
      this.workLock.writeLock().lock();

      try {
         ++this.flushEpoch;
      } finally {
         this.workLock.writeLock().unlock();
      }

      ArrayList flushes = new ArrayList(this.concurrency);

      try {
         FlushTask flush;
         for(int i = 0; i < this.concurrency; ++i) {
            flush = new FlushTask(this.timeSource, shutdown);
            this.queues[i].offer(flush);
            flushes.add(flush);
         }

         Iterator var18 = flushes.iterator();

         long start;
         boolean success;
         while(var18.hasNext()) {
            flush = (FlushTask)var18.next();
            if(allowTimeout) {
               start = this.timeSource.currentTimeMillis();
               success = flush.await(currentTimeout, TimeUnit.MILLISECONDS);
               if(!success) {
                  this.doFlushError();
               }

               currentTimeout -= this.timeSource.currentTimeMillis() - start;
            } else {
               flush.await();
            }
         }

         var18 = this.flushQueue.iterator();

         while(var18.hasNext()) {
            Task task = (Task)var18.next();
            if(allowTimeout) {
               start = this.timeSource.currentTimeMillis();
               success = task.await(currentTimeout, TimeUnit.MILLISECONDS);
               if(!success) {
                  this.doFlushError();
               }

               currentTimeout -= this.timeSource.currentTimeMillis() - start;
            } else {
               task.await();
            }
         }
      } finally {
         this.flushQueue.clear();
      }

   }

   private void doFlushError() throws TimeoutException {
      String error = String.format("Timeout while waiting for workers when flushing pool %s; current timeout is %s millis, consider increasing it, or reducing load on the node.\n%s", new Object[]{this.poolName, Integer.valueOf(this.flushMaxTimeMillis), this.flushErrorMessage});
      logger.warn(error);
      throw new TimeoutException(error);
   }

   private long computeTotalQueueSize() {
      long result = 0L;

      for(int i = 0; i < this.concurrency; ++i) {
         result += (long)this.queues[i].size();
      }

      return result;
   }

   private void onBackPressure() {
      double backPressureRatio = 0.0D;
      long averagePauseNanos = TimeUnit.NANOSECONDS.convert((long)this.averageBackPressurePause.computeAverage(), TimeUnit.MICROSECONDS);
      if(averagePauseNanos > 0L) {
         double incomingRatePerSec = this.incomingRate.get(TimeUnit.SECONDS);
         double outgoingRatePerSec = this.outgoingRate.get(TimeUnit.SECONDS);
         double totalPauseNanos = (double)averagePauseNanos * Math.max(1.0D, incomingRatePerSec);
         double omittedRatePerSec = incomingRatePerSec * (totalPauseNanos / (double)TimeUnit.SECONDS.toNanos(1L));
         double correctedRatePerSec = incomingRatePerSec + omittedRatePerSec;
         backPressureRatio = correctedRatePerSec / outgoingRatePerSec;
      }

      Iterator var16 = this.listeners.iterator();

      while(var16.hasNext()) {
         WorkPoolListener listener = (WorkPoolListener)var16.next();

         try {
            listener.onBackPressure(backPressureRatio);
         } catch (Throwable var15) {
            logger.warn(String.format("Listener %s failed for pool %s with exception: %s", new Object[]{listener, this.poolName, var15.getMessage()}), var15);
         }
      }

   }

   private MetricName buildCMRName(String metricName, String metricType, String index) {
      List<String> keys = new ArrayList();
      List<String> values = new ArrayList();
      keys.add("scope");
      values.add("search");
      keys.add("index");
      values.add(index);
      keys.add("metricType");
      values.add(metricType);
      keys.add("name");
      values.add(metricName);
      String mbeanName = JMX.buildMBeanName(JMX.Type.METRICS, MapBuilder.immutable().withKeys(keys.toArray(new String[0])).withValues(values.toArray(new String[0])).build());
      MetricName retval = new MetricName("com.datastax.bdp", "search", metricName, index, mbeanName);
      return retval;
   }

   private class BackPressureTask implements Runnable {
      private BackPressureTask() {
      }

      public void run() {
         WorkPool.this.onBackPressure();
         WorkPool.this.incomingRate.prune();
         WorkPool.this.outgoingRate.prune();
         WorkPool.this.throughput.prune();
      }
   }

   protected class CompletionListener implements Worker.TaskListener {
      protected CompletionListener() {
      }

      public void onComplete(Worker w, Task t, int workUnits) {
         WorkPool.this.outgoingRate.update(1);
         WorkPool.this.throughput.update(workUnits);
      }
   }
}
