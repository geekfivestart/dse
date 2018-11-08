package org.apache.cassandra.utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.RandomAccessFile;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.net.OutboundTcpConnection;
import org.jctools.queues.MessagePassingQueue;
import org.jctools.queues.MessagePassingQueue.Consumer;
import org.jctools.queues.MessagePassingQueue.ExitCondition;
import org.jctools.queues.MessagePassingQueue.WaitStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CoalescingStrategies {
   protected static final Logger logger = LoggerFactory.getLogger(CoalescingStrategies.class);
   private static final String DEBUG_COALESCING_PROPERTY = "cassandra.coalescing_debug";
   private static final boolean DEBUG_COALESCING = Boolean.getBoolean("cassandra.coalescing_debug");
   private static final String DEBUG_COALESCING_PATH_PROPERTY = "cassandra.coalescing_debug_path";
   private static final String DEBUG_COALESCING_PATH = System.getProperty("cassandra.coalescing_debug_path", "/tmp/coleascing_debug");
   @VisibleForTesting
   static CoalescingStrategies.Clock CLOCK;
   private static final CoalescingStrategies.Parker PARKER;

   public CoalescingStrategies() {
   }

   @VisibleForTesting
   static void parkLoop(long nanos) {
      long now = System.nanoTime();
      long timer = now + nanos;
      long limit = timer - nanos / 16L;

      do {
         LockSupport.parkNanos(timer - now);
         now = System.nanoTime();
      } while(now < limit);

   }

   private static boolean maybeSleep(int messages, long averageGap, long maxCoalesceWindow, CoalescingStrategies.Parker parker) {
      if(messages >= DatabaseDescriptor.getOtcCoalescingEnoughCoalescedMessages()) {
         return false;
      } else {
         long sleep = (long)messages * averageGap;
         if(sleep > 0L && sleep <= maxCoalesceWindow) {
            while(sleep * 2L < maxCoalesceWindow) {
               sleep *= 2L;
            }

            parker.park(sleep);
            return true;
         } else {
            return false;
         }
      }
   }

   @VisibleForTesting
   static CoalescingStrategies.CoalescingStrategy newCoalescingStrategy(String strategy, int coalesceWindow, OutboundTcpConnection owner, CoalescingStrategies.Parker parker, Logger logger, String displayName, Runnable runBeforeBlocking) {
      String classname = null;
      String strategyCleaned = strategy.trim().toUpperCase(Locale.ENGLISH);
      byte var10 = -1;
      switch(strategyCleaned.hashCode()) {
      case -2005403122:
         if(strategyCleaned.equals("TIMEHORIZON")) {
            var10 = 2;
         }
         break;
      case -864683537:
         if(strategyCleaned.equals("MOVINGAVERAGE")) {
            var10 = 0;
         }
         break;
      case 66907988:
         if(strategyCleaned.equals("FIXED")) {
            var10 = 1;
         }
         break;
      case 1053567612:
         if(strategyCleaned.equals("DISABLED")) {
            var10 = 3;
         }
      }

      switch(var10) {
      case 0:
         classname = CoalescingStrategies.MovingAverageCoalescingStrategy.class.getName();
         break;
      case 1:
         classname = CoalescingStrategies.FixedCoalescingStrategy.class.getName();
         break;
      case 2:
         classname = CoalescingStrategies.TimeHorizonMovingAverageCoalescingStrategy.class.getName();
         break;
      case 3:
         classname = CoalescingStrategies.DisabledCoalescingStrategy.class.getName();
         break;
      default:
         classname = strategy;
      }

      try {
         Class<?> clazz = Class.forName(classname);
         if(!CoalescingStrategies.CoalescingStrategy.class.isAssignableFrom(clazz)) {
            throw new RuntimeException(classname + " is not an instance of CoalescingStrategy");
         } else {
            Constructor<?> constructor = clazz.getConstructor(new Class[]{Integer.TYPE, OutboundTcpConnection.class, CoalescingStrategies.Parker.class, Logger.class, String.class, Runnable.class});
            return (CoalescingStrategies.CoalescingStrategy)constructor.newInstance(new Object[]{Integer.valueOf(coalesceWindow), owner, parker, logger, displayName, runBeforeBlocking});
         }
      } catch (Exception var11) {
         throw new RuntimeException(var11);
      }
   }

   public static CoalescingStrategies.CoalescingStrategy newCoalescingStrategy(String strategy, int coalesceWindow, OutboundTcpConnection owner, Logger logger, String displayName) {
      return newCoalescingStrategy(strategy, coalesceWindow, owner, PARKER, logger, displayName, (Runnable)null);
   }

   @VisibleForTesting
   public static CoalescingStrategies.CoalescingStrategy newCoalescingStrategy(String strategy, int coalesceWindow, OutboundTcpConnection owner, Logger logger, String displayName, Runnable runBeforeBlocking) {
      return newCoalescingStrategy(strategy, coalesceWindow, owner, PARKER, logger, displayName, runBeforeBlocking);
   }

   static {
      if(DEBUG_COALESCING) {
         File directory = new File(DEBUG_COALESCING_PATH);
         if(directory.exists()) {
            FileUtils.deleteRecursive(directory);
         }

         if(!directory.mkdirs()) {
            throw new ExceptionInInitializerError("Couldn't create log dir");
         }
      }

      CLOCK = new CoalescingStrategies.Clock() {
         public long nanoTime() {
            return System.nanoTime();
         }
      };
      PARKER = new CoalescingStrategies.Parker() {
         public void park(long nanos) {
            CoalescingStrategies.parkLoop(nanos);
         }
      };
   }

   @VisibleForTesting
   static class DisabledCoalescingStrategy extends CoalescingStrategies.CoalescingStrategy {
      public DisabledCoalescingStrategy(int coalesceWindowMicros, OutboundTcpConnection owner, CoalescingStrategies.Parker parker, Logger logger, String displayName, Runnable runBeforeBlocking) {
         super(owner, parker, logger, displayName, runBeforeBlocking);
      }

      protected <C extends CoalescingStrategies.Coalescable> void coalesceInternal(MessagePassingQueue<C> input, List<C> out, int maxItems) throws InterruptedException {
         if(input.drain(out::add, maxItems) == 0) {
            this.blockingDrain(input, out, maxItems);
         }

         this.debugTimestamps(out);
      }

      public String toString() {
         return "Disabled";
      }
   }

   @VisibleForTesting
   static class FixedCoalescingStrategy extends CoalescingStrategies.CoalescingStrategy {
      private final long coalesceWindow;
      private boolean doneWaiting = false;

      public FixedCoalescingStrategy(int coalesceWindowMicros, OutboundTcpConnection owner, CoalescingStrategies.Parker parker, Logger logger, String displayName, Runnable runBeforeBlocking) {
         super(owner, parker, logger, displayName, runBeforeBlocking);
         this.coalesceWindow = TimeUnit.MICROSECONDS.toNanos((long)coalesceWindowMicros);
      }

      protected <C extends CoalescingStrategies.Coalescable> void coalesceInternal(MessagePassingQueue<C> input, List<C> out, int maxItems) throws InterruptedException {
         int enough = DatabaseDescriptor.getOtcCoalescingEnoughCoalescedMessages();
         if(input.drain(out::add, maxItems) == 0) {
            this.blockingDrain(input, out, maxItems);
            if(out.size() < enough) {
               this.parker.park(this.coalesceWindow);
               input.drain(out::add, maxItems - out.size());
            }
         }

         this.debugTimestamps(out);
      }

      public String toString() {
         return "Fixed";
      }
   }

   @VisibleForTesting
   static class MovingAverageCoalescingStrategy extends CoalescingStrategies.CoalescingStrategy {
      private final int[] samples = new int[16];
      private long lastSample = 0L;
      private int index = 0;
      private long sum = 0L;
      private boolean doneWaiting = false;
      private final long maxCoalesceWindow;

      public MovingAverageCoalescingStrategy(int maxCoalesceWindow, OutboundTcpConnection owner, CoalescingStrategies.Parker parker, Logger logger, String displayName, Runnable runBeforeBlocking) {
         super(owner, parker, logger, displayName, runBeforeBlocking);
         this.maxCoalesceWindow = TimeUnit.MICROSECONDS.toNanos((long)maxCoalesceWindow);

         for(int ii = 0; ii < this.samples.length; ++ii) {
            this.samples[ii] = 2147483647;
         }

         this.sum = 2147483647L * (long)this.samples.length;
      }

      private long logSample(int value) {
         this.sum -= (long)this.samples[this.index];
         this.sum += (long)value;
         this.samples[this.index] = value;
         ++this.index;
         this.index &= 15;
         return this.sum / 16L;
      }

      private long notifyOfSample(long sample) {
         this.debugTimestamp(sample);
         if(sample > this.lastSample) {
            int delta = (int)Math.min(2147483647L, sample - this.lastSample);
            this.lastSample = sample;
            return this.logSample(delta);
         } else {
            return this.logSample(1);
         }
      }

      protected <C extends CoalescingStrategies.Coalescable> void coalesceInternal(MessagePassingQueue<C> input, List<C> out, int maxItems) throws InterruptedException {
         if(input.drain(out::add, maxItems) == 0) {
            this.blockingDrain(input, out, maxItems);
         }

         long average = this.notifyOfSample(((CoalescingStrategies.Coalescable)out.get(0)).timestampNanos());
         this.debugGap(average);
         if(CoalescingStrategies.maybeSleep(out.size(), average, this.maxCoalesceWindow, this.parker)) {
            input.drain(out::add, maxItems - out.size());
         }

         for(int ii = 1; ii < out.size(); ++ii) {
            this.notifyOfSample(((CoalescingStrategies.Coalescable)out.get(ii)).timestampNanos());
         }

      }

      public String toString() {
         return "Moving average";
      }
   }

   @VisibleForTesting
   static class TimeHorizonMovingAverageCoalescingStrategy extends CoalescingStrategies.CoalescingStrategy {
      private static final int INDEX_SHIFT = 26;
      private static final long BUCKET_INTERVAL = 67108864L;
      private static final int BUCKET_COUNT = 16;
      private static final long INTERVAL = 1073741824L;
      private static final long MEASURED_INTERVAL = 1006632960L;
      private long epoch;
      private final int[] samples;
      private long sum;
      private final long maxCoalesceWindow;

      public TimeHorizonMovingAverageCoalescingStrategy(int maxCoalesceWindow, OutboundTcpConnection owner, CoalescingStrategies.Parker parker, Logger logger, String displayName, Runnable runBeforeBlocking) {
         super(owner, parker, logger, displayName, runBeforeBlocking);
         this.epoch = CoalescingStrategies.CLOCK.nanoTime();
         this.samples = new int[16];
         this.sum = 0L;
         this.maxCoalesceWindow = TimeUnit.MICROSECONDS.toNanos((long)maxCoalesceWindow);
         this.sum = 0L;
      }

      private void logSample(long nanos) {
         this.debugTimestamp(nanos);
         long epoch = this.epoch;
         long delta = nanos - epoch;
         if(delta >= 0L) {
            if(delta > 1073741824L) {
               epoch = this.rollepoch(delta, epoch, nanos);
            }

            int ix = this.ix(nanos);
            ++this.samples[ix];
            if(ix != this.ix(epoch - 1L)) {
               ++this.sum;
            }

         }
      }

      private long averageGap() {
         return this.sum == 0L?2147483647L:1006632960L / this.sum;
      }

      private long rollepoch(long delta, long epoch, long nanos) {
         if(delta > 2147483648L) {
            epoch = this.epoch(nanos);
            this.sum = 0L;
            Arrays.fill(this.samples, 0);
         } else {
            for(this.sum += (long)this.samples[this.ix(epoch - 1L)]; epoch + 1073741824L < nanos; epoch += 67108864L) {
               int index = this.ix(epoch);
               this.sum -= (long)this.samples[index];
               this.samples[index] = 0;
            }
         }

         this.epoch = epoch;
         return epoch;
      }

      private long epoch(long latestNanos) {
         return latestNanos - 1006632960L & -67108864L;
      }

      private int ix(long nanos) {
         return (int)(nanos >>> 26 & 15L);
      }

      protected <C extends CoalescingStrategies.Coalescable> void coalesceInternal(MessagePassingQueue<C> input, List<C> out, int maxItems) throws InterruptedException {
         if(input.drain(out::add, maxItems) == 0) {
            this.blockingDrain(input, out, maxItems);
         }

         int count = out.size();
         Iterator var5 = out.iterator();

         while(var5.hasNext()) {
            CoalescingStrategies.Coalescable qm = (CoalescingStrategies.Coalescable)var5.next();
            this.logSample(qm.timestampNanos());
         }

         long averageGap = this.averageGap();
         this.debugGap(averageGap);
         if(CoalescingStrategies.maybeSleep(count, averageGap, this.maxCoalesceWindow, this.parker)) {
            int prevCount = count;
            count += input.drain(out::add, maxItems - count);

            for(int i = prevCount; i < count; ++i) {
               this.logSample(((CoalescingStrategies.Coalescable)out.get(i)).timestampNanos());
            }
         }

      }

      public String toString() {
         return "Time horizon moving average";
      }
   }

   @VisibleForTesting
   interface Parker {
      void park(long var1);
   }

   public abstract static class CoalescingStrategy {
      protected final CoalescingStrategies.Parker parker;
      protected final Logger logger;
      protected volatile boolean shouldLogAverage = false;
      protected final ByteBuffer logBuffer;
      private RandomAccessFile ras;
      private final String displayName;
      protected boolean doneWaiting = false;
      private final Runnable runBeforeBlocking;
      private final OutboundTcpConnection owner;

      protected CoalescingStrategy(OutboundTcpConnection owner, CoalescingStrategies.Parker parker, Logger logger, String displayName, Runnable runBeforeBlocking) {
         this.owner = owner;
         this.parker = parker;
         this.logger = logger;
         this.displayName = displayName;
         this.runBeforeBlocking = runBeforeBlocking;
         if(CoalescingStrategies.DEBUG_COALESCING) {
            NamedThreadFactory.createThread(() -> {
               while(true) {
                  try {
                     Thread.sleep(5000L);
                  } catch (InterruptedException var2) {
                     throw new AssertionError();
                  }

                  this.shouldLogAverage = true;
               }
            }, displayName + " debug thread").start();
         }

         RandomAccessFile rasTemp = null;
         ByteBuffer logBufferTemp = null;
         if(CoalescingStrategies.DEBUG_COALESCING) {
            try {
               File outFile = File.createTempFile("coalescing_" + this.displayName + "_", ".log", new File(CoalescingStrategies.DEBUG_COALESCING_PATH));
               rasTemp = new RandomAccessFile(outFile, "rw");
               logBufferTemp = this.ras.getChannel().map(MapMode.READ_WRITE, 0L, 2147483647L);
               logBufferTemp.putLong(0L);
            } catch (Exception var9) {
               logger.error("Unable to create output file for debugging coalescing", var9);
            }
         }

         this.ras = rasTemp;
         this.logBuffer = logBufferTemp;
      }

      protected <C> void blockingDrain(MessagePassingQueue<C> input, List<C> out, int maxItems) {
         this.doneWaiting = false;
         if(this.runBeforeBlocking != null) {
            this.runBeforeBlocking.run();
         }

         input.drain((entry) -> {
            out.add(entry);
            this.doneWaiting = true;
         }, (i) -> {
            if(this.owner != null) {
               this.owner.park();
            } else {
               LockSupport.parkNanos(10000L);
            }

            return i + 1;
         }, () -> {
            return !this.doneWaiting;
         });
         input.drain(out::add, maxItems - out.size());
      }

      protected final void debugGap(long averageGap) {
         if(CoalescingStrategies.DEBUG_COALESCING && this.shouldLogAverage) {
            this.shouldLogAverage = false;
            this.logger.info("{} gap {}μs", this, Long.valueOf(TimeUnit.NANOSECONDS.toMicros(averageGap)));
         }

      }

      protected final void debugTimestamp(long timestamp) {
         if(CoalescingStrategies.DEBUG_COALESCING && this.logBuffer != null) {
            this.logBuffer.putLong(0, this.logBuffer.getLong(0) + 1L);
            this.logBuffer.putLong(timestamp);
         }

      }

      protected final <C extends CoalescingStrategies.Coalescable> void debugTimestamps(Collection<C> coalescables) {
         if(CoalescingStrategies.DEBUG_COALESCING) {
            Iterator var2 = coalescables.iterator();

            while(var2.hasNext()) {
               C coalescable = (CoalescingStrategies.Coalescable)var2.next();
               this.debugTimestamp(coalescable.timestampNanos());
            }
         }

      }

      public <C extends CoalescingStrategies.Coalescable> void coalesce(MessagePassingQueue<C> input, List<C> out, int maxItems) throws InterruptedException {
         Preconditions.checkArgument(out.isEmpty(), "out list should be empty");
         this.coalesceInternal(input, out, maxItems);
      }

      protected abstract <C extends CoalescingStrategies.Coalescable> void coalesceInternal(MessagePassingQueue<C> var1, List<C> var2, int var3) throws InterruptedException;
   }

   public interface Coalescable {
      long timestampNanos();
   }

   @VisibleForTesting
   interface Clock {
      long nanoTime();
   }
}
