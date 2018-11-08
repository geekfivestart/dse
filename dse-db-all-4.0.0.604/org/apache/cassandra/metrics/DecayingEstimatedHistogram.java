package org.apache.cassandra.metrics;

import com.codahale.metrics.Clock;
import com.google.common.annotations.VisibleForTesting;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.apache.cassandra.concurrent.TPC;

final class DecayingEstimatedHistogram extends Histogram {
   private final DecayingEstimatedHistogram.ForwardDecayingReservoir reservoir;
   private final DecayingEstimatedHistogram.Recorder recorder;

   DecayingEstimatedHistogram(boolean considerZeroes, long maxTrackableValue, int updateTimeMillis, Clock clock) {
      DecayingEstimatedHistogram.BucketProperties bucketProperties = new DecayingEstimatedHistogram.BucketProperties(considerZeroes, maxTrackableValue);
      this.reservoir = new DecayingEstimatedHistogram.ForwardDecayingReservoir(bucketProperties, clock, considerZeroes, updateTimeMillis, false);
      this.recorder = new DecayingEstimatedHistogram.Recorder(bucketProperties, this.reservoir);
   }

   static Histogram.Reservoir makeCompositeReservoir(boolean considerZeroes, long maxTrackableValue, int updateIntervalMillis, Clock clock) {
      return new DecayingEstimatedHistogram.ForwardDecayingReservoir(new DecayingEstimatedHistogram.BucketProperties(considerZeroes, maxTrackableValue), clock, considerZeroes, updateIntervalMillis, true);
   }

   public final void update(long value) {
      this.recorder.update(value);
   }

   public int size() {
      return this.reservoir.bucketProperties.size();
   }

   public com.codahale.metrics.Snapshot getSnapshot() {
      return this.reservoir.getSnapshot();
   }

   @VisibleForTesting
   boolean isOverflowed() {
      return this.reservoir.isOverflowed();
   }

   @VisibleForTesting
   public void clear() {
      this.recorder.clear();
      this.reservoir.clear();
   }

   public void aggregate() {
      this.reservoir.onReadAggregate();
   }

   public boolean considerZeroes() {
      return this.reservoir.considerZeroes();
   }

   public long maxTrackableValue() {
      return this.reservoir.maxTrackableValue();
   }

   public long[] getOffsets() {
      return this.reservoir.getOffsets();
   }

   public long getCount() {
      return this.reservoir.getCount();
   }

   public Composable.Type getType() {
      return Composable.Type.SINGLE;
   }

   @VisibleForTesting
   public static class Snapshot extends com.codahale.metrics.Snapshot {
      private final long[] bucketOffsets;
      private final boolean isOverflowed;
      private final long[] buckets;
      private final long[] decayingBuckets;
      private final long count;
      private final long countWithDecay;

      public Snapshot(DecayingEstimatedHistogram.ForwardDecayingReservoir reservoir) {
         this.bucketOffsets = reservoir.getOffsets();
         this.isOverflowed = reservoir.isOverflowed;
         this.buckets = this.copyBuckets(reservoir.buckets, reservoir.considerZeroes);
         this.decayingBuckets = this.rescaleBuckets(this.copyBuckets(reservoir.decayingBuckets, reservoir.considerZeroes), reservoir.forwardDecayWeight());
         this.count = this.countBuckets(this.buckets);
         this.countWithDecay = this.countBuckets(this.decayingBuckets);
      }

      private long[] copyBuckets(long[] buckets, boolean considerZeroes) {
         long[] ret = new long[this.bucketOffsets.length];
         int index = 0;
         int var6;
         if(considerZeroes) {
            var6 = index + 1;
            ret[index] = buckets[0];
            ret[var6++] = buckets[1];
         } else {
            var6 = index + 1;
            ret[index] = buckets[0] + buckets[1];
         }

         for(int i = 2; i < buckets.length; ++i) {
            ret[var6++] = buckets[i];
         }

         return ret;
      }

      private long[] rescaleBuckets(long[] buckets, double rescaleFactor) {
         int length = buckets.length;

         for(int i = 0; i < length; ++i) {
            buckets[i] = Math.round((double)buckets[i] / rescaleFactor);
         }

         return buckets;
      }

      private long countBuckets(long[] buckets) {
         long sum = 0L;

         for(int i = 0; i < buckets.length; ++i) {
            sum += buckets[i];
         }

         return sum;
      }

      long getCount() {
         return this.count;
      }

      public double getValue(double quantile) {
         assert quantile >= 0.0D && quantile <= 1.0D;

         if(this.isOverflowed) {
            throw new IllegalStateException("Unable to compute when histogram overflowed");
         } else {
            long qcount = (long)Math.ceil((double)this.countWithDecay * quantile);
            if(qcount == 0L) {
               return 0.0D;
            } else {
               long elements = 0L;

               for(int i = 0; i < this.decayingBuckets.length; ++i) {
                  elements += this.decayingBuckets[i];
                  if(elements >= qcount) {
                     return (double)this.bucketOffsets[i];
                  }
               }

               return 0.0D;
            }
         }
      }

      @VisibleForTesting
      public long[] getOffsets() {
         return this.bucketOffsets;
      }

      public long[] getValues() {
         return this.buckets;
      }

      public int size() {
         return this.decayingBuckets.length;
      }

      public long getMax() {
         if(this.isOverflowed) {
            return 9223372036854775807L;
         } else {
            for(int i = this.decayingBuckets.length - 1; i >= 0; --i) {
               if(this.decayingBuckets[i] > 0L) {
                  return this.bucketOffsets[i + 1] - 1L;
               }
            }

            return 0L;
         }
      }

      public double getMean() {
         if(this.isOverflowed) {
            throw new IllegalStateException("Unable to compute when histogram overflowed");
         } else {
            long elements = 0L;
            long sum = 0L;

            for(int i = 0; i < this.decayingBuckets.length; ++i) {
               long bCount = this.decayingBuckets[i];
               elements += bCount;
               long delta = this.bucketOffsets[i] - (i == 0?0L:this.bucketOffsets[i - 1]);
               sum += bCount * (this.bucketOffsets[i] + (delta >> 1));
            }

            return (double)sum / (double)elements;
         }
      }

      public long getMin() {
         for(int i = 0; i < this.decayingBuckets.length; ++i) {
            if(this.decayingBuckets[i] > 0L) {
               return this.bucketOffsets[i];
            }
         }

         return 0L;
      }

      public double getStdDev() {
         if(this.isOverflowed) {
            throw new IllegalStateException("Unable to compute when histogram overflowed");
         } else if(this.countWithDecay <= 1L) {
            return 0.0D;
         } else {
            double mean = this.getMean();
            double sum = 0.0D;

            for(int i = 0; i < this.decayingBuckets.length; ++i) {
               long value = this.bucketOffsets[i];
               double diff = (double)value - mean;
               sum += diff * diff * (double)this.decayingBuckets[i];
            }

            return Math.sqrt(sum / (double)(this.countWithDecay - 1L));
         }
      }

      public void dump(OutputStream output) {
         PrintWriter out = new PrintWriter(new OutputStreamWriter(output, StandardCharsets.UTF_8));
         Throwable var3 = null;

         try {
            for(int i = 0; i < this.decayingBuckets.length; ++i) {
               out.printf("%d%n", new Object[]{Long.valueOf(this.decayingBuckets[i])});
            }
         } catch (Throwable var12) {
            var3 = var12;
            throw var12;
         } finally {
            if(out != null) {
               if(var3 != null) {
                  try {
                     out.close();
                  } catch (Throwable var11) {
                     var3.addSuppressed(var11);
                  }
               } else {
                  out.close();
               }
            }

         }

      }
   }

   private static final class Buffer {
      private final AtomicBuffer buffer;
      private final DecayingEstimatedHistogram.BucketProperties bucketProperties;
      private volatile boolean isOverflowed;

      Buffer(DecayingEstimatedHistogram.BucketProperties bucketProperties) {
         this.buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(bucketProperties.numBuckets << 3));
         this.bucketProperties = bucketProperties;
         this.isOverflowed = false;
      }

      void update(long value, boolean lazy) {
         if(value > this.bucketProperties.maxTrackableValue) {
            this.isOverflowed = true;
         } else {
            int index = this.bucketProperties.getIndex(value) << 3;
            if(lazy) {
               this.buffer.putLongOrdered(index, this.buffer.getLongVolatile(index) + 1L);
            } else {
               this.buffer.getAndAddLong(index, 1L);
            }

         }
      }

      long getAndSet(int index, long value) {
         return this.buffer.getAndSetLong(index << 3, value);
      }

      long getLongVolatile(int index) {
         return this.buffer.getLongVolatile(index << 3);
      }

      void clear() {
         for(int i = 0; i < this.bucketProperties.size(); ++i) {
            this.getAndSet(i, 0L);
         }

      }
   }

   private static final class Recorder {
      private final DecayingEstimatedHistogram.BucketProperties bucketProperties;
      private final int numCores;
      private final DecayingEstimatedHistogram.Buffer[] buffers;
      private final DecayingEstimatedHistogram.ForwardDecayingReservoir reservoir;

      Recorder(DecayingEstimatedHistogram.BucketProperties bucketProperties, DecayingEstimatedHistogram.ForwardDecayingReservoir reservoir) {
         this.bucketProperties = bucketProperties;
         this.numCores = TPC.getNumCores();
         this.buffers = new DecayingEstimatedHistogram.Buffer[this.numCores + 1];
         this.reservoir = reservoir;
         this.buffers[this.numCores] = new DecayingEstimatedHistogram.Buffer(bucketProperties);
         reservoir.add(this);
      }

      void update(long value) {
         this.reservoir.maybeSchedule();
         int coreId = TPC.getCoreId();
         DecayingEstimatedHistogram.Buffer buffer = this.buffers[coreId];
         if(buffer == null) {
            assert TPC.isValidCoreId(coreId);

            buffer = this.buffers[coreId] = new DecayingEstimatedHistogram.Buffer(this.bucketProperties);
         }

         buffer.update(value, coreId < this.numCores);
      }

      boolean isOverFlowed() {
         DecayingEstimatedHistogram.Buffer[] var1 = this.buffers;
         int var2 = var1.length;

         for(int var3 = 0; var3 < var2; ++var3) {
            DecayingEstimatedHistogram.Buffer buffer = var1[var3];
            if(buffer != null && buffer.isOverflowed) {
               return true;
            }
         }

         return false;
      }

      long getValue(int index) {
         long ret = 0L;
         DecayingEstimatedHistogram.Buffer[] var4 = this.buffers;
         int var5 = var4.length;

         for(int var6 = 0; var6 < var5; ++var6) {
            DecayingEstimatedHistogram.Buffer buffer = var4[var6];
            if(buffer != null) {
               ret += buffer.getLongVolatile(index);
            }
         }

         return ret;
      }

      void clear() {
         Arrays.stream(this.buffers).filter(Objects::nonNull).forEach(DecayingEstimatedHistogram.Buffer::clear);
      }
   }

   static final class ForwardDecayingReservoir implements Histogram.Reservoir {
      static final long HALF_TIME_IN_S = 60L;
      static final double MEAN_LIFETIME_IN_S = 60.0D / Math.log(2.0D);
      static final long LANDMARK_RESET_INTERVAL_IN_MS = 1800000L;
      private final DecayingEstimatedHistogram.BucketProperties bucketProperties;
      private final Clock clock;
      private final boolean considerZeroes;
      private final int updateIntervalMillis;
      private long decayLandmark;
      private final CopyOnWriteArrayList<DecayingEstimatedHistogram.Recorder> recorders;
      private final long[] buckets;
      private final long[] decayingBuckets;
      private volatile DecayingEstimatedHistogram.Snapshot snapshot;
      private final boolean isComposite;
      private final AtomicBoolean scheduled;
      private volatile boolean isOverflowed;

      ForwardDecayingReservoir(DecayingEstimatedHistogram.BucketProperties bucketProperties, Clock clock, boolean considerZeroes, int updateIntervalMillis, boolean isComposite) {
         this.bucketProperties = bucketProperties;
         this.clock = clock;
         this.considerZeroes = considerZeroes;
         this.updateIntervalMillis = updateIntervalMillis;
         this.isOverflowed = false;
         this.buckets = new long[bucketProperties.numBuckets];
         this.decayingBuckets = new long[bucketProperties.numBuckets];
         this.recorders = new CopyOnWriteArrayList();
         this.decayLandmark = clock.getTime();
         this.snapshot = new DecayingEstimatedHistogram.Snapshot(this);
         this.scheduled = new AtomicBoolean(false);
         this.isComposite = isComposite;
         this.scheduleIfComposite();
      }

      void add(DecayingEstimatedHistogram.Recorder recorder) {
         this.recorders.add(recorder);
      }

      void maybeSchedule() {
         if(this.updateIntervalMillis > 0 && !this.scheduled.get()) {
            if(this.scheduled.compareAndSet(false, true)) {
               TPC.bestTPCTimer().onTimeout(this::aggregate, (long)this.updateIntervalMillis, TimeUnit.MILLISECONDS);
            }

         }
      }

      void scheduleIfComposite() {
         if(this.updateIntervalMillis > 0 && this.isComposite) {
            TPC.bestTPCTimer().onTimeout(this::aggregate, (long)this.updateIntervalMillis, TimeUnit.MILLISECONDS);
         }

      }

      public boolean considerZeroes() {
         return this.considerZeroes;
      }

      public long maxTrackableValue() {
         return this.bucketProperties.maxTrackableValue;
      }

      void onReadAggregate() {
         if(this.updateIntervalMillis <= 0) {
            this.aggregate();
         }

      }

      boolean isOverflowed() {
         this.onReadAggregate();
         return this.isOverflowed;
      }

      public long getCount() {
         this.onReadAggregate();
         return this.snapshot.getCount();
      }

      public DecayingEstimatedHistogram.Snapshot getSnapshot() {
         this.onReadAggregate();
         return this.snapshot;
      }

      @VisibleForTesting
      void aggregate() {
         try {
            this.scheduled.set(false);
            long now = this.clock.getTime();
            this.rescaleIfNeeded(now);
            long weight = Math.round(forwardDecayWeight(now, this.decayLandmark));
            this.isOverflowed = false;
            Iterator var5 = this.recorders.iterator();

            while(true) {
               if(var5.hasNext()) {
                  DecayingEstimatedHistogram.Recorder recorder = (DecayingEstimatedHistogram.Recorder)var5.next();
                  if(!recorder.isOverFlowed()) {
                     continue;
                  }

                  this.isOverflowed = true;
               }

               for(int i = 0; i < this.buckets.length; ++i) {
                  long value = this.getRecordersSum(i);
                  long delta = value - this.buckets[i];
                  if(delta > 0L) {
                     this.buckets[i] += delta;
                     this.decayingBuckets[i] += delta * weight;
                  }
               }

               this.snapshot = new DecayingEstimatedHistogram.Snapshot(this);
               return;
            }
         } finally {
            this.scheduleIfComposite();
         }
      }

      private long getRecordersSum(int index) {
         long ret = 0L;

         DecayingEstimatedHistogram.Recorder recorder;
         for(Iterator var4 = this.recorders.iterator(); var4.hasNext(); ret += recorder.getValue(index)) {
            recorder = (DecayingEstimatedHistogram.Recorder)var4.next();
         }

         return ret;
      }

      private boolean isCompatible(Histogram.Reservoir other) {
         if(!(other instanceof DecayingEstimatedHistogram.ForwardDecayingReservoir)) {
            return false;
         } else {
            DecayingEstimatedHistogram.ForwardDecayingReservoir otherFDReservoir = (DecayingEstimatedHistogram.ForwardDecayingReservoir)other;
            return otherFDReservoir.considerZeroes == this.considerZeroes && otherFDReservoir.buckets.length == this.buckets.length && otherFDReservoir.decayingBuckets.length == this.decayingBuckets.length;
         }
      }

      public void add(Histogram histogram) {
         if(!(histogram instanceof DecayingEstimatedHistogram)) {
            throw new IllegalArgumentException("Histogram is not compatible");
         } else {
            DecayingEstimatedHistogram decayingEstimatedHistogram = (DecayingEstimatedHistogram)histogram;
            if(!this.isCompatible(decayingEstimatedHistogram.reservoir)) {
               throw new IllegalArgumentException("Histogram reservoir is not compatible");
            } else {
               this.add(decayingEstimatedHistogram.recorder);
            }
         }
      }

      public long[] getOffsets() {
         return this.bucketProperties.offsets;
      }

      void clear() {
         this.isOverflowed = false;

         for(int i = 0; i < this.bucketProperties.size(); ++i) {
            this.buckets[i] = 0L;
            this.decayingBuckets[i] = 0L;
         }

         this.snapshot = new DecayingEstimatedHistogram.Snapshot(this);
      }

      private void rescaleIfNeeded(long now) {
         if(this.needRescale(now)) {
            this.rescale(now);
         }

      }

      private void rescale(long now) {
         double rescaleFactor = forwardDecayWeight(now, this.decayLandmark);
         this.decayLandmark = now;

         for(int i = 0; i < this.decayingBuckets.length; ++i) {
            this.decayingBuckets[i] = Math.round((double)this.decayingBuckets[i] / rescaleFactor);
         }

      }

      private boolean needRescale(long now) {
         return now - this.decayLandmark > 1800000L;
      }

      private double forwardDecayWeight() {
         return forwardDecayWeight(this.clock.getTime(), this.decayLandmark);
      }

      private static double forwardDecayWeight(long now, long decayLandmark) {
         return Math.exp((double)(now - decayLandmark) / 1000.0D / MEAN_LIFETIME_IN_S);
      }
   }

   static final class BucketProperties {
      final boolean considerZeros;
      final long maxTrackableValue;
      final long[] offsets;
      final int numBuckets;
      static final int subBucketCount = 8;
      static final int subBucketHalfCount = 4;
      static final int unitMagnitude = 0;
      static final int subBucketCountMagnitude = 3;
      static final int subBucketHalfCountMagnitude = 2;
      final long subBucketMask;
      final int leadingZeroCountBase;

      BucketProperties(boolean considerZeros, long maxTrackableValue) {
         this.considerZeros = considerZeros;
         this.maxTrackableValue = maxTrackableValue;
         this.offsets = this.makeOffsets(considerZeros);
         this.numBuckets = this.offsets.length + (!considerZeros?1:0);
         this.subBucketMask = 7L;
         this.leadingZeroCountBase = 61;
      }

      private long[] makeOffsets(boolean considerZeroes) {
         ArrayList<Long> ret = new ArrayList();
         if(considerZeroes) {
            ret.add(Long.valueOf(0L));
         }

         for(int i = 1; i <= 8; ++i) {
            ret.add(Long.valueOf((long)i));
            if((long)i >= this.maxTrackableValue) {
               break;
            }
         }

         long last = 8L;

         for(long unit = 2L; last < this.maxTrackableValue; unit *= 2L) {
            for(int i = 0; i < 4; ++i) {
               last += unit;
               ret.add(Long.valueOf(last));
               if(last >= this.maxTrackableValue) {
                  break;
               }
            }
         }

         return ret.stream().mapToLong((i) -> {
            return i.longValue();
         }).toArray();
      }

      final int getIndex(long value) {
         if(value < 0L) {
            throw new ArrayIndexOutOfBoundsException("Histogram recorded value cannot be negative.");
         } else {
            int bucketIndex = this.leadingZeroCountBase - Long.numberOfLeadingZeros(value | this.subBucketMask);
            int subBucketIndex = (int)(value >>> bucketIndex + 0);
            int bucketBaseIndex = bucketIndex + 1 << 2;
            int offsetInBucket = subBucketIndex - 4;
            return bucketBaseIndex + offsetInBucket;
         }
      }

      private int size() {
         return this.numBuckets;
      }
   }
}
