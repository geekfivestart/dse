package org.apache.cassandra.utils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLongArray;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.slf4j.Logger;

public class EstimatedHistogram {
   public static final EstimatedHistogram.EstimatedHistogramSerializer serializer = new EstimatedHistogram.EstimatedHistogramSerializer();
   private final long[] bucketOffsets;
   final AtomicLongArray buckets;

   public EstimatedHistogram() {
      this(90);
   }

   public EstimatedHistogram(int bucketCount) {
      this(bucketCount, false);
   }

   public EstimatedHistogram(int bucketCount, boolean considerZeroes) {
      this.bucketOffsets = newOffsets(bucketCount, considerZeroes);
      this.buckets = new AtomicLongArray(this.bucketOffsets.length + 1);
   }

   public EstimatedHistogram(long[] bucketData) {
      assert bucketData != null && bucketData.length > 0 : "Bucket data must be an array of size more than 0";

      this.bucketOffsets = newOffsets(bucketData.length - 1, false);
      this.buckets = new AtomicLongArray(bucketData);
   }

   public EstimatedHistogram(long[] offsets, long[] bucketData) {
      assert bucketData.length == offsets.length + 1;

      this.bucketOffsets = offsets;
      this.buckets = new AtomicLongArray(bucketData);
   }

   public static long[] newOffsets(int size, boolean considerZeroes) {
      long[] result = new long[size + (considerZeroes?1:0)];
      int i = 0;
      if(considerZeroes) {
         result[i++] = 0L;
      }

      long last = 1L;

      for(result[i++] = last; i < result.length; ++i) {
         long next = Math.round((double)last * 1.2D);
         if(next == last) {
            ++next;
         }

         result[i] = next;
         last = next;
      }

      return result;
   }

   public long[] getBucketOffsets() {
      return this.bucketOffsets;
   }

   private int findIndex(long n) {
      int index = Arrays.binarySearch(this.bucketOffsets, n);
      if(index < 0) {
         index = -index - 1;
      }

      return index;
   }

   public void add(long n) {
      this.buckets.incrementAndGet(this.findIndex(n));
   }

   public void add(long n, long delta) {
      this.buckets.addAndGet(this.findIndex(n), delta);
   }

   long get(int bucket) {
      return this.buckets.get(bucket);
   }

   public long[] getBuckets(boolean reset) {
      int len = this.buckets.length();
      long[] rv = new long[len];
      int i;
      if(reset) {
         for(i = 0; i < len; ++i) {
            rv[i] = this.buckets.getAndSet(i, 0L);
         }
      } else {
         for(i = 0; i < len; ++i) {
            rv[i] = this.buckets.get(i);
         }
      }

      return rv;
   }

   public long min() {
      for(int i = 0; i < this.buckets.length(); ++i) {
         if(this.buckets.get(i) > 0L) {
            return i == 0?0L:1L + this.bucketOffsets[i - 1];
         }
      }

      return 0L;
   }

   public long max() {
      int lastBucket = this.buckets.length() - 1;
      if(this.buckets.get(lastBucket) > 0L) {
         return 9223372036854775807L;
      } else {
         for(int i = lastBucket - 1; i >= 0; --i) {
            if(this.buckets.get(i) > 0L) {
               return this.bucketOffsets[i];
            }
         }

         return 0L;
      }
   }

   public long percentile(double percentile) {
      assert percentile >= 0.0D && percentile <= 1.0D;

      int lastBucket = this.buckets.length() - 1;
      if(this.buckets.get(lastBucket) > 0L) {
         throw new IllegalStateException("Unable to compute when histogram overflowed");
      } else {
         long pcount = (long)Math.ceil((double)this.count() * percentile);
         if(pcount == 0L) {
            return 0L;
         } else {
            long elements = 0L;

            for(int i = 0; i < lastBucket; ++i) {
               elements += this.buckets.get(i);
               if(elements >= pcount) {
                  return this.bucketOffsets[i];
               }
            }

            return 0L;
         }
      }
   }

   public long mean() {
      return (long)Math.ceil(this.rawMean());
   }

   public double rawMean() {
      int lastBucket = this.buckets.length() - 1;
      if(this.buckets.get(lastBucket) > 0L) {
         throw new IllegalStateException("Unable to compute ceiling for max when histogram overflowed");
      } else {
         long elements = 0L;
         long sum = 0L;

         for(int i = 0; i < lastBucket; ++i) {
            long bCount = this.buckets.get(i);
            elements += bCount;
            sum += bCount * this.bucketOffsets[i];
         }

         return (double)sum / (double)elements;
      }
   }

   public long count() {
      long sum = 0L;

      for(int i = 0; i < this.buckets.length(); ++i) {
         sum += this.buckets.get(i);
      }

      return sum;
   }

   public long getLargestBucketOffset() {
      return this.bucketOffsets[this.bucketOffsets.length - 1];
   }

   public boolean isOverflowed() {
      return this.buckets.get(this.buckets.length() - 1) > 0L;
   }

   public void log(Logger log) {
      int nameCount;
      if(this.buckets.get(this.buckets.length() - 1) == 0L) {
         nameCount = this.buckets.length() - 1;
      } else {
         nameCount = this.buckets.length();
      }

      String[] names = new String[nameCount];
      int maxNameLength = 0;

      for(int i = 0; i < nameCount; ++i) {
         names[i] = nameOfRange(this.bucketOffsets, i);
         maxNameLength = Math.max(maxNameLength, names[i].length());
      }

      String formatstr = "%" + maxNameLength + "s: %d";

      for(int i = 0; i < nameCount; ++i) {
         long count = this.buckets.get(i);
         if(i != 0 || count != 0L) {
            log.debug(String.format(formatstr, new Object[]{names[i], Long.valueOf(count)}));
         }
      }

   }

   private static String nameOfRange(long[] bucketOffsets, int index) {
      StringBuilder sb = new StringBuilder();
      appendRange(sb, bucketOffsets, index);
      return sb.toString();
   }

   private static void appendRange(StringBuilder sb, long[] bucketOffsets, int index) {
      sb.append("[");
      if(index == 0) {
         if(bucketOffsets[0] > 0L) {
            sb.append("1");
         } else {
            sb.append("-Inf");
         }
      } else {
         sb.append(bucketOffsets[index - 1] + 1L);
      }

      sb.append("..");
      if(index == bucketOffsets.length) {
         sb.append("Inf");
      } else {
         sb.append(bucketOffsets[index]);
      }

      sb.append("]");
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(!(o instanceof EstimatedHistogram)) {
         return false;
      } else {
         EstimatedHistogram that = (EstimatedHistogram)o;
         return Arrays.equals(this.getBucketOffsets(), that.getBucketOffsets()) && Arrays.equals(this.getBuckets(false), that.getBuckets(false));
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.getBucketOffsets(), this.getBuckets(false)});
   }

   public static class EstimatedHistogramSerializer implements ISerializer<EstimatedHistogram> {
      public EstimatedHistogramSerializer() {
      }

      public void serialize(EstimatedHistogram eh, DataOutputPlus out) throws IOException {
         long[] offsets = eh.getBucketOffsets();
         long[] buckets = eh.getBuckets(false);
         out.writeInt(buckets.length);

         for(int i = 0; i < buckets.length; ++i) {
            out.writeLong(offsets[i == 0?0:i - 1]);
            out.writeLong(buckets[i]);
         }

      }

      public EstimatedHistogram deserialize(DataInputPlus in) throws IOException {
         int size = in.readInt();
         long[] offsets = new long[size - 1];
         long[] buckets = new long[size];

         for(int i = 0; i < size; ++i) {
            offsets[i == 0?0:i - 1] = in.readLong();
            buckets[i] = in.readLong();
         }

         return new EstimatedHistogram(offsets, buckets);
      }

      public long serializedSize(EstimatedHistogram eh) {
         int size = 0;
         long[] offsets = eh.getBucketOffsets();
         long[] buckets = eh.getBuckets(false);
         size = size + TypeSizes.sizeof(buckets.length);

         for(int i = 0; i < buckets.length; ++i) {
            size += TypeSizes.sizeof(offsets[i == 0?0:i - 1]);
            size += TypeSizes.sizeof(buckets[i]);
         }

         return (long)size;
      }
   }
}
