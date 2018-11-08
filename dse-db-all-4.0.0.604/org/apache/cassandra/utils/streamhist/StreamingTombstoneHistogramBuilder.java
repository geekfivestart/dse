package org.apache.cassandra.utils.streamhist;

import com.google.common.math.IntMath;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.function.Function;
import java.util.function.LongPredicate;
import java.util.stream.Collectors;

public class StreamingTombstoneHistogramBuilder {
   private final StreamingTombstoneHistogramBuilder.DataHolder bin;
   private final StreamingTombstoneHistogramBuilder.DistanceHolder distances;
   private final StreamingTombstoneHistogramBuilder.Spool spool;
   private final int roundSeconds;

   public StreamingTombstoneHistogramBuilder(int maxBinSize, int maxSpoolSize, int roundSeconds) {
      this.roundSeconds = roundSeconds;
      this.bin = new StreamingTombstoneHistogramBuilder.DataHolder(maxBinSize + 1, roundSeconds);
      this.distances = new StreamingTombstoneHistogramBuilder.DistanceHolder(maxBinSize);
      maxSpoolSize = maxSpoolSize == 0?0:IntMath.pow(2, IntMath.log2(maxSpoolSize, RoundingMode.CEILING));
      this.spool = new StreamingTombstoneHistogramBuilder.Spool(maxSpoolSize);
   }

   public void update(int p) {
      this.update(p, 1);
   }

   public void update(int p, int m) {
      p = roundKey(p, this.roundSeconds);
      if(this.spool.capacity > 0) {
         if(!this.spool.tryAddOrAccumulate(p, m)) {
            this.flushHistogram();
            boolean success = this.spool.tryAddOrAccumulate(p, m);

            assert success : "Can not add value to spool";
         }
      } else {
         this.flushValue(p, m);
      }

   }

   public void flushHistogram() {
      this.spool.forEach(this::flushValue);
      this.spool.clear();
   }

   private void flushValue(int key, int spoolValue) {
      StreamingTombstoneHistogramBuilder.DataHolder.NeighboursAndResult addResult = this.bin.addValue(key, spoolValue);
      if(addResult.result == StreamingTombstoneHistogramBuilder.AddResult.INSERTED) {
         int prevPoint = addResult.prevPoint;
         int nextPoint = addResult.nextPoint;
         if(prevPoint != -1 && nextPoint != -1) {
            this.distances.remove(prevPoint, nextPoint);
         }

         if(prevPoint != -1) {
            this.distances.add(prevPoint, key);
         }

         if(nextPoint != -1) {
            this.distances.add(key, nextPoint);
         }
      }

      if(this.bin.isFull()) {
         this.mergeBin();
      }

   }

   private void mergeBin() {
      int[] smallestDifference = this.distances.getFirstAndRemove();
      int point1 = smallestDifference[0];
      int point2 = smallestDifference[1];
      StreamingTombstoneHistogramBuilder.DataHolder.MergeResult mergeResult = this.bin.merge(point1, point2);
      int nextPoint = mergeResult.nextPoint;
      int prevPoint = mergeResult.prevPoint;
      int newPoint = mergeResult.newPoint;
      if(nextPoint != -1) {
         this.distances.remove(point2, nextPoint);
         this.distances.add(newPoint, nextPoint);
      }

      if(prevPoint != -1) {
         this.distances.remove(prevPoint, point1);
         this.distances.add(prevPoint, newPoint);
      }

   }

   public TombstoneHistogram build() {
      this.flushHistogram();
      return new TombstoneHistogram(this.bin);
   }

   private static int roundKey(int p, int roundSeconds) {
      int d = p % roundSeconds;
      return d > 0?p + (roundSeconds - d):p;
   }

   static class Spool {
      final int[] map;
      final int capacity;
      int size;

      Spool(int capacity) {
         this.capacity = capacity;
         if(capacity == 0) {
            this.map = new int[0];
         } else {
            assert IntMath.isPowerOfTwo(capacity) : "should be power of two";

            this.map = new int[capacity * 2 * 2];
            this.clear();
         }

      }

      void clear() {
         Arrays.fill(this.map, -1);
         this.size = 0;
      }

      boolean tryAddOrAccumulate(int point, int delta) {
         if(this.size > this.capacity) {
            return false;
         } else {
            int cell = 2 * (this.capacity - 1 & this.hash(point));

            for(int attempt = 0; attempt < 100; ++attempt) {
               if(this.tryCell(cell + attempt * 2, point, delta)) {
                  return true;
               }
            }

            return false;
         }
      }

      private int hash(int i) {
         long largePrime = 948701839L;
         return (int)((long)i * largePrime);
      }

      <E extends Exception> void forEach(HistogramDataConsumer<E> consumer) throws E {
         for(int i = 0; i < this.map.length; i += 2) {
            if(this.map[i] != -1) {
               consumer.consume(this.map[i], this.map[i + 1]);
            }
         }

      }

      private boolean tryCell(int cell, int point, int delta) {
         cell %= this.map.length;
         if(this.map[cell] == -1) {
            this.map[cell] = point;
            this.map[cell + 1] = delta;
            ++this.size;
            return true;
         } else if(this.map[cell] == point) {
            this.map[cell + 1] += delta;
            return true;
         } else {
            return false;
         }
      }
   }

   public static enum AddResult {
      INSERTED,
      ACCUMULATED;

      private AddResult() {
      }
   }

   static class DataHolder {
      private static final long EMPTY = 9223372036854775807L;
      private final long[] data;
      private final int roundSeconds;

      DataHolder(int maxCapacity, int roundSeconds) {
         this.data = new long[maxCapacity];
         Arrays.fill(this.data, 9223372036854775807L);
         this.roundSeconds = roundSeconds;
      }

      DataHolder(StreamingTombstoneHistogramBuilder.DataHolder holder) {
         this.data = Arrays.copyOf(holder.data, holder.data.length);
         this.roundSeconds = holder.roundSeconds;
      }

      StreamingTombstoneHistogramBuilder.DataHolder.NeighboursAndResult addValue(int point, int delta) {
         long key = this.wrap(point, 0);
         int index = Arrays.binarySearch(this.data, key);
         StreamingTombstoneHistogramBuilder.AddResult addResult;
         if(index < 0) {
            index = -index - 1;

            assert index < this.data.length : "No more space in array";

            if(this.unwrapPoint(this.data[index]) != point) {
               assert this.data[this.data.length - 1] == 9223372036854775807L : "No more space in array";

               System.arraycopy(this.data, index, this.data, index + 1, this.data.length - index - 1);
               this.data[index] = this.wrap(point, delta);
               addResult = StreamingTombstoneHistogramBuilder.AddResult.INSERTED;
            } else {
               this.data[index] += (long)delta;
               addResult = StreamingTombstoneHistogramBuilder.AddResult.ACCUMULATED;
            }
         } else {
            this.data[index] += (long)delta;
            addResult = StreamingTombstoneHistogramBuilder.AddResult.ACCUMULATED;
         }

         return new StreamingTombstoneHistogramBuilder.DataHolder.NeighboursAndResult(this.getPrevPoint(index), this.getNextPoint(index), addResult);
      }

      public StreamingTombstoneHistogramBuilder.DataHolder.MergeResult merge(int point1, int point2) {
         long key = this.wrap(point1, 0);
         int index = Arrays.binarySearch(this.data, key);
         if(index < 0) {
            index = -index - 1;

            assert index < this.data.length : "Not found in array";

            assert this.unwrapPoint(this.data[index]) == point1 : "Not found in array";
         }

         int prevPoint = this.getPrevPoint(index);
         int nextPoint = this.getNextPoint(index + 1);
         int value1 = this.unwrapValue(this.data[index]);
         int value2 = this.unwrapValue(this.data[index + 1]);

         assert this.unwrapPoint(this.data[index + 1]) == point2 : "point2 should follow point1";

         int sum = value1 + value2;
         int newPoint = (int)(((long)point1 * (long)value1 + (long)point2 * (long)value2) / (long)(value1 + value2));
         newPoint = StreamingTombstoneHistogramBuilder.roundKey(newPoint, this.roundSeconds);
         this.data[index] = this.wrap(newPoint, sum);
         System.arraycopy(this.data, index + 2, this.data, index + 1, this.data.length - index - 2);
         this.data[this.data.length - 1] = 9223372036854775807L;
         return new StreamingTombstoneHistogramBuilder.DataHolder.MergeResult(prevPoint, newPoint, nextPoint);
      }

      private int getPrevPoint(int index) {
         return index > 0?(this.data[index - 1] != 9223372036854775807L?(int)(this.data[index - 1] >> 32):-1):-1;
      }

      private int getNextPoint(int index) {
         return index < this.data.length - 1?(this.data[index + 1] != 9223372036854775807L?(int)(this.data[index + 1] >> 32):-1):-1;
      }

      private int[] unwrap(long key) {
         int point = this.unwrapPoint(key);
         int value = this.unwrapValue(key);
         return new int[]{point, value};
      }

      private int unwrapPoint(long key) {
         return (int)(key >> 32);
      }

      private int unwrapValue(long key) {
         return (int)(key & 4294967295L);
      }

      private long wrap(int point, int value) {
         return (long)point << 32 | (long)value;
      }

      public String toString() {
         return (String)Arrays.stream(this.data).filter((x) -> {
            return x != 9223372036854775807L;
         }).boxed().map(this::unwrap).map(Arrays::toString).collect(Collectors.joining());
      }

      public boolean isFull() {
         return this.data[this.data.length - 1] != 9223372036854775807L;
      }

      public <E extends Exception> void forEach(HistogramDataConsumer<E> histogramDataConsumer) throws E {
         long[] var2 = this.data;
         int var3 = var2.length;

         for(int var4 = 0; var4 < var3; ++var4) {
            long datum = var2[var4];
            if(datum == 9223372036854775807L) {
               break;
            }

            histogramDataConsumer.consume(this.unwrapPoint(datum), this.unwrapValue(datum));
         }

      }

      public int size() {
         int[] accumulator = new int[1];
         this.forEach((point, value) -> {
            ++accumulator[0];
         });
         return accumulator[0];
      }

      public double sum(int b) {
         double sum = 0.0D;

         for(int i = 0; i < this.data.length; ++i) {
            long pointAndValue = this.data[i];
            if(pointAndValue == 9223372036854775807L) {
               break;
            }

            int point = this.unwrapPoint(pointAndValue);
            int value = this.unwrapValue(pointAndValue);
            if(point > b) {
               if(i == 0) {
                  return 0.0D;
               }

               int prevPoint = this.unwrapPoint(this.data[i - 1]);
               int prevValue = this.unwrapValue(this.data[i - 1]);
               double weight = (double)(b - prevPoint) / (double)(point - prevPoint);
               double mb = (double)prevValue + (double)(value - prevValue) * weight;
               sum -= (double)prevValue;
               sum += ((double)prevValue + mb) * weight / 2.0D;
               sum += (double)prevValue / 2.0D;
               return sum;
            }

            sum += (double)value;
         }

         return sum;
      }

      public int hashCode() {
         return Arrays.hashCode(this.data);
      }

      public boolean equals(Object o) {
         if(!(o instanceof StreamingTombstoneHistogramBuilder.DataHolder)) {
            return false;
         } else {
            StreamingTombstoneHistogramBuilder.DataHolder other = (StreamingTombstoneHistogramBuilder.DataHolder)o;
            if(this.size() != other.size()) {
               return false;
            } else {
               for(int i = 0; i < this.size(); ++i) {
                  if(this.data[i] != other.data[i]) {
                     return false;
                  }
               }

               return true;
            }
         }
      }

      static class NeighboursAndResult {
         int prevPoint;
         int nextPoint;
         StreamingTombstoneHistogramBuilder.AddResult result;

         NeighboursAndResult(int prevPoint, int nextPoint, StreamingTombstoneHistogramBuilder.AddResult result) {
            this.prevPoint = prevPoint;
            this.nextPoint = nextPoint;
            this.result = result;
         }
      }

      static class MergeResult {
         int prevPoint;
         int newPoint;
         int nextPoint;

         MergeResult(int prevPoint, int newPoint, int nextPoint) {
            this.prevPoint = prevPoint;
            this.newPoint = newPoint;
            this.nextPoint = nextPoint;
         }
      }
   }

   private static class DistanceHolder {
      private static final long EMPTY = 9223372036854775807L;
      private final long[] data;

      DistanceHolder(int maxCapacity) {
         this.data = new long[maxCapacity];
         Arrays.fill(this.data, 9223372036854775807L);
      }

      void add(int prev, int next) {
         long key = this.getKey(prev, next);
         int index = Arrays.binarySearch(this.data, key);

         assert index < 0 : "Element already exists";

         assert this.data[this.data.length - 1] == 9223372036854775807L : "No more space in array";

         index = -index - 1;
         System.arraycopy(this.data, index, this.data, index + 1, this.data.length - index - 1);
         this.data[index] = key;
      }

      void remove(int prev, int next) {
         long key = this.getKey(prev, next);
         int index = Arrays.binarySearch(this.data, key);
         if(index >= 0) {
            if(index < this.data.length) {
               System.arraycopy(this.data, index + 1, this.data, index, this.data.length - index - 1);
            }

            this.data[this.data.length - 1] = 9223372036854775807L;
         }

      }

      int[] getFirstAndRemove() {
         if(this.data[0] == 9223372036854775807L) {
            return null;
         } else {
            int[] result = this.unwrapKey(this.data[0]);
            System.arraycopy(this.data, 1, this.data, 0, this.data.length - 1);
            this.data[this.data.length - 1] = 9223372036854775807L;
            return result;
         }
      }

      private int[] unwrapKey(long key) {
         int distance = (int)(key >> 32);
         int prev = (int)(key & 4294967295L);
         return new int[]{prev, prev + distance};
      }

      private long getKey(int prev, int next) {
         long distance = (long)(next - prev);
         return distance << 32 | (long)prev;
      }

      public String toString() {
         return (String)Arrays.stream(this.data).filter((x) -> {
            return x != 9223372036854775807L;
         }).boxed().map(this::unwrapKey).map(Arrays::toString).collect(Collectors.joining());
      }
   }
}
