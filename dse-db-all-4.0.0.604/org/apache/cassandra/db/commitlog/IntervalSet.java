package org.apache.cassandra.db.commitlog;

import com.google.common.collect.ImmutableSortedMap;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.Map.Entry;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class IntervalSet<T extends Comparable<T>> {
   private static final IntervalSet EMPTY = new IntervalSet(ImmutableSortedMap.of());
   private final NavigableMap<T, T> ranges;

   private IntervalSet(ImmutableSortedMap<T, T> ranges) {
      this.ranges = ranges;
   }

   public IntervalSet(T start, T end) {
      this(ImmutableSortedMap.of(start, end));
   }

   public static <T extends Comparable<T>> IntervalSet<T> empty() {
      return EMPTY;
   }

   public boolean contains(T position) {
      Entry<T, T> range = this.ranges.floorEntry(position);
      return range != null && position.compareTo(range.getValue()) <= 0;
   }

   public boolean isEmpty() {
      return this.ranges.isEmpty();
   }

   public Optional<T> lowerBound() {
      return this.isEmpty()?Optional.empty():Optional.of(this.ranges.firstKey());
   }

   public Optional<T> upperBound() {
      return this.isEmpty()?Optional.empty():Optional.of(this.ranges.lastEntry().getValue());
   }

   public Collection<T> starts() {
      return this.ranges.keySet();
   }

   public Collection<T> ends() {
      return this.ranges.values();
   }

   public String toString() {
      return this.ranges.toString();
   }

   public int hashCode() {
      return this.ranges.hashCode();
   }

   public boolean equals(Object obj) {
      return obj instanceof IntervalSet && this.ranges.equals(((IntervalSet)obj).ranges);
   }

   public static final <T extends Comparable<T>> ISerializer<IntervalSet<T>> serializer(final ISerializer<T> pointSerializer) {
      return new ISerializer<IntervalSet<T>>() {
         public void serialize(IntervalSet<T> intervals, DataOutputPlus out) throws IOException {
            out.writeInt(intervals.ranges.size());
            Iterator var3 = intervals.ranges.entrySet().iterator();

            while(var3.hasNext()) {
               Entry<T, T> en = (Entry)var3.next();
               pointSerializer.serialize(en.getKey(), out);
               pointSerializer.serialize(en.getValue(), out);
            }

         }

         public IntervalSet<T> deserialize(DataInputPlus in) throws IOException {
            int count = in.readInt();
            NavigableMap<T, T> ranges = new TreeMap();

            for(int i = 0; i < count; ++i) {
               ranges.put(pointSerializer.deserialize(in), pointSerializer.deserialize(in));
            }

            return new IntervalSet(ImmutableSortedMap.copyOfSorted(ranges), null);
         }

         public long serializedSize(IntervalSet<T> intervals) {
            long size = (long)TypeSizes.sizeof(intervals.ranges.size());

            Entry en;
            for(Iterator var4 = intervals.ranges.entrySet().iterator(); var4.hasNext(); size += pointSerializer.serializedSize(en.getValue())) {
               en = (Entry)var4.next();
               size += pointSerializer.serializedSize(en.getKey());
            }

            return size;
         }
      };
   }

   public static class Builder<T extends Comparable<T>> {
      final NavigableMap<T, T> ranges;

      public Builder() {
         this.ranges = new TreeMap();
      }

      public Builder(T start, T end) {
         this();

         assert start.compareTo(end) <= 0;

         this.ranges.put(start, end);
      }

      public void add(T start, T end) {
         assert start.compareTo(end) <= 0;

         Entry<T, T> extend = this.ranges.floorEntry(end);
         if(extend != null && ((Comparable)extend.getValue()).compareTo(end) > 0) {
            end = (Comparable)extend.getValue();
         }

         extend = this.ranges.lowerEntry(start);
         if(extend != null && ((Comparable)extend.getValue()).compareTo(start) >= 0) {
            start = (Comparable)extend.getKey();
         }

         this.ranges.subMap(start, end).clear();
         this.ranges.put(start, end);
      }

      public void addAll(IntervalSet<T> otherSet) {
         Iterator var2 = otherSet.ranges.entrySet().iterator();

         while(var2.hasNext()) {
            Entry<T, T> en = (Entry)var2.next();
            this.add((Comparable)en.getKey(), (Comparable)en.getValue());
         }

      }

      public IntervalSet<T> build() {
         return new IntervalSet(ImmutableSortedMap.copyOfSorted(this.ranges), null);
      }
   }
}
