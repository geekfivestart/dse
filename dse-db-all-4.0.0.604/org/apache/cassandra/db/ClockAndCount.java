package org.apache.cassandra.db;

import java.util.Objects;
import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.utils.ObjectSizes;

public class ClockAndCount implements IMeasurableMemory {
   private static final long EMPTY_SIZE = ObjectSizes.measure(new ClockAndCount(0L, 0L));
   public static ClockAndCount BLANK = create(0L, 0L);
   public final long clock;
   public final long count;

   private ClockAndCount(long clock, long count) {
      this.clock = clock;
      this.count = count;
   }

   public static ClockAndCount create(long clock, long count) {
      return new ClockAndCount(clock, count);
   }

   public long unsharedHeapSize() {
      return EMPTY_SIZE;
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(!(o instanceof ClockAndCount)) {
         return false;
      } else {
         ClockAndCount other = (ClockAndCount)o;
         return this.clock == other.clock && this.count == other.count;
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{Long.valueOf(this.clock), Long.valueOf(this.count)});
   }

   public String toString() {
      return String.format("ClockAndCount(%s,%s)", new Object[]{Long.valueOf(this.clock), Long.valueOf(this.count)});
   }
}
