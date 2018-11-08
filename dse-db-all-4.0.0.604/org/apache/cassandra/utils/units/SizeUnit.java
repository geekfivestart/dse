package org.apache.cassandra.utils.units;

import com.google.common.annotations.VisibleForTesting;

public enum SizeUnit {
   BYTES("B") {
      public long convert(long s, SizeUnit u) {
         return u.toBytes(s);
      }

      public long toBytes(long s) {
         return s;
      }

      public long toKiloBytes(long s) {
         return s / 1024L;
      }

      public long toMegaBytes(long s) {
         return s / 1048576L;
      }

      public long toGigaBytes(long s) {
         return s / 1073741824L;
      }

      public long toTeraBytes(long s) {
         return s / 1099511627776L;
      }
   },
   KILOBYTES("kB") {
      public long convert(long s, SizeUnit u) {
         return u.toKiloBytes(s);
      }

      public long toBytes(long s) {
         return x(s, 1024L, 9007199254740991L);
      }

      public long toKiloBytes(long s) {
         return s;
      }

      public long toMegaBytes(long s) {
         return s / 1024L;
      }

      public long toGigaBytes(long s) {
         return s / 1048576L;
      }

      public long toTeraBytes(long s) {
         return s / 1073741824L;
      }
   },
   MEGABYTES("MB") {
      public long convert(long s, SizeUnit u) {
         return u.toMegaBytes(s);
      }

      public long toBytes(long s) {
         return x(s, 1048576L, 8796093022207L);
      }

      public long toKiloBytes(long s) {
         return x(s, 1024L, 9007199254740991L);
      }

      public long toMegaBytes(long s) {
         return s;
      }

      public long toGigaBytes(long s) {
         return s / 1024L;
      }

      public long toTeraBytes(long s) {
         return s / 1048576L;
      }
   },
   GIGABYTES("GB") {
      public long convert(long s, SizeUnit u) {
         return u.toGigaBytes(s);
      }

      public long toBytes(long s) {
         return x(s, 1073741824L, 8589934591L);
      }

      public long toKiloBytes(long s) {
         return x(s, 1048576L, 8796093022207L);
      }

      public long toMegaBytes(long s) {
         return x(s, 1024L, 9007199254740991L);
      }

      public long toGigaBytes(long s) {
         return s;
      }

      public long toTeraBytes(long s) {
         return s / 1024L;
      }
   },
   TERABYTES("TB") {
      public long convert(long s, SizeUnit u) {
         return u.toTeraBytes(s);
      }

      public long toBytes(long s) {
         return x(s, 1099511627776L, 8388607L);
      }

      public long toKiloBytes(long s) {
         return x(s, 1073741824L, 8589934591L);
      }

      public long toMegaBytes(long s) {
         return x(s, 1048576L, 8796093022207L);
      }

      public long toGigaBytes(long s) {
         return x(s, 1024L, 9007199254740991L);
      }

      public long toTeraBytes(long s) {
         return s;
      }
   };

   public final String symbol;
   static final long C0 = 1L;
   static final long C1 = 1024L;
   static final long C2 = 1048576L;
   static final long C3 = 1073741824L;
   static final long C4 = 1099511627776L;
   private static final long MAX = 9223372036854775807L;

   private SizeUnit(String symbol) {
      this.symbol = symbol;
   }

   @VisibleForTesting
   static long x(long d, long m, long over) {
      return d > over?9223372036854775807L:(d < -over?-9223372036854775808L:d * m);
   }

   public abstract long convert(long var1, SizeUnit var3);

   public abstract long toBytes(long var1);

   public abstract long toKiloBytes(long var1);

   public abstract long toMegaBytes(long var1);

   public abstract long toGigaBytes(long var1);

   public abstract long toTeraBytes(long var1);

   public SizeValue value(long value) {
      return SizeValue.of(value, this);
   }

   public String toHRString(long value) {
      return Units.toString(value, this);
   }

   public String toLogString(long value) {
      return Units.toLogString(value, this);
   }

   public String toString(long value) {
      return Units.formatValue(value) + this.symbol;
   }

   SizeUnit smallestRepresentableUnit(long value) {
      int i;
      for(i = this.ordinal(); i > 0 && value < 9223372036854775807L; --i) {
         value = x(value, 1024L, 9007199254740991L);
      }

      return values()[i];
   }
}
