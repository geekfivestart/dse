package org.apache.cassandra.cql3;

import com.google.common.collect.MapMaker;
import java.nio.ByteBuffer;
import java.util.Locale;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.memory.AbstractAllocator;

public class ColumnIdentifier implements IMeasurableMemory, Comparable<ColumnIdentifier> {
   private static final Pattern PATTERN_DOUBLE_QUOTE = Pattern.compile("\"", 16);
   private static final String ESCAPED_DOUBLE_QUOTE = Matcher.quoteReplacement("\"\"");
   public final ByteBuffer bytes;
   private final String text;
   public final long prefixComparison;
   private final boolean interned;
   private static final Pattern UNQUOTED_IDENTIFIER = Pattern.compile("[a-z][a-z0-9_]*");
   private static final long EMPTY_SIZE;
   private static final ConcurrentMap<ColumnIdentifier.InternedKey, ColumnIdentifier> internedInstances;
   private int hashCode;

   private static long prefixComparison(ByteBuffer bytes) {
      long prefix = 0L;
      ByteBuffer read = bytes.duplicate();

      int i;
      for(i = 0; read.hasRemaining() && i < 8; ++i) {
         prefix <<= 8;
         prefix |= (long)(read.get() & 255);
      }

      prefix <<= (8 - i) * 8;
      prefix ^= -9223372036854775808L;
      return prefix;
   }

   public ColumnIdentifier(String rawText, boolean keepCase) {
      this.hashCode = -1;
      this.text = keepCase?rawText:rawText.toLowerCase(Locale.US);
      this.bytes = ByteBufferUtil.bytes(this.text);
      this.prefixComparison = prefixComparison(this.bytes);
      this.interned = false;
   }

   public ColumnIdentifier(ByteBuffer bytes, AbstractType<?> type) {
      this(bytes, type.getString(bytes), false);
   }

   private ColumnIdentifier(ByteBuffer bytes, String text, boolean interned) {
      this.hashCode = -1;
      this.bytes = bytes;
      this.text = text;
      this.interned = interned;
      this.prefixComparison = prefixComparison(bytes);
   }

   public static ColumnIdentifier getInterned(ByteBuffer bytes, AbstractType<?> type) {
      return getInterned(type, bytes, type.getString(bytes));
   }

   public static ColumnIdentifier getInterned(String rawText, boolean keepCase) {
      String text = keepCase?rawText:rawText.toLowerCase(Locale.US);
      ByteBuffer bytes = ByteBufferUtil.bytes(text);
      return getInterned(UTF8Type.instance, bytes, text);
   }

   public static ColumnIdentifier getInterned(AbstractType<?> type, ByteBuffer bytes, String text) {
      bytes = ByteBufferUtil.minimalBufferFor(bytes);
      ColumnIdentifier.InternedKey key = new ColumnIdentifier.InternedKey(type, bytes);
      ColumnIdentifier id = (ColumnIdentifier)internedInstances.get(key);
      if(id != null) {
         return id;
      } else {
         ColumnIdentifier created = new ColumnIdentifier(bytes, text, true);
         ColumnIdentifier previous = (ColumnIdentifier)internedInstances.putIfAbsent(key, created);
         return previous == null?created:previous;
      }
   }

   public boolean isInterned() {
      return this.interned;
   }

   public final int hashCode() {
      int hashCode = this.hashCode;
      if(hashCode == -1) {
         hashCode = this.bytes.hashCode();
         this.hashCode = hashCode;
      }

      return hashCode;
   }

   public final boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(!(o instanceof ColumnIdentifier)) {
         return false;
      } else {
         ColumnIdentifier that = (ColumnIdentifier)o;
         return this.bytes.equals(that.bytes);
      }
   }

   public String toString() {
      return this.text;
   }

   public String toCQLString() {
      return maybeQuote(this.text);
   }

   public long unsharedHeapSize() {
      return EMPTY_SIZE + ObjectSizes.sizeOnHeapOf(this.bytes) + ObjectSizes.sizeOf(this.text);
   }

   public long unsharedHeapSizeExcludingData() {
      return EMPTY_SIZE + ObjectSizes.sizeOnHeapExcludingData(this.bytes) + ObjectSizes.sizeOf(this.text);
   }

   public ColumnIdentifier clone(AbstractAllocator allocator) {
      return this.interned?this:new ColumnIdentifier(allocator.clone(this.bytes), this.text, false);
   }

   public int compareTo(ColumnIdentifier that) {
      int c = Long.compare(this.prefixComparison, that.prefixComparison);
      return c != 0?c:(this == that?0:ByteBufferUtil.compareUnsigned(this.bytes, that.bytes));
   }

   public static String maybeQuote(String text) {
      return UNQUOTED_IDENTIFIER.matcher(text).matches() && !ReservedKeywords.isReserved(text)?text:'"' + PATTERN_DOUBLE_QUOTE.matcher(text).replaceAll(ESCAPED_DOUBLE_QUOTE) + '"';
   }

   static {
      EMPTY_SIZE = ObjectSizes.measure(new ColumnIdentifier(ByteBufferUtil.EMPTY_BYTE_BUFFER, "", false));
      internedInstances = (new MapMaker()).weakValues().makeMap();
   }

   private static final class InternedKey {
      private final AbstractType<?> type;
      private final ByteBuffer bytes;
      private int hashCode = -1;

      InternedKey(AbstractType<?> type, ByteBuffer bytes) {
         this.type = type;
         this.bytes = bytes;
      }

      public boolean equals(Object o) {
         if(this == o) {
            return true;
         } else if(o != null && this.getClass() == o.getClass()) {
            ColumnIdentifier.InternedKey that = (ColumnIdentifier.InternedKey)o;
            return this.bytes.equals(that.bytes) && this.type.equals(that.type);
         } else {
            return false;
         }
      }

      public int hashCode() {
         int currHashCode = this.hashCode;
         if(currHashCode == -1) {
            currHashCode = this.bytes.hashCode() + 31 * this.type.hashCode();
            this.hashCode = currHashCode;
         }

         return currHashCode;
      }
   }
}
