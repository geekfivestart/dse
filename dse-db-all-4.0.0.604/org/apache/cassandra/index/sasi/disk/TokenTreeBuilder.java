package org.apache.cassandra.index.sasi.disk;

import com.carrotsearch.hppc.LongSet;
import java.io.IOException;
import java.util.Iterator;
import java.util.SortedMap;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.Pair;

public interface TokenTreeBuilder extends Iterable<Pair<Long, LongSet>> {
   int BLOCK_BYTES = 4096;
   int BLOCK_HEADER_BYTES = 64;
   int BLOCK_ENTRY_BYTES = 16;
   int OVERFLOW_TRAILER_BYTES = 64;
   int OVERFLOW_ENTRY_BYTES = 8;
   int OVERFLOW_TRAILER_CAPACITY = 8;
   int TOKENS_PER_BLOCK = 248;
   long MAX_OFFSET = 140737488355327L;
   byte LAST_LEAF_SHIFT = 1;
   byte SHARED_HEADER_BYTES = 19;
   byte ENTRY_TYPE_MASK = 3;
   short AB_MAGIC = 23121;

   void add(Long var1, long var2);

   void add(SortedMap<Long, LongSet> var1);

   void add(Iterator<Pair<Long, LongSet>> var1);

   void add(TokenTreeBuilder var1);

   boolean isEmpty();

   long getTokenCount();

   TokenTreeBuilder finish();

   int serializedSize();

   void write(DataOutputPlus var1) throws IOException;

   public static enum EntryType {
      SIMPLE,
      FACTORED,
      PACKED,
      OVERFLOW;

      private EntryType() {
      }

      public static TokenTreeBuilder.EntryType of(int ordinal) {
         if(ordinal == SIMPLE.ordinal()) {
            return SIMPLE;
         } else if(ordinal == FACTORED.ordinal()) {
            return FACTORED;
         } else if(ordinal == PACKED.ordinal()) {
            return PACKED;
         } else if(ordinal == OVERFLOW.ordinal()) {
            return OVERFLOW;
         } else {
            throw new IllegalArgumentException("Unknown ordinal: " + ordinal);
         }
      }
   }
}
