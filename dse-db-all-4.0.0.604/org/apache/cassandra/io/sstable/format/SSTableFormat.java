package org.apache.cassandra.io.sstable.format;

import com.google.common.base.CharMatcher;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.trieindex.TrieIndexFormat;

public interface SSTableFormat {
   boolean enableSSTableDevelopmentTestMode = Boolean.getBoolean("cassandra.test.sstableformatdevelopment");

   Version getLatestVersion();

   Version getVersion(String var1);

   SSTableWriter.Factory getWriterFactory();

   SSTableReader.Factory getReaderFactory();

   static default SSTableFormat current() {
      return SSTableFormat.Type.TRIE_INDEX.info;
   }

   static default SSTableFormat.Type streamWriteFormat() {
      return SSTableFormat.Type.TRIE_INDEX;
   }

   boolean validateVersion(String var1);

   public static enum Type {
      BIG("big", BigFormat.instance),
      TRIE_INDEX("bti", TrieIndexFormat.instance);

      public final SSTableFormat info;
      public final String name;

      public static SSTableFormat.Type current() {
         return TRIE_INDEX;
      }

      private Type(String name, SSTableFormat info) {
         assert !CharMatcher.DIGIT.matchesAllOf(name);

         this.name = name;
         this.info = info;
      }

      public boolean validateVersion(String ver) {
         return this.info.validateVersion(ver);
      }

      public static SSTableFormat.Type validate(String name) {
         SSTableFormat.Type[] var1 = values();
         int var2 = var1.length;

         for(int var3 = 0; var3 < var2; ++var3) {
            SSTableFormat.Type valid = var1[var3];
            if(valid.name.equalsIgnoreCase(name)) {
               return valid;
            }
         }

         throw new IllegalArgumentException("No Type constant " + name);
      }
   }
}
