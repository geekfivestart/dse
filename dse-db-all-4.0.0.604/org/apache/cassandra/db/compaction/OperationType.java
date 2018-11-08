package org.apache.cassandra.db.compaction;

import com.google.common.base.Predicate;

public enum OperationType {
   COMPACTION("Compaction"),
   VALIDATION("Validation"),
   KEY_CACHE_SAVE("Key cache save"),
   ROW_CACHE_SAVE("Row cache save"),
   COUNTER_CACHE_SAVE("Counter cache save"),
   CLEANUP("Cleanup"),
   SCRUB("Scrub"),
   UPGRADE_SSTABLES("Upgrade sstables"),
   INDEX_BUILD("Secondary index build"),
   TOMBSTONE_COMPACTION("Tombstone Compaction"),
   UNKNOWN("Unknown compaction type"),
   ANTICOMPACTION("Anticompaction after repair"),
   VERIFY("Verify"),
   FLUSH("Flush"),
   STREAM("Stream"),
   WRITE("Write"),
   VIEW_BUILD("View build"),
   INDEX_SUMMARY("Index summary redistribution"),
   RELOCATE("Relocate sstables to correct disk"),
   GARBAGE_COLLECT("Remove deleted data");

   public final String type;
   public final String fileName;
   public static final Predicate<OperationType> EXCEPT_VALIDATIONS = (o) -> {
      return o != VALIDATION;
   };
   public static final Predicate<OperationType> COMPACTIONS_ONLY = (o) -> {
      return o == COMPACTION || o == TOMBSTONE_COMPACTION;
   };

   private OperationType(String type) {
      this.type = type;
      this.fileName = type.toLowerCase().replace(" ", "");
   }

   public static OperationType fromFileName(String fileName) {
      OperationType[] var1 = values();
      int var2 = var1.length;

      for(int var3 = 0; var3 < var2; ++var3) {
         OperationType opType = var1[var3];
         if(opType.fileName.equals(fileName)) {
            return opType;
         }
      }

      throw new IllegalArgumentException("Invalid fileName for operation type: " + fileName);
   }

   public String toString() {
      return this.type;
   }
}
