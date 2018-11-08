package org.apache.cassandra.streaming;

public enum StreamOperation {
   OTHER("Other"),
   RESTORE_REPLICA_COUNT("Restore replica count", false),
   DECOMMISSION("Unbootstrap", false),
   RELOCATION("Relocation", false),
   BOOTSTRAP("Bootstrap", false),
   REBUILD("Rebuild", false),
   BULK_LOAD("Bulk Load"),
   REPAIR("Repair");

   private final String description;
   private final boolean requiresViewBuild;

   private StreamOperation(String description) {
      this(description, true);
   }

   private StreamOperation(String description, boolean requiresViewBuild) {
      this.description = description;
      this.requiresViewBuild = requiresViewBuild;
   }

   public static StreamOperation fromString(String text) {
      StreamOperation[] var1 = values();
      int var2 = var1.length;

      for(int var3 = 0; var3 < var2; ++var3) {
         StreamOperation b = var1[var3];
         if(b.description.equalsIgnoreCase(text)) {
            return b;
         }
      }

      return OTHER;
   }

   public String getDescription() {
      return this.description;
   }

   public boolean requiresViewBuild() {
      return this.requiresViewBuild;
   }
}
