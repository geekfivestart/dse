package org.apache.cassandra.concurrent;

public enum Stage {
   GOSSIP,
   REQUEST_RESPONSE,
   ANTI_ENTROPY,
   MIGRATION,
   MISC,
   INTERNAL_RESPONSE,
   READ_REPAIR,
   BACKGROUND_IO,
   AUTHZ;

   private Stage() {
   }

   public String getJmxType() {
      switch(null.$SwitchMap$org$apache$cassandra$concurrent$Stage[this.ordinal()]) {
      case 1:
      case 2:
      case 3:
      case 4:
      case 5:
      case 6:
         return "internal";
      case 7:
      case 8:
      case 9:
         return "request";
      default:
         throw new AssertionError("Unknown stage " + this);
      }
   }

   public String getJmxName() {
      String name = "";
      String[] var2 = this.toString().split("_");
      int var3 = var2.length;

      for(int var4 = 0; var4 < var3; ++var4) {
         String word = var2[var4];
         name = name + word.substring(0, 1) + word.substring(1).toLowerCase();
      }

      return name + "Stage";
   }
}
