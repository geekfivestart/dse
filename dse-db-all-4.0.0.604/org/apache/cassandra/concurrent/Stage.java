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
        switch (this) {
            case ANTI_ENTROPY:
            case GOSSIP:
            case MIGRATION:
            case MISC:
            case INTERNAL_RESPONSE:
            case BACKGROUND_IO: {
                return "internal";
            }
            case REQUEST_RESPONSE:
            case READ_REPAIR:
            case AUTHZ: {
                return "request";
            }
        }
        throw new AssertionError((Object)("Unknown stage " + (Object)((Object)this)));
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
