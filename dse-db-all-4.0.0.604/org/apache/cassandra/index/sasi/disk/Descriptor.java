package org.apache.cassandra.index.sasi.disk;

public class Descriptor {
   public static final String VERSION_AA = "aa";
   public static final String VERSION_AB = "ab";
   public static final String CURRENT_VERSION = "ab";
   public static final Descriptor CURRENT = new Descriptor("ab");
   public final Descriptor.Version version;

   public Descriptor(String v) {
      this.version = new Descriptor.Version(v);
   }

   public static class Version {
      public final String version;

      public Version(String version) {
         this.version = version;
      }

      public String toString() {
         return this.version;
      }
   }
}
