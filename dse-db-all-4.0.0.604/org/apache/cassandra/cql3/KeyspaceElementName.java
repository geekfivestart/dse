package org.apache.cassandra.cql3;

import java.util.Locale;

abstract class KeyspaceElementName {
   private String ksName;

   KeyspaceElementName() {
   }

   public final void setKeyspace(String ks, boolean keepCase) {
      this.ksName = toInternalName(ks, keepCase);
   }

   public final boolean hasKeyspace() {
      return this.ksName != null;
   }

   public final String getKeyspace() {
      return this.ksName;
   }

   protected static String toInternalName(String name, boolean keepCase) {
      return keepCase?name:name.toLowerCase(Locale.US);
   }

   public String toString() {
      return this.hasKeyspace()?this.getKeyspace() + ".":"";
   }
}
