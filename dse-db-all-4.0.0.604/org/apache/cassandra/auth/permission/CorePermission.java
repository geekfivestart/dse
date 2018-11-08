package org.apache.cassandra.auth.permission;

import org.apache.cassandra.auth.Permission;

public enum CorePermission implements Permission {
   /** @deprecated */
   @Deprecated
   READ,
   /** @deprecated */
   @Deprecated
   WRITE,
   CREATE,
   ALTER,
   DROP,
   SELECT,
   MODIFY,
   AUTHORIZE,
   DESCRIBE,
   EXECUTE;

   private CorePermission() {
   }

   public String domain() {
      return getDomain();
   }

   public String getFullName() {
      return this.name();
   }

   public static String getDomain() {
      return "CORE";
   }
}
