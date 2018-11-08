package org.apache.cassandra.cql3;

import java.util.Locale;

public class RoleName {
   private String name;

   public RoleName() {
   }

   public void setName(String name, boolean keepCase) {
      this.name = keepCase?name:(name == null?name:name.toLowerCase(Locale.US));
   }

   public boolean hasName() {
      return this.name != null;
   }

   public String getName() {
      return this.name;
   }

   public String toString() {
      return this.name;
   }
}
