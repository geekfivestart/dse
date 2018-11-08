package org.apache.cassandra.auth;

import java.io.Serializable;
import java.security.Principal;

public class CassandraPrincipal implements Principal, Serializable {
   private static final long serialVersionUID = 1L;
   private final String name;

   public CassandraPrincipal(String name) {
      if(name == null) {
         throw new NullPointerException("illegal null input");
      } else {
         this.name = name;
      }
   }

   public String getName() {
      return this.name;
   }

   public String toString() {
      return "CassandraPrincipal:  " + this.name;
   }

   public boolean equals(Object o) {
      if(o == null) {
         return false;
      } else if(this == o) {
         return true;
      } else if(!(o instanceof CassandraPrincipal)) {
         return false;
      } else {
         CassandraPrincipal that = (CassandraPrincipal)o;
         return this.getName().equals(that.getName());
      }
   }

   public int hashCode() {
      return this.name.hashCode();
   }
}
