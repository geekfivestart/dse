package org.apache.cassandra.cql3;

public class CFName extends KeyspaceElementName {
   private String cfName;

   public CFName() {
   }

   public void setColumnFamily(String cf, boolean keepCase) {
      this.cfName = toInternalName(cf, keepCase);
   }

   public String getColumnFamily() {
      return this.cfName;
   }

   public String toString() {
      return super.toString() + this.cfName;
   }
}
