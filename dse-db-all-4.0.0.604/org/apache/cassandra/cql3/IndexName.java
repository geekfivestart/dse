package org.apache.cassandra.cql3;

public final class IndexName extends KeyspaceElementName {
   private String idxName;

   public IndexName() {
   }

   public void setIndex(String idx, boolean keepCase) {
      this.idxName = toInternalName(idx, keepCase);
   }

   public String getIdx() {
      return this.idxName;
   }

   public CFName getCfName() {
      CFName cfName = new CFName();
      if(this.hasKeyspace()) {
         cfName.setKeyspace(this.getKeyspace(), true);
      }

      return cfName;
   }

   public String toString() {
      return super.toString() + this.idxName;
   }
}
