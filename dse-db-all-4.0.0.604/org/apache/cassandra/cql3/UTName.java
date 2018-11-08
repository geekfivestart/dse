package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;

public class UTName {
   private String ksName;
   private final ColumnIdentifier utName;

   public UTName(ColumnIdentifier ksName, ColumnIdentifier utName) {
      this.ksName = ksName == null?null:ksName.toString();
      this.utName = utName;
   }

   public boolean hasKeyspace() {
      return this.ksName != null;
   }

   public void setKeyspace(String keyspace) {
      this.ksName = keyspace;
   }

   public String getKeyspace() {
      return this.ksName;
   }

   public ByteBuffer getUserTypeName() {
      return this.utName.bytes;
   }

   public String getStringTypeName() {
      return this.utName.toString();
   }

   public String toString() {
      return (this.hasKeyspace()?this.ksName + ".":"") + this.utName;
   }
}
