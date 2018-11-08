package org.apache.cassandra.db;

import org.apache.cassandra.utils.Serializer;

public class Truncation {
   public static final Serializer<Truncation> serializer = new TruncationSerializer();
   public final String keyspace;
   public final String columnFamily;

   public Truncation(String keyspace, String columnFamily) {
      this.keyspace = keyspace;
      this.columnFamily = columnFamily;
   }

   public String toString() {
      return "Truncation(keyspace='" + this.keyspace + '\'' + ", cf='" + this.columnFamily + "')";
   }
}
