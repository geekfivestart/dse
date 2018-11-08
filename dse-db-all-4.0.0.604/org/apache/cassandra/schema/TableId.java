package org.apache.cassandra.schema;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.commons.lang3.ArrayUtils;

public class TableId implements Comparable<TableId>, Serializable {
   private static final long serialVersionUID = 42L;
   private final UUID id;

   private TableId(UUID id) {
      this.id = id;
   }

   public static TableId fromUUID(UUID id) {
      return new TableId(id);
   }

   public static TableId generate() {
      return new TableId(UUIDGen.getTimeUUID());
   }

   public static TableId fromString(String idString) {
      return new TableId(UUID.fromString(idString));
   }

   private static TableId fromHexString(String nonDashUUID) {
      ByteBuffer bytes = ByteBufferUtil.hexToBytes(nonDashUUID);
      long msb = bytes.getLong(0);
      long lsb = bytes.getLong(8);
      return fromUUID(new UUID(msb, lsb));
   }

   public static Pair<String, TableId> tableNameAndIdFromFilename(String filename) {
      int dash = filename.lastIndexOf(45);
      if(dash > 0 && dash == filename.length() - 32 - 1) {
         TableId id = fromHexString(filename.substring(dash + 1));
         String tableName = filename.substring(0, dash);
         return Pair.create(tableName, id);
      } else {
         return null;
      }
   }

   public static TableId forSystemTable(String keyspace, String table) {
      assert SchemaConstants.isLocalSystemKeyspace(keyspace) || SchemaConstants.isReplicatedSystemKeyspace(keyspace);

      return new TableId(UUID.nameUUIDFromBytes(ArrayUtils.addAll(keyspace.getBytes(), table.getBytes())));
   }

   public String toHexString() {
      return ByteBufferUtil.bytesToHex(ByteBufferUtil.bytes(this.id));
   }

   public UUID asUUID() {
      return this.id;
   }

   public final int hashCode() {
      return this.id.hashCode();
   }

   public final boolean equals(Object o) {
      return this == o || o instanceof TableId && this.id.equals(((TableId)o).id);
   }

   public String toString() {
      return this.id.toString();
   }

   public void serialize(DataOutput out) throws IOException {
      out.writeLong(this.id.getMostSignificantBits());
      out.writeLong(this.id.getLeastSignificantBits());
   }

   public int serializedSize() {
      return 16;
   }

   public static TableId deserialize(DataInput in) throws IOException {
      return new TableId(new UUID(in.readLong(), in.readLong()));
   }

   public int compareTo(TableId tableId) {
      return this.id.compareTo(tableId.id);
   }
}
