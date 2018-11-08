package org.apache.cassandra.db;

import java.io.IOException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.Serializer;

public class SnapshotCommand {
   public static final Serializer<SnapshotCommand> serializer = new Serializer<SnapshotCommand>() {
      public void serialize(SnapshotCommand command, DataOutputPlus out) throws IOException {
         out.writeUTF(command.keyspace);
         out.writeUTF(command.table);
         out.writeUTF(command.snapshotName);
         out.writeBoolean(command.clearSnapshot);
      }

      public SnapshotCommand deserialize(DataInputPlus in) throws IOException {
         String keyspace = in.readUTF();
         String table = in.readUTF();
         String snapshotName = in.readUTF();
         boolean clearSnapshot = in.readBoolean();
         return new SnapshotCommand(keyspace, table, snapshotName, clearSnapshot);
      }

      public long serializedSize(SnapshotCommand command) {
         return (long)(TypeSizes.sizeof(command.keyspace) + TypeSizes.sizeof(command.table) + TypeSizes.sizeof(command.snapshotName) + TypeSizes.sizeof(command.clearSnapshot));
      }
   };
   public final String keyspace;
   public final String table;
   public final String snapshotName;
   public final boolean clearSnapshot;

   public SnapshotCommand(String keyspace, String columnFamily, String snapshotName, boolean clearSnapshot) {
      this.keyspace = keyspace;
      this.table = columnFamily;
      this.snapshotName = snapshotName;
      this.clearSnapshot = clearSnapshot;
   }

   public String toString() {
      return "SnapshotCommand{keyspace='" + this.keyspace + '\'' + ", table='" + this.table + '\'' + ", snapshotName=" + this.snapshotName + ", clearSnapshot=" + this.clearSnapshot + '}';
   }
}
