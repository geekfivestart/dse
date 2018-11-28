package org.apache.cassandra.service;

import java.io.IOException;
import java.util.function.Function;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.versioning.VersionDependent;
import org.apache.cassandra.utils.versioning.Versioned;

public class TruncateResponse {
   public static final Versioned<OperationsVerbs.OperationsVersion, Serializer<TruncateResponse>> serializers = OperationsVerbs.OperationsVersion.versioned(TruncateResponse.TruncateResponseSerializer::new);
   private static final TruncateResponse EMPTY = new TruncateResponse((String)null, (String)null);
   private final String keyspace;
   private final String columnFamily;

   TruncateResponse(String keyspace, String columnFamily) {
      this.keyspace = keyspace;
      this.columnFamily = columnFamily;
   }

   public static class TruncateResponseSerializer extends VersionDependent<OperationsVerbs.OperationsVersion> implements Serializer<TruncateResponse> {
      public TruncateResponseSerializer(OperationsVerbs.OperationsVersion version) {
         super(version);
      }

      public void serialize(TruncateResponse tr, DataOutputPlus out) throws IOException {
         if(((OperationsVerbs.OperationsVersion)this.version).compareTo(OperationsVerbs.OperationsVersion.DSE_60) < 0) {
            out.writeUTF(tr.keyspace);
            out.writeUTF(tr.columnFamily);
            out.writeBoolean(true);
         }
      }

      public TruncateResponse deserialize(DataInputPlus in) throws IOException {
         if(((OperationsVerbs.OperationsVersion)this.version).compareTo(OperationsVerbs.OperationsVersion.DSE_60) >= 0) {
            return TruncateResponse.EMPTY;
         } else {
            String keyspace = in.readUTF();
            String columnFamily = in.readUTF();
            in.readBoolean();
            return new TruncateResponse(keyspace, columnFamily);
         }
      }

      public long serializedSize(TruncateResponse tr) {
         return ((OperationsVerbs.OperationsVersion)this.version).compareTo(OperationsVerbs.OperationsVersion.DSE_60) >= 0?0L:(long)(TypeSizes.sizeof(tr.keyspace) + TypeSizes.sizeof(tr.columnFamily) + TypeSizes.sizeof(true));
      }
   }
}
