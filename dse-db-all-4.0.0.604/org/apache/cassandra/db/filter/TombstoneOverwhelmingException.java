package org.apache.cassandra.db.filter;

import java.nio.ByteBuffer;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.exceptions.InternalRequestExecutionException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.schema.TableMetadata;

public class TombstoneOverwhelmingException extends InternalRequestExecutionException {
   public TombstoneOverwhelmingException(int numTombstones, String query, TableMetadata metadata, DecoratedKey lastPartitionKey, ClusteringPrefix lastClustering) {
      super(RequestFailureReason.READ_TOO_MANY_TOMBSTONES, String.format("Scanned over %d tombstones during query '%s' (last scanned row partion key was (%s)); query aborted", new Object[]{Integer.valueOf(numTombstones), query, makePKString(metadata, lastPartitionKey.getKey(), lastClustering)}));
   }

   private static String makePKString(TableMetadata metadata, ByteBuffer partitionKey, ClusteringPrefix clustering) {
      StringBuilder sb = new StringBuilder();
      if(clustering.size() > 0) {
         sb.append("(");
      }

      AbstractType<?> pkType = metadata.partitionKeyType;
      if(pkType instanceof CompositeType) {
         CompositeType ct = (CompositeType)pkType;
         ByteBuffer[] values = ct.split(partitionKey);

         for(int i = 0; i < values.length; ++i) {
            if(i > 0) {
               sb.append(", ");
            }

            sb.append(((AbstractType)ct.types.get(i)).getString(values[i]));
         }
      } else {
         sb.append(pkType.getString(partitionKey));
      }

      if(clustering.size() > 0) {
         sb.append(")");
      }

      for(int i = 0; i < clustering.size(); ++i) {
         sb.append(", ").append(metadata.comparator.subtype(i).getString(clustering.get(i)));
      }

      return sb.toString();
   }
}
