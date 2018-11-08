package org.apache.cassandra.cql3.restrictions;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.schema.TableMetadata;

interface PartitionKeyRestrictions extends Restrictions {
   PartitionKeyRestrictions mergeWith(Restriction var1);

   List<ByteBuffer> values(QueryOptions var1);

   List<ByteBuffer> bounds(Bound var1, QueryOptions var2);

   boolean hasBound(Bound var1);

   boolean isInclusive(Bound var1);

   boolean needFiltering(TableMetadata var1);

   boolean hasUnrestrictedPartitionKeyComponents(TableMetadata var1);
}
