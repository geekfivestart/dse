package org.apache.cassandra.index;

import java.util.Collection;
import org.apache.cassandra.schema.IndexMetadata;

public interface IndexRegistry {
   void registerIndex(Index var1);

   void unregisterIndex(Index var1);

   Index getIndex(IndexMetadata var1);

   Collection<Index> listIndexes();
}
