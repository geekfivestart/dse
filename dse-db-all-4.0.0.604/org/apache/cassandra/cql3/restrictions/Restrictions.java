package org.apache.cassandra.cql3.restrictions;

import java.util.Set;
import org.apache.cassandra.schema.ColumnMetadata;

public interface Restrictions extends Restriction {
   Set<Restriction> getRestrictions(ColumnMetadata var1);

   boolean isEmpty();

   int size();

   boolean hasIN();

   boolean hasContains();

   boolean hasSlice();

   boolean hasOnlyEqualityRestrictions();
}
