package org.apache.cassandra.triggers;

import java.util.Collection;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.Partition;

public interface ITrigger {
   Collection<Mutation> augmentNonBlocking(Partition var1);
}
