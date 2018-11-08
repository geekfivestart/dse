package org.apache.cassandra.service;

import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.InvalidRequestException;

public interface CASRequest {
   SinglePartitionReadCommand readCommand(int var1);

   boolean appliesTo(FilteredPartition var1) throws InvalidRequestException;

   PartitionUpdate makeUpdates(FilteredPartition var1) throws InvalidRequestException;
}
