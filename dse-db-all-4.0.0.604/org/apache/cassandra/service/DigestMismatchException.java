package org.apache.cassandra.service;

import java.nio.ByteBuffer;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.flow.Flow;

public class DigestMismatchException extends Exception implements Flow.NonWrappableException {
   public DigestMismatchException(ReadCommand command, ByteBuffer digest1, ByteBuffer digest2) {
      super(String.format("Mismatch for %s (%s vs %s)", new Object[]{mismatchRegion(command), ByteBufferUtil.bytesToHex(digest1), ByteBufferUtil.bytesToHex(digest2)}));
   }

   private static String mismatchRegion(ReadCommand command) {
      return command instanceof SinglePartitionReadCommand?"key " + ((SinglePartitionReadCommand)command).partitionKey():"range " + ((PartitionRangeReadCommand)command).dataRange().toCQLString(command.metadata());
   }
}
