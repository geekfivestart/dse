package org.apache.cassandra.cql3.functions;

import java.nio.ByteBuffer;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.transport.ProtocolVersion;

public interface AggregateFunction extends Function {
   AggregateFunction.Aggregate newAggregate() throws InvalidRequestException;

   public interface Aggregate {
      void addInput(Arguments var1) throws InvalidRequestException;

      ByteBuffer compute(ProtocolVersion var1) throws InvalidRequestException;

      void reset();
   }
}
