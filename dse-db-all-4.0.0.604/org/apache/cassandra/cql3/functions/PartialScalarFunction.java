package org.apache.cassandra.cql3.functions;

import java.nio.ByteBuffer;
import java.util.List;

public interface PartialScalarFunction extends ScalarFunction {
   Function getFunction();

   List<ByteBuffer> getPartialArguments();
}
