package org.apache.cassandra.cql3.functions;

import java.nio.ByteBuffer;
import org.apache.cassandra.transport.ProtocolVersion;

public abstract class JavaUDF {
   protected final UDFContext udfContext;
   private UDFDataType returnType;

   protected JavaUDF(UDFDataType returnType, UDFContext udfContext) {
      this.returnType = returnType;
      this.udfContext = udfContext;
   }

   protected abstract ByteBuffer executeImpl(Arguments var1);

   protected abstract Object executeAggregateImpl(Object var1, Arguments var2);

   protected final ByteBuffer decompose(ProtocolVersion protocolVersion, Object value) {
      return this.returnType.decompose(protocolVersion, value);
   }

   protected final ByteBuffer decompose(ProtocolVersion protocolVersion, byte value) {
      return this.returnType.decompose(protocolVersion, value);
   }

   protected final ByteBuffer decompose(ProtocolVersion protocolVersion, short value) {
      return this.returnType.decompose(protocolVersion, value);
   }

   protected final ByteBuffer decompose(ProtocolVersion protocolVersion, int value) {
      return this.returnType.decompose(protocolVersion, value);
   }

   protected final ByteBuffer decompose(ProtocolVersion protocolVersion, long value) {
      return this.returnType.decompose(protocolVersion, value);
   }

   protected final ByteBuffer decompose(ProtocolVersion protocolVersion, float value) {
      return this.returnType.decompose(protocolVersion, value);
   }

   protected final ByteBuffer decompose(ProtocolVersion protocolVersion, double value) {
      return this.returnType.decompose(protocolVersion, value);
   }
}
