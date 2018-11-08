package org.apache.cassandra.cql3.functions;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.transport.ProtocolVersion;

class PreComputedFunction extends NativeScalarFunction implements PartialScalarFunction {
   private final ByteBuffer value;
   private final ProtocolVersion valueVersion;
   private final ScalarFunction function;
   private final List<ByteBuffer> arguments;

   PreComputedFunction(AbstractType<?> returnType, ByteBuffer value, ProtocolVersion valueVersion, ScalarFunction function, List<ByteBuffer> arguments) {
      super("__constant__", returnType, new AbstractType[0]);
      this.value = value;
      this.valueVersion = valueVersion;
      this.function = function;
      this.arguments = arguments;
   }

   public Function getFunction() {
      return this.function;
   }

   public List<ByteBuffer> getPartialArguments() {
      return this.arguments;
   }

   public ByteBuffer execute(Arguments nothing) throws InvalidRequestException {
      if(nothing.getProtocolVersion() == this.valueVersion) {
         return this.value;
      } else {
         Arguments args = this.function.newArguments(nothing.getProtocolVersion());
         int i = 0;

         for(int m = this.arguments.size(); i < m; ++i) {
            args.set(i, (ByteBuffer)this.arguments.get(i));
         }

         return this.function.execute(args);
      }
   }

   public ScalarFunction partialApplication(ProtocolVersion protocolVersion, List<ByteBuffer> nothing) throws InvalidRequestException {
      return this;
   }
}
