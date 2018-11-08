package org.apache.cassandra.cql3.functions;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.transport.ProtocolVersion;

public interface ScalarFunction extends Function {
   boolean isCalledOnNullInput();

   default boolean isMonotonic() {
      return false;
   }

   ByteBuffer execute(Arguments var1) throws InvalidRequestException;

   default ScalarFunction partialApplication(ProtocolVersion protocolVersion, List<ByteBuffer> partialArguments) {
      int unresolvedCount = 0;
      Iterator var4 = partialArguments.iterator();

      while(var4.hasNext()) {
         ByteBuffer parameter = (ByteBuffer)var4.next();
         if(parameter == UNRESOLVED) {
            ++unresolvedCount;
         }
      }

      if(unresolvedCount == this.argTypes().size()) {
         return this;
      } else if(this.isDeterministic() && unresolvedCount == 0) {
         Arguments arguments = this.newArguments(protocolVersion);
         int i = 0;

         for(int m = partialArguments.size(); i < m; ++i) {
            arguments.set(i, (ByteBuffer)partialArguments.get(i));
         }

         return new PreComputedFunction(this.returnType(), this.execute(arguments), protocolVersion, this, partialArguments);
      } else {
         return new PartiallyAppliedScalarFunction(this, partialArguments, unresolvedCount);
      }
   }

   default boolean isPartialApplicationMonotonic(List<ByteBuffer> partialParameters) {
      return this.isMonotonic();
   }
}
