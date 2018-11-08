package org.apache.cassandra.cql3.functions;

import java.util.Arrays;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.transport.ProtocolVersion;

public abstract class NativeFunction extends AbstractFunction {
   protected NativeFunction(String name, AbstractType<?> returnType, AbstractType... argTypes) {
      super(FunctionName.nativeFunction(name), Arrays.asList(argTypes), returnType);
   }

   public boolean isDeterministic() {
      return true;
   }

   public Arguments newArguments(ProtocolVersion version) {
      return FunctionArguments.newInstanceForNativeFunction(version, this.argTypes);
   }

   public boolean isNative() {
      return true;
   }
}
