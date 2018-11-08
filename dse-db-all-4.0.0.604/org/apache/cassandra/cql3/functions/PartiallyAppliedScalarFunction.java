package org.apache.cassandra.cql3.functions;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.transport.ProtocolVersion;

final class PartiallyAppliedScalarFunction extends NativeScalarFunction implements PartialScalarFunction {
   private final ScalarFunction function;
   private final List<ByteBuffer> partialParameters;

   PartiallyAppliedScalarFunction(ScalarFunction function, List<ByteBuffer> partialParameters, int unresolvedCount) {
      super("__partial_application__", function.returnType(), computeArgTypes(function, partialParameters, unresolvedCount));
      this.function = function;
      this.partialParameters = partialParameters;
   }

   public Arguments newArguments(ProtocolVersion version) {
      return new PartiallyAppliedScalarFunction.PartialFunctionArguments(version, this.function, this.partialParameters, this.argTypes.size());
   }

   public boolean isMonotonic() {
      return this.function.isPartialApplicationMonotonic(this.partialParameters);
   }

   public boolean isDeterministic() {
      return this.function.isDeterministic();
   }

   public Function getFunction() {
      return this.function;
   }

   public List<ByteBuffer> getPartialArguments() {
      return this.partialParameters;
   }

   private static AbstractType[] computeArgTypes(ScalarFunction function, List<ByteBuffer> partialParameters, int unresolvedCount) {
      AbstractType[] argTypes = new AbstractType[unresolvedCount];
      int arg = 0;

      for(int i = 0; i < partialParameters.size(); ++i) {
         if(partialParameters.get(i) == UNRESOLVED) {
            argTypes[arg++] = (AbstractType)function.argTypes().get(i);
         }
      }

      return argTypes;
   }

   public ByteBuffer execute(Arguments arguments) throws InvalidRequestException {
      return this.function.execute(arguments);
   }

   public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(this.function.name()).append(" : (");
      List<AbstractType<?>> types = this.function.argTypes();
      int i = 0;

      for(int m = types.size(); i < m; ++i) {
         if(i > 0) {
            sb.append(", ");
         }

         sb.append(((AbstractType)types.get(i)).asCQL3Type());
         if(this.partialParameters.get(i) != Function.UNRESOLVED) {
            sb.append("(constant)");
         }
      }

      sb.append(") -> ").append(this.returnType.asCQL3Type());
      return sb.toString();
   }

   private static final class PartialFunctionArguments implements Arguments {
      private final Arguments arguments;
      private final int[] mapping;

      public PartialFunctionArguments(ProtocolVersion version, ScalarFunction function, List<ByteBuffer> partialArguments, int unresolvedCount) {
         this.arguments = function.newArguments(version);
         this.mapping = new int[unresolvedCount];
         int mappingIndex = 0;
         int i = 0;

         for(int m = partialArguments.size(); i < m; ++i) {
            ByteBuffer argument = (ByteBuffer)partialArguments.get(i);
            if(argument != Function.UNRESOLVED) {
               this.arguments.set(i, argument);
            } else {
               this.mapping[mappingIndex++] = i;
            }
         }

      }

      public ProtocolVersion getProtocolVersion() {
         return this.arguments.getProtocolVersion();
      }

      public void set(int i, ByteBuffer buffer) {
         this.arguments.set(this.mapping[i], buffer);
      }

      public boolean containsNulls() {
         return this.arguments.containsNulls();
      }

      public <T> T get(int i) {
         return this.arguments.get(i);
      }

      public int size() {
         return this.arguments.size();
      }
   }
}
