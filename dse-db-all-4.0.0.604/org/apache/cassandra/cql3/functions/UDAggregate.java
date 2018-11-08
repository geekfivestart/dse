package org.apache.cassandra.cql3.functions;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.Functions;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.ProtocolVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UDAggregate extends AbstractFunction implements AggregateFunction {
   protected static final Logger logger = LoggerFactory.getLogger(UDAggregate.class);
   private final UDFDataType stateType;
   private final List<UDFDataType> argumentTypes;
   private final UDFDataType resultType;
   protected final ByteBuffer initcond;
   private final ScalarFunction stateFunction;
   private final ScalarFunction finalFunction;
   private final boolean deterministic;

   public UDAggregate(FunctionName name, List<AbstractType<?>> argTypes, AbstractType<?> returnType, ScalarFunction stateFunc, ScalarFunction finalFunc, ByteBuffer initcond, boolean deterministic) {
      super(name, argTypes, returnType);
      this.stateFunction = stateFunc;
      this.finalFunction = finalFunc;
      this.argumentTypes = UDFDataType.wrap(argTypes, false);
      this.resultType = UDFDataType.wrap(returnType, false);
      this.stateType = stateFunc != null?UDFDataType.wrap(stateFunc.returnType(), false):null;
      this.initcond = initcond;
      this.deterministic = deterministic;
   }

   public static UDAggregate create(Functions functions, FunctionName name, List<AbstractType<?>> argTypes, AbstractType<?> returnType, FunctionName stateFunc, FunctionName finalFunc, AbstractType<?> stateType, ByteBuffer initcond, boolean deterministic) throws InvalidRequestException {
      List<AbstractType<?>> stateTypes = new ArrayList(argTypes.size() + 1);
      stateTypes.add(stateType);
      stateTypes.addAll(argTypes);
      List<AbstractType<?>> finalTypes = Collections.singletonList(stateType);
      return new UDAggregate(name, argTypes, returnType, resolveScalar(functions, name, stateFunc, stateTypes), finalFunc != null?resolveScalar(functions, name, finalFunc, finalTypes):null, initcond, deterministic);
   }

   public static UDAggregate createBroken(FunctionName name, List<AbstractType<?>> argTypes, AbstractType<?> returnType, ByteBuffer initcond, boolean deterministic, final InvalidRequestException reason) {
      return new UDAggregate(name, argTypes, returnType, (ScalarFunction)null, (ScalarFunction)null, initcond, deterministic) {
         public AggregateFunction.Aggregate newAggregate() throws InvalidRequestException {
            throw new InvalidRequestException(String.format("Aggregate '%s' exists but hasn't been loaded successfully for the following reason: %s. Please see the server log for more details", new Object[]{this, reason.getMessage()}));
         }
      };
   }

   public boolean isDeterministic() {
      return this.deterministic;
   }

   public Arguments newArguments(ProtocolVersion version) {
      return FunctionArguments.newInstanceForUdf(version, this.argumentTypes);
   }

   public boolean hasReferenceTo(Function function) {
      return this.stateFunction == function || this.finalFunction == function;
   }

   public void addFunctionsTo(List<Function> functions) {
      functions.add(this);
      if(this.stateFunction != null) {
         this.stateFunction.addFunctionsTo(functions);
         if(this.finalFunction != null) {
            this.finalFunction.addFunctionsTo(functions);
         }
      }

   }

   public boolean isAggregate() {
      return true;
   }

   public boolean isNative() {
      return false;
   }

   public ScalarFunction stateFunction() {
      return this.stateFunction;
   }

   public ScalarFunction finalFunction() {
      return this.finalFunction;
   }

   public ByteBuffer initialCondition() {
      return this.initcond;
   }

   public AbstractType<?> stateType() {
      return this.stateType == null?null:this.stateType.toAbstractType();
   }

   public AggregateFunction.Aggregate newAggregate() throws InvalidRequestException {
      return new AggregateFunction.Aggregate() {
         private long stateFunctionCount;
         private long stateFunctionDuration;
         private Object state;
         private boolean needsInit = true;

         public void addInput(Arguments arguments) throws InvalidRequestException {
            this.maybeInit(arguments.getProtocolVersion());
            long startTime = System.nanoTime();
            ++this.stateFunctionCount;
            if(UDAggregate.this.stateFunction instanceof UDFunction) {
               UDFunction udf = (UDFunction)UDAggregate.this.stateFunction;
               if(udf.isCallableWrtNullable(arguments)) {
                  this.state = udf.executeForAggregate(this.state, arguments);
               }

               this.stateFunctionDuration += (System.nanoTime() - startTime) / 1000L;
            } else {
               throw new UnsupportedOperationException("UDAs only support UDFs");
            }
         }

         private void maybeInit(ProtocolVersion protocolVersion) {
            if(this.needsInit) {
               this.state = UDAggregate.this.initcond != null?UDAggregate.this.stateType.compose(protocolVersion, UDAggregate.this.initcond.duplicate()):null;
               this.stateFunctionDuration = 0L;
               this.stateFunctionCount = 0L;
               this.needsInit = false;
            }

         }

         public ByteBuffer compute(ProtocolVersion protocolVersion) throws InvalidRequestException {
            this.maybeInit(protocolVersion);
            Tracing.trace("Executed UDA {}: {} call(s) to state function {} in {}μs", new Object[]{UDAggregate.this.name(), Long.valueOf(this.stateFunctionCount), UDAggregate.this.stateFunction.name(), Long.valueOf(this.stateFunctionDuration)});
            if(UDAggregate.this.finalFunction == null) {
               return UDAggregate.this.stateType.decompose(protocolVersion, this.state);
            } else if(UDAggregate.this.finalFunction instanceof UDFunction) {
               UDFunction udf = (UDFunction)UDAggregate.this.finalFunction;
               Object result = udf.executeForAggregate(this.state, FunctionArguments.emptyInstance(protocolVersion));
               return UDAggregate.this.resultType.decompose(protocolVersion, result);
            } else {
               throw new UnsupportedOperationException("UDAs only support UDFs");
            }
         }

         public void reset() {
            this.needsInit = true;
         }
      };
   }

   private static ScalarFunction resolveScalar(Functions functions, FunctionName aName, FunctionName fName, List<AbstractType<?>> argTypes) throws InvalidRequestException {
      Optional<Function> fun = functions.find(fName, argTypes);
      if(!fun.isPresent()) {
         throw new InvalidRequestException(String.format("Referenced state function '%s %s' for aggregate '%s' does not exist", new Object[]{fName, AbstractType.asCQLTypeStringList(argTypes), aName}));
      } else if(!(fun.get() instanceof ScalarFunction)) {
         throw new InvalidRequestException(String.format("Referenced state function '%s %s' for aggregate '%s' is not a scalar function", new Object[]{fName, AbstractType.asCQLTypeStringList(argTypes), aName}));
      } else {
         return (ScalarFunction)fun.get();
      }
   }

   public boolean equals(Object o) {
      if(!(o instanceof UDAggregate)) {
         return false;
      } else {
         UDAggregate that = (UDAggregate)o;
         return Objects.equals(this.name, that.name) && Functions.typesMatch(this.argTypes, that.argTypes) && Functions.typesMatch(this.returnType, that.returnType) && Objects.equals(this.stateFunction, that.stateFunction) && Objects.equals(this.finalFunction, that.finalFunction) && (this.stateType == that.stateType || this.stateType != null && this.stateType.equals(that.stateType)) && Objects.equals(this.initcond, that.initcond);
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.name, Integer.valueOf(Functions.typeHashCode(this.argTypes)), Integer.valueOf(Functions.typeHashCode(this.returnType)), this.stateFunction, this.finalFunction, this.stateType, this.initcond});
   }
}
