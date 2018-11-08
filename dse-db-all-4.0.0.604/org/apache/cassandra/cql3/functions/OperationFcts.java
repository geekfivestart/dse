package org.apache.cassandra.cql3.functions;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.cassandra.cql3.Duration;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.DurationType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.NumberType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.TemporalType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.exceptions.OperationExecutionException;

public final class OperationFcts {
   public static final String NEGATION_FUNCTION_NAME = "_negate";

   public static Collection<Function> all() {
      List<Function> functions = new ArrayList();
      NumberType<?>[] numericTypes = new NumberType[]{ByteType.instance, ShortType.instance, Int32Type.instance, LongType.instance, FloatType.instance, DoubleType.instance, DecimalType.instance, IntegerType.instance, CounterColumnType.instance};
      NumberType[] var2 = numericTypes;
      int var3 = numericTypes.length;

      int var4;
      for(var4 = 0; var4 < var3; ++var4) {
         NumberType<?> left = var2[var4];
         NumberType[] var6 = numericTypes;
         int var7 = numericTypes.length;

         for(int var8 = 0; var8 < var7; ++var8) {
            NumberType<?> right = var6[var8];
            NumberType<?> returnType = returnType(left, right);
            OperationFcts.OPERATION[] var11 = OperationFcts.OPERATION.values();
            int var12 = var11.length;

            for(int var13 = 0; var13 < var12; ++var13) {
               OperationFcts.OPERATION operation = var11[var13];
               functions.add(new OperationFcts.NumericOperationFunction(returnType, left, operation, right));
            }
         }

         functions.add(new OperationFcts.NumericNegationFunction(left));
      }

      OperationFcts.OPERATION[] var15 = new OperationFcts.OPERATION[]{OperationFcts.OPERATION.ADDITION, OperationFcts.OPERATION.SUBSTRACTION};
      var3 = var15.length;

      for(var4 = 0; var4 < var3; ++var4) {
         OperationFcts.OPERATION operation = var15[var4];
         functions.add(new OperationFcts.TemporalOperationFunction(TimestampType.instance, operation));
         functions.add(new OperationFcts.TemporalOperationFunction(SimpleDateType.instance, operation));
      }

      return functions;
   }

   public static boolean isOperation(FunctionName function) {
      return "system".equals(function.keyspace) && OperationFcts.OPERATION.fromFunctionName(function.name) != null;
   }

   public static boolean isNegation(FunctionName function) {
      return "system".equals(function.keyspace) && "_negate".equals(function.name);
   }

   public static char getOperator(FunctionName function) {
      assert "system".equals(function.keyspace);

      return OperationFcts.OPERATION.fromFunctionName(function.name).symbol;
   }

   public static FunctionName getFunctionNameFromOperator(char operator) {
      return FunctionName.nativeFunction(OperationFcts.OPERATION.fromSymbol(operator).functionName);
   }

   private static NumberType<?> returnType(NumberType<?> left, NumberType<?> right) {
      boolean isFloatingPoint = left.isFloatingPoint() || right.isFloatingPoint();
      int size = Math.max(size(left), size(right));
      return isFloatingPoint?floatPointType(size):integerType(size);
   }

   private static int size(NumberType<?> type) {
      int size = type.valueLengthIfFixed();
      return size > 0?size:(type == ByteType.instance?1:(type == ShortType.instance?2:(type.isCounter()?LongType.instance.valueLengthIfFixed():2147483647)));
   }

   private static NumberType<?> floatPointType(int size) {
      switch(size) {
      case 4:
         return FloatType.instance;
      case 8:
         return DoubleType.instance;
      default:
         return DecimalType.instance;
      }
   }

   private static NumberType<?> integerType(int size) {
      switch(size) {
      case 1:
         return ByteType.instance;
      case 2:
         return ShortType.instance;
      case 3:
      case 5:
      case 6:
      case 7:
      default:
         return IntegerType.instance;
      case 4:
         return Int32Type.instance;
      case 8:
         return LongType.instance;
      }
   }

   private OperationFcts() {
   }

   private static class NumericNegationFunction extends NativeScalarFunction {
      public NumericNegationFunction(NumberType<?> inputType) {
         super("_negate", inputType, new AbstractType[]{inputType});
      }

      public final String columnName(List<String> columnNames) {
         return String.format("-%s", new Object[]{columnNames.get(0)});
      }

      public final ByteBuffer execute(Arguments arguments) {
         if(arguments.containsNulls()) {
            return null;
         } else {
            NumberType<?> inputType = (NumberType)this.argTypes().get(0);
            return inputType.negate((Number)arguments.get(0));
         }
      }
   }

   private static class TemporalOperationFunction extends OperationFcts.OperationFunction {
      public TemporalOperationFunction(TemporalType<?> type, OperationFcts.OPERATION operation) {
         super(type, type, operation, DurationType.instance);
      }

      protected ByteBuffer doExecute(Object left, OperationFcts.OPERATION operation, Object right) {
         TemporalType<?> resultType = (TemporalType)this.returnType();
         return operation.executeOnTemporals(resultType, (Number)left, (Duration)right);
      }
   }

   private static class NumericOperationFunction extends OperationFcts.OperationFunction {
      public NumericOperationFunction(NumberType<?> returnType, NumberType<?> left, OperationFcts.OPERATION operation, NumberType<?> right) {
         super(returnType, left, operation, right);
      }

      protected ByteBuffer doExecute(Object left, OperationFcts.OPERATION operation, Object right) {
         NumberType<?> resultType = (NumberType)this.returnType();
         return operation.executeOnNumerics(resultType, (Number)left, (Number)right);
      }
   }

   private abstract static class OperationFunction extends NativeScalarFunction {
      private final OperationFcts.OPERATION operation;

      public OperationFunction(AbstractType<?> returnType, AbstractType<?> left, OperationFcts.OPERATION operation, AbstractType<?> right) {
         super(operation.functionName, returnType, new AbstractType[]{left, right});
         this.operation = operation;
      }

      public final String columnName(List<String> columnNames) {
         return String.format("%s %s %s", new Object[]{columnNames.get(0), Character.valueOf(this.getOperator()), columnNames.get(1)});
      }

      public final ByteBuffer execute(Arguments arguments) {
         if(arguments.containsNulls()) {
            return null;
         } else {
            try {
               return this.doExecute(arguments.get(0), this.operation, arguments.get(1));
            } catch (Exception var3) {
               throw OperationExecutionException.create(this.getOperator(), this.argTypes, var3);
            }
         }
      }

      protected abstract ByteBuffer doExecute(Object var1, OperationFcts.OPERATION var2, Object var3);

      private final char getOperator() {
         return this.operation.symbol;
      }
   }

   private static enum OPERATION {
      ADDITION('+', "_add") {
         protected ByteBuffer executeOnNumerics(NumberType<?> resultType, Number left, Number right) {
            return resultType.add(left, right);
         }

         protected ByteBuffer executeOnTemporals(TemporalType<?> type, Number temporal, Duration duration) {
            return type.addDuration(temporal, duration);
         }
      },
      SUBSTRACTION('-', "_substract") {
         protected ByteBuffer executeOnNumerics(NumberType<?> resultType, Number left, Number right) {
            return resultType.substract(left, right);
         }

         protected ByteBuffer executeOnTemporals(TemporalType<?> type, Number temporal, Duration duration) {
            return type.substractDuration(temporal, duration);
         }
      },
      MULTIPLICATION('*', "_multiply") {
         protected ByteBuffer executeOnNumerics(NumberType<?> resultType, Number left, Number right) {
            return resultType.multiply(left, right);
         }
      },
      DIVISION('/', "_divide") {
         protected ByteBuffer executeOnNumerics(NumberType<?> resultType, Number left, Number right) {
            return resultType.divide(left, right);
         }
      },
      MODULO('%', "_modulo") {
         protected ByteBuffer executeOnNumerics(NumberType<?> resultType, Number left, Number right) {
            return resultType.mod(left, right);
         }
      };

      private final char symbol;
      private final String functionName;

      private OPERATION(char symbol, String functionName) {
         this.symbol = symbol;
         this.functionName = functionName;
      }

      protected abstract ByteBuffer executeOnNumerics(NumberType<?> var1, Number var2, Number var3);

      protected ByteBuffer executeOnTemporals(TemporalType<?> type, Number temporal, Duration duration) {
         throw new UnsupportedOperationException();
      }

      public static OperationFcts.OPERATION fromFunctionName(String functionName) {
         OperationFcts.OPERATION[] var1 = values();
         int var2 = var1.length;

         for(int var3 = 0; var3 < var2; ++var3) {
            OperationFcts.OPERATION operator = var1[var3];
            if(operator.functionName.equals(functionName)) {
               return operator;
            }
         }

         return null;
      }

      public static OperationFcts.OPERATION fromSymbol(char symbol) {
         OperationFcts.OPERATION[] var1 = values();
         int var2 = var1.length;

         for(int var3 = 0; var3 < var2; ++var3) {
            OperationFcts.OPERATION operator = var1[var3];
            if(operator.symbol == symbol) {
               return operator;
            }
         }

         return null;
      }
   }
}
