package org.apache.cassandra.cql3.functions;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.TimeType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.commons.lang3.text.WordUtils;

public final class CastFcts {
   private static final String FUNCTION_NAME_PREFIX = "castAs";

   public static Collection<Function> all() {
      List<Function> functions = new ArrayList();
      AbstractType<? extends Number>[] numericTypes = new AbstractType[]{ByteType.instance, ShortType.instance, Int32Type.instance, LongType.instance, FloatType.instance, DoubleType.instance, DecimalType.instance, CounterColumnType.instance, IntegerType.instance};
      AbstractType[] var2 = numericTypes;
      int var3 = numericTypes.length;

      for(int var4 = 0; var4 < var3; ++var4) {
         AbstractType<? extends Number> inputType = var2[var4];
         addFunctionIfNeeded(functions, inputType, ByteType.instance, Number::byteValue);
         addFunctionIfNeeded(functions, inputType, ShortType.instance, Number::shortValue);
         addFunctionIfNeeded(functions, inputType, Int32Type.instance, Number::intValue);
         addFunctionIfNeeded(functions, inputType, LongType.instance, Number::longValue);
         addFunctionIfNeeded(functions, inputType, FloatType.instance, Number::floatValue);
         addFunctionIfNeeded(functions, inputType, DoubleType.instance, Number::doubleValue);
         addFunctionIfNeeded(functions, inputType, DecimalType.instance, getDecimalConversionFunction(inputType));
         addFunctionIfNeeded(functions, inputType, IntegerType.instance, (p) -> {
            return BigInteger.valueOf(p.longValue());
         });
         functions.add(CastFcts.CastAsTextFunction.create(inputType, AsciiType.instance));
         functions.add(CastFcts.CastAsTextFunction.create(inputType, UTF8Type.instance));
      }

      functions.add(CastFcts.JavaFunctionWrapper.create(AsciiType.instance, UTF8Type.instance, (p) -> {
         return p;
      }));
      functions.add(CastFcts.CastAsTextFunction.create(InetAddressType.instance, AsciiType.instance));
      functions.add(CastFcts.CastAsTextFunction.create(InetAddressType.instance, UTF8Type.instance));
      functions.add(CastFcts.CastAsTextFunction.create(BooleanType.instance, AsciiType.instance));
      functions.add(CastFcts.CastAsTextFunction.create(BooleanType.instance, UTF8Type.instance));
      functions.add(CastFcts.CassandraFunctionWrapper.create(TimeUUIDType.instance, SimpleDateType.instance, TimeFcts.toDate(TimeUUIDType.instance)));
      functions.add(CastFcts.CassandraFunctionWrapper.create(TimeUUIDType.instance, TimestampType.instance, TimeFcts.toTimestamp(TimeUUIDType.instance)));
      functions.add(CastFcts.CastAsTextFunction.create(TimeUUIDType.instance, AsciiType.instance));
      functions.add(CastFcts.CastAsTextFunction.create(TimeUUIDType.instance, UTF8Type.instance));
      functions.add(CastFcts.CassandraFunctionWrapper.create(TimestampType.instance, SimpleDateType.instance, TimeFcts.toDate(TimestampType.instance)));
      functions.add(CastFcts.CastAsTextFunction.create(TimestampType.instance, AsciiType.instance));
      functions.add(CastFcts.CastAsTextFunction.create(TimestampType.instance, UTF8Type.instance));
      functions.add(CastFcts.CassandraFunctionWrapper.create(SimpleDateType.instance, TimestampType.instance, TimeFcts.toTimestamp(SimpleDateType.instance)));
      functions.add(CastFcts.CastAsTextFunction.create(SimpleDateType.instance, AsciiType.instance));
      functions.add(CastFcts.CastAsTextFunction.create(SimpleDateType.instance, UTF8Type.instance));
      functions.add(CastFcts.CastAsTextFunction.create(TimeType.instance, AsciiType.instance));
      functions.add(CastFcts.CastAsTextFunction.create(TimeType.instance, UTF8Type.instance));
      functions.add(CastFcts.CastAsTextFunction.create(UUIDType.instance, AsciiType.instance));
      functions.add(CastFcts.CastAsTextFunction.create(UUIDType.instance, UTF8Type.instance));
      return functions;
   }

   private static <I extends Number> java.util.function.Function<I, BigDecimal> getDecimalConversionFunction(AbstractType<? extends Number> inputType) {
      return inputType != FloatType.instance && inputType != DoubleType.instance?(inputType == IntegerType.instance?(p) -> {
         return new BigDecimal((BigInteger)p);
      }:(p) -> {
         return BigDecimal.valueOf(p.longValue());
      }):(p) -> {
         return BigDecimal.valueOf(p.doubleValue());
      };
   }

   public static String getFunctionName(AbstractType<?> outputType) {
      return getFunctionName(outputType.asCQL3Type());
   }

   public static String getFunctionName(CQL3Type outputType) {
      return "castAs" + WordUtils.capitalize(toLowerCaseString(outputType));
   }

   private static <I, O> void addFunctionIfNeeded(List<Function> functions, AbstractType<I> inputType, AbstractType<O> outputType, java.util.function.Function<I, O> converter) {
      if(!inputType.equals(outputType)) {
         functions.add(wrapJavaFunction(inputType, outputType, converter));
      }

   }

   private static <O, I> Function wrapJavaFunction(AbstractType<I> inputType, AbstractType<O> outputType, java.util.function.Function<I, O> converter) {
      return CastFcts.JavaFunctionWrapper.create(inputType, outputType, converter);
   }

   private static String toLowerCaseString(CQL3Type type) {
      return type.toString().toLowerCase();
   }

   private CastFcts() {
   }

   private static final class CastAsTextFunction<I> extends CastFcts.CastFunction<I, String> {
      public static <I> CastFcts.CastAsTextFunction<I> create(AbstractType<I> inputType, AbstractType<String> outputType) {
         return new CastFcts.CastAsTextFunction(inputType, outputType);
      }

      private CastAsTextFunction(AbstractType<I> inputType, AbstractType<String> outputType) {
         super(inputType, outputType);
      }

      public Arguments newArguments(ProtocolVersion version) {
         return new FunctionArguments(version, new ArgumentDeserializer[]{new ArgumentDeserializer() {
            public Object deserialize(ProtocolVersion protocolVersion, ByteBuffer buffer) {
               AbstractType<?> argType = (AbstractType)CastAsTextFunction.this.argTypes.get(0);
               return buffer != null && (buffer.hasRemaining() || !argType.isEmptyValueMeaningless())?argType.getSerializer().toCQLLiteral(buffer):null;
            }
         }});
      }

      public ByteBuffer execute(Arguments arguments) {
         return arguments.containsNulls()?null:this.outputType().decompose(arguments.get(0));
      }
   }

   private static final class CassandraFunctionWrapper<I, O> extends CastFcts.CastFunction<I, O> {
      private final NativeScalarFunction delegate;

      public static <I, O> CastFcts.CassandraFunctionWrapper<I, O> create(AbstractType<I> inputType, AbstractType<O> outputType, NativeScalarFunction delegate) {
         return new CastFcts.CassandraFunctionWrapper(inputType, outputType, delegate);
      }

      private CassandraFunctionWrapper(AbstractType<I> inputType, AbstractType<O> outputType, NativeScalarFunction delegate) {
         super(inputType, outputType);

         assert delegate.argTypes().size() == 1 && inputType.equals(delegate.argTypes().get(0));

         assert outputType.equals(delegate.returnType());

         this.delegate = delegate;
      }

      public ByteBuffer execute(Arguments arguments) {
         return this.delegate.execute(arguments);
      }
   }

   private static class JavaFunctionWrapper<I, O> extends CastFcts.CastFunction<I, O> {
      private final java.util.function.Function<I, O> converter;

      public static <I, O> CastFcts.JavaFunctionWrapper<I, O> create(AbstractType<I> inputType, AbstractType<O> outputType, java.util.function.Function<I, O> converter) {
         return new CastFcts.JavaFunctionWrapper(inputType, outputType, converter);
      }

      protected JavaFunctionWrapper(AbstractType<I> inputType, AbstractType<O> outputType, java.util.function.Function<I, O> converter) {
         super(inputType, outputType);
         this.converter = converter;
      }

      public final ByteBuffer execute(Arguments arguments) {
         return arguments.containsNulls()?null:this.outputType().decompose(this.converter.apply(arguments.get(0)));
      }
   }

   private abstract static class CastFunction<I, O> extends NativeScalarFunction {
      public CastFunction(AbstractType<I> inputType, AbstractType<O> outputType) {
         super(CastFcts.getFunctionName(outputType), outputType, new AbstractType[]{inputType});
      }

      public String columnName(List<String> columnNames) {
         return String.format("cast(%s as %s)", new Object[]{columnNames.get(0), CastFcts.toLowerCaseString(this.outputType().asCQL3Type())});
      }

      protected AbstractType<O> outputType() {
         return this.returnType;
      }
   }
}
