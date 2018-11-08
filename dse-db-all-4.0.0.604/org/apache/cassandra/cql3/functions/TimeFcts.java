package org.apache.cassandra.cql3.functions;

import com.google.common.collect.ImmutableList;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import org.apache.cassandra.cql3.Duration;
import org.apache.cassandra.cql3.statements.RequestValidations;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.DurationType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.TemporalType;
import org.apache.cassandra.db.marshal.TimeType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TimeFcts {
   public static Logger logger = LoggerFactory.getLogger(TimeFcts.class);
   public static final Function minTimeuuidFct;
   public static final Function maxTimeuuidFct;
   /** @deprecated */
   public static final NativeScalarFunction dateOfFct;
   /** @deprecated */
   public static final NativeScalarFunction unixTimestampOfFct;
   public static final NativeScalarFunction floorTime;

   public TimeFcts() {
   }

   public static Collection<Function> all() {
      return ImmutableList.of(now("now", TimeUUIDType.instance), now("currenttimeuuid", TimeUUIDType.instance), now("currenttimestamp", TimestampType.instance), now("currentdate", SimpleDateType.instance), now("currenttime", TimeType.instance), minTimeuuidFct, maxTimeuuidFct, dateOfFct, unixTimestampOfFct, toDate(TimeUUIDType.instance), toDate(TimestampType.instance), toTimestamp(TimeUUIDType.instance), new Function[]{toUnixTimestamp(TimeUUIDType.instance), toUnixTimestamp(TimestampType.instance), toUnixTimestamp(SimpleDateType.instance), toTimestamp(SimpleDateType.instance), TimeFcts.FloorTimestampFunction.newInstance(), TimeFcts.FloorTimestampFunction.newInstanceWithStartTimeArgument(), TimeFcts.FloorTimeUuidFunction.newInstance(), TimeFcts.FloorTimeUuidFunction.newInstanceWithStartTimeArgument(), TimeFcts.FloorDateFunction.newInstance(), TimeFcts.FloorDateFunction.newInstanceWithStartTimeArgument(), floorTime});
   }

   public static final Function now(String name, final TemporalType<?> type) {
      return new NativeScalarFunction(name, type, new AbstractType[0]) {
         public ByteBuffer execute(Arguments arguments) {
            return type.now();
         }

         public boolean isDeterministic() {
            return false;
         }
      };
   }

   public static final NativeScalarFunction toDate(TemporalType<?> type) {
      return new TimeFcts.TemporalConversionFunction("todate", SimpleDateType.instance, new AbstractType[]{type}) {
         protected ByteBuffer convertArgument(long timeInMillis) {
            return SimpleDateType.instance.fromTimeInMillis(timeInMillis);
         }

         public boolean isMonotonic() {
            return true;
         }
      };
   }

   public static final NativeScalarFunction toTimestamp(TemporalType<?> type) {
      return new TimeFcts.TemporalConversionFunction("totimestamp", TimestampType.instance, new AbstractType[]{type}) {
         protected ByteBuffer convertArgument(long timeInMillis) {
            return TimestampType.instance.fromTimeInMillis(timeInMillis);
         }

         public boolean isMonotonic() {
            return true;
         }
      };
   }

   public static final NativeScalarFunction toUnixTimestamp(TemporalType<?> type) {
      return new TimeFcts.TemporalConversionFunction("tounixtimestamp", LongType.instance, new AbstractType[]{type}) {
         protected ByteBuffer convertArgument(long timeInMillis) {
            return ByteBufferUtil.bytes(timeInMillis);
         }

         public boolean isMonotonic() {
            return true;
         }
      };
   }

   static {
      minTimeuuidFct = new TimeFcts.TemporalConversionFunction("mintimeuuid", TimeUUIDType.instance, new AbstractType[]{TimestampType.instance}) {
         protected ByteBuffer convertArgument(long timeInMillis) {
            return UUIDGen.toByteBuffer(UUIDGen.minTimeUUID(timeInMillis));
         }
      };
      maxTimeuuidFct = new TimeFcts.TemporalConversionFunction("maxtimeuuid", TimeUUIDType.instance, new AbstractType[]{TimestampType.instance}) {
         protected ByteBuffer convertArgument(long timeInMillis) {
            return UUIDGen.toByteBuffer(UUIDGen.maxTimeUUID(timeInMillis));
         }
      };
      dateOfFct = new TimeFcts.TemporalConversionFunction("dateof", TimestampType.instance, new AbstractType[]{TimeUUIDType.instance}) {
         private volatile boolean hasLoggedDeprecationWarning;

         public void beforeExecution() {
            if(!this.hasLoggedDeprecationWarning) {
               this.hasLoggedDeprecationWarning = true;
               TimeFcts.logger.warn("The function 'dateof' is deprecated. Use the function 'toTimestamp' instead.");
            }

         }

         protected ByteBuffer convertArgument(long timeInMillis) {
            return ByteBufferUtil.bytes(timeInMillis);
         }
      };
      unixTimestampOfFct = new TimeFcts.TemporalConversionFunction("unixtimestampof", LongType.instance, new AbstractType[]{TimeUUIDType.instance}) {
         private volatile boolean hasLoggedDeprecationWarning;

         public void beforeExecution() {
            if(!this.hasLoggedDeprecationWarning) {
               this.hasLoggedDeprecationWarning = true;
               TimeFcts.logger.warn("The function 'unixtimestampof' is deprecated. Use the function 'toUnixTimestamp' instead.");
            }

         }

         protected ByteBuffer convertArgument(long timeInMillis) {
            return ByteBufferUtil.bytes(timeInMillis);
         }
      };
      floorTime = new NativeScalarFunction("floor", TimeType.instance, new AbstractType[]{TimeType.instance, DurationType.instance}) {
         public boolean isPartialApplicationMonotonic(List<ByteBuffer> partialParameters) {
            return partialParameters.get(0) == UNRESOLVED && partialParameters.get(1) != UNRESOLVED;
         }

         public final ByteBuffer execute(Arguments arguments) {
            if(arguments.containsNulls()) {
               return null;
            } else {
               long time = arguments.getAsLong(0);
               Duration duration = (Duration)arguments.get(1);
               long floor = Duration.floorTime(time, duration);
               return TimeType.instance.decompose(Long.valueOf(floor));
            }
         }
      };
   }

   public static final class FloorDateFunction extends TimeFcts.FloorFunction {
      public static TimeFcts.FloorDateFunction newInstance() {
         return new TimeFcts.FloorDateFunction(SimpleDateType.instance, new AbstractType[]{SimpleDateType.instance, DurationType.instance});
      }

      public static TimeFcts.FloorDateFunction newInstanceWithStartTimeArgument() {
         return new TimeFcts.FloorDateFunction(SimpleDateType.instance, new AbstractType[]{SimpleDateType.instance, DurationType.instance, SimpleDateType.instance});
      }

      private FloorDateFunction(AbstractType<?> returnType, AbstractType... argTypes) {
         super(returnType, argTypes);
      }

      protected ByteBuffer fromTimeInMillis(long timeInMillis) {
         return SimpleDateType.instance.fromTimeInMillis(timeInMillis);
      }

      protected void validateDuration(Duration duration) {
         if(duration.getNanoseconds() != 0L) {
            throw RequestValidations.invalidRequest("The floor on %s values cannot be computed for the %s duration as precision is below 1 day", new Object[]{SimpleDateType.instance.asCQL3Type(), duration});
         }
      }
   }

   public static final class FloorTimeUuidFunction extends TimeFcts.FloorFunction {
      public static TimeFcts.FloorTimeUuidFunction newInstance() {
         return new TimeFcts.FloorTimeUuidFunction(TimestampType.instance, new AbstractType[]{TimeUUIDType.instance, DurationType.instance});
      }

      public static TimeFcts.FloorTimeUuidFunction newInstanceWithStartTimeArgument() {
         return new TimeFcts.FloorTimeUuidFunction(TimestampType.instance, new AbstractType[]{TimeUUIDType.instance, DurationType.instance, TimestampType.instance});
      }

      private FloorTimeUuidFunction(AbstractType<?> returnType, AbstractType... argTypes) {
         super(returnType, argTypes);
      }

      protected ByteBuffer fromTimeInMillis(long timeInMillis) {
         return TimestampType.instance.fromTimeInMillis(timeInMillis);
      }
   }

   public static final class FloorTimestampFunction extends TimeFcts.FloorFunction {
      public static TimeFcts.FloorTimestampFunction newInstance() {
         return new TimeFcts.FloorTimestampFunction(TimestampType.instance, new AbstractType[]{TimestampType.instance, DurationType.instance});
      }

      public static TimeFcts.FloorTimestampFunction newInstanceWithStartTimeArgument() {
         return new TimeFcts.FloorTimestampFunction(TimestampType.instance, new AbstractType[]{TimestampType.instance, DurationType.instance, TimestampType.instance});
      }

      private FloorTimestampFunction(AbstractType<?> returnType, AbstractType... argTypes) {
         super(returnType, argTypes);
      }

      protected ByteBuffer fromTimeInMillis(long timeInMillis) {
         return TimestampType.instance.fromTimeInMillis(timeInMillis);
      }
   }

   private abstract static class FloorFunction extends NativeScalarFunction {
      protected FloorFunction(AbstractType<?> returnType, AbstractType... argsType) {
         super("floor", returnType, argsType);
      }

      public boolean isPartialApplicationMonotonic(List<ByteBuffer> partialArguments) {
         return partialArguments.get(0) == UNRESOLVED && partialArguments.get(1) != UNRESOLVED && (partialArguments.size() == 2 || partialArguments.get(2) != UNRESOLVED);
      }

      public final ByteBuffer execute(Arguments arguments) {
         if(arguments.containsNulls()) {
            return null;
         } else {
            long time = arguments.getAsLong(0);
            Duration duration = (Duration)arguments.get(1);
            long startingTime = this.getStartingTime(arguments);
            this.validateDuration(duration);
            long floor = Duration.floorTimestamp(time, duration, startingTime);
            return this.fromTimeInMillis(floor);
         }
      }

      private long getStartingTime(Arguments arguments) {
         return arguments.size() == 3?arguments.getAsLong(2):0L;
      }

      protected void validateDuration(Duration duration) {
         if(Long.numberOfTrailingZeros(duration.getNanoseconds()) < 6) {
            throw RequestValidations.invalidRequest("The floor cannot be computed for the %s duration as precision is below 1 millisecond", new Object[]{duration});
         }
      }

      protected abstract ByteBuffer fromTimeInMillis(long var1);
   }

   public abstract static class TemporalConversionFunction extends NativeScalarFunction {
      protected TemporalConversionFunction(String name, AbstractType<?> returnType, AbstractType... argsType) {
         super(name, returnType, argsType);
      }

      public ByteBuffer execute(Arguments arguments) {
         this.beforeExecution();
         return arguments.containsNulls()?null:this.convertArgument(arguments.getAsLong(0));
      }

      protected void beforeExecution() {
      }

      protected abstract ByteBuffer convertArgument(long var1);
   }
}
