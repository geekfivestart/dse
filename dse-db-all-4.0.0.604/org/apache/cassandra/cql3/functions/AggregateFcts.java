package org.apache.cassandra.cql3.functions;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.transport.ProtocolVersion;

public abstract class AggregateFcts {
   public static final AggregateFunction countRowsFunction;
   public static final AggregateFunction sumFunctionForDecimal;
   public static final AggregateFunction avgFunctionForDecimal;
   public static final AggregateFunction sumFunctionForVarint;
   public static final AggregateFunction avgFunctionForVarint;
   public static final AggregateFunction sumFunctionForByte;
   public static final AggregateFunction avgFunctionForByte;
   public static final AggregateFunction sumFunctionForShort;
   public static final AggregateFunction avgFunctionForShort;
   public static final AggregateFunction sumFunctionForInt32;
   public static final AggregateFunction avgFunctionForInt32;
   public static final AggregateFunction sumFunctionForLong;
   public static final AggregateFunction avgFunctionForLong;
   public static final AggregateFunction sumFunctionForFloat;
   public static final AggregateFunction avgFunctionForFloat;
   public static final AggregateFunction sumFunctionForDouble;
   public static final AggregateFunction avgFunctionForDouble;
   public static final AggregateFunction sumFunctionForCounter;
   public static final AggregateFunction avgFunctionForCounter;
   public static final AggregateFunction minFunctionForCounter;
   public static final AggregateFunction maxFunctionForCounter;

   public AggregateFcts() {
   }

   public static Collection<AggregateFunction> all() {
      Collection<AggregateFunction> functions = new ArrayList();
      functions.add(countRowsFunction);
      functions.add(sumFunctionForByte);
      functions.add(sumFunctionForShort);
      functions.add(sumFunctionForInt32);
      functions.add(sumFunctionForLong);
      functions.add(sumFunctionForFloat);
      functions.add(sumFunctionForDouble);
      functions.add(sumFunctionForDecimal);
      functions.add(sumFunctionForVarint);
      functions.add(sumFunctionForCounter);
      functions.add(avgFunctionForByte);
      functions.add(avgFunctionForShort);
      functions.add(avgFunctionForInt32);
      functions.add(avgFunctionForLong);
      functions.add(avgFunctionForFloat);
      functions.add(avgFunctionForDouble);
      functions.add(avgFunctionForDecimal);
      functions.add(avgFunctionForVarint);
      functions.add(avgFunctionForCounter);
      CQL3Type.Native[] var1 = CQL3Type.Native.values();
      int var2 = var1.length;

      for(int var3 = 0; var3 < var2; ++var3) {
         CQL3Type type = var1[var3];
         if(type != CQL3Type.Native.VARCHAR) {
            functions.add(makeCountFunction(type.getType()));
            if(type != CQL3Type.Native.COUNTER) {
               functions.add(makeMaxFunction(type.getType()));
               functions.add(makeMinFunction(type.getType()));
            } else {
               functions.add(maxFunctionForCounter);
               functions.add(minFunctionForCounter);
            }
         }
      }

      return functions;
   }

   public static AggregateFunction makeMaxFunction(AbstractType<?> inputType) {
      return new NativeAggregateFunction("max", inputType, new AbstractType[]{inputType}) {
         public Arguments newArguments(ProtocolVersion version) {
            return FunctionArguments.newNoopInstance(version, 1);
         }

         public AggregateFunction.Aggregate newAggregate() {
            return new AggregateFunction.Aggregate() {
               private ByteBuffer max;

               public void reset() {
                  this.max = null;
               }

               public ByteBuffer compute(ProtocolVersion protocolVersion) {
                  return this.max;
               }

               public void addInput(Arguments arguments) {
                  ByteBuffer value = (ByteBuffer)arguments.get(0);
                  if(value != null) {
                     if(this.max == null || returnType().compare(this.max, value) < 0) {
                        this.max = value;
                     }

                  }
               }
            };
         }
      };
   }

   public static AggregateFunction makeMinFunction(AbstractType<?> inputType) {
      return new NativeAggregateFunction("min", inputType, new AbstractType[]{inputType}) {
         public Arguments newArguments(ProtocolVersion version) {
            return FunctionArguments.newNoopInstance(version, 1);
         }

         public AggregateFunction.Aggregate newAggregate() {
            return new AggregateFunction.Aggregate() {
               private ByteBuffer min;

               public void reset() {
                  this.min = null;
               }

               public ByteBuffer compute(ProtocolVersion protocolVersion) {
                  return this.min;
               }

               public void addInput(Arguments arguments) {
                  ByteBuffer value = (ByteBuffer)arguments.get(0);
                  if(value != null) {
                     if(this.min == null || returnType().compare(this.min, value) > 0) {
                        this.min = value;
                     }

                  }
               }
            };
         }
      };
   }

   public static AggregateFunction makeCountFunction(AbstractType<?> inputType) {
      return new NativeAggregateFunction("count", LongType.instance, new AbstractType[]{inputType}) {
         public Arguments newArguments(ProtocolVersion version) {
            return FunctionArguments.newNoopInstance(version, 1);
         }

         public AggregateFunction.Aggregate newAggregate() {
            return new AggregateFunction.Aggregate() {
               private long count;

               public void reset() {
                  this.count = 0L;
               }

               public ByteBuffer compute(ProtocolVersion protocolVersion) {
                  return ((LongType)returnType()).decompose(Long.valueOf(this.count));
               }

               public void addInput(Arguments arguments) {
                  if(arguments.get(0) != null) {
                     ++this.count;
                  }
               }
            };
         }
      };
   }

   static {
      countRowsFunction = new NativeAggregateFunction("countRows", LongType.instance, new AbstractType[0]) {
         public AggregateFunction.Aggregate newAggregate() {
            return new AggregateFunction.Aggregate() {
               private long count;

               public void reset() {
                  this.count = 0L;
               }

               public ByteBuffer compute(ProtocolVersion protocolVersion) {
                  return LongType.instance.decompose(Long.valueOf(this.count));
               }

               public void addInput(Arguments arguments) {
                  ++this.count;
               }
            };
         }

         public String columnName(List<String> columnNames) {
            return "count";
         }
      };
      sumFunctionForDecimal = new NativeAggregateFunction("sum", DecimalType.instance, new AbstractType[]{DecimalType.instance}) {
         public AggregateFunction.Aggregate newAggregate() {
            return new AggregateFunction.Aggregate() {
               private BigDecimal sum;

               {
                  this.sum = BigDecimal.ZERO;
               }

               public void reset() {
                  this.sum = BigDecimal.ZERO;
               }

               public ByteBuffer compute(ProtocolVersion protocolVersion) {
                  return ((DecimalType)returnType()).decompose(this.sum);
               }

               public void addInput(Arguments arguments) {
                  BigDecimal number = (BigDecimal)arguments.get(0);
                  if(number != null) {
                     this.sum = this.sum.add(number);
                  }
               }
            };
         }
      };
      avgFunctionForDecimal = new NativeAggregateFunction("avg", DecimalType.instance, new AbstractType[]{DecimalType.instance}) {
         public AggregateFunction.Aggregate newAggregate() {
            return new AggregateFunction.Aggregate() {
               private BigDecimal avg;
               private int count;

               {
                  this.avg = BigDecimal.ZERO;
               }

               public void reset() {
                  this.count = 0;
                  this.avg = BigDecimal.ZERO;
               }

               public ByteBuffer compute(ProtocolVersion protocolVersion) {
                  return DecimalType.instance.decompose(this.avg);
               }

               public void addInput(Arguments arguments) {
                  BigDecimal number = (BigDecimal)arguments.get(0);
                  if(number != null) {
                     ++this.count;
                     this.avg = this.avg.add(number.subtract(this.avg).divide(BigDecimal.valueOf((long)this.count), RoundingMode.HALF_EVEN));
                  }
               }
            };
         }
      };
      sumFunctionForVarint = new NativeAggregateFunction("sum", IntegerType.instance, new AbstractType[]{IntegerType.instance}) {
         public AggregateFunction.Aggregate newAggregate() {
            return new AggregateFunction.Aggregate() {
               private BigInteger sum;

               {
                  this.sum = BigInteger.ZERO;
               }

               public void reset() {
                  this.sum = BigInteger.ZERO;
               }

               public ByteBuffer compute(ProtocolVersion protocolVersion) {
                  return ((IntegerType)returnType()).decompose(this.sum);
               }

               public void addInput(Arguments arguments) {
                  BigInteger number = (BigInteger)arguments.get(0);
                  if(number != null) {
                     this.sum = this.sum.add(number);
                  }
               }
            };
         }
      };
      avgFunctionForVarint = new NativeAggregateFunction("avg", IntegerType.instance, new AbstractType[]{IntegerType.instance}) {
         public AggregateFunction.Aggregate newAggregate() {
            return new AggregateFunction.Aggregate() {
               private BigInteger sum;
               private int count;

               {
                  this.sum = BigInteger.ZERO;
               }

               public void reset() {
                  this.count = 0;
                  this.sum = BigInteger.ZERO;
               }

               public ByteBuffer compute(ProtocolVersion protocolVersion) {
                  return this.count == 0?IntegerType.instance.decompose(BigInteger.ZERO):IntegerType.instance.decompose(this.sum.divide(BigInteger.valueOf((long)this.count)));
               }

               public void addInput(Arguments arguments) {
                  BigInteger number = (BigInteger)arguments.get(0);
                  if(number != null) {
                     ++this.count;
                     this.sum = this.sum.add(number);
                  }
               }
            };
         }
      };
      sumFunctionForByte = new NativeAggregateFunction("sum", ByteType.instance, new AbstractType[]{ByteType.instance}) {
         public AggregateFunction.Aggregate newAggregate() {
            return new AggregateFunction.Aggregate() {
               private byte sum;

               public void reset() {
                  this.sum = 0;
               }

               public ByteBuffer compute(ProtocolVersion protocolVersion) {
                  return ((ByteType)returnType()).decompose(Byte.valueOf(this.sum));
               }

               public void addInput(Arguments arguments) {
                  Number number = (Number)arguments.get(0);
                  if(number != null) {
                     this.sum += number.byteValue();
                  }
               }
            };
         }
      };
      avgFunctionForByte = new NativeAggregateFunction("avg", ByteType.instance, new AbstractType[]{ByteType.instance}) {
         public AggregateFunction.Aggregate newAggregate() {
            return new AggregateFcts.AvgAggregate() {
               public ByteBuffer compute(ProtocolVersion protocolVersion) throws InvalidRequestException {
                  return ByteType.instance.decompose(Byte.valueOf((byte)((int)this.computeInternal())));
               }
            };
         }
      };
      sumFunctionForShort = new NativeAggregateFunction("sum", ShortType.instance, new AbstractType[]{ShortType.instance}) {
         public AggregateFunction.Aggregate newAggregate() {
            return new AggregateFunction.Aggregate() {
               private short sum;

               public void reset() {
                  this.sum = 0;
               }

               public ByteBuffer compute(ProtocolVersion protocolVersion) {
                  return ((ShortType)returnType()).decompose(Short.valueOf(this.sum));
               }

               public void addInput(Arguments arguments) {
                  Number number = (Number)arguments.get(0);
                  if(number != null) {
                     this.sum += number.shortValue();
                  }
               }
            };
         }
      };
      avgFunctionForShort = new NativeAggregateFunction("avg", ShortType.instance, new AbstractType[]{ShortType.instance}) {
         public AggregateFunction.Aggregate newAggregate() {
            return new AggregateFcts.AvgAggregate() {
               public ByteBuffer compute(ProtocolVersion protocolVersion) {
                  return ShortType.instance.decompose(Short.valueOf((short)((int)this.computeInternal())));
               }
            };
         }
      };
      sumFunctionForInt32 = new NativeAggregateFunction("sum", Int32Type.instance, new AbstractType[]{Int32Type.instance}) {
         public AggregateFunction.Aggregate newAggregate() {
            return new AggregateFunction.Aggregate() {
               private int sum;

               public void reset() {
                  this.sum = 0;
               }

               public ByteBuffer compute(ProtocolVersion protocolVersion) {
                  return ((Int32Type)returnType()).decompose(Integer.valueOf(this.sum));
               }

               public void addInput(Arguments arguments) {
                  Number number = (Number)arguments.get(0);
                  if(number != null) {
                     this.sum += number.intValue();
                  }
               }
            };
         }
      };
      avgFunctionForInt32 = new NativeAggregateFunction("avg", Int32Type.instance, new AbstractType[]{Int32Type.instance}) {
         public AggregateFunction.Aggregate newAggregate() {
            return new AggregateFcts.AvgAggregate() {
               public ByteBuffer compute(ProtocolVersion protocolVersion) {
                  return Int32Type.instance.decompose(Integer.valueOf((int)this.computeInternal()));
               }
            };
         }
      };
      sumFunctionForLong = new NativeAggregateFunction("sum", LongType.instance, new AbstractType[]{LongType.instance}) {
         public AggregateFunction.Aggregate newAggregate() {
            return new AggregateFcts.LongSumAggregate();
         }
      };
      avgFunctionForLong = new NativeAggregateFunction("avg", LongType.instance, new AbstractType[]{LongType.instance}) {
         public AggregateFunction.Aggregate newAggregate() {
            return new AggregateFcts.AvgAggregate() {
               public ByteBuffer compute(ProtocolVersion protocolVersion) {
                  return LongType.instance.decompose(Long.valueOf(this.computeInternal()));
               }
            };
         }
      };
      sumFunctionForFloat = new NativeAggregateFunction("sum", FloatType.instance, new AbstractType[]{FloatType.instance}) {
         public AggregateFunction.Aggregate newAggregate() {
            return new AggregateFcts.FloatSumAggregate() {
               public ByteBuffer compute(ProtocolVersion protocolVersion) throws InvalidRequestException {
                  return FloatType.instance.decompose(Float.valueOf((float)this.computeInternal()));
               }
            };
         }
      };
      avgFunctionForFloat = new NativeAggregateFunction("avg", FloatType.instance, new AbstractType[]{FloatType.instance}) {
         public AggregateFunction.Aggregate newAggregate() {
            return new AggregateFcts.FloatAvgAggregate() {
               public ByteBuffer compute(ProtocolVersion protocolVersion) throws InvalidRequestException {
                  return FloatType.instance.decompose(Float.valueOf((float)this.computeInternal()));
               }
            };
         }
      };
      sumFunctionForDouble = new NativeAggregateFunction("sum", DoubleType.instance, new AbstractType[]{DoubleType.instance}) {
         public AggregateFunction.Aggregate newAggregate() {
            return new AggregateFcts.FloatSumAggregate() {
               public ByteBuffer compute(ProtocolVersion protocolVersion) throws InvalidRequestException {
                  return DoubleType.instance.decompose(Double.valueOf(this.computeInternal()));
               }
            };
         }
      };
      avgFunctionForDouble = new NativeAggregateFunction("avg", DoubleType.instance, new AbstractType[]{DoubleType.instance}) {
         public AggregateFunction.Aggregate newAggregate() {
            return new AggregateFcts.FloatAvgAggregate() {
               public ByteBuffer compute(ProtocolVersion protocolVersion) throws InvalidRequestException {
                  return DoubleType.instance.decompose(Double.valueOf(this.computeInternal()));
               }
            };
         }
      };
      sumFunctionForCounter = new NativeAggregateFunction("sum", CounterColumnType.instance, new AbstractType[]{CounterColumnType.instance}) {
         public AggregateFunction.Aggregate newAggregate() {
            return new AggregateFcts.LongSumAggregate();
         }
      };
      avgFunctionForCounter = new NativeAggregateFunction("avg", CounterColumnType.instance, new AbstractType[]{CounterColumnType.instance}) {
         public AggregateFunction.Aggregate newAggregate() {
            return new AggregateFcts.AvgAggregate() {
               public ByteBuffer compute(ProtocolVersion protocolVersion) throws InvalidRequestException {
                  return CounterColumnType.instance.decompose(Long.valueOf(this.computeInternal()));
               }
            };
         }
      };
      minFunctionForCounter = new NativeAggregateFunction("min", CounterColumnType.instance, new AbstractType[]{CounterColumnType.instance}) {
         public AggregateFunction.Aggregate newAggregate() {
            return new AggregateFunction.Aggregate() {
               private Long min;

               public void reset() {
                  this.min = null;
               }

               public ByteBuffer compute(ProtocolVersion protocolVersion) {
                  return this.min != null?LongType.instance.decompose(this.min):null;
               }

               public void addInput(Arguments arguments) {
                  Number number = (Number)arguments.get(0);
                  if(number != null) {
                     long lval = number.longValue();
                     if(this.min == null || lval < this.min.longValue()) {
                        this.min = Long.valueOf(lval);
                     }

                  }
               }
            };
         }
      };
      maxFunctionForCounter = new NativeAggregateFunction("max", CounterColumnType.instance, new AbstractType[]{CounterColumnType.instance}) {
         public AggregateFunction.Aggregate newAggregate() {
            return new AggregateFunction.Aggregate() {
               private Long max;

               public void reset() {
                  this.max = null;
               }

               public ByteBuffer compute(ProtocolVersion protocolVersion) {
                  return this.max != null?LongType.instance.decompose(this.max):null;
               }

               public void addInput(Arguments arguments) {
                  Number number = (Number)arguments.get(0);
                  if(number != null) {
                     long lval = number.longValue();
                     if(this.max == null || lval > this.max.longValue()) {
                        this.max = Long.valueOf(lval);
                     }

                  }
               }
            };
         }
      };
   }

   private abstract static class AvgAggregate implements AggregateFunction.Aggregate {
      private long sum;
      private int count;
      private BigInteger bigSum;
      private boolean overflow;

      private AvgAggregate() {
         this.bigSum = null;
         this.overflow = false;
      }

      public void reset() {
         this.count = 0;
         this.sum = 0L;
         this.overflow = false;
         this.bigSum = null;
      }

      long computeInternal() {
         return this.overflow?this.bigSum.divide(BigInteger.valueOf((long)this.count)).longValue():(this.count == 0?0L:this.sum / (long)this.count);
      }

      public void addInput(Arguments arguments) {
         Number number = (Number)arguments.get(0);
         if(number != null) {
            ++this.count;
            long l = number.longValue();
            if(this.overflow) {
               this.bigSum = this.bigSum.add(BigInteger.valueOf(l));
            } else {
               long prev = this.sum;
               this.sum += l;
               if(((prev ^ this.sum) & (l ^ this.sum)) < 0L) {
                  this.overflow = true;
                  this.bigSum = BigInteger.valueOf(prev).add(BigInteger.valueOf(l));
               }
            }

         }
      }
   }

   private static class LongSumAggregate implements AggregateFunction.Aggregate {
      private long sum;

      private LongSumAggregate() {
      }

      public void reset() {
         this.sum = 0L;
      }

      public ByteBuffer compute(ProtocolVersion protocolVersion) {
         return LongType.instance.decompose(Long.valueOf(this.sum));
      }

      public void addInput(Arguments arguments) {
         Number number = (Number)arguments.get(0);
         if(number != null) {
            this.sum += number.longValue();
         }
      }
   }

   private abstract static class FloatAvgAggregate implements AggregateFunction.Aggregate {
      private double sum;
      private double compensation;
      private double simpleSum;
      private int count;
      private BigDecimal bigSum;
      private boolean overflow;

      private FloatAvgAggregate() {
         this.bigSum = null;
         this.overflow = false;
      }

      public void reset() {
         this.sum = 0.0D;
         this.compensation = 0.0D;
         this.simpleSum = 0.0D;
         this.count = 0;
         this.bigSum = null;
         this.overflow = false;
      }

      public double computeInternal() {
         if(this.count == 0) {
            return 0.0D;
         } else if(this.overflow) {
            return this.bigSum.divide(BigDecimal.valueOf((long)this.count), RoundingMode.HALF_EVEN).doubleValue();
         } else {
            double tmp = this.sum + this.compensation;
            if(Double.isNaN(tmp) && Double.isInfinite(this.simpleSum)) {
               this.sum = this.simpleSum;
            } else {
               this.sum = tmp;
            }

            return this.sum / (double)this.count;
         }
      }

      public void addInput(Arguments arguments) {
         Number number = (Number)arguments.get(0);
         if(number != null) {
            ++this.count;
            double d = number.doubleValue();
            if(this.overflow) {
               this.bigSum = this.bigSum.add(BigDecimal.valueOf(d));
            } else {
               this.simpleSum += d;
               double prev = this.sum;
               double tmp = d - this.compensation;
               double rounded = this.sum + tmp;
               this.compensation = rounded - this.sum - tmp;
               this.sum = rounded;
               if(Double.isInfinite(this.sum) && !Double.isInfinite(d)) {
                  this.overflow = true;
                  this.bigSum = BigDecimal.valueOf(prev).add(BigDecimal.valueOf(d));
               }
            }

         }
      }
   }

   private abstract static class FloatSumAggregate implements AggregateFunction.Aggregate {
      private double sum;
      private double compensation;
      private double simpleSum;

      private FloatSumAggregate() {
      }

      public void reset() {
         this.sum = 0.0D;
         this.compensation = 0.0D;
         this.simpleSum = 0.0D;
      }

      public void addInput(Arguments arguments) {
         Number number = (Number)arguments.get(0);
         if(number != null) {
            double d = number.doubleValue();
            this.simpleSum += d;
            double tmp = d - this.compensation;
            double rounded = this.sum + tmp;
            this.compensation = rounded - this.sum - tmp;
            this.sum = rounded;
         }
      }

      public double computeInternal() {
         double tmp = this.sum + this.compensation;
         return Double.isNaN(tmp) && Double.isInfinite(this.simpleSum)?this.simpleSum:tmp;
      }
   }
}
