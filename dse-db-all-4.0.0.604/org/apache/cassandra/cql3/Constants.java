package org.apache.cassandra.cql3;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Constants {
   private static final Logger logger = LoggerFactory.getLogger(Constants.class);
   public static final Constants.UnsetLiteral UNSET_LITERAL = new Constants.UnsetLiteral(null);
   public static final Constants.Value UNSET_VALUE;
   public static final Constants.NullLiteral NULL_LITERAL;
   public static final Term.Terminal NULL_VALUE;

   public Constants() {
   }

   static {
      UNSET_VALUE = new Constants.Value(ByteBufferUtil.UNSET_BYTE_BUFFER);
      NULL_LITERAL = new Constants.NullLiteral(null);
      NULL_VALUE = new Constants.Value((ByteBuffer)null) {
         public Term.Terminal bind(QueryOptions options) {
            return null;
         }

         public String toString() {
            return "null";
         }
      };
   }

   public static class Deleter extends Operation {
      public Deleter(ColumnMetadata column) {
         super(column, (Term)null);
      }

      public void execute(DecoratedKey partitionKey, UpdateParameters params) throws InvalidRequestException {
         if(this.column.type.isMultiCell()) {
            params.setComplexDeletionTime(this.column);
         } else {
            params.addTombstone(this.column);
         }

      }
   }

   public static class Substracter extends Operation {
      public Substracter(ColumnMetadata column, Term t) {
         super(column, t);
      }

      public void execute(DecoratedKey partitionKey, UpdateParameters params) throws InvalidRequestException {
         ByteBuffer bytes = this.t.bindAndGet(params.options);
         if(bytes == null) {
            throw new InvalidRequestException("Invalid null value for counter increment");
         } else if(bytes != ByteBufferUtil.UNSET_BYTE_BUFFER) {
            long increment = ByteBufferUtil.toLong(bytes);
            if(increment == -9223372036854775808L) {
               throw new InvalidRequestException("The negation of " + increment + " overflows supported counter precision (signed 8 bytes integer)");
            } else {
               params.addCounter(this.column, -increment);
            }
         }
      }
   }

   public static class Adder extends Operation {
      public Adder(ColumnMetadata column, Term t) {
         super(column, t);
      }

      public void execute(DecoratedKey partitionKey, UpdateParameters params) throws InvalidRequestException {
         ByteBuffer bytes = this.t.bindAndGet(params.options);
         if(bytes == null) {
            throw new InvalidRequestException("Invalid null value for counter increment");
         } else if(bytes != ByteBufferUtil.UNSET_BYTE_BUFFER) {
            long increment = ByteBufferUtil.toLong(bytes);
            params.addCounter(this.column, increment);
         }
      }
   }

   public static class Setter extends Operation {
      public Setter(ColumnMetadata column, Term t) {
         super(column, t);
      }

      public void execute(DecoratedKey partitionKey, UpdateParameters params) throws InvalidRequestException {
         ByteBuffer value = this.t.bindAndGet(params.options);
         if(value == null) {
            params.addTombstone(this.column);
         } else if(value != ByteBufferUtil.UNSET_BYTE_BUFFER) {
            params.addCell(this.column, value);
         }

      }
   }

   public static class Marker extends AbstractMarker {
      protected Marker(int bindIndex, ColumnSpecification receiver) {
         super(bindIndex, receiver);

         assert !receiver.type.isCollection();

      }

      public ByteBuffer bindAndGet(QueryOptions options) throws InvalidRequestException {
         try {
            ByteBuffer value = (ByteBuffer)options.getValues().get(this.bindIndex);
            if(value != null && value != ByteBufferUtil.UNSET_BYTE_BUFFER) {
               this.receiver.type.validate(value);
            }

            return value;
         } catch (MarshalException var3) {
            throw new InvalidRequestException(var3.getMessage());
         }
      }

      public Constants.Value bind(QueryOptions options) throws InvalidRequestException {
         ByteBuffer bytes = this.bindAndGet(options);
         return bytes == null?null:(bytes == ByteBufferUtil.UNSET_BYTE_BUFFER?Constants.UNSET_VALUE:new Constants.Value(bytes));
      }
   }

   public static class Value extends Term.Terminal {
      public final ByteBuffer bytes;

      public Value(ByteBuffer bytes) {
         this.bytes = bytes;
      }

      public ByteBuffer get(ProtocolVersion protocolVersion) {
         return this.bytes;
      }

      public ByteBuffer bindAndGet(QueryOptions options) {
         return this.bytes;
      }

      public String toString() {
         return ByteBufferUtil.bytesToHex(this.bytes);
      }
   }

   public static class Literal extends Term.Raw {
      private final Constants.Type type;
      private final String text;
      private final AbstractType<?> preferedType;

      private Literal(Constants.Type type, String text) {
         assert type != null && text != null;

         this.type = type;
         this.text = text;
         this.preferedType = type.getPreferedTypeFor(text);
      }

      public static Constants.Literal string(String text) {
         return new Constants.Literal(Constants.Type.STRING, text);
      }

      public static Constants.Literal integer(String text) {
         return new Constants.Literal(Constants.Type.INTEGER, text);
      }

      public static Constants.Literal floatingPoint(String text) {
         return new Constants.Literal(Constants.Type.FLOAT, text);
      }

      public static Constants.Literal uuid(String text) {
         return new Constants.Literal(Constants.Type.UUID, text);
      }

      public static Constants.Literal bool(String text) {
         return new Constants.Literal(Constants.Type.BOOLEAN, text);
      }

      public static Constants.Literal hex(String text) {
         return new Constants.Literal(Constants.Type.HEX, text);
      }

      public static Constants.Literal duration(String text) {
         return new Constants.Literal(Constants.Type.DURATION, text);
      }

      public Constants.Value prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException {
         if(!this.testAssignment(keyspace, receiver).isAssignable()) {
            throw new InvalidRequestException(String.format("Invalid %s constant (%s) for \"%s\" of type %s", new Object[]{this.type, this.text, receiver.name, receiver.type.asCQL3Type()}));
         } else {
            return new Constants.Value(this.parsedValue(receiver.type));
         }
      }

      private ByteBuffer parsedValue(AbstractType<?> validator) throws InvalidRequestException {
         if(validator instanceof ReversedType) {
            validator = ((ReversedType)validator).baseType;
         }

         try {
            return this.type == Constants.Type.HEX?BytesType.instance.fromString(this.text.substring(2)):(validator instanceof CounterColumnType?LongType.instance.fromString(this.text):validator.fromString(this.text));
         } catch (MarshalException var3) {
            throw new InvalidRequestException(var3.getMessage());
         }
      }

      public AssignmentTestable.TestResult testAssignment(String keyspace, ColumnSpecification receiver) {
         CQL3Type receiverType = receiver.type.asCQL3Type();
         if(!receiverType.isCollection() && !receiverType.isUDT()) {
            if(!(receiverType instanceof CQL3Type.Native)) {
               return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
            } else {
               CQL3Type.Native nt = (CQL3Type.Native)receiverType;
               if(nt.getType().equals(this.preferedType)) {
                  return AssignmentTestable.TestResult.EXACT_MATCH;
               } else {
                  switch(null.$SwitchMap$org$apache$cassandra$cql3$Constants$Type[this.type.ordinal()]) {
                  case 1:
                     switch(null.$SwitchMap$org$apache$cassandra$cql3$CQL3Type$Native[nt.ordinal()]) {
                     case 1:
                     case 2:
                     case 3:
                     case 4:
                     case 5:
                     case 6:
                     case 7:
                        return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
                     default:
                        return AssignmentTestable.TestResult.NOT_ASSIGNABLE;
                     }
                  case 2:
                     switch(null.$SwitchMap$org$apache$cassandra$cql3$CQL3Type$Native[nt.ordinal()]) {
                     case 5:
                     case 6:
                     case 7:
                     case 8:
                     case 9:
                     case 10:
                     case 11:
                     case 12:
                     case 13:
                     case 14:
                     case 15:
                     case 16:
                     case 17:
                        return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
                     default:
                        return AssignmentTestable.TestResult.NOT_ASSIGNABLE;
                     }
                  case 3:
                     switch(null.$SwitchMap$org$apache$cassandra$cql3$CQL3Type$Native[nt.ordinal()]) {
                     case 18:
                     case 19:
                        return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
                     default:
                        return AssignmentTestable.TestResult.NOT_ASSIGNABLE;
                     }
                  case 4:
                     switch(null.$SwitchMap$org$apache$cassandra$cql3$CQL3Type$Native[nt.ordinal()]) {
                     case 10:
                     case 11:
                     case 13:
                        return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
                     case 12:
                     default:
                        return AssignmentTestable.TestResult.NOT_ASSIGNABLE;
                     }
                  case 5:
                     switch(null.$SwitchMap$org$apache$cassandra$cql3$CQL3Type$Native[nt.ordinal()]) {
                     case 20:
                        return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
                     default:
                        return AssignmentTestable.TestResult.NOT_ASSIGNABLE;
                     }
                  case 6:
                     switch(null.$SwitchMap$org$apache$cassandra$cql3$CQL3Type$Native[nt.ordinal()]) {
                     case 21:
                        return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
                     default:
                        return AssignmentTestable.TestResult.NOT_ASSIGNABLE;
                     }
                  case 7:
                     switch(null.$SwitchMap$org$apache$cassandra$cql3$CQL3Type$Native[nt.ordinal()]) {
                     case 12:
                        return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
                     }
                  }

                  return AssignmentTestable.TestResult.NOT_ASSIGNABLE;
               }
            }
         } else {
            return AssignmentTestable.TestResult.NOT_ASSIGNABLE;
         }
      }

      public AbstractType<?> getExactTypeIfKnown(String keyspace) {
         return null;
      }

      public String getRawText() {
         return this.text;
      }

      public String getText() {
         return this.type == Constants.Type.STRING?String.format("'%s'", new Object[]{this.text}):this.text;
      }
   }

   private static class NullLiteral extends Term.Raw {
      private NullLiteral() {
      }

      public Term prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException {
         if(!this.testAssignment(keyspace, receiver).isAssignable()) {
            throw new InvalidRequestException("Invalid null value for counter increment/decrement");
         } else {
            return Constants.NULL_VALUE;
         }
      }

      public AssignmentTestable.TestResult testAssignment(String keyspace, ColumnSpecification receiver) {
         return receiver.type instanceof CounterColumnType?AssignmentTestable.TestResult.NOT_ASSIGNABLE:AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
      }

      public String getText() {
         return "NULL";
      }

      public AbstractType<?> getExactTypeIfKnown(String keyspace) {
         return null;
      }
   }

   private static class UnsetLiteral extends Term.Raw {
      private UnsetLiteral() {
      }

      public Term prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException {
         return Constants.UNSET_VALUE;
      }

      public AssignmentTestable.TestResult testAssignment(String keyspace, ColumnSpecification receiver) {
         return AssignmentTestable.TestResult.NOT_ASSIGNABLE;
      }

      public String getText() {
         return "";
      }

      public AbstractType<?> getExactTypeIfKnown(String keyspace) {
         return null;
      }
   }

   public static enum Type {
      STRING,
      INTEGER {
         public AbstractType<?> getPreferedTypeFor(String text) {
            BigInteger b = new BigInteger(text);
            return (AbstractType)(b.equals(BigInteger.valueOf((long)b.intValue()))?Int32Type.instance:(b.equals(BigInteger.valueOf(b.longValue()))?LongType.instance:IntegerType.instance));
         }
      },
      UUID,
      FLOAT {
         public AbstractType<?> getPreferedTypeFor(String text) {
            if(!"NaN".equals(text) && !"-NaN".equals(text) && !"Infinity".equals(text) && !"-Infinity".equals(text)) {
               BigDecimal b = new BigDecimal(text);
               return (AbstractType)(b.compareTo(BigDecimal.valueOf(b.doubleValue())) == 0?DoubleType.instance:DecimalType.instance);
            } else {
               return DoubleType.instance;
            }
         }
      },
      BOOLEAN,
      HEX,
      DURATION;

      private Type() {
      }

      public AbstractType<?> getPreferedTypeFor(String text) {
         return null;
      }
   }
}
