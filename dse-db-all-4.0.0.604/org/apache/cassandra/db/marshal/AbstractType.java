package org.apache.cassandra.db.marshal;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.cassandra.cql3.AssignmentTestable;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ByteSource;
import org.apache.cassandra.utils.FastByteOperations;
import org.github.jamm.Unmetered;

@Unmetered
public abstract class AbstractType<T> implements Comparator<ByteBuffer>, AssignmentTestable {
   static final int VARIABLE_LENGTH = -1;
   protected final AbstractType.ComparisonType comparisonType;
   protected final AbstractType.PrimitiveType primitiveType;
   protected final int fixedCompareReturns;
   private final boolean isReversed;
   private final int valueLength;

   protected AbstractType(AbstractType.ComparisonType comparisonType) {
      this(comparisonType, -1, false);
   }

   protected AbstractType(AbstractType.ComparisonType comparisonType, int valueLength) {
      this(comparisonType, valueLength, false);
   }

   protected AbstractType(AbstractType.ComparisonType comparisonType, int valueLength, boolean isReversed) {
      this(comparisonType, valueLength, AbstractType.PrimitiveType.NONE, 0, isReversed);
   }

   protected AbstractType(AbstractType.ComparisonType comparisonType, int valueLength, AbstractType.PrimitiveType primitiveType) {
      this(comparisonType, valueLength, primitiveType, 0, false);
   }

   protected AbstractType(AbstractType.ComparisonType comparisonType, int valueLength, AbstractType.PrimitiveType primitiveType, int fixedCompareReturns) {
      this(comparisonType, valueLength, primitiveType, fixedCompareReturns, false);
   }

   protected AbstractType(AbstractType.ComparisonType comparisonType, int valueLength, AbstractType.PrimitiveType primitiveType, int fixedCompareReturns, boolean isReversed) {
      this.comparisonType = comparisonType;
      this.valueLength = valueLength;
      this.primitiveType = primitiveType;
      this.fixedCompareReturns = fixedCompareReturns;
      this.isReversed = isReversed;
      if(comparisonType == AbstractType.ComparisonType.PRIMITIVE_COMPARE && primitiveType == AbstractType.PrimitiveType.NONE) {
         throw new IllegalArgumentException("PRIMITIVE_COMPARE must have valid type");
      } else if(comparisonType != AbstractType.ComparisonType.PRIMITIVE_COMPARE && primitiveType != AbstractType.PrimitiveType.NONE) {
         throw new IllegalArgumentException("only PRIMITIVE_COMPARE can have fixed sized type");
      } else {
         try {
            Method custom = this.getClass().getMethod("compareCustom", new Class[]{ByteBuffer.class, ByteBuffer.class});
            if(custom.getDeclaringClass() == AbstractType.class == (comparisonType == AbstractType.ComparisonType.CUSTOM) && !isReversed) {
               throw new IllegalStateException((comparisonType == AbstractType.ComparisonType.CUSTOM?"compareCustom must be overridden if ComparisonType is CUSTOM":"compareCustom should not be overridden if ComparisonType is not CUSTOM") + " (" + this.getClass().getSimpleName() + ")");
            }
         } catch (NoSuchMethodException var7) {
            throw new IllegalStateException();
         }
      }
   }

   public static List<String> asCQLTypeStringList(List<AbstractType<?>> abstractTypes) {
      List<String> r = new ArrayList(abstractTypes.size());
      Iterator var2 = abstractTypes.iterator();

      while(var2.hasNext()) {
         AbstractType<?> abstractType = (AbstractType)var2.next();
         r.add(abstractType.asCQL3Type().toString());
      }

      return r;
   }

   public T compose(ByteBuffer bytes) {
      return this.getSerializer().deserialize(bytes);
   }

   public ByteBuffer decompose(T value) {
      return this.getSerializer().serialize(value);
   }

   public String getString(ByteBuffer bytes) {
      if(bytes == null) {
         return "null";
      } else if(bytes == ByteBufferUtil.UNSET_BYTE_BUFFER) {
         return "unset";
      } else {
         TypeSerializer<T> serializer = this.getSerializer();
         serializer.validate(bytes);
         return serializer.toString(serializer.deserialize(bytes));
      }
   }

   public String getString(CellPath path, ByteBuffer bytes) {
      throw new IllegalStateException("Must be implemented by sub-classes that return true in isMultiCell");
   }

   public abstract ByteBuffer fromString(String var1) throws MarshalException;

   public abstract Term fromJSONObject(Object var1) throws MarshalException;

   public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion) {
      return '"' + this.getSerializer().deserialize(buffer).toString() + '"';
   }

   public void validate(ByteBuffer bytes) throws MarshalException {
      this.getSerializer().validate(bytes);
   }

   public final int compare(ByteBuffer left, ByteBuffer right) {
      if (this.comparisonType == ComparisonType.FIXED_COMPARE) {
         return this.fixedCompareReturns;
      }
      if (this.isReversed) {
         ByteBuffer t = left;
         left = right;
         right = t;
      }
      if (!left.hasRemaining() || !right.hasRemaining()) {
         return left.hasRemaining() ? 1 : (right.hasRemaining() ? -1 : 0);
      }
      if (this.comparisonType == ComparisonType.PRIMITIVE_COMPARE) {
         switch (this.primitiveType) {
            case BOOLEAN: {
               return BooleanType.compareType(left, right);
            }
            case BYTE: {
               return ByteType.compareType(left, right);
            }
            case SHORT: {
               return ShortType.compareType(left, right);
            }
            case INT32: {
               return Int32Type.compareType(left, right);
            }
            case FLOAT: {
               return FloatType.compareType(left, right);
            }
            case LONG: {
               return LongType.compareType(left, right);
            }
            case DOUBLE: {
               return DoubleType.compareType(left, right);
            }
         }
         throw new IllegalStateException();
      }
      if (this.comparisonType == ComparisonType.BYTE_ORDER) {
         return FastByteOperations.compareUnsigned(left, right);
      }
      if (this.isReversed) {
         return ((ReversedType)this).baseType.compareCustom(left, right);
      }
      return this.compareCustom(left, right);
   }

   public int compareCustom(ByteBuffer left, ByteBuffer right) {
      throw new UnsupportedOperationException();
   }

   public void validateCellValue(ByteBuffer cellValue) throws MarshalException {
      this.validate(cellValue);
   }

   public CQL3Type asCQL3Type() {
      return new CQL3Type.Custom(this);
   }

   public int compareForCQL(ByteBuffer v1, ByteBuffer v2) {
      return this.compare(v1, v2);
   }

   public abstract TypeSerializer<T> getSerializer();

   public ArgumentDeserializer getArgumentDeserializer() {
      return new AbstractType.DefaultArgumentDerserializer(this);
   }

   public String getString(Collection<ByteBuffer> names) {
      StringBuilder builder = new StringBuilder();
      Iterator var3 = names.iterator();

      while(var3.hasNext()) {
         ByteBuffer name = (ByteBuffer)var3.next();
         builder.append(this.getString(name)).append(",");
      }

      return builder.toString();
   }

   public boolean isCounter() {
      return false;
   }

   public boolean isFrozenCollection() {
      return this.isCollection() && !this.isMultiCell();
   }

   public final boolean isReversed() {
      return this.isReversed;
   }

   public static AbstractType<?> parseDefaultParameters(AbstractType<?> baseType, TypeParser parser) throws SyntaxException {
      Map<String, String> parameters = parser.getKeyValueParameters();
      String reversed = (String)parameters.get("reversed");
      return (AbstractType)(reversed == null || !reversed.isEmpty() && !reversed.equals("true")?baseType:ReversedType.getInstance(baseType));
   }

   public boolean isCompatibleWith(AbstractType<?> previous) {
      return this.equals(previous);
   }

   public boolean isValueCompatibleWith(AbstractType<?> otherType) {
      return this.isValueCompatibleWithInternal(otherType instanceof ReversedType?((ReversedType)otherType).baseType:otherType);
   }

   protected boolean isValueCompatibleWithInternal(AbstractType<?> otherType) {
      return this.isCompatibleWith(otherType);
   }

   public int compareCollectionMembers(ByteBuffer v1, ByteBuffer v2, ByteBuffer collectionName) {
      return this.compare(v1, v2);
   }

   public void validateCollectionMember(ByteBuffer bytes, ByteBuffer collectionName) throws MarshalException {
      this.validate(bytes);
   }

   public boolean isCollection() {
      return false;
   }

   public boolean isUDT() {
      return false;
   }

   public boolean isTuple() {
      return false;
   }

   public boolean isMultiCell() {
      return false;
   }

   public boolean isFreezable() {
      return false;
   }

   public AbstractType<?> freeze() {
      return this;
   }

   public AbstractType<?> freezeNestedMulticellTypes() {
      return this;
   }

   public boolean isEmptyValueMeaningless() {
      return false;
   }

   public String toString(boolean ignoreFreezing) {
      return this.toString();
   }

   public int componentsCount() {
      return 1;
   }

   public List<AbstractType<?>> getComponents() {
      return Collections.singletonList(this);
   }

   public final int valueLengthIfFixed() {
      return this.valueLength;
   }

   private boolean isValueLengthFixed() {
      return this.isValueLengthFixed(this.valueLength);
   }

   private boolean isValueLengthFixed(int valueLength) {
      return valueLength != -1;
   }

   public final void writeValue(ByteBuffer value, DataOutputPlus out) throws IOException {
      assert value.hasRemaining();

      if(this.isValueLengthFixed()) {
         out.write(value);
      } else {
         ByteBufferUtil.writeWithVIntLength(value, out);
      }

   }

   public final int writtenLength(ByteBuffer value) {
      assert value.hasRemaining();

      return this.isValueLengthFixed()?value.remaining():TypeSizes.sizeofWithVIntLength(value);
   }

   public final ByteBuffer readValue(DataInputPlus in) throws IOException {
      return this.readValue(in, 2147483647);
   }

   public final ByteBuffer readValue(DataInputPlus in, int maxValueSize) throws IOException {
      return this.readValue(in, maxValueSize, (ByteBuffer)null);
   }

   public final ByteBuffer readValue(DataInputPlus in, int maxValueSize, ByteBuffer byteBuffer) throws IOException {
      int length = this.valueLengthIfFixed();
      if(this.isValueLengthFixed(length)) {
         return ByteBufferUtil.read(in, length, byteBuffer);
      } else {
         int l = (int)in.readUnsignedVInt();
         if(l < 0) {
            throw new IOException("Corrupt (negative) value length encountered");
         } else if(l > maxValueSize) {
            throw new IOException(String.format("Corrupt value length %d encountered, as it exceeds the maximum of %d, which is set via max_value_size_in_mb in cassandra.yaml", new Object[]{Integer.valueOf(l), Integer.valueOf(maxValueSize)}));
         } else {
            return ByteBufferUtil.read(in, l, byteBuffer);
         }
      }
   }

   public final void skipValue(DataInputPlus in) throws IOException {
      int length = this.valueLengthIfFixed();
      if(this.isValueLengthFixed(length)) {
         in.skipBytesFully(length);
      } else {
         ByteBufferUtil.skipWithVIntLength(in);
      }

   }

   public boolean referencesUserType(String userTypeName) {
      return false;
   }

   public boolean referencesDuration() {
      return false;
   }

   public AssignmentTestable.TestResult testAssignment(AbstractType<?> receiverType) {
      if(this.isFreezable() && !this.isMultiCell()) {
         receiverType = ((AbstractType)receiverType).freeze();
      }

      if(this.isReversed() && !((AbstractType)receiverType).isReversed()) {
         receiverType = ReversedType.getInstance((AbstractType)receiverType);
      }

      return this.equals(receiverType)?AssignmentTestable.TestResult.EXACT_MATCH:(((AbstractType)receiverType).isValueCompatibleWith(this)?AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE:AssignmentTestable.TestResult.NOT_ASSIGNABLE);
   }

   public String toString() {
      return this.getClass().getName();
   }

   public boolean equals(Object other, boolean ignoreFreezing) {
      return this.equals(other);
   }

   public void checkComparable() {
      if(this.comparisonType == AbstractType.ComparisonType.NOT_COMPARABLE) {
         throw new IllegalArgumentException(this + " cannot be used in comparisons, so cannot be used as a clustering column");
      }
   }

   public final AssignmentTestable.TestResult testAssignment(String keyspace, ColumnSpecification receiver) {
      return this.testAssignment(receiver.type);
   }

   public ByteSource asByteComparableSource(ByteBuffer byteBuffer) {
      if(this.comparisonType == AbstractType.ComparisonType.BYTE_ORDER) {
         return ByteSource.of(byteBuffer);
      } else {
         throw new UnsupportedOperationException(this.getClass().getSimpleName() + " does not implement asByteComparableSource");
      }
   }

   protected static class DefaultArgumentDerserializer implements ArgumentDeserializer {
      private final AbstractType<?> type;

      public DefaultArgumentDerserializer(AbstractType<?> type) {
         this.type = type;
      }

      public Object deserialize(ProtocolVersion protocolVersion, ByteBuffer buffer) {
         return buffer != null && (buffer.hasRemaining() || !this.type.isEmptyValueMeaningless())?this.type.compose(buffer):null;
      }
   }

   public static enum PrimitiveType {
      BOOLEAN,
      BYTE,
      SHORT,
      INT32,
      LONG,
      FLOAT,
      DOUBLE,
      NONE;

      private PrimitiveType() {
      }
   }

   public static enum ComparisonType {
      NOT_COMPARABLE,
      BYTE_ORDER,
      FIXED_COMPARE,
      PRIMITIVE_COMPARE,
      CUSTOM;

      private ComparisonType() {
      }
   }
}
