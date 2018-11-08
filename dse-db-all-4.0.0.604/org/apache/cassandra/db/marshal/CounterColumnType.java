package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.serializers.CounterSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

public class CounterColumnType extends NumberType<Long> {
   public static final CounterColumnType instance = new CounterColumnType();

   CounterColumnType() {
      super(AbstractType.ComparisonType.NOT_COMPARABLE, -1);
   }

   public boolean isEmptyValueMeaningless() {
      return true;
   }

   public boolean isCounter() {
      return true;
   }

   public Long compose(ByteBuffer bytes) {
      return Long.valueOf(CounterContext.instance().total(bytes));
   }

   public ByteBuffer decompose(Long value) {
      return ByteBufferUtil.bytes(value.longValue());
   }

   public void validateCellValue(ByteBuffer cellValue) throws MarshalException {
      CounterContext.instance().validateContext(cellValue);
   }

   public String getString(ByteBuffer bytes) {
      return ByteBufferUtil.bytesToHex(bytes);
   }

   public ByteBuffer fromString(String source) {
      return ByteBufferUtil.hexToBytes(source);
   }

   public Term fromJSONObject(Object parsed) {
      throw new UnsupportedOperationException();
   }

   public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion) {
      return CounterSerializer.instance.deserialize(buffer).toString();
   }

   public CQL3Type asCQL3Type() {
      return CQL3Type.Native.COUNTER;
   }

   public TypeSerializer<Long> getSerializer() {
      return CounterSerializer.instance;
   }

   public ByteBuffer add(Number left, Number right) {
      return ByteBufferUtil.bytes(left.longValue() + right.longValue());
   }

   public ByteBuffer substract(Number left, Number right) {
      return ByteBufferUtil.bytes(left.longValue() - right.longValue());
   }

   public ByteBuffer multiply(Number left, Number right) {
      return ByteBufferUtil.bytes(left.longValue() * right.longValue());
   }

   public ByteBuffer divide(Number left, Number right) {
      return ByteBufferUtil.bytes(left.longValue() / right.longValue());
   }

   public ByteBuffer mod(Number left, Number right) {
      return ByteBufferUtil.bytes(left.longValue() % right.longValue());
   }

   public ByteBuffer negate(Number input) {
      return ByteBufferUtil.bytes(-input.longValue());
   }

   public ArgumentDeserializer getArgumentDeserializer() {
      return LongType.instance.getArgumentDeserializer();
   }
}
