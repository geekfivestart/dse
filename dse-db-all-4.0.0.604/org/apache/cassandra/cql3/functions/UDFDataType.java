package org.apache.cassandra.cql3.functions;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.TypeCodec.PrimitiveByteCodec;
import com.datastax.driver.core.TypeCodec.PrimitiveDoubleCodec;
import com.datastax.driver.core.TypeCodec.PrimitiveFloatCodec;
import com.datastax.driver.core.TypeCodec.PrimitiveIntCodec;
import com.datastax.driver.core.TypeCodec.PrimitiveLongCodec;
import com.datastax.driver.core.TypeCodec.PrimitiveShortCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.google.common.reflect.TypeToken;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.JavaDriverUtils;

public final class UDFDataType {
   private static final Pattern JAVA_LANG_PREFIX = Pattern.compile("\\bjava\\.lang\\.");
   private final AbstractType<?> abstractType;
   private final TypeCodec<?> typeCodec;
   private final TypeToken<?> javaType;

   private UDFDataType(AbstractType<?> abstractType, boolean usePrimitive) {
      this.abstractType = abstractType;
      this.typeCodec = JavaDriverUtils.codecFor(abstractType);
      TypeToken<?> token = this.typeCodec.getJavaType();
      this.javaType = usePrimitive?token.unwrap():token;
   }

   public AbstractType<?> toAbstractType() {
      return this.abstractType;
   }

   public Class<?> toJavaClass() {
      return this.typeCodec.getJavaType().getRawType();
   }

   public String getJavaTypeName() {
      String n = this.javaType.toString();
      return JAVA_LANG_PREFIX.matcher(n).replaceAll("");
   }

   public boolean isPrimitive() {
      return this.javaType.isPrimitive();
   }

   public static UDFDataType wrap(AbstractType<?> abstractType, boolean usePrimitive) {
      return new UDFDataType(abstractType, usePrimitive);
   }

   public static List<UDFDataType> wrap(List<AbstractType<?>> argTypes, boolean usePrimitive) {
      List<UDFDataType> types = new ArrayList(argTypes.size());
      Iterator var3 = argTypes.iterator();

      while(var3.hasNext()) {
         AbstractType<?> argType = (AbstractType)var3.next();
         types.add(wrap(argType, usePrimitive));
      }

      return types;
   }

   public DataType toDataType() {
      return this.typeCodec.getCqlType();
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.abstractType, this.javaType});
   }

   public boolean equals(Object obj) {
      if(!(obj instanceof UDFDataType)) {
         return false;
      } else {
         UDFDataType that = (UDFDataType)obj;
         return this.abstractType.equals(that.abstractType, true) && this.javaType.equals(that.javaType);
      }
   }

   public Object compose(ProtocolVersion protocolVersion, ByteBuffer buffer) {
      return buffer != null && (buffer.remaining() != 0 || !this.abstractType.isEmptyValueMeaningless())?this.typeCodec.deserialize(buffer, com.datastax.driver.core.ProtocolVersion.fromInt(protocolVersion.asInt())):null;
   }

   public ByteBuffer decompose(ProtocolVersion protocolVersion, Object value) {
      if(value == null) {
         return null;
      } else if(!this.toJavaClass().isAssignableFrom(value.getClass())) {
         throw new InvalidTypeException("Invalid value for CQL type " + this.toDataType().getName());
      } else {
         return this.typeCodec.serialize(value, com.datastax.driver.core.ProtocolVersion.fromInt(protocolVersion.asInt()));
      }
   }

   public ByteBuffer decompose(ProtocolVersion protocolVersion, byte value) {
      if(!(this.typeCodec instanceof PrimitiveByteCodec)) {
         throw new InvalidTypeException("Invalid value for CQL type " + this.toDataType().getName());
      } else {
         return ((PrimitiveByteCodec)this.typeCodec).serializeNoBoxing(value, com.datastax.driver.core.ProtocolVersion.fromInt(protocolVersion.asInt()));
      }
   }

   public ByteBuffer decompose(ProtocolVersion protocolVersion, short value) {
      if(!(this.typeCodec instanceof PrimitiveShortCodec)) {
         throw new InvalidTypeException("Invalid value for CQL type " + this.toDataType().getName());
      } else {
         return ((PrimitiveShortCodec)this.typeCodec).serializeNoBoxing(value, com.datastax.driver.core.ProtocolVersion.fromInt(protocolVersion.asInt()));
      }
   }

   public ByteBuffer decompose(ProtocolVersion protocolVersion, int value) {
      if(!(this.typeCodec instanceof PrimitiveIntCodec)) {
         throw new InvalidTypeException("Invalid value for CQL type " + this.toDataType().getName());
      } else {
         return ((PrimitiveIntCodec)this.typeCodec).serializeNoBoxing(value, com.datastax.driver.core.ProtocolVersion.fromInt(protocolVersion.asInt()));
      }
   }

   public ByteBuffer decompose(ProtocolVersion protocolVersion, long value) {
      if(!(this.typeCodec instanceof PrimitiveLongCodec)) {
         throw new InvalidTypeException("Invalid value for CQL type " + this.toDataType().getName());
      } else {
         return ((PrimitiveLongCodec)this.typeCodec).serializeNoBoxing(value, com.datastax.driver.core.ProtocolVersion.fromInt(protocolVersion.asInt()));
      }
   }

   public ByteBuffer decompose(ProtocolVersion protocolVersion, float value) {
      if(!(this.typeCodec instanceof PrimitiveFloatCodec)) {
         throw new InvalidTypeException("Invalid value for CQL type " + this.toDataType().getName());
      } else {
         return ((PrimitiveFloatCodec)this.typeCodec).serializeNoBoxing(value, com.datastax.driver.core.ProtocolVersion.fromInt(protocolVersion.asInt()));
      }
   }

   public ByteBuffer decompose(ProtocolVersion protocolVersion, double value) {
      if(!(this.typeCodec instanceof PrimitiveDoubleCodec)) {
         throw new InvalidTypeException("Invalid value for CQL type " + this.toDataType().getName());
      } else {
         return ((PrimitiveDoubleCodec)this.typeCodec).serializeNoBoxing(value, com.datastax.driver.core.ProtocolVersion.fromInt(protocolVersion.asInt()));
      }
   }

   public ArgumentDeserializer getArgumentDeserializer() {
      return this.isPrimitive()?this.abstractType.getArgumentDeserializer():new ArgumentDeserializer() {
         public Object deserialize(ProtocolVersion protocolVersion, ByteBuffer buffer) {
            return UDFDataType.this.compose(protocolVersion, buffer);
         }
      };
   }
}
