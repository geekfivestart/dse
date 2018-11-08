package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteSource;

public class ReversedType<T> extends AbstractType<T> {
   private static final Map<AbstractType<?>, ReversedType> instances = new HashMap();
   public final AbstractType<T> baseType;

   public static <T> ReversedType<T> getInstance(TypeParser parser) throws ConfigurationException, SyntaxException {
      List<AbstractType<?>> types = parser.getTypeParameters();
      if(types.size() != 1) {
         throw new ConfigurationException("ReversedType takes exactly one argument, " + types.size() + " given");
      } else {
         return getInstance((AbstractType)types.get(0));
      }
   }

   public static synchronized <T> ReversedType<T> getInstance(AbstractType<T> baseType) {
      ReversedType<T> type = (ReversedType)instances.get(baseType);
      if(type == null) {
         type = new ReversedType(baseType);
         instances.put(baseType, type);
      }

      return type;
   }

   private ReversedType(AbstractType<T> baseType) {
      super(baseType.comparisonType, baseType.valueLengthIfFixed(), baseType.primitiveType, baseType.fixedCompareReturns, true);
      this.baseType = baseType;
   }

   public boolean isEmptyValueMeaningless() {
      return this.baseType.isEmptyValueMeaningless();
   }

   public ByteSource asByteComparableSource(ByteBuffer b) {
      final ByteSource src = this.baseType.asByteComparableSource(b);
      return src == null?null:new ByteSource.WithToString() {
         public void reset() {
            src.reset();
         }

         public int next() {
            int v = src.next();
            return v == -1?v:v ^ 255;
         }
      };
   }

   public int compareForCQL(ByteBuffer v1, ByteBuffer v2) {
      return this.baseType.compare(v1, v2);
   }

   public String getString(ByteBuffer bytes) {
      return this.baseType.getString(bytes);
   }

   public ByteBuffer fromString(String source) {
      return this.baseType.fromString(source);
   }

   public Term fromJSONObject(Object parsed) throws MarshalException {
      return this.baseType.fromJSONObject(parsed);
   }

   public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion) {
      return this.baseType.toJSONString(buffer, protocolVersion);
   }

   public boolean isCompatibleWith(AbstractType<?> otherType) {
      return !(otherType instanceof ReversedType)?false:this.baseType.isCompatibleWith(((ReversedType)otherType).baseType);
   }

   public boolean isValueCompatibleWith(AbstractType<?> otherType) {
      return this.baseType.isValueCompatibleWith(otherType);
   }

   public CQL3Type asCQL3Type() {
      return this.baseType.asCQL3Type();
   }

   public TypeSerializer<T> getSerializer() {
      return this.baseType.getSerializer();
   }

   public boolean referencesUserType(String userTypeName) {
      return this.baseType.referencesUserType(userTypeName);
   }

   public String toString() {
      return this.getClass().getName() + "(" + this.baseType + ")";
   }

   public ArgumentDeserializer getArgumentDeserializer() {
      return this.baseType.getArgumentDeserializer();
   }
}
