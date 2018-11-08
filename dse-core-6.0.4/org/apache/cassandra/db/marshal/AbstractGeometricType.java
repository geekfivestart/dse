package org.apache.cassandra.db.marshal;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.cassandra.cql3.Json;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.Constants.Value;
import org.apache.cassandra.db.marshal.AbstractType.ComparisonType;
import org.apache.cassandra.db.marshal.geometry.GeometricType;
import org.apache.cassandra.db.marshal.geometry.OgcGeometry;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.transport.ProtocolVersion;

public abstract class AbstractGeometricType<T extends OgcGeometry> extends AbstractType<T> {
   private final TypeSerializer<T> serializer = new TypeSerializer<T>() {
      public ByteBuffer serialize(T geometry) {
         return AbstractGeometricType.this.geoSerializer.toWellKnownBinary(geometry);
      }

      public T deserialize(ByteBuffer byteBuffer) {
         return AbstractGeometricType.this.geoSerializer.fromWellKnownBinary(byteBuffer.slice());
      }

      public void validate(ByteBuffer byteBuffer) throws MarshalException {
         int pos = byteBuffer.position();
         AbstractGeometricType.this.geoSerializer.fromWellKnownBinary(byteBuffer.slice()).validate();
         byteBuffer.position(pos);
      }

      public String toString(T geometry) {
         return AbstractGeometricType.this.geoSerializer.toWellKnownText(geometry);
      }

      public Class<T> getType() {
         return AbstractGeometricType.this.klass;
      }
   };
   private final GeometricType type;
   private final Class<T> klass;
   private final OgcGeometry.Serializer<T> geoSerializer;

   public AbstractGeometricType(GeometricType type) {
      super(ComparisonType.BYTE_ORDER);
      this.type = type;
      this.klass = type.getGeoClass();
      this.geoSerializer = type.getSerializer();
   }

   public GeometricType getGeoType() {
      return this.type;
   }

   public ByteBuffer fromString(String s) throws MarshalException {
      try {
         T geometry = this.geoSerializer.fromWellKnownText(s);
         geometry.validate();
         return this.geoSerializer.toWellKnownBinary(geometry);
      } catch (Exception var5) {
         String parentMsg = var5.getMessage() != null?" " + var5.getMessage():"";
         String msg = String.format("Unable to make %s from '%s'", new Object[]{this.getClass().getSimpleName(), s}) + parentMsg;
         throw new MarshalException(msg, var5);
      }
   }

   public Term fromJSONObject(Object parsed) throws MarshalException {
      if(!(parsed instanceof String)) {
         try {
            parsed = Json.JSON_OBJECT_MAPPER.writeValueAsString(parsed);
         } catch (IOException var7) {
            throw new MarshalException(var7.getMessage());
         }
      }

      OgcGeometry geometry;
      try {
         geometry = this.geoSerializer.fromGeoJson((String)parsed);
      } catch (MarshalException var6) {
         try {
            geometry = this.geoSerializer.fromWellKnownText((String)parsed);
         } catch (MarshalException var5) {
            throw new MarshalException(var6.getMessage());
         }
      }

      geometry.validate();
      return new Value(this.geoSerializer.toWellKnownBinary(geometry));
   }

   public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion) {
      return this.geoSerializer.toGeoJson(this.geoSerializer.fromWellKnownBinary(buffer.slice()));
   }

   public TypeSerializer<T> getSerializer() {
      return this.serializer;
   }
}
