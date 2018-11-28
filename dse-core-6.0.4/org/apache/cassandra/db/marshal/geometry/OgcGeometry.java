package org.apache.cassandra.db.marshal.geometry;

import com.esri.core.geometry.GeometryException;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCGeometry;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.cassandra.serializers.MarshalException;
import org.json.JSONException;

public abstract class OgcGeometry {
   public static final SpatialReference SPATIAL_REFERENCE_4326 = SpatialReference.create(4326);
   public static final String SERIALIZE_NATIVE_ORDER_SYS_PROP = "dse.test.geotype.serialize_native_order";
   private static final boolean SERIALIZE_NATIVE_ORDER = Boolean.getBoolean("dse.test.geotype.serialize_native_order");

   public OgcGeometry() {
   }

   public abstract GeometricType getType();

   public abstract void validate() throws MarshalException;

   public abstract OgcGeometry.Serializer getSerializer();

   static void validateType(OGCGeometry geometry, Class<? extends OGCGeometry> klass) {
      if(!geometry.getClass().equals(klass)) {
         throw new MarshalException(String.format("%s is not of type %s", new Object[]{geometry.getClass().getSimpleName(), klass.getSimpleName()}));
      }
   }

   static ByteBuffer getWkb(OGCGeometry geometry) {
      try {
         return geometry.asBinary();
      } catch (IllegalArgumentException | GeometryException var2) {
         throw new MarshalException("Invalid Geometry", var2);
      }
   }

   static String getWkt(OGCGeometry geometry) {
      try {
         return geometry.asText();
      } catch (IllegalArgumentException | GeometryException var2) {
         throw new MarshalException("Invalid Geometry", var2);
      }
   }

   static void validateNormalization(OGCGeometry geometry, ByteBuffer source) {
      ByteBuffer normalized = getWkb(geometry);
      ByteBuffer inputCopy = source.slice();
      if(inputCopy.remaining() > normalized.remaining()) {
         inputCopy.limit(normalized.remaining());
      }

      if(!normalized.equals(inputCopy)) {
         String klass = geometry.getClass().getSimpleName();
         String msg = String.format("%s is not normalized. %s should be defined/serialized as: %s", new Object[]{klass, klass, getWkt(geometry)});
         throw new MarshalException(msg);
      }
   }

   static <T extends OGCGeometry> T fromOgcWellKnownText(String source, Class<T> klass) {
      OGCGeometry geometry;
      try {
         geometry = OGCGeometry.fromText(source);
      } catch (IllegalArgumentException var4) {
         throw new MarshalException(var4.getMessage());
      }

      validateType(geometry, klass);
      return (T)geometry;
   }

   static <T extends OGCGeometry> T fromOgcWellKnownBinary(ByteBuffer source, Class<T> klass) {
      OGCGeometry geometry;
      try {
         geometry = OGCGeometry.fromBinary(source);
      } catch (IllegalArgumentException var4) {
         throw new MarshalException(var4.getMessage());
      }

      validateType(geometry, klass);
      validateNormalization(geometry, source);
      return (T)geometry;
   }

   static <T extends OGCGeometry> T fromOgcGeoJson(String source, Class<T> klass) {
      OGCGeometry geometry;
      try {
         geometry = OGCGeometry.fromGeoJson(source);
      } catch (JSONException | IllegalArgumentException var4) {
         throw new MarshalException(var4.getMessage());
      }

      validateType(geometry, klass);
      return (T)geometry;
   }

   public boolean contains(OgcGeometry geometry) {
      if(!(geometry instanceof OgcGeometry)) {
         throw new UnsupportedOperationException(String.format("%s is not compatible with %s.contains", new Object[]{geometry.getClass().getSimpleName(), this.getClass().getSimpleName()}));
      } else {
         OGCGeometry thisGeometry = this.getOgcGeometry();
         OGCGeometry thatGeometry = geometry.getOgcGeometry();
         return thisGeometry != null && thatGeometry != null?thisGeometry.contains(thatGeometry):false;
      }
   }

   protected abstract OGCGeometry getOgcGeometry();

   static void validateOgcGeometry(OGCGeometry geometry) {
      try {
         if(geometry.is3D()) {
            throw new MarshalException(String.format("'%s' is not 2D", new Object[]{getWkt(geometry)}));
         } else if(!geometry.isSimple()) {
            throw new MarshalException(String.format("'%s' is not simple. Points and edges cannot self-intersect.", new Object[]{getWkt(geometry)}));
         }
      } catch (GeometryException var2) {
         throw new MarshalException("Invalid geometry", var2);
      }
   }

   public String asWellKnownText() {
      return this.getSerializer().toWellKnownText(this);
   }

   public ByteBuffer asWellKnownBinary() {
      return this.getSerializer().toWellKnownBinary(this);
   }

   public String asGeoJson() {
      return this.getSerializer().toGeoJson(this);
   }

   public interface Serializer<T extends OgcGeometry> {
      String toWellKnownText(T var1);

      default ByteBuffer toWellKnownBinary(T geometry) {
         ByteBuffer wkbNativeOrder = this.toWellKnownBinaryNativeOrder(geometry);
         if(OgcGeometry.SERIALIZE_NATIVE_ORDER) {
            return wkbNativeOrder;
         } else {
            ByteBuffer wkbBigEndianOrder = wkbNativeOrder.duplicate();
            wkbBigEndianOrder.order(ByteOrder.BIG_ENDIAN);
            return wkbBigEndianOrder;
         }
      }

      ByteBuffer toWellKnownBinaryNativeOrder(T var1);

      String toGeoJson(T var1);

      T fromWellKnownText(String var1);

      T fromWellKnownBinary(ByteBuffer var1);

      T fromGeoJson(String var1);
   }
}
