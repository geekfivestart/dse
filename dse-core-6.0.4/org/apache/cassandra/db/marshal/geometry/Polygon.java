package org.apache.cassandra.db.marshal.geometry;

import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCPolygon;
import java.nio.ByteBuffer;
import org.apache.cassandra.serializers.MarshalException;

public class Polygon extends OgcGeometry {
   public static final OgcGeometry.Serializer<Polygon> serializer = new OgcGeometry.Serializer<Polygon>() {
      public String toWellKnownText(Polygon geometry) {
         return geometry.polygon.asText();
      }

      public ByteBuffer toWellKnownBinaryNativeOrder(Polygon geometry) {
         return geometry.polygon.asBinary();
      }

      public String toGeoJson(Polygon geometry) {
         return geometry.polygon.asGeoJson();
      }

      public Polygon fromWellKnownText(String source) {
         return new Polygon((OGCPolygon)OgcGeometry.fromOgcWellKnownText(source, OGCPolygon.class));
      }

      public Polygon fromWellKnownBinary(ByteBuffer source) {
         return new Polygon((OGCPolygon)OgcGeometry.fromOgcWellKnownBinary(source, OGCPolygon.class));
      }

      public Polygon fromGeoJson(String source) {
         return new Polygon((OGCPolygon)OgcGeometry.fromOgcGeoJson(source, OGCPolygon.class));
      }
   };
   OGCPolygon polygon;

   public Polygon(OGCPolygon polygon) {
      this.polygon = polygon;
      this.validate();
   }

   protected OGCGeometry getOgcGeometry() {
      return this.polygon;
   }

   public GeometricType getType() {
      return GeometricType.POLYGON;
   }

   public void validate() throws MarshalException {
      validateOgcGeometry(this.polygon);
   }

   public OgcGeometry.Serializer getSerializer() {
      return serializer;
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(o != null && this.getClass() == o.getClass()) {
         boolean var10000;
         label35: {
            Polygon polygon1 = (Polygon)o;
            if(this.polygon != null) {
               if(this.polygon.equals(polygon1.polygon)) {
                  break label35;
               }
            } else if(polygon1.polygon == null) {
               break label35;
            }

            var10000 = false;
            return var10000;
         }

         var10000 = true;
         return var10000;
      } else {
         return false;
      }
   }

   public int hashCode() {
      return this.polygon != null?this.polygon.hashCode():0;
   }

   public String toString() {
      return this.asWellKnownText();
   }
}
