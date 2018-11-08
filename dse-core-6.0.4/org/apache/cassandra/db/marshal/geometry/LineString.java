package org.apache.cassandra.db.marshal.geometry;

import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCLineString;
import java.nio.ByteBuffer;
import org.apache.cassandra.serializers.MarshalException;

public class LineString extends OgcGeometry {
   public static final OgcGeometry.Serializer<LineString> serializer = new OgcGeometry.Serializer<LineString>() {
      public String toWellKnownText(LineString geometry) {
         return geometry.lineString.asText();
      }

      public ByteBuffer toWellKnownBinaryNativeOrder(LineString geometry) {
         return geometry.lineString.asBinary();
      }

      public String toGeoJson(LineString geometry) {
         return geometry.lineString.asGeoJson();
      }

      public LineString fromWellKnownText(String source) {
         return new LineString((OGCLineString)OgcGeometry.fromOgcWellKnownText(source, OGCLineString.class));
      }

      public LineString fromWellKnownBinary(ByteBuffer source) {
         return new LineString((OGCLineString)OgcGeometry.fromOgcWellKnownBinary(source, OGCLineString.class));
      }

      public LineString fromGeoJson(String source) {
         return new LineString((OGCLineString)OgcGeometry.fromOgcGeoJson(source, OGCLineString.class));
      }
   };
   private final OGCLineString lineString;

   public LineString(OGCLineString lineString) {
      this.lineString = lineString;
      this.validate();
   }

   public GeometricType getType() {
      return GeometricType.LINESTRING;
   }

   public void validate() throws MarshalException {
      validateOgcGeometry(this.lineString);
   }

   public OgcGeometry.Serializer getSerializer() {
      return serializer;
   }

   protected OGCGeometry getOgcGeometry() {
      return this.lineString;
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(o != null && this.getClass() == o.getClass()) {
         boolean var10000;
         label35: {
            LineString that = (LineString)o;
            if(this.lineString != null) {
               if(this.lineString.equals(that.lineString)) {
                  break label35;
               }
            } else if(that.lineString == null) {
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
      return this.lineString != null?this.lineString.hashCode():0;
   }

   public String toString() {
      return this.asWellKnownText();
   }
}
