package org.apache.cassandra.db.marshal.geometry;

import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCPoint;
import java.nio.ByteBuffer;
import org.apache.cassandra.serializers.MarshalException;

public class Point extends OgcGeometry {
   public static final OgcGeometry.Serializer<Point> serializer = new OgcGeometry.Serializer<Point>() {
      public String toWellKnownText(Point geometry) {
         return geometry.point.asText();
      }

      public ByteBuffer toWellKnownBinaryNativeOrder(Point geometry) {
         return geometry.point.asBinary();
      }

      public String toGeoJson(Point geometry) {
         return geometry.point.asGeoJson();
      }

      public Point fromWellKnownText(String source) {
         return new Point((OGCPoint)OgcGeometry.fromOgcWellKnownText(source, OGCPoint.class), null);
      }

      public Point fromWellKnownBinary(ByteBuffer source) {
         return new Point((OGCPoint)OgcGeometry.fromOgcWellKnownBinary(source, OGCPoint.class), null);
      }

      public Point fromGeoJson(String source) {
         return new Point((OGCPoint)OgcGeometry.fromOgcGeoJson(source, OGCPoint.class), null);
      }
   };
   final OGCPoint point;

   public Point(double x, double y) {
      this(new OGCPoint(new com.esri.core.geometry.Point(x, y), OgcGeometry.SPATIAL_REFERENCE_4326));
   }

   private Point(OGCPoint point) {
      this.point = point;
      this.validate();
   }

   public boolean contains(OgcGeometry geometry) {
      return false;
   }

   public GeometricType getType() {
      return GeometricType.POINT;
   }

   public void validate() throws MarshalException {
      validateOgcGeometry(this.point);
      if(this.point.isEmpty() || this.point.is3D()) {
         throw new MarshalException(this.getClass().getSimpleName() + " requires exactly 2 coordinate values");
      }
   }

   protected OGCGeometry getOgcGeometry() {
      return this.point;
   }

   public OgcGeometry.Serializer getSerializer() {
      return serializer;
   }

   public OGCPoint getOgcPoint() {
      return this.point;
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(o != null && this.getClass() == o.getClass()) {
         boolean var10000;
         label35: {
            Point point1 = (Point)o;
            if(this.point != null) {
               if(this.point.equals(point1.point)) {
                  break label35;
               }
            } else if(point1.point == null) {
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
      return this.point != null?this.point.hashCode():0;
   }

   public String toString() {
      return this.asWellKnownText();
   }
}
