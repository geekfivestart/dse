package org.apache.cassandra.db.marshal;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import java.nio.ByteBuffer;
import org.apache.cassandra.db.marshal.geometry.LineString;
import org.apache.cassandra.db.marshal.geometry.OgcGeometry;
import org.apache.cassandra.db.marshal.geometry.Point;
import org.apache.cassandra.db.marshal.geometry.Polygon;

public class GeometryCodec<T extends OgcGeometry> extends TypeCodec<T> {
   public static TypeCodec<Point> pointCodec;
   public static TypeCodec<LineString> lineStringCodec;
   public static TypeCodec<Polygon> polygonCodec;
   private final OgcGeometry.Serializer<T> serializer;

   public GeometryCodec(AbstractGeometricType type) {
      super(DataType.custom(type.getClass().getName()), type.getGeoType().getGeoClass());
      this.serializer = type.getGeoType().getSerializer();
   }

   public T deserialize(ByteBuffer bb, ProtocolVersion protocolVersion) throws InvalidTypeException {
      return bb != null && bb.remaining() != 0?this.serializer.fromWellKnownBinary(bb):null;
   }

   public ByteBuffer serialize(T geometry, ProtocolVersion protocolVersion) throws InvalidTypeException {
      return geometry == null?null:geometry.asWellKnownBinary();
   }

   public T parse(String s) throws InvalidTypeException {
      return s != null && !s.isEmpty() && !s.equalsIgnoreCase("NULL")?this.serializer.fromWellKnownText(s):null;
   }

   public String format(T geometry) throws InvalidTypeException {
      return geometry == null?"NULL":geometry.asWellKnownText();
   }

   static {
      pointCodec = new GeometryCodec(PointType.instance);
      lineStringCodec = new GeometryCodec(LineStringType.instance);
      polygonCodec = new GeometryCodec(PolygonType.instance);
   }
}
