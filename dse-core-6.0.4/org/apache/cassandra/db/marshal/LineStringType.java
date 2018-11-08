package org.apache.cassandra.db.marshal;

import org.apache.cassandra.db.marshal.geometry.GeometricType;
import org.apache.cassandra.db.marshal.geometry.LineString;

public class LineStringType extends AbstractGeometricType<LineString> {
   public static final LineStringType instance = new LineStringType();

   public LineStringType() {
      super(GeometricType.LINESTRING);
   }
}
