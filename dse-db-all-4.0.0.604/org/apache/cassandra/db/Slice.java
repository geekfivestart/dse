package org.apache.cassandra.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class Slice {
   public static final Slice.Serializer serializer = new Slice.Serializer();
   public static final Slice ALL;
   private final ClusteringBound start;
   private final ClusteringBound end;

   private Slice(ClusteringBound start, ClusteringBound end) {
      assert start.isStart() && end.isEnd();

      this.start = start;
      this.end = end;
   }

   public static Slice make(ClusteringBound start, ClusteringBound end) {
      return start == ClusteringBound.BOTTOM && end == ClusteringBound.TOP?ALL:new Slice(start, end);
   }

   public static Slice make(ClusteringComparator comparator, Object... values) {
      CBuilder builder = CBuilder.create(comparator);
      Object[] var3 = values;
      int var4 = values.length;

      for(int var5 = 0; var5 < var4; ++var5) {
         Object val = var3[var5];
         if(val instanceof ByteBuffer) {
            builder.add((ByteBuffer)val);
         } else {
            builder.add(val);
         }
      }

      return new Slice(builder.buildBound(true, true), builder.buildBound(false, true));
   }

   public static Slice make(Clustering clustering) {
      assert clustering != Clustering.STATIC_CLUSTERING;

      ByteBuffer[] values = extractValues(clustering);
      return new Slice(ClusteringBound.inclusiveStartOf(values), ClusteringBound.inclusiveEndOf(values));
   }

   public static Slice make(Clustering start, Clustering end) {
      assert start != Clustering.STATIC_CLUSTERING && end != Clustering.STATIC_CLUSTERING;

      ByteBuffer[] startValues = extractValues(start);
      ByteBuffer[] endValues = extractValues(end);
      return new Slice(ClusteringBound.inclusiveStartOf(startValues), ClusteringBound.inclusiveEndOf(endValues));
   }

   private static ByteBuffer[] extractValues(ClusteringPrefix clustering) {
      ByteBuffer[] values = new ByteBuffer[clustering.size()];

      for(int i = 0; i < clustering.size(); ++i) {
         values[i] = clustering.get(i);
      }

      return values;
   }

   public ClusteringBound start() {
      return this.start;
   }

   public ClusteringBound end() {
      return this.end;
   }

   public ClusteringBound open(boolean reversed) {
      return reversed?this.end:this.start;
   }

   public ClusteringBound close(boolean reversed) {
      return reversed?this.start:this.end;
   }

   public boolean isEmpty(ClusteringComparator comparator) {
      return isEmpty(comparator, this.start(), this.end());
   }

   public static boolean isEmpty(ClusteringComparator comparator, ClusteringBound start, ClusteringBound end) {
      assert start.isStart() && end.isEnd();

      return comparator.compare((ClusteringPrefix)end, (ClusteringPrefix)start) <= 0;
   }

   public boolean includes(ClusteringComparator comparator, ClusteringPrefix bound) {
      return comparator.compare((ClusteringPrefix)this.start, (ClusteringPrefix)bound) <= 0 && comparator.compare((ClusteringPrefix)bound, (ClusteringPrefix)this.end) <= 0;
   }

   public Slice forPaging(ClusteringComparator comparator, Clustering lastReturned, boolean inclusive, boolean reversed) {
      if(lastReturned == null) {
         return this;
      } else {
         int cmp;
         ByteBuffer[] values;
         if(reversed) {
            cmp = comparator.compare((ClusteringPrefix)lastReturned, (ClusteringPrefix)this.start);
            if(cmp >= 0 && (inclusive || cmp != 0)) {
               cmp = comparator.compare((ClusteringPrefix)this.end, (ClusteringPrefix)lastReturned);
               if(cmp < 0 || inclusive && cmp == 0) {
                  return this;
               } else {
                  values = extractValues(lastReturned);
                  return new Slice(this.start, inclusive?ClusteringBound.inclusiveEndOf(values):ClusteringBound.exclusiveEndOf(values));
               }
            } else {
               return null;
            }
         } else {
            cmp = comparator.compare((ClusteringPrefix)this.end, (ClusteringPrefix)lastReturned);
            if(cmp >= 0 && (inclusive || cmp != 0)) {
               cmp = comparator.compare((ClusteringPrefix)lastReturned, (ClusteringPrefix)this.start);
               if(cmp < 0 || inclusive && cmp == 0) {
                  return this;
               } else {
                  values = extractValues(lastReturned);
                  return new Slice(inclusive?ClusteringBound.inclusiveStartOf(values):ClusteringBound.exclusiveStartOf(values), this.end);
               }
            } else {
               return null;
            }
         }
      }
   }

   public boolean intersects(ClusteringComparator comparator, List<ByteBuffer> minClusteringValues, List<ByteBuffer> maxClusteringValues) {
      if(this.start.compareTo(comparator, maxClusteringValues) <= 0 && this.end.compareTo(comparator, minClusteringValues) >= 0) {
         int j = 0;

         while(true) {
            if(j < minClusteringValues.size() && j < maxClusteringValues.size()) {
               ByteBuffer s = j < this.start.size()?this.start.get(j):null;
               ByteBuffer f = j < this.end.size()?this.end.get(j):null;
               if(j > 0 && (j < this.end.size() && comparator.compareComponent(j, f, (ByteBuffer)minClusteringValues.get(j)) < 0 || j < this.start.size() && comparator.compareComponent(j, s, (ByteBuffer)maxClusteringValues.get(j)) > 0)) {
                  return false;
               }

               if(j < this.start.size() && j < this.end.size() && comparator.compareComponent(j, s, f) == 0) {
                  ++j;
                  continue;
               }
            }

            return true;
         }
      } else {
         return false;
      }
   }

   public String toString(ClusteringComparator comparator) {
      StringBuilder sb = new StringBuilder();
      sb.append(this.start.isInclusive()?"[":"(");

      int i;
      for(i = 0; i < this.start.size(); ++i) {
         if(i > 0) {
            sb.append(':');
         }

         sb.append(comparator.subtype(i).getString(this.start.get(i)));
      }

      sb.append(", ");

      for(i = 0; i < this.end.size(); ++i) {
         if(i > 0) {
            sb.append(':');
         }

         sb.append(comparator.subtype(i).getString(this.end.get(i)));
      }

      sb.append(this.end.isInclusive()?"]":")");
      return sb.toString();
   }

   public boolean equals(Object other) {
      if(!(other instanceof Slice)) {
         return false;
      } else {
         Slice that = (Slice)other;
         return this.start().equals(that.start()) && this.end().equals(that.end());
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.start(), this.end()});
   }

   static {
      ALL = new Slice(ClusteringBound.BOTTOM, ClusteringBound.TOP) {
         public boolean includes(ClusteringComparator comparator, ClusteringPrefix clustering) {
            return true;
         }

         public boolean intersects(ClusteringComparator comparator, List<ByteBuffer> minClusteringValues, List<ByteBuffer> maxClusteringValues) {
            return true;
         }

         public String toString(ClusteringComparator comparator) {
            return "ALL";
         }
      };
   }

   public static class Serializer {
      public Serializer() {
      }

      public void serialize(Slice slice, DataOutputPlus out, ClusteringVersion version, List<AbstractType<?>> types) throws IOException {
         ClusteringBound.serializer.serialize(slice.start, out, version, types);
         ClusteringBound.serializer.serialize(slice.end, out, version, types);
      }

      public long serializedSize(Slice slice, ClusteringVersion version, List<AbstractType<?>> types) {
         return ClusteringBound.serializer.serializedSize(slice.start, version, types) + ClusteringBound.serializer.serializedSize(slice.end, version, types);
      }

      public Slice deserialize(DataInputPlus in, ClusteringVersion version, List<AbstractType<?>> types) throws IOException {
         ClusteringBound start = (ClusteringBound)ClusteringBound.serializer.deserialize(in, version, types);
         ClusteringBound end = (ClusteringBound)ClusteringBound.serializer.deserialize(in, version, types);
         return new Slice(start, end, null);
      }
   }
}
