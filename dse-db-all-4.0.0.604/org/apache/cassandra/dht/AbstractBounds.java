package org.apache.cassandra.dht;

import java.io.DataInput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.Pair;

public abstract class AbstractBounds<T extends RingPosition<T>> implements Serializable {
   private static final long serialVersionUID = 1L;
   public static final IPartitionerDependentSerializer<AbstractBounds<Token>, BoundsVersion> tokenSerializer;
   public static final IPartitionerDependentSerializer<AbstractBounds<PartitionPosition>, BoundsVersion> rowPositionSerializer;
   public final T left;
   public final T right;

   public AbstractBounds(T left, T right) {
      assert left.getPartitioner() == right.getPartitioner();

      this.left = left;
      this.right = right;
   }

   public abstract Pair<AbstractBounds<T>, AbstractBounds<T>> split(T var1);

   public abstract boolean inclusiveLeft();

   public abstract boolean inclusiveRight();

   public static <T extends RingPosition<T>> boolean strictlyWrapsAround(T left, T right) {
      return left.compareTo(right) > 0 && !right.isMinimum();
   }

   public static <T extends RingPosition<T>> boolean noneStrictlyWrapsAround(Collection<AbstractBounds<T>> bounds) {
      Iterator var1 = bounds.iterator();

      AbstractBounds b;
      do {
         if(!var1.hasNext()) {
            return true;
         }

         b = (AbstractBounds)var1.next();
      } while(!strictlyWrapsAround(b.left, b.right));

      return false;
   }

   public int hashCode() {
      return 31 * this.left.hashCode() + this.right.hashCode();
   }

   public boolean intersects(Iterable<Range<T>> ranges) {
      Iterator var2 = ranges.iterator();

      Range range2;
      do {
         if(!var2.hasNext()) {
            return false;
         }

         range2 = (Range)var2.next();
      } while(!range2.intersects(this));

      return true;
   }

   public abstract boolean contains(T var1);

   public abstract List<? extends AbstractBounds<T>> unwrap();

   public String getString(AbstractType<?> keyValidator) {
      return this.getOpeningString() + this.format(this.left, keyValidator) + ", " + this.format(this.right, keyValidator) + this.getClosingString();
   }

   private String format(T value, AbstractType<?> keyValidator) {
      return value instanceof DecoratedKey?keyValidator.getString(((DecoratedKey)value).getKey()):value.toString();
   }

   protected abstract String getOpeningString();

   protected abstract String getClosingString();

   public abstract boolean isStartInclusive();

   public abstract boolean isEndInclusive();

   public abstract AbstractBounds<T> withNewRight(T var1);

   public static <T extends RingPosition<T>> AbstractBounds<T> bounds(AbstractBounds.Boundary<T> min, AbstractBounds.Boundary<T> max) {
      return bounds(min.boundary, min.inclusive, max.boundary, max.inclusive);
   }

   public static <T extends RingPosition<T>> AbstractBounds<T> bounds(T min, boolean inclusiveMin, T max, boolean inclusiveMax) {
      return (AbstractBounds)(inclusiveMin && inclusiveMax?new Bounds(min, max):(inclusiveMax?new Range(min, max):(inclusiveMin?new IncludingExcludingBounds(min, max):new ExcludingBounds(min, max))));
   }

   public AbstractBounds.Boundary<T> leftBoundary() {
      return new AbstractBounds.Boundary(this.left, this.inclusiveLeft());
   }

   public AbstractBounds.Boundary<T> rightBoundary() {
      return new AbstractBounds.Boundary(this.right, this.inclusiveRight());
   }

   public static <T extends RingPosition<T>> boolean isEmpty(AbstractBounds.Boundary<T> left, AbstractBounds.Boundary<T> right) {
      int c = left.boundary.compareTo(right.boundary);
      return c > 0 || c == 0 && (!left.inclusive || !right.inclusive);
   }

   public static <T extends RingPosition<T>> AbstractBounds.Boundary<T> minRight(AbstractBounds.Boundary<T> right1, T right2, boolean isInclusiveRight2) {
      return minRight(right1, new AbstractBounds.Boundary(right2, isInclusiveRight2));
   }

   public static <T extends RingPosition<T>> AbstractBounds.Boundary<T> minRight(AbstractBounds.Boundary<T> right1, AbstractBounds.Boundary<T> right2) {
      int c = right1.boundary.compareTo(right2.boundary);
      return c != 0?(c < 0?right1:right2):(right2.inclusive?right1:right2);
   }

   public static <T extends RingPosition<T>> AbstractBounds.Boundary<T> maxLeft(AbstractBounds.Boundary<T> left1, T left2, boolean isInclusiveLeft2) {
      return maxLeft(left1, new AbstractBounds.Boundary(left2, isInclusiveLeft2));
   }

   public static <T extends RingPosition<T>> AbstractBounds.Boundary<T> maxLeft(AbstractBounds.Boundary<T> left1, AbstractBounds.Boundary<T> left2) {
      int c = left1.boundary.compareTo(left2.boundary);
      return c != 0?(c > 0?left1:left2):(left2.inclusive?left1:left2);
   }

   static {
      tokenSerializer = new AbstractBounds.AbstractBoundsSerializer(Token.serializer);
      rowPositionSerializer = new AbstractBounds.AbstractBoundsSerializer(PartitionPosition.serializer);
   }

   public static class Boundary<T extends RingPosition<T>> {
      public final T boundary;
      public final boolean inclusive;

      public Boundary(T boundary, boolean inclusive) {
         this.boundary = boundary;
         this.inclusive = inclusive;
      }
   }

   public static class AbstractBoundsSerializer<T extends RingPosition<T>> implements IPartitionerDependentSerializer<AbstractBounds<T>, BoundsVersion> {
      private static final int IS_TOKEN_FLAG = 1;
      private static final int START_INCLUSIVE_FLAG = 2;
      private static final int END_INCLUSIVE_FLAG = 4;
      IPartitionerDependentSerializer<T, BoundsVersion> serializer;

      private static int kindInt(AbstractBounds<?> ab) {
         int kind = ab instanceof Range?AbstractBounds.Type.RANGE.ordinal():AbstractBounds.Type.BOUNDS.ordinal();
         if(!(ab.left instanceof Token)) {
            kind = -(kind + 1);
         }

         return kind;
      }

      private static int kindFlags(AbstractBounds<?> ab) {
         int flags = 0;
         if(ab.left instanceof Token) {
            flags |= 1;
         }

         if(ab.isStartInclusive()) {
            flags |= 2;
         }

         if(ab.isEndInclusive()) {
            flags |= 4;
         }

         return flags;
      }

      public AbstractBoundsSerializer(IPartitionerDependentSerializer<T, BoundsVersion> serializer) {
         this.serializer = serializer;
      }

      public void serialize(AbstractBounds<T> range, DataOutputPlus out, BoundsVersion version) throws IOException {
         if(version == BoundsVersion.LEGACY) {
            out.writeInt(kindInt(range));
         } else {
            out.writeByte(kindFlags(range));
         }

         this.serializer.serialize(range.left, out, version);
         this.serializer.serialize(range.right, out, version);
      }

      public AbstractBounds<T> deserialize(DataInput in, IPartitioner p, BoundsVersion version) throws IOException {
         boolean isToken;
         boolean startInclusive;
         boolean endInclusive;
         int kind;
         if(version == BoundsVersion.LEGACY) {
            kind = in.readInt();
            isToken = kind >= 0;
            if(!isToken) {
               kind = -(kind + 1);
            }

            startInclusive = kind != AbstractBounds.Type.RANGE.ordinal();
            endInclusive = true;
         } else {
            kind = in.readUnsignedByte();
            isToken = (kind & 1) != 0;
            startInclusive = (kind & 2) != 0;
            endInclusive = (kind & 4) != 0;
         }

         T left = (T)this.serializer.deserialize(in, p, version);
         T right = (T)this.serializer.deserialize(in, p, version);

         assert isToken == (left instanceof Token);

         return (AbstractBounds)(startInclusive?(endInclusive?new Bounds(left, right):new IncludingExcludingBounds(left, right)):(endInclusive?new Range(left, right):new ExcludingBounds(left, right)));
      }

      public int serializedSize(AbstractBounds<T> ab, BoundsVersion version) {
         int size = version == BoundsVersion.LEGACY?TypeSizes.sizeof(kindInt(ab)):1;
         size += this.serializer.serializedSize(ab.left, version);
         size += this.serializer.serializedSize(ab.right, version);
         return size;
      }
   }

   private static enum Type {
      RANGE,
      BOUNDS;

      private Type() {
      }
   }
}
