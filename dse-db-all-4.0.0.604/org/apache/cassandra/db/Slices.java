package org.apache.cassandra.db;

import com.google.common.collect.Iterators;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

public abstract class Slices implements Iterable<Slice> {
   public static final Slices.Serializer serializer = new Slices.Serializer();
   public static final Slices ALL = new Slices.SelectAllSlices();
   public static final Slices NONE = new Slices.SelectNoSlices();

   protected Slices() {
   }

   public static Slices with(ClusteringComparator comparator, Slice slice) {
      if(slice.start() == ClusteringBound.BOTTOM && slice.end() == ClusteringBound.TOP) {
         return ALL;
      } else {
         assert comparator.compare((ClusteringPrefix)slice.start(), (ClusteringPrefix)slice.end()) <= 0;

         return new Slices.ArrayBackedSlices(comparator, new Slice[]{slice});
      }
   }

   public abstract boolean hasLowerBound();

   public abstract boolean hasUpperBound();

   public abstract int size();

   public abstract Slice get(int var1);

   public abstract Slices forPaging(ClusteringComparator var1, Clustering var2, boolean var3, boolean var4);

   public abstract Slices.InOrderTester inOrderTester(boolean var1);

   public abstract boolean selects(Clustering var1);

   public abstract boolean intersects(List<ByteBuffer> var1, List<ByteBuffer> var2);

   public abstract String toCQLString(TableMetadata var1);

   public final boolean isEmpty() {
      return this.size() == 0;
   }

   private static class SelectNoSlices extends Slices {
      private static final Slices.InOrderTester trivialTester = new Slices.InOrderTester() {
         public boolean includes(Clustering value) {
            return false;
         }

         public boolean isDone() {
            return true;
         }
      };

      private SelectNoSlices() {
      }

      public int size() {
         return 0;
      }

      public Slice get(int i) {
         throw new UnsupportedOperationException();
      }

      public boolean hasLowerBound() {
         return false;
      }

      public boolean hasUpperBound() {
         return false;
      }

      public Slices forPaging(ClusteringComparator comparator, Clustering lastReturned, boolean inclusive, boolean reversed) {
         return this;
      }

      public boolean selects(Clustering clustering) {
         return false;
      }

      public Slices.InOrderTester inOrderTester(boolean reversed) {
         return trivialTester;
      }

      public boolean intersects(List<ByteBuffer> minClusteringValues, List<ByteBuffer> maxClusteringValues) {
         return false;
      }

      public Iterator<Slice> iterator() {
         return Collections.emptyIterator();
      }

      public String toString() {
         return "NONE";
      }

      public String toCQLString(TableMetadata metadata) {
         return "";
      }
   }

   private static class SelectAllSlices extends Slices {
      private static final Slices.InOrderTester trivialTester = new Slices.InOrderTester() {
         public boolean includes(Clustering value) {
            return true;
         }

         public boolean isDone() {
            return false;
         }
      };

      private SelectAllSlices() {
      }

      public int size() {
         return 1;
      }

      public Slice get(int i) {
         return Slice.ALL;
      }

      public boolean hasLowerBound() {
         return false;
      }

      public boolean hasUpperBound() {
         return false;
      }

      public boolean selects(Clustering clustering) {
         return true;
      }

      public Slices forPaging(ClusteringComparator comparator, Clustering lastReturned, boolean inclusive, boolean reversed) {
         return new Slices.ArrayBackedSlices(comparator, new Slice[]{Slice.ALL.forPaging(comparator, lastReturned, inclusive, reversed)});
      }

      public Slices.InOrderTester inOrderTester(boolean reversed) {
         return trivialTester;
      }

      public boolean intersects(List<ByteBuffer> minClusteringValues, List<ByteBuffer> maxClusteringValues) {
         return true;
      }

      public Iterator<Slice> iterator() {
         return Iterators.singletonIterator(Slice.ALL);
      }

      public String toString() {
         return "ALL";
      }

      public String toCQLString(TableMetadata metadata) {
         return "";
      }
   }

   private static class ArrayBackedSlices extends Slices {
      private final ClusteringComparator comparator;
      private final Slice[] slices;

      private ArrayBackedSlices(ClusteringComparator comparator, Slice[] slices) {
         this.comparator = comparator;
         this.slices = slices;
      }

      public int size() {
         return this.slices.length;
      }

      public boolean hasLowerBound() {
         return this.slices[0].start().size() != 0;
      }

      public boolean hasUpperBound() {
         return this.slices[this.slices.length - 1].end().size() != 0;
      }

      public Slice get(int i) {
         return this.slices[i];
      }

      public boolean selects(Clustering clustering) {
         for(int i = 0; i < this.slices.length; ++i) {
            Slice slice = this.slices[i];
            if(this.comparator.compare((ClusteringPrefix)clustering, (ClusteringPrefix)slice.start()) < 0) {
               return false;
            }

            if(this.comparator.compare((ClusteringPrefix)clustering, (ClusteringPrefix)slice.end()) <= 0) {
               return true;
            }
         }

         return false;
      }

      public Slices.InOrderTester inOrderTester(boolean reversed) {
         return (Slices.InOrderTester)(reversed?new Slices.ArrayBackedSlices.InReverseOrderTester():new Slices.ArrayBackedSlices.InForwardOrderTester());
      }

      public Slices forPaging(ClusteringComparator comparator, Clustering lastReturned, boolean inclusive, boolean reversed) {
         return reversed?this.forReversePaging(comparator, lastReturned, inclusive):this.forForwardPaging(comparator, lastReturned, inclusive);
      }

      private Slices forForwardPaging(ClusteringComparator comparator, Clustering lastReturned, boolean inclusive) {
         for(int i = 0; i < this.slices.length; ++i) {
            Slice slice = this.slices[i];
            Slice newSlice = slice.forPaging(comparator, lastReturned, inclusive, false);
            if(newSlice != null) {
               if(slice == newSlice && i == 0) {
                  return this;
               }

               Slices.ArrayBackedSlices newSlices = new Slices.ArrayBackedSlices(comparator, (Slice[])Arrays.copyOfRange(this.slices, i, this.slices.length));
               newSlices.slices[0] = newSlice;
               return newSlices;
            }
         }

         return Slices.NONE;
      }

      private Slices forReversePaging(ClusteringComparator comparator, Clustering lastReturned, boolean inclusive) {
         for(int i = this.slices.length - 1; i >= 0; --i) {
            Slice slice = this.slices[i];
            Slice newSlice = slice.forPaging(comparator, lastReturned, inclusive, true);
            if(newSlice != null) {
               if(slice == newSlice && i == this.slices.length - 1) {
                  return this;
               }

               Slices.ArrayBackedSlices newSlices = new Slices.ArrayBackedSlices(comparator, (Slice[])Arrays.copyOfRange(this.slices, 0, i + 1));
               newSlices.slices[i] = newSlice;
               return newSlices;
            }
         }

         return Slices.NONE;
      }

      public boolean intersects(List<ByteBuffer> minClusteringValues, List<ByteBuffer> maxClusteringValues) {
         Iterator var3 = this.iterator();

         Slice slice;
         do {
            if(!var3.hasNext()) {
               return false;
            }

            slice = (Slice)var3.next();
         } while(!slice.intersects(this.comparator, minClusteringValues, maxClusteringValues));

         return true;
      }

      public Iterator<Slice> iterator() {
         return Iterators.forArray(this.slices);
      }

      public String toString() {
         StringBuilder sb = new StringBuilder();
         sb.append("{");

         for(int i = 0; i < this.slices.length; ++i) {
            if(i > 0) {
               sb.append(", ");
            }

            sb.append(this.slices[i].toString(this.comparator));
         }

         return sb.append("}").toString();
      }

      public String toCQLString(TableMetadata metadata) {
         StringBuilder sb = new StringBuilder();
         int clusteringSize = metadata.clusteringColumns().size();
         List<List<Slices.ArrayBackedSlices.ComponentOfSlice>> columnComponents = new ArrayList(clusteringSize);

         for(int i = 0; i < clusteringSize; ++i) {
            List<Slices.ArrayBackedSlices.ComponentOfSlice> perSlice = new ArrayList();
            columnComponents.add(perSlice);

            for(int j = 0; j < this.slices.length; ++j) {
               Slices.ArrayBackedSlices.ComponentOfSlice c = Slices.ArrayBackedSlices.ComponentOfSlice.fromSlice(i, this.slices[j]);
               if(c != null) {
                  perSlice.add(c);
               }
            }
         }

         boolean needAnd = false;

         for(int i = 0; i < clusteringSize; ++i) {
            ColumnMetadata column = (ColumnMetadata)metadata.clusteringColumns().get(i);
            List<Slices.ArrayBackedSlices.ComponentOfSlice> componentInfo = (List)columnComponents.get(i);
            if(componentInfo.isEmpty()) {
               break;
            }

            Slices.ArrayBackedSlices.ComponentOfSlice first = (Slices.ArrayBackedSlices.ComponentOfSlice)componentInfo.get(0);
            if(first.isEQ()) {
               if(needAnd) {
                  sb.append(" AND ");
               }

               needAnd = true;
               sb.append(column.name);
               Set<ByteBuffer> values = new LinkedHashSet();

               int j;
               for(j = 0; j < componentInfo.size(); ++j) {
                  values.add(((Slices.ArrayBackedSlices.ComponentOfSlice)componentInfo.get(j)).startValue);
               }

               if(values.size() == 1) {
                  sb.append(" = ").append(column.type.getString(first.startValue));
               } else {
                  sb.append(" IN (");
                  j = 0;
                  Iterator var12 = values.iterator();

                  while(var12.hasNext()) {
                     ByteBuffer value = (ByteBuffer)var12.next();
                     sb.append(j++ == 0?"":", ").append(column.type.getString(value));
                  }

                  sb.append(")");
               }
            } else {
               if(first.startValue != null) {
                  if(needAnd) {
                     sb.append(" AND ");
                  }

                  needAnd = true;
                  sb.append(column.name).append(first.startInclusive?" >= ":" > ").append(column.type.getString(first.startValue));
               }

               if(first.endValue != null) {
                  if(needAnd) {
                     sb.append(" AND ");
                  }

                  needAnd = true;
                  sb.append(column.name).append(first.endInclusive?" <= ":" < ").append(column.type.getString(first.endValue));
               }
            }
         }

         return sb.toString();
      }

      private static class ComponentOfSlice {
         public final boolean startInclusive;
         public final ByteBuffer startValue;
         public final boolean endInclusive;
         public final ByteBuffer endValue;

         private ComponentOfSlice(boolean startInclusive, ByteBuffer startValue, boolean endInclusive, ByteBuffer endValue) {
            this.startInclusive = startInclusive;
            this.startValue = startValue;
            this.endInclusive = endInclusive;
            this.endValue = endValue;
         }

         public static Slices.ArrayBackedSlices.ComponentOfSlice fromSlice(int component, Slice slice) {
            ClusteringBound start = slice.start();
            ClusteringBound end = slice.end();
            if(component >= start.size() && component >= end.size()) {
               return null;
            } else {
               boolean startInclusive = true;
               boolean endInclusive = true;
               ByteBuffer startValue = null;
               ByteBuffer endValue = null;
               if(component < start.size()) {
                  startInclusive = start.isInclusive();
                  startValue = start.get(component);
               }

               if(component < end.size()) {
                  endInclusive = end.isInclusive();
                  endValue = end.get(component);
               }

               return new Slices.ArrayBackedSlices.ComponentOfSlice(startInclusive, startValue, endInclusive, endValue);
            }
         }

         public boolean isEQ() {
            return Objects.equals(this.startValue, this.endValue);
         }
      }

      private class InReverseOrderTester implements Slices.InOrderTester {
         private int idx;
         private boolean inSlice;

         public InReverseOrderTester() {
            this.idx = ArrayBackedSlices.this.slices.length - 1;
         }

         public boolean includes(Clustering value) {
            while(this.idx >= 0) {
               if(!this.inSlice) {
                  int cmp = ArrayBackedSlices.this.comparator.compare((ClusteringPrefix)ArrayBackedSlices.this.slices[this.idx].end(), (ClusteringPrefix)value);
                  if(cmp > 0) {
                     return false;
                  }

                  this.inSlice = true;
                  if(cmp == 0) {
                     return true;
                  }
               }

               if(ArrayBackedSlices.this.comparator.compare((ClusteringPrefix)ArrayBackedSlices.this.slices[this.idx].start(), (ClusteringPrefix)value) <= 0) {
                  return true;
               }

               --this.idx;
               this.inSlice = false;
            }

            return false;
         }

         public boolean isDone() {
            return this.idx < 0;
         }
      }

      private class InForwardOrderTester implements Slices.InOrderTester {
         private int idx;
         private boolean inSlice;

         private InForwardOrderTester() {
         }

         public boolean includes(Clustering value) {
            while(this.idx < ArrayBackedSlices.this.slices.length) {
               if(!this.inSlice) {
                  int cmp = ArrayBackedSlices.this.comparator.compare((ClusteringPrefix)value, (ClusteringPrefix)ArrayBackedSlices.this.slices[this.idx].start());
                  if(cmp < 0) {
                     return false;
                  }

                  this.inSlice = true;
                  if(cmp == 0) {
                     return true;
                  }
               }

               if(ArrayBackedSlices.this.comparator.compare((ClusteringPrefix)value, (ClusteringPrefix)ArrayBackedSlices.this.slices[this.idx].end()) <= 0) {
                  return true;
               }

               ++this.idx;
               this.inSlice = false;
            }

            return false;
         }

         public boolean isDone() {
            return this.idx >= ArrayBackedSlices.this.slices.length;
         }
      }
   }

   public static class Serializer {
      public Serializer() {
      }

      public void serialize(Slices slices, DataOutputPlus out, ClusteringVersion version) throws IOException {
         int size = slices.size();
         out.writeUnsignedVInt((long)size);
         if(size != 0) {
            List<AbstractType<?>> types = slices == Slices.ALL?Collections.emptyList():((Slices.ArrayBackedSlices)slices).comparator.subtypes();
            Iterator var6 = slices.iterator();

            while(var6.hasNext()) {
               Slice slice = (Slice)var6.next();
               Slice.serializer.serialize(slice, out, version, types);
            }

         }
      }

      public long serializedSize(Slices slices, ClusteringVersion version) {
         long size = (long)TypeSizes.sizeofUnsignedVInt((long)slices.size());
         if(slices.size() == 0) {
            return size;
         } else {
            List<AbstractType<?>> types = slices instanceof Slices.SelectAllSlices?Collections.emptyList():((Slices.ArrayBackedSlices)slices).comparator.subtypes();

            Slice slice;
            for(Iterator var6 = slices.iterator(); var6.hasNext(); size += Slice.serializer.serializedSize(slice, version, types)) {
               slice = (Slice)var6.next();
            }

            return size;
         }
      }

      public Slices deserialize(DataInputPlus in, ClusteringVersion version, TableMetadata metadata) throws IOException {
         int size = (int)in.readUnsignedVInt();
         if(size == 0) {
            return Slices.NONE;
         } else {
            Slice[] slices = new Slice[size];

            for(int i = 0; i < size; ++i) {
               slices[i] = Slice.serializer.deserialize(in, version, metadata.comparator.subtypes());
            }

            return (Slices)(size == 1 && slices[0].start() == ClusteringBound.BOTTOM && slices[0].end() == ClusteringBound.TOP?Slices.ALL:new Slices.ArrayBackedSlices(metadata.comparator, slices));
         }
      }
   }

   public static class Builder {
      private final ClusteringComparator comparator;
      private final List<Slice> slices;
      private boolean needsNormalizing;

      public Builder(ClusteringComparator comparator) {
         this.comparator = comparator;
         this.slices = new ArrayList();
      }

      public Builder(ClusteringComparator comparator, int initialSize) {
         this.comparator = comparator;
         this.slices = new ArrayList(initialSize);
      }

      public Slices.Builder add(ClusteringBound start, ClusteringBound end) {
         return this.add(Slice.make(start, end));
      }

      public Slices.Builder add(Slice slice) {
         assert this.comparator.compare((ClusteringPrefix)slice.start(), (ClusteringPrefix)slice.end()) <= 0;

         if(this.slices.size() > 0 && this.comparator.compare((ClusteringPrefix)((Slice)this.slices.get(this.slices.size() - 1)).end(), (ClusteringPrefix)slice.start()) > 0) {
            this.needsNormalizing = true;
         }

         this.slices.add(slice);
         return this;
      }

      public Slices.Builder addAll(Slices slices) {
         Iterator var2 = slices.iterator();

         while(var2.hasNext()) {
            Slice slice = (Slice)var2.next();
            this.add(slice);
         }

         return this;
      }

      public int size() {
         return this.slices.size();
      }

      public Slices build() {
         if(this.slices.isEmpty()) {
            return Slices.NONE;
         } else if(this.slices.size() == 1 && this.slices.get(0) == Slice.ALL) {
            return Slices.ALL;
         } else {
            List<Slice> normalized = this.needsNormalizing?this.normalize(this.slices):this.slices;
            return new Slices.ArrayBackedSlices(this.comparator, (Slice[])normalized.toArray(new Slice[0]));
         }
      }

      private List<Slice> normalize(List<Slice> slices) {
         if(slices.size() <= 1) {
            return slices;
         } else {
            Collections.sort(slices, new Comparator<Slice>() {
               public int compare(Slice s1, Slice s2) {
                  int c = Builder.this.comparator.compare((ClusteringPrefix)s1.start(), (ClusteringPrefix)s2.start());
                  return c != 0?c:Builder.this.comparator.compare((ClusteringPrefix)s1.end(), (ClusteringPrefix)s2.end());
               }
            });
            List<Slice> slicesCopy = new ArrayList(slices.size());
            Slice last = (Slice)slices.get(0);

            for(int i = 1; i < slices.size(); ++i) {
               Slice s2 = (Slice)slices.get(i);
               boolean includesStart = last.includes(this.comparator, s2.start());
               boolean includesFinish = last.includes(this.comparator, s2.end());
               if(!includesStart || !includesFinish) {
                  if(!includesStart && !includesFinish) {
                     slicesCopy.add(last);
                     last = s2;
                  } else if(includesStart) {
                     last = Slice.make(last.start(), s2.end());
                  } else {
                     assert !includesFinish;
                  }
               }
            }

            slicesCopy.add(last);
            return slicesCopy;
         }
      }
   }

   public interface InOrderTester {
      boolean includes(Clustering var1);

      boolean isDone();
   }
}
