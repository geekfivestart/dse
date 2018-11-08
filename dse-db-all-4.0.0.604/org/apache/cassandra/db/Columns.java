package org.apache.cassandra.db;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.UnmodifiableIterator;
import com.google.common.hash.Hasher;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.AbstractCollection;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.SortedSet;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import org.apache.cassandra.exceptions.UnknownColumnException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractIndexedListIterator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.HashingUtils;
import org.apache.cassandra.utils.MergeIterator;
import org.apache.cassandra.utils.Reducer;
import org.apache.cassandra.utils.SearchIterator;

public class Columns extends AbstractCollection<ColumnMetadata> {
   public static final Columns.Serializer serializer = new Columns.Serializer();
   public static final ColumnMetadata[] EMPTY = new ColumnMetadata[0];
   public static final Columns NONE;
   private final ColumnMetadata[] columns;
   private final int complexIdx;

   private Columns(ColumnMetadata[] columns, int complexIdx) {
      assert complexIdx <= columns.length;

      this.columns = columns;
      this.complexIdx = complexIdx;
   }

   private Columns(ColumnMetadata[] columns) {
      this(columns, findFirstComplexIdx(columns));
   }

   public static Columns of(ColumnMetadata c) {
      return new Columns(new ColumnMetadata[]{c}, c.isComplex()?0:1);
   }

   public static Columns from(Collection<ColumnMetadata> s) {
      assert s instanceof List || s instanceof SortedSet : "Must pass an ordered collection";

      ColumnMetadata[] columns = new ColumnMetadata[s.size()];
      int i = 0;
      int firstComplexId = columns.length;
      ColumnMetadata last = null;

      ColumnMetadata c;
      for(Iterator var5 = s.iterator(); var5.hasNext(); last = c) {
         c = (ColumnMetadata)var5.next();

         assert c != null && (last == null || c.compareTo(last) >= 0) : "Must be passed a sorted collection with no nulls";

         if(c.isComplex()) {
            firstComplexId = Math.min(firstComplexId, i);
         }

         columns[i++] = c;
      }

      return new Columns(columns, firstComplexId);
   }

   private static int findFirstComplexIdx(ColumnMetadata[] columns) {
      if(columns.length == 0) {
         return 0;
      } else {
         for(int i = columns.length - 1; i >= 0; --i) {
            if(columns[i].isSimple()) {
               return i + 1;
            }
         }

         return 0;
      }
   }

   public boolean isEmpty() {
      return this.columns.length == 0;
   }

   public int simpleColumnCount() {
      return this.complexIdx;
   }

   public int complexColumnCount() {
      return this.columns.length - this.complexIdx;
   }

   public int size() {
      return this.columns.length;
   }

   public boolean hasSimple() {
      return this.complexIdx > 0;
   }

   public boolean hasComplex() {
      return this.complexIdx < this.columns.length;
   }

   public ColumnMetadata getSimple(int i) {
      return this.columns[i];
   }

   public ColumnMetadata getComplex(int i) {
      return this.columns[this.complexIdx + i];
   }

   public int simpleIdx(ColumnMetadata c) {
      return Arrays.binarySearch(this.columns, c);
   }

   public int complexIdx(ColumnMetadata c) {
      return Arrays.binarySearch(this.columns, c) - this.complexIdx;
   }

   public boolean contains(ColumnMetadata c) {
      return Arrays.binarySearch(this.columns, c) >= 0;
   }

   public Columns mergeTo(Columns other) {
      if(this != other && other != NONE) {
         if(this == NONE) {
            return other;
         } else if(Arrays.equals(this.columns, other.columns)) {
            return this;
         } else {
            CloseableIterator<ColumnMetadata> merge = MergeIterator.get(Lists.newArrayList(new UnmodifiableIterator[]{Iterators.forArray(this.columns), Iterators.forArray(other.columns)}), Comparator.naturalOrder(), new Reducer<ColumnMetadata, ColumnMetadata>() {
               ColumnMetadata reduced = null;

               public boolean trivialReduceIsTrivial() {
                  return true;
               }

               public void reduce(int idx, ColumnMetadata current) {
                  this.reduced = current;
               }

               public ColumnMetadata getReduced() {
                  return this.reduced;
               }
            });
            Throwable var3 = null;

            Columns var4;
            try {
               var4 = from(Lists.newArrayList(merge));
            } catch (Throwable var13) {
               var3 = var13;
               throw var13;
            } finally {
               if(merge != null) {
                  if(var3 != null) {
                     try {
                        merge.close();
                     } catch (Throwable var12) {
                        var3.addSuppressed(var12);
                     }
                  } else {
                     merge.close();
                  }
               }

            }

            return var4;
         }
      } else {
         return this;
      }
   }

   public boolean containsAll(Collection<?> other) {
      if(other == this) {
         return true;
      } else if(other.size() > this.size()) {
         return false;
      } else {
         Iterator var2 = other.iterator();

         Object def;
         do {
            if(!var2.hasNext()) {
               return true;
            }

            def = var2.next();
         } while(Arrays.binarySearch(this.columns, (ColumnMetadata)def, Comparator.naturalOrder()) >= 0);

         return false;
      }
   }

   public Iterator<ColumnMetadata> simpleColumns() {
      return (Iterator)(this.isEmpty()?Iterators.emptyIterator():new AbstractIndexedListIterator<ColumnMetadata>(this.complexIdx, 0) {
         protected ColumnMetadata get(int index) {
            return Columns.this.columns[index];
         }
      });
   }

   public Iterator<ColumnMetadata> complexColumns() {
      return new AbstractIndexedListIterator<ColumnMetadata>(this.columns.length, this.complexIdx) {
         protected ColumnMetadata get(int index) {
            return Columns.this.columns[index];
         }
      };
   }

   public Iterator<ColumnMetadata> iterator() {
      return new AbstractIndexedListIterator<ColumnMetadata>(this.columns.length, 0) {
         protected ColumnMetadata get(int index) {
            return Columns.this.columns[index];
         }
      };
   }

   public Columns.ColumnSearchIterator searchIterator() {
      return new Columns.ColumnSearchIterator(this.columns);
   }

   public Iterator<ColumnMetadata> selectOrderIterator() {
      return Iterators.mergeSorted(ImmutableList.of(this.simpleColumns(), this.complexColumns()), (s, c) -> {
         assert !s.kind.isPrimaryKeyKind();

         return s.name.bytes.compareTo(c.name.bytes);
      });
   }

   public Columns without(ColumnMetadata column) {
      if(!this.contains(column)) {
         return this;
      } else {
         ColumnMetadata[] newColumns = (ColumnMetadata[])Arrays.stream(this.columns).filter((c) -> {
            return c.compareTo(column) != 0;
         }).toArray((x$0) -> {
            return new ColumnMetadata[x$0];
         });
         return new Columns(newColumns);
      }
   }

   public void apply(Consumer<ColumnMetadata> function) {
      ColumnMetadata[] var2 = this.columns;
      int var3 = var2.length;

      for(int var4 = 0; var4 < var3; ++var4) {
         ColumnMetadata cm = var2[var4];
         function.accept(cm);
      }

   }

   public Predicate<ColumnMetadata> inOrderInclusionTester() {
      SearchIterator<ColumnMetadata, ColumnMetadata> iter = this.searchIterator();
      return (column) -> {
         return iter.next(column) != null;
      };
   }

   public void digest(Hasher hasher) {
      ColumnMetadata[] var2 = this.columns;
      int var3 = var2.length;

      for(int var4 = 0; var4 < var3; ++var4) {
         ColumnMetadata c = var2[var4];
         HashingUtils.updateBytes(hasher, c.name.bytes.duplicate());
      }

   }

   public boolean equals(Object other) {
      if(other == this) {
         return true;
      } else if(!(other instanceof Columns)) {
         return false;
      } else {
         Columns that = (Columns)other;
         return this.complexIdx == that.complexIdx && Arrays.equals(this.columns, that.columns);
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{Integer.valueOf(this.complexIdx), Integer.valueOf(Arrays.hashCode(this.columns))});
   }

   public String toString() {
      StringBuilder sb = new StringBuilder("[");
      boolean first = true;
      ColumnMetadata[] var3 = this.columns;
      int var4 = var3.length;

      for(int var5 = 0; var5 < var4; ++var5) {
         ColumnMetadata def = var3[var5];
         if(first) {
            first = false;
         } else {
            sb.append(" ");
         }

         sb.append(def.name);
      }

      return sb.append("]").toString();
   }

   static {
      NONE = new Columns(EMPTY, 0);
   }

   public static class Serializer {
      public Serializer() {
      }

      public void serialize(Columns columns, DataOutputPlus out) throws IOException {
         out.writeUnsignedVInt((long)columns.size());
         ColumnMetadata[] var3 = columns.columns;
         int var4 = var3.length;

         for(int var5 = 0; var5 < var4; ++var5) {
            ColumnMetadata column = var3[var5];
            ByteBufferUtil.writeWithVIntLength(column.name.bytes, out);
         }

      }

      public long serializedSize(Columns columns) {
         long size = (long)TypeSizes.sizeofUnsignedVInt((long)columns.size());
         ColumnMetadata[] var4 = columns.columns;
         int var5 = var4.length;

         for(int var6 = 0; var6 < var5; ++var6) {
            ColumnMetadata column = var4[var6];
            size += (long)ByteBufferUtil.serializedSizeWithVIntLength(column.name.bytes);
         }

         return size;
      }

      public Columns deserialize(DataInputPlus in, TableMetadata metadata) throws IOException {
         int length = (int)in.readUnsignedVInt();
         ColumnMetadata[] columns = new ColumnMetadata[length];

         for(int i = 0; i < length; ++i) {
            ByteBuffer name = ByteBufferUtil.readWithVIntLength(in);
            ColumnMetadata column = metadata.getColumn(name);
            if(column == null) {
               column = metadata.getDroppedColumn(name);
               if(column == null) {
                  throw new UnknownColumnException(metadata, name);
               }
            }

            columns[i] = column;
         }

         return new Columns(columns, null);
      }

      public void serializeSubset(Collection<ColumnMetadata> columns, Columns superset, DataOutputPlus out) throws IOException {
         int columnCount = columns.size();
         int supersetCount = superset.size();
         if(columnCount == supersetCount) {
            out.writeUnsignedVInt(0L);
         } else if(supersetCount < 64) {
            out.writeUnsignedVInt(encodeBitmap(columns, superset, supersetCount));
         } else {
            this.serializeLargeSubset(columns, columnCount, superset, supersetCount, out);
         }

      }

      public long serializedSubsetSize(Collection<ColumnMetadata> columns, Columns superset) {
         int columnCount = columns.size();
         int supersetCount = superset.size();
         return columnCount == supersetCount?(long)TypeSizes.sizeofUnsignedVInt(0L):(supersetCount < 64?(long)TypeSizes.sizeofUnsignedVInt(encodeBitmap(columns, superset, supersetCount)):(long)this.serializeLargeSubsetSize(columns, columnCount, superset, supersetCount));
      }

      public Columns deserializeSubset(Columns superset, DataInputPlus in) throws IOException {
         long encoded = in.readUnsignedVInt();
         if(encoded == 0L) {
            return superset;
         } else if(superset.size() >= 64) {
            return this.deserializeLargeSubset(in, superset, (int)encoded);
         } else {
            ColumnMetadata[] columns = new ColumnMetadata[superset.size()];
            int index = 0;
            int firstComplexIdx = 0;
            ColumnMetadata[] var8 = superset.columns;
            int var9 = var8.length;

            for(int var10 = 0; var10 < var9; ++var10) {
               ColumnMetadata column = var8[var10];
               if((encoded & 1L) == 0L) {
                  columns[index++] = column;
                  if(column.isSimple()) {
                     ++firstComplexIdx;
                  }
               }

               encoded >>>= 1;
               if(index == columns.length) {
                  break;
               }
            }

            return new Columns(columns.length == index?columns:(ColumnMetadata[])Arrays.copyOf(columns, index), firstComplexIdx, null);
         }
      }

      private static long encodeBitmap(Collection<ColumnMetadata> columns, Columns superset, int supersetCount) {
         long bitmap = 0L;
         Columns.ColumnSearchIterator iter = superset.searchIterator();
         int expectIndex = 0;

         int currentIndex;
         for(Iterator var7 = columns.iterator(); var7.hasNext(); expectIndex = currentIndex + 1) {
            ColumnMetadata column = (ColumnMetadata)var7.next();
            if(iter.next(column) == null) {
               throw new IllegalStateException(columns + " is not a subset of " + superset);
            }

            currentIndex = iter.indexOfCurrent();
            int count = currentIndex - expectIndex;
            bitmap |= (1L << count) - 1L << expectIndex;
         }

         int count = supersetCount - expectIndex;
         bitmap |= (1L << count) - 1L << expectIndex;
         return bitmap;
      }

      private void serializeLargeSubset(Collection<ColumnMetadata> columns, int columnCount, Columns superset, int supersetCount, DataOutputPlus out) throws IOException {
         out.writeUnsignedVInt((long)(supersetCount - columnCount));
         Columns.ColumnSearchIterator iter = superset.searchIterator();
         if(columnCount < supersetCount / 2) {
            Iterator var7 = columns.iterator();

            while(var7.hasNext()) {
               ColumnMetadata column = (ColumnMetadata)var7.next();
               if(iter.next(column) == null) {
                  throw new IllegalStateException();
               }

               out.writeUnsignedVInt((long)iter.indexOfCurrent());
            }
         } else {
            int prev = -1;
            Iterator var12 = columns.iterator();

            while(var12.hasNext()) {
               ColumnMetadata column = (ColumnMetadata)var12.next();
               if(iter.next(column) == null) {
                  throw new IllegalStateException();
               }

               int cur = iter.indexOfCurrent();

               while(true) {
                  ++prev;
                  if(prev == cur) {
                     break;
                  }

                  out.writeUnsignedVInt((long)prev);
               }
            }

            while(true) {
               ++prev;
               if(prev == supersetCount) {
                  break;
               }

               out.writeUnsignedVInt((long)prev);
            }
         }

      }

      private Columns deserializeLargeSubset(DataInputPlus in, Columns superset, int delta) throws IOException {
         int supersetCount = superset.size();
         int columnCount = supersetCount - delta;
         ColumnMetadata[] columns = new ColumnMetadata[columnCount];
         int idx;
         if(columnCount < supersetCount / 2) {
            for(int i = 0; i < columnCount; ++i) {
               idx = (int)in.readUnsignedVInt();
               columns[i] = superset.columns[idx];
            }
         } else {
            Iterator<ColumnMetadata> iter = superset.iterator();
            idx = 0;
            int skipped = 0;

            while(true) {
               for(int nextMissingIndex = skipped < delta?(int)in.readUnsignedVInt():supersetCount; idx < nextMissingIndex; ++idx) {
                  ColumnMetadata def = (ColumnMetadata)iter.next();
                  columns[idx - skipped] = def;
               }

               if(idx == supersetCount) {
                  break;
               }

               iter.next();
               ++idx;
               ++skipped;
            }
         }

         return new Columns(columns, null);
      }

      private int serializeLargeSubsetSize(Collection<ColumnMetadata> columns, int columnCount, Columns superset, int supersetCount) {
         int size = TypeSizes.sizeofUnsignedVInt((long)(supersetCount - columnCount));
         Columns.ColumnSearchIterator iter = superset.searchIterator();
         if(columnCount < supersetCount / 2) {
            for(Iterator var7 = columns.iterator(); var7.hasNext(); size += TypeSizes.sizeofUnsignedVInt((long)iter.indexOfCurrent())) {
               ColumnMetadata column = (ColumnMetadata)var7.next();
               if(iter.next(column) == null) {
                  throw new IllegalStateException();
               }
            }
         } else {
            int prev = -1;
            Iterator var12 = columns.iterator();

            while(var12.hasNext()) {
               ColumnMetadata column = (ColumnMetadata)var12.next();
               if(iter.next(column) == null) {
                  throw new IllegalStateException();
               }

               int cur = iter.indexOfCurrent();

               while(true) {
                  ++prev;
                  if(prev == cur) {
                     break;
                  }

                  size += TypeSizes.sizeofUnsignedVInt((long)prev);
               }
            }

            while(true) {
               ++prev;
               if(prev == supersetCount) {
                  break;
               }

               size += TypeSizes.sizeofUnsignedVInt((long)prev);
            }
         }

         return size;
      }
   }

   static class ColumnSearchIterator implements SearchIterator<ColumnMetadata, ColumnMetadata> {
      final ColumnMetadata[] columns;
      int next = 0;

      ColumnSearchIterator(ColumnMetadata[] columns) {
         this.columns = columns;
      }

      public ColumnMetadata next(ColumnMetadata key) {
         if(this.next >= this.columns.length) {
            return null;
         } else {
            int n = Arrays.binarySearch(this.columns, this.next, this.columns.length, key);
            if(n >= 0) {
               this.next = n;
               return key;
            } else {
               this.next = -n - 1;
               return null;
            }
         }
      }

      public int indexOfCurrent() {
         return this.next;
      }

      public void rewind() {
         this.next = 0;
      }
   }
}
