package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.btree.BTreeSet;

public abstract class MultiCBuilder {
   protected final ClusteringComparator comparator;
   protected int size;
   protected boolean built;
   protected boolean containsNull;
   protected boolean containsUnset;
   protected boolean hasMissingElements;

   protected MultiCBuilder(ClusteringComparator comparator) {
      this.comparator = comparator;
   }

   public static MultiCBuilder create(ClusteringComparator comparator, boolean forMultipleValues) {
      return (MultiCBuilder)(forMultipleValues?new MultiCBuilder.MultiClusteringBuilder(comparator):new MultiCBuilder.OneClusteringBuilder(comparator));
   }

   public abstract MultiCBuilder addElementToAll(ByteBuffer var1);

   public abstract MultiCBuilder addEachElementToAll(List<ByteBuffer> var1);

   public abstract MultiCBuilder addAllElementsToAll(List<List<ByteBuffer>> var1);

   protected void checkUpdateable() {
      if(!this.hasRemaining() || this.built) {
         throw new IllegalStateException("this builder cannot be updated anymore");
      }
   }

   public int remainingCount() {
      return this.comparator.size() - this.size;
   }

   public boolean containsNull() {
      return this.containsNull;
   }

   public boolean containsUnset() {
      return this.containsUnset;
   }

   public boolean hasMissingElements() {
      return this.hasMissingElements;
   }

   public abstract NavigableSet<Clustering> build();

   public abstract List<ByteBuffer> buildSerializedPartitionKeys();

   public abstract NavigableSet<ClusteringBound> buildBoundForSlice(boolean var1, boolean var2, boolean var3, List<ColumnMetadata> var4);

   public abstract NavigableSet<ClusteringBound> buildBound(boolean var1, boolean var2);

   public boolean hasRemaining() {
      return this.remainingCount() > 0;
   }

   private static class MultiClusteringBuilder extends MultiCBuilder {
      private final List<List<ByteBuffer>> elementsList = new ArrayList();

      public MultiClusteringBuilder(ClusteringComparator comparator) {
         super(comparator);
      }

      public MultiCBuilder addElementToAll(ByteBuffer value) {
         this.checkUpdateable();
         if(this.elementsList.isEmpty()) {
            this.elementsList.add(new ArrayList());
         }

         if(value == null) {
            this.containsNull = true;
         } else if(value == ByteBufferUtil.UNSET_BYTE_BUFFER) {
            this.containsUnset = true;
         }

         int i = 0;

         for(int m = this.elementsList.size(); i < m; ++i) {
            ((List)this.elementsList.get(i)).add(value);
         }

         ++this.size;
         return this;
      }

      public MultiCBuilder addEachElementToAll(List<ByteBuffer> values) {
         this.checkUpdateable();
         if(this.elementsList.isEmpty()) {
            this.elementsList.add(new ArrayList());
         }

         if(values.isEmpty()) {
            this.hasMissingElements = true;
         } else {
            int i = 0;

            for(int m = this.elementsList.size(); i < m; ++i) {
               List<ByteBuffer> oldComposite = (List)this.elementsList.remove(0);
               int j = 0;

               for(int n = values.size(); j < n; ++j) {
                  List<ByteBuffer> newComposite = new ArrayList(oldComposite);
                  this.elementsList.add(newComposite);
                  ByteBuffer value = (ByteBuffer)values.get(j);
                  if(value == null) {
                     this.containsNull = true;
                  }

                  if(value == ByteBufferUtil.UNSET_BYTE_BUFFER) {
                     this.containsUnset = true;
                  }

                  newComposite.add(values.get(j));
               }
            }
         }

         ++this.size;
         return this;
      }

      public MultiCBuilder addAllElementsToAll(List<List<ByteBuffer>> values) {
         this.checkUpdateable();
         if(this.elementsList.isEmpty()) {
            this.elementsList.add(new ArrayList());
         }

         if(values.isEmpty()) {
            this.hasMissingElements = true;
         } else {
            int i = 0;

            for(int m = this.elementsList.size(); i < m; ++i) {
               List<ByteBuffer> oldComposite = (List)this.elementsList.remove(0);
               int j = 0;

               for(int n = values.size(); j < n; ++j) {
                  List<ByteBuffer> newComposite = new ArrayList(oldComposite);
                  this.elementsList.add(newComposite);
                  List<ByteBuffer> value = (List)values.get(j);
                  if(value.contains((Object)null)) {
                     this.containsNull = true;
                  }

                  if(value.contains(ByteBufferUtil.UNSET_BYTE_BUFFER)) {
                     this.containsUnset = true;
                  }

                  newComposite.addAll(value);
               }
            }

            this.size += ((List)values.get(0)).size();
         }

         return this;
      }

      public NavigableSet<Clustering> build() {
         this.built = true;
         if(this.hasMissingElements) {
            return BTreeSet.empty(this.comparator);
         } else {
            CBuilder builder = CBuilder.create(this.comparator);
            if(this.elementsList.isEmpty()) {
               return BTreeSet.of(builder.comparator(), builder.build());
            } else {
               BTreeSet.Builder<Clustering> set = BTreeSet.builder(builder.comparator());
               int i = 0;

               for(int m = this.elementsList.size(); i < m; ++i) {
                  List<ByteBuffer> elements = (List)this.elementsList.get(i);
                  set.add(builder.buildWith(elements));
               }

               return set.build();
            }
         }
      }

      public List<ByteBuffer> buildSerializedPartitionKeys() {
         this.built = true;
         if(this.hasMissingElements) {
            return Collections.emptyList();
         } else {
            TreeSet<ByteBuffer> set = this.comparator.size() == 1?new TreeSet(this.comparator.subtype(0)):new TreeSet(CompositeType.getInstance(this.comparator.subtypes()));
            int i = 0;

            for(int m = this.elementsList.size(); i < m; ++i) {
               List<ByteBuffer> elements = (List)this.elementsList.get(i);
               set.add(this.comparator.size() == 1?(ByteBuffer)elements.get(0):this.toComposite(elements));
            }

            return new ArrayList(set);
         }
      }

      private ByteBuffer toComposite(List<ByteBuffer> elements) {
         ByteBuffer[] tmp = new ByteBuffer[elements.size()];
         int i = 0;

         for(int m = elements.size(); i < m; ++i) {
            tmp[i] = (ByteBuffer)elements.get(i);
         }

         return CompositeType.build(tmp);
      }

      public NavigableSet<ClusteringBound> buildBoundForSlice(boolean isStart, boolean isInclusive, boolean isOtherBoundInclusive, List<ColumnMetadata> columnDefs) {
         this.built = true;
         if(this.hasMissingElements) {
            return BTreeSet.empty(this.comparator);
         } else {
            CBuilder builder = CBuilder.create(this.comparator);
            if(this.elementsList.isEmpty()) {
               return BTreeSet.of(this.comparator, builder.buildBound(isStart, isInclusive));
            } else {
               BTreeSet.Builder<ClusteringBound> set = BTreeSet.builder(this.comparator);
               int offset = ((ColumnMetadata)columnDefs.get(0)).position();
               int i = 0;

               for(int m = this.elementsList.size(); i < m; ++i) {
                  List<ByteBuffer> elements = (List)this.elementsList.get(i);
                  if(elements.size() == offset) {
                     set.add(builder.buildBoundWith(elements, isStart, true));
                  } else {
                     ColumnMetadata lastColumn = (ColumnMetadata)columnDefs.get(columnDefs.size() - 1);
                     if(elements.size() <= lastColumn.position() && i < m - 1 && elements.equals(this.elementsList.get(i + 1))) {
                        set.add(builder.buildBoundWith(elements, isStart, false));
                        set.add(builder.buildBoundWith((List)this.elementsList.get(i++), isStart, true));
                     } else {
                        ColumnMetadata column = (ColumnMetadata)columnDefs.get(elements.size() - 1 - offset);
                        set.add(builder.buildBoundWith(elements, isStart, column.isReversedType()?isOtherBoundInclusive:isInclusive));
                     }
                  }
               }

               return set.build();
            }
         }
      }

      public NavigableSet<ClusteringBound> buildBound(boolean isStart, boolean isInclusive) {
         this.built = true;
         if(this.hasMissingElements) {
            return BTreeSet.empty(this.comparator);
         } else {
            CBuilder builder = CBuilder.create(this.comparator);
            if(this.elementsList.isEmpty()) {
               return BTreeSet.of(this.comparator, builder.buildBound(isStart, isInclusive));
            } else {
               BTreeSet.Builder<ClusteringBound> set = BTreeSet.builder(this.comparator);
               int i = 0;

               for(int m = this.elementsList.size(); i < m; ++i) {
                  List<ByteBuffer> elements = (List)this.elementsList.get(i);
                  set.add(builder.buildBoundWith(elements, isStart, isInclusive));
               }

               return set.build();
            }
         }
      }
   }

   private static class OneClusteringBuilder extends MultiCBuilder {
      private final ByteBuffer[] elements;

      public OneClusteringBuilder(ClusteringComparator comparator) {
         super(comparator);
         this.elements = new ByteBuffer[comparator.size()];
      }

      public MultiCBuilder addElementToAll(ByteBuffer value) {
         this.checkUpdateable();
         if(value == null) {
            this.containsNull = true;
         }

         if(value == ByteBufferUtil.UNSET_BYTE_BUFFER) {
            this.containsUnset = true;
         }

         this.elements[this.size++] = value;
         return this;
      }

      public MultiCBuilder addEachElementToAll(List<ByteBuffer> values) {
         if(values.isEmpty()) {
            this.hasMissingElements = true;
            return this;
         } else {
            assert values.size() == 1;

            return this.addElementToAll((ByteBuffer)values.get(0));
         }
      }

      public MultiCBuilder addAllElementsToAll(List<List<ByteBuffer>> values) {
         if(values.isEmpty()) {
            this.hasMissingElements = true;
            return this;
         } else {
            assert values.size() == 1;

            return this.addEachElementToAll((List)values.get(0));
         }
      }

      public NavigableSet<Clustering> build() {
         this.built = true;
         return this.hasMissingElements?BTreeSet.empty(this.comparator):BTreeSet.of(this.comparator, this.size == 0?Clustering.EMPTY:Clustering.make(this.elements));
      }

      public List<ByteBuffer> buildSerializedPartitionKeys() {
         this.built = true;
         return this.hasMissingElements?Collections.emptyList():(this.size == 0?Collections.singletonList(ByteBufferUtil.EMPTY_BYTE_BUFFER):(this.size == 1?Collections.singletonList(this.elements[0]):Collections.singletonList(CompositeType.build(this.elements))));
      }

      public NavigableSet<ClusteringBound> buildBoundForSlice(boolean isStart, boolean isInclusive, boolean isOtherBoundInclusive, List<ColumnMetadata> columnDefs) {
         return this.buildBound(isStart, ((ColumnMetadata)columnDefs.get(0)).isReversedType()?isOtherBoundInclusive:isInclusive);
      }

      public NavigableSet<ClusteringBound> buildBound(boolean isStart, boolean isInclusive) {
         this.built = true;
         if(this.hasMissingElements) {
            return BTreeSet.empty(this.comparator);
         } else if(this.size == 0) {
            return BTreeSet.of(this.comparator, isStart?ClusteringBound.BOTTOM:ClusteringBound.TOP);
         } else {
            ByteBuffer[] newValues = this.size == this.elements.length?this.elements:(ByteBuffer[])Arrays.copyOf(this.elements, this.size);
            return BTreeSet.of(this.comparator, ClusteringBound.create(ClusteringBound.boundKind(isStart, isInclusive), newValues));
         }
      }
   }
}
