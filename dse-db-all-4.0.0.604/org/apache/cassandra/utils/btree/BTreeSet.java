package org.apache.cassandra.utils.btree;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.SortedSet;
import java.util.Spliterator;
import java.util.Spliterators;

public class BTreeSet<V> implements NavigableSet<V>, List<V> {
   protected final Comparator<? super V> comparator;
   protected final Object[] tree;

   public BTreeSet(Object[] tree, Comparator<? super V> comparator) {
      this.tree = tree;
      this.comparator = comparator;
   }

   public BTreeSet<V> update(Collection<V> updateWith) {
      return new BTreeSet(BTree.update(this.tree, this.comparator, updateWith, UpdateFunction.noOp()), this.comparator);
   }

   public Comparator<? super V> comparator() {
      return this.comparator;
   }

   protected BTreeSearchIterator<V, V> slice(BTree.Dir dir) {
      return BTree.slice(this.tree, this.comparator, dir);
   }

   public Object[] tree() {
      return this.tree;
   }

   public int indexOf(Object item) {
      return BTree.findIndex(this.tree, this.comparator, item);
   }

   public V get(int index) {
      return BTree.findByIndex(this.tree, index);
   }

   public int lastIndexOf(Object o) {
      return this.indexOf(o);
   }

   public BTreeSet<V> subList(int fromIndex, int toIndex) {
      return new BTreeSet.BTreeRange(this.tree, this.comparator, fromIndex, toIndex - 1);
   }

   public int size() {
      return BTree.size(this.tree);
   }

   public boolean isEmpty() {
      return BTree.isEmpty(this.tree);
   }

   public BTreeSearchIterator<V, V> iterator() {
      return this.slice(BTree.Dir.ASC);
   }

   public BTreeSearchIterator<V, V> descendingIterator() {
      return this.slice(BTree.Dir.DESC);
   }

   public Object[] toArray() {
      return this.toArray(new Object[0]);
   }

   public <T> T[] toArray(T[] a) {
      return this.toArray(a, 0);
   }

   public <T> T[] toArray(T[] a, int offset) {
      int size = this.size();
      if(a.length < size + offset) {
         a = Arrays.copyOf(a, size);
      }

      BTree.toArray(this.tree, a, offset);
      return a;
   }

   public Spliterator<V> spliterator() {
      return Spliterators.spliterator(this, 1361);
   }

   public BTreeSet<V> subSet(V fromElement, boolean fromInclusive, V toElement, boolean toInclusive) {
      return new BTreeSet.BTreeRange(this.tree, this.comparator, fromElement, fromInclusive, toElement, toInclusive);
   }

   public BTreeSet<V> headSet(V toElement, boolean inclusive) {
      return new BTreeSet.BTreeRange(this.tree, this.comparator, (Object)null, true, toElement, inclusive);
   }

   public BTreeSet<V> tailSet(V fromElement, boolean inclusive) {
      return new BTreeSet.BTreeRange(this.tree, this.comparator, fromElement, inclusive, (Object)null, true);
   }

   public SortedSet<V> subSet(V fromElement, V toElement) {
      return this.subSet(fromElement, true, toElement, false);
   }

   public SortedSet<V> headSet(V toElement) {
      return this.headSet(toElement, false);
   }

   public SortedSet<V> tailSet(V fromElement) {
      return this.tailSet(fromElement, true);
   }

   public BTreeSet<V> descendingSet() {
      return (new BTreeSet.BTreeRange(this.tree, this.comparator)).descendingSet();
   }

   public V first() {
      return this.get(0);
   }

   public V last() {
      return this.get(this.size() - 1);
   }

   public V lower(V v) {
      return BTree.lower(this.tree, this.comparator, v);
   }

   public V floor(V v) {
      return BTree.floor(this.tree, this.comparator, v);
   }

   public V ceiling(V v) {
      return BTree.ceil(this.tree, this.comparator, v);
   }

   public V higher(V v) {
      return BTree.higher(this.tree, this.comparator, v);
   }

   public boolean contains(Object o) {
      return this.indexOf(o) >= 0;
   }

   public boolean containsAll(Collection<?> c) {
      Iterator var2 = c.iterator();

      Object o;
      do {
         if(!var2.hasNext()) {
            return true;
         }

         o = var2.next();
      } while(this.contains(o));

      return false;
   }

   public int hashCode() {
      int result = 1;

      Object v;
      for(BTreeSearchIterator var2 = this.iterator(); var2.hasNext(); result = 31 * result + Objects.hashCode(v)) {
         v = var2.next();
      }

      return result;
   }

   public boolean addAll(Collection<? extends V> c) {
      throw new UnsupportedOperationException();
   }

   public boolean addAll(int index, Collection<? extends V> c) {
      throw new UnsupportedOperationException();
   }

   public boolean retainAll(Collection<?> c) {
      throw new UnsupportedOperationException();
   }

   public boolean removeAll(Collection<?> c) {
      throw new UnsupportedOperationException();
   }

   public void clear() {
      throw new UnsupportedOperationException();
   }

   public V pollFirst() {
      throw new UnsupportedOperationException();
   }

   public V pollLast() {
      throw new UnsupportedOperationException();
   }

   public boolean add(V v) {
      throw new UnsupportedOperationException();
   }

   public boolean remove(Object o) {
      throw new UnsupportedOperationException();
   }

   public V set(int index, V element) {
      throw new UnsupportedOperationException();
   }

   public void add(int index, V element) {
      throw new UnsupportedOperationException();
   }

   public V remove(int index) {
      throw new UnsupportedOperationException();
   }

   public ListIterator<V> listIterator() {
      throw new UnsupportedOperationException();
   }

   public ListIterator<V> listIterator(int index) {
      throw new UnsupportedOperationException();
   }

   public static <V> BTreeSet.Builder<V> builder(Comparator<? super V> comparator) {
      return new BTreeSet.Builder(comparator);
   }

   public static <V> BTreeSet<V> wrap(Object[] btree, Comparator<V> comparator) {
      return new BTreeSet(btree, comparator);
   }

   public static <V extends Comparable<V>> BTreeSet<V> of(Collection<V> sortedValues) {
      return new BTreeSet(BTree.build(sortedValues, UpdateFunction.noOp()), Ordering.natural());
   }

   public static <V extends Comparable<V>> BTreeSet<V> of(V value) {
      return new BTreeSet(BTree.build((Collection)ImmutableList.of(value), UpdateFunction.noOp()), Ordering.natural());
   }

   public static <V> BTreeSet<V> empty(Comparator<? super V> comparator) {
      return new BTreeSet(BTree.empty(), comparator);
   }

   public static <V> BTreeSet<V> of(Comparator<? super V> comparator, V value) {
      return new BTreeSet(BTree.singleton(value), comparator);
   }

   public static class Builder<V> {
      final BTree.Builder<V> builder;

      protected Builder(Comparator<? super V> comparator) {
         this.builder = BTree.builder(comparator);
      }

      public BTreeSet.Builder<V> add(V v) {
         this.builder.add(v);
         return this;
      }

      public BTreeSet.Builder<V> addAll(Collection<V> iter) {
         this.builder.addAll(iter);
         return this;
      }

      public boolean isEmpty() {
         return this.builder.isEmpty();
      }

      public BTreeSet<V> build() {
         return new BTreeSet(this.builder.build(), this.builder.comparator);
      }
   }

   public static class BTreeDescRange<V> extends BTreeSet.BTreeRange<V> {
      BTreeDescRange(BTreeSet.BTreeRange<V> from) {
         super(from.tree, from.comparator, from.lowerBound, from.upperBound);
      }

      protected BTreeSearchIterator<V, V> slice(BTree.Dir dir) {
         return super.slice(dir.invert());
      }

      public V higher(V v) {
         return super.lower(v);
      }

      public V ceiling(V v) {
         return super.floor(v);
      }

      public V floor(V v) {
         return super.ceiling(v);
      }

      public V lower(V v) {
         return super.higher(v);
      }

      public V get(int index) {
         index = this.upperBound - index;
         if(this.outOfBounds(index)) {
            throw new NoSuchElementException();
         } else {
            return BTree.findByIndex(this.tree, index);
         }
      }

      public int indexOf(Object item) {
         int i = super.indexOf(item);
         return i < 0?-2 - this.size() - i:this.size() - (i + 1);
      }

      public BTreeSet<V> subList(int fromIndex, int toIndex) {
         if(fromIndex >= 0 && toIndex <= this.size()) {
            return new BTreeSet.BTreeDescRange(new BTreeSet.BTreeRange(this.tree, this.comparator, this.upperBound - (toIndex - 1), this.upperBound - fromIndex));
         } else {
            throw new IndexOutOfBoundsException();
         }
      }

      public BTreeSet<V> subSet(V fromElement, boolean fromInclusive, V toElement, boolean toInclusive) {
         return super.subSet(toElement, toInclusive, fromElement, fromInclusive).descendingSet();
      }

      public BTreeSet<V> headSet(V toElement, boolean inclusive) {
         return super.tailSet(toElement, inclusive).descendingSet();
      }

      public BTreeSet<V> tailSet(V fromElement, boolean inclusive) {
         return super.headSet(fromElement, inclusive).descendingSet();
      }

      public BTreeSet<V> descendingSet() {
         return new BTreeSet.BTreeRange(this);
      }

      public Comparator<V> comparator() {
         return (a, b) -> {
            return this.comparator.compare(b, a);
         };
      }

      public <T> T[] toArray(T[] a, int offset) {
         a = super.toArray(a, offset);
         int count = this.size();
         int flip = count / 2;

         for(int i = 0; i < flip; ++i) {
            int j = count - (i + 1);
            T t = a[i + offset];
            a[i + offset] = a[j + offset];
            a[j + offset] = t;
         }

         return a;
      }
   }

   public static class BTreeRange<V> extends BTreeSet<V> {
      protected final int lowerBound;
      protected final int upperBound;

      BTreeRange(Object[] tree, Comparator<? super V> comparator) {
         this(tree, comparator, (Object)null, true, (Object)null, true);
      }

      BTreeRange(BTreeSet.BTreeRange<V> from) {
         super(from.tree, from.comparator);
         this.lowerBound = from.lowerBound;
         this.upperBound = from.upperBound;
      }

      BTreeRange(Object[] tree, Comparator<? super V> comparator, int lowerBound, int upperBound) {
         super(tree, comparator);
         if(upperBound < lowerBound - 1) {
            upperBound = lowerBound - 1;
         }

         this.lowerBound = lowerBound;
         this.upperBound = upperBound;
      }

      BTreeRange(Object[] tree, Comparator<? super V> comparator, V lowerBound, boolean inclusiveLowerBound, V upperBound, boolean inclusiveUpperBound) {
         this(tree, comparator, lowerBound == null?0:(inclusiveLowerBound?BTree.ceilIndex(tree, comparator, lowerBound):BTree.higherIndex(tree, comparator, lowerBound)), upperBound == null?BTree.size(tree) - 1:(inclusiveUpperBound?BTree.floorIndex(tree, comparator, upperBound):BTree.lowerIndex(tree, comparator, upperBound)));
      }

      BTreeRange(BTreeSet.BTreeRange<V> a, BTreeSet.BTreeRange<V> b) {
         this(a.tree, a.comparator, Math.max(a.lowerBound, b.lowerBound), Math.min(a.upperBound, b.upperBound));

         assert a.tree == b.tree;

      }

      protected BTreeSearchIterator<V, V> slice(BTree.Dir dir) {
         return BTree.slice(this.tree, this.comparator, this.lowerBound, this.upperBound, dir);
      }

      public boolean isEmpty() {
         return this.upperBound < this.lowerBound;
      }

      public int size() {
         return this.upperBound - this.lowerBound + 1;
      }

      boolean outOfBounds(int i) {
         return i < this.lowerBound | i > this.upperBound;
      }

      public V get(int index) {
         index += this.lowerBound;
         if(this.outOfBounds(index)) {
            throw new NoSuchElementException();
         } else {
            return super.get(index);
         }
      }

      public int indexOf(Object item) {
         int i = super.indexOf(item);
         boolean negate = i < 0;
         if(negate) {
            i = -1 - i;
         }

         if(this.outOfBounds(i)) {
            return i < this.lowerBound?-1:-1 - this.size();
         } else {
            i -= this.lowerBound;
            if(negate) {
               i = -1 - i;
            }

            return i;
         }
      }

      public V lower(V v) {
         return this.maybe(Math.min(this.upperBound, BTree.lowerIndex(this.tree, this.comparator, v)));
      }

      public V floor(V v) {
         return this.maybe(Math.min(this.upperBound, BTree.floorIndex(this.tree, this.comparator, v)));
      }

      public V ceiling(V v) {
         return this.maybe(Math.max(this.lowerBound, BTree.ceilIndex(this.tree, this.comparator, v)));
      }

      public V higher(V v) {
         return this.maybe(Math.max(this.lowerBound, BTree.higherIndex(this.tree, this.comparator, v)));
      }

      private V maybe(int i) {
         return this.outOfBounds(i)?null:super.get(i);
      }

      public BTreeSet<V> subSet(V fromElement, boolean fromInclusive, V toElement, boolean toInclusive) {
         return new BTreeSet.BTreeRange(this, new BTreeSet.BTreeRange(this.tree, this.comparator, fromElement, fromInclusive, toElement, toInclusive));
      }

      public BTreeSet<V> headSet(V toElement, boolean inclusive) {
         return new BTreeSet.BTreeRange(this, new BTreeSet.BTreeRange(this.tree, this.comparator, (Object)null, true, toElement, inclusive));
      }

      public BTreeSet<V> tailSet(V fromElement, boolean inclusive) {
         return new BTreeSet.BTreeRange(this, new BTreeSet.BTreeRange(this.tree, this.comparator, fromElement, inclusive, (Object)null, true));
      }

      public BTreeSet<V> descendingSet() {
         return new BTreeSet.BTreeDescRange(this);
      }

      public BTreeSet<V> subList(int fromIndex, int toIndex) {
         if(fromIndex >= 0 && toIndex <= this.size()) {
            return new BTreeSet.BTreeRange(this.tree, this.comparator, this.lowerBound + fromIndex, this.lowerBound + toIndex - 1);
         } else {
            throw new IndexOutOfBoundsException();
         }
      }

      public <T> T[] toArray(T[] a) {
         return this.toArray(a, 0);
      }

      public <T> T[] toArray(T[] a, int offset) {
         if(this.size() + offset < a.length) {
            a = Arrays.copyOf(a, this.size() + offset);
         }

         BTree.toArray(this.tree, this.lowerBound, this.upperBound + 1, a, offset);
         return a;
      }
   }
}
