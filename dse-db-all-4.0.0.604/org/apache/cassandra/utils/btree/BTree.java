package org.apache.cassandra.utils.btree;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Ordering;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.SortedSet;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import org.apache.cassandra.utils.ObjectSizes;

public class BTree {
   static final int FAN_SHIFT;
   static final int FAN_FACTOR;
   static final int MINIMAL_NODE_SIZE;
   static final Object[] EMPTY_LEAF;
   static final Object[] EMPTY_BRANCH;
   static final Recycler<BTree.Builder> builderRecycler;
   static Object POSITIVE_INFINITY;
   static Object NEGATIVE_INFINITY;

   public BTree() {
   }

   public static Object[] empty() {
      return EMPTY_LEAF;
   }

   public static Object[] singleton(Object value) {
      return new Object[]{value};
   }

   public static <C, K extends C, V extends C> Object[] build(Collection<K> source, UpdateFunction<K, V> updateF) {
      return buildInternal(source, source.size(), updateF);
   }

   public static <C, K extends C, V extends C> Object[] build(Iterable<K> source, UpdateFunction<K, V> updateF) {
      return buildInternal(source, -1, updateF);
   }

   public static <C, K extends C, V extends C> Object[] build(Iterable<K> source, int size, UpdateFunction<K, V> updateF) {
      if(size < 0) {
         throw new IllegalArgumentException(Integer.toString(size));
      } else {
         return buildInternal(source, size, updateF);
      }
   }

   private static <C, K extends C, V extends C> Object[] buildInternal(Iterable<K> source, int size, UpdateFunction<K, V> updateF) {
      if (size >= 0 & size < FAN_FACTOR) {
         if (size == 0) {
            return EMPTY_LEAF;
         }
         Object[] values = new Object[size | 1];
         int i = 0;
         for (K k : source) {
            values[i++] = updateF.apply(k);
         }
         if (updateF != UpdateFunction.noOp()) {
            updateF.allocated(ObjectSizes.sizeOfArray(values));
         }
         return values;
      }
      TreeBuilder builder = TreeBuilder.newInstance();
      Object[] btree = builder.build(source, updateF, size);
      return btree;
   }

   public static <C, K extends C, V extends C> Object[] update(Object[] btree, Comparator<C> comparator, Collection<K> updateWith, UpdateFunction<K, V> updateF) {
      return update(btree, comparator, updateWith, updateWith.size(), updateF);
   }

   public static <C, K extends C, V extends C> Object[] update(Object[] btree, Comparator<C> comparator, Iterable<K> updateWith, int updateWithLength, UpdateFunction<K, V> updateF) {
      if(isEmpty(btree)) {
         return build(updateWith, updateWithLength, updateF);
      } else {
         TreeBuilder builder = TreeBuilder.newInstance();
         btree = builder.update(btree, comparator, updateWith, updateF);
         return btree;
      }
   }

   public static <K> Object[] merge(Object[] tree1, Object[] tree2, Comparator<? super K> comparator, UpdateFunction<K, K> updateF) {
      if(size(tree1) < size(tree2)) {
         Object[] tmp = tree1;
         tree1 = tree2;
         tree2 = tmp;
      }

      return update(tree1, comparator, new BTreeSet(tree2, comparator), updateF);
   }

   public static <V> Iterator<V> iterator(Object[] btree) {
      return iterator(btree, BTree.Dir.ASC);
   }

   public static <V> Iterator<V> iterator(Object[] btree, BTree.Dir dir) {
      return (Iterator)(isLeaf(btree)?new LeafBTreeSearchIterator(btree, (Comparator)null, dir):new FullBTreeSearchIterator(btree, (Comparator)null, dir));
   }

   public static <V> Iterator<V> iterator(Object[] btree, int lb, int ub, BTree.Dir dir) {
      return (Iterator)(isLeaf(btree)?new LeafBTreeSearchIterator(btree, (Comparator)null, dir, lb, ub):new FullBTreeSearchIterator(btree, (Comparator)null, dir, lb, ub));
   }

   public static <V> Iterable<V> iterable(Object[] btree) {
      return iterable(btree, BTree.Dir.ASC);
   }

   public static <V> Iterable<V> iterable(Object[] btree, BTree.Dir dir) {
      return () -> {
         return iterator(btree, dir);
      };
   }

   public static <V> Iterable<V> iterable(Object[] btree, int lb, int ub, BTree.Dir dir) {
      return () -> {
         return iterator(btree, lb, ub, dir);
      };
   }

   public static <K, V> BTreeSearchIterator<K, V> slice(Object[] btree, Comparator<? super K> comparator, BTree.Dir dir) {
      return (BTreeSearchIterator)(isLeaf(btree)?new LeafBTreeSearchIterator(btree, comparator, dir):new FullBTreeSearchIterator(btree, comparator, dir));
   }

   public static <K, V extends K> BTreeSearchIterator<K, V> slice(Object[] btree, Comparator<? super K> comparator, K start, K end, BTree.Dir dir) {
      return slice(btree, comparator, start, true, end, false, dir);
   }

   public static <K, V extends K> BTreeSearchIterator<K, V> slice(Object[] btree, Comparator<? super K> comparator, int startIndex, int endIndex, BTree.Dir dir) {
      return (BTreeSearchIterator)(isLeaf(btree)?new LeafBTreeSearchIterator(btree, comparator, dir, startIndex, endIndex):new FullBTreeSearchIterator(btree, comparator, dir, startIndex, endIndex));
   }

   public static <K, V extends K> BTreeSearchIterator<K, V> slice(Object[] btree, Comparator<? super K> comparator, K start, boolean startInclusive, K end, boolean endInclusive, BTree.Dir dir) {
      int inclusiveLowerBound = Math.max(0, start == null?-2147483648:(startInclusive?ceilIndex(btree, comparator, start):higherIndex(btree, comparator, start)));
      int inclusiveUpperBound = Math.min(size(btree) - 1, end == null?2147483647:(endInclusive?floorIndex(btree, comparator, end):lowerIndex(btree, comparator, end)));
      return (BTreeSearchIterator)(isLeaf(btree)?new LeafBTreeSearchIterator(btree, comparator, dir, inclusiveLowerBound, inclusiveUpperBound):new FullBTreeSearchIterator(btree, comparator, dir, inclusiveLowerBound, inclusiveUpperBound));
   }

   public static <V> V find(Object[] node, Comparator<? super V> comparator, V find) {
      int i;
      int keyEnd;
      while ((i = Arrays.<Object>binarySearch(node, 0, keyEnd = BTree.getKeyEnd(node), find, (Comparator<? super Object>)comparator)) < 0) {
         if (BTree.isLeaf(node)) {
            return null;
         }
         i = -1 - i;
         node = (Object[])node[keyEnd + i];
      }
      return (V)node[i];
   }

   public static <V> void replaceInSitu(Object[] tree, int index, V replace) {
      if(index < 0 | index >= size(tree)) {
         throw new IndexOutOfBoundsException(index + " not in range [0.." + size(tree) + ")");
      } else {
         int boundary;
         for(; !isLeaf(tree); tree = (Object[])((Object[])tree[getChildStart(tree) + boundary])) {
            int[] sizeMap = getSizeMap(tree);
            boundary = Arrays.binarySearch(sizeMap, index);
            if(boundary >= 0) {
               assert boundary < sizeMap.length - 1;

               tree[boundary] = replace;
               return;
            }

            boundary = -1 - boundary;
            if(boundary > 0) {
               assert boundary < sizeMap.length;

               index -= 1 + sizeMap[boundary - 1];
            }
         }

         assert index < getLeafKeyEnd(tree);

         tree[index] = replace;
      }
   }

   public static <V> void replaceInSitu(Object[] node, Comparator<? super V> comparator, V find, V replace) {
      while(true) {
         int keyEnd = getKeyEnd(node);
         int i = Arrays.<Object>binarySearch(node, 0, keyEnd, find, (Comparator<? super Object>)comparator);
         if(i >= 0) {
            assert find == node[i];

            node[i] = replace;
            return;
         }

         if(isLeaf(node)) {
            throw new NoSuchElementException();
         }

         i = -1 - i;
         node = (Object[])((Object[])node[keyEnd + i]);
      }
   }

   public static <V> int findIndex(Object[] node, final Comparator<? super V> comparator, final V find) {
      int lb = 0;
      while (true) {
         final int keyEnd = getKeyEnd(node);
         int i = Arrays.binarySearch(node, 0, keyEnd, find, (Comparator<? super Object>)comparator);
         final boolean exact = i >= 0;
         if (isLeaf(node)) {
            return exact ? (lb + i) : (i - lb);
         }
         if (!exact) {
            i = -1 - i;
         }
         final int[] sizeMap = getSizeMap(node);
         if (exact) {
            return lb + sizeMap[i];
         }
         if (i > 0) {
            lb += sizeMap[i - 1] + 1;
         }
         node = (Object[])node[keyEnd + i];
      }
   }

   public static <V> V findByIndex(final Object[] tree, int index) {
      if (index < 0 | index >= size(tree)) {
         throw new IndexOutOfBoundsException(index + " not in range [0.." + size(tree) + ")");
      }
      Object[] node = tree;
      while (!isLeaf(node)) {
         final int[] sizeMap = getSizeMap(node);
         int boundary = Arrays.binarySearch(sizeMap, index);
         if (boundary >= 0) {
            assert boundary < sizeMap.length - 1;
            return (V)node[boundary];
         }
         else {
            boundary = -1 - boundary;
            if (boundary > 0) {
               assert boundary < sizeMap.length;
               index -= 1 + sizeMap[boundary - 1];
            }
            node = (Object[])node[getChildStart(node) + boundary];
         }
      }
      final int keyEnd = getLeafKeyEnd(node);
      assert index < keyEnd;
      return (V)node[index];
   }

   public static <V> int lowerIndex(Object[] btree, Comparator<? super V> comparator, V find) {
      int i = findIndex(btree, comparator, find);
      if(i < 0) {
         i = -1 - i;
      }

      return i - 1;
   }

   public static <V> V lower(Object[] btree, Comparator<? super V> comparator, V find) {
      int i = lowerIndex(btree, comparator, find);
      return i >= 0?findByIndex(btree, i):null;
   }

   public static <V> int floorIndex(Object[] btree, Comparator<? super V> comparator, V find) {
      int i = findIndex(btree, comparator, find);
      if(i < 0) {
         i = -2 - i;
      }

      return i;
   }

   public static <V> V floor(Object[] btree, Comparator<? super V> comparator, V find) {
      int i = floorIndex(btree, comparator, find);
      return i >= 0?findByIndex(btree, i):null;
   }

   public static <V> int higherIndex(Object[] btree, Comparator<? super V> comparator, V find) {
      int i = findIndex(btree, comparator, find);
      if(i < 0) {
         i = -1 - i;
      } else {
         ++i;
      }

      return i;
   }

   public static <V> V higher(Object[] btree, Comparator<? super V> comparator, V find) {
      int i = higherIndex(btree, comparator, find);
      return i < size(btree)?findByIndex(btree, i):null;
   }

   public static <V> int ceilIndex(Object[] btree, Comparator<? super V> comparator, V find) {
      int i = findIndex(btree, comparator, find);
      if(i < 0) {
         i = -1 - i;
      }

      return i;
   }

   public static <V> V ceil(Object[] btree, Comparator<? super V> comparator, V find) {
      int i = ceilIndex(btree, comparator, find);
      return i < size(btree)?findByIndex(btree, i):null;
   }

   static int getKeyEnd(Object[] node) {
      return isLeaf(node)?getLeafKeyEnd(node):getBranchKeyEnd(node);
   }

   static int getLeafKeyEnd(Object[] node) {
      int len = node.length;
      return node[len - 1] == null?len - 1:len;
   }

   static int getBranchKeyEnd(Object[] branchNode) {
      return branchNode.length / 2 - 1;
   }

   static int getChildStart(Object[] branchNode) {
      return getBranchKeyEnd(branchNode);
   }

   static int getChildEnd(Object[] branchNode) {
      return branchNode.length - 1;
   }

   static int getChildCount(Object[] branchNode) {
      return branchNode.length / 2;
   }

   static int[] getSizeMap(Object[] branchNode) {
      return (int[])((int[])branchNode[getChildEnd(branchNode)]);
   }

   static int lookupSizeMap(Object[] branchNode, int index) {
      return getSizeMap(branchNode)[index];
   }

   public static int size(Object[] tree) {
      if(isLeaf(tree)) {
         return getLeafKeyEnd(tree);
      } else {
         int length = tree.length;
         return ((int[])((int[])tree[length - 1]))[length / 2 - 1];
      }
   }

   public static long sizeOfStructureOnHeap(Object[] tree) {
      long size = ObjectSizes.sizeOfArray(tree);
      if(isLeaf(tree)) {
         return size;
      } else {
         for(int i = getChildStart(tree); i < getChildEnd(tree); ++i) {
            size += sizeOfStructureOnHeap((Object[])((Object[])tree[i]));
         }

         return size;
      }
   }

   static boolean isLeaf(Object[] node) {
      return (node.length & 1) == 1;
   }

   public static boolean isEmpty(Object[] tree) {
      return tree == EMPTY_LEAF;
   }

   public static int depth(Object[] tree) {
      int depth;
      for(depth = 1; !isLeaf(tree); tree = (Object[])((Object[])tree[getKeyEnd(tree)])) {
         ++depth;
      }

      return depth;
   }

   public static int toArray(Object[] tree, Object[] target, int targetOffset) {
      return toArray(tree, 0, size(tree), target, targetOffset);
   }

   public static int toArray(Object[] tree, int treeStart, int treeEnd, Object[] target, int targetOffset) {
      int newTargetOffset;
      if(isLeaf(tree)) {
         newTargetOffset = treeEnd - treeStart;
         System.arraycopy(tree, treeStart, target, targetOffset, newTargetOffset);
         return newTargetOffset;
      } else {
         newTargetOffset = targetOffset;
         int childCount = getChildCount(tree);
         int childOffset = getChildStart(tree);

         for(int i = 0; i < childCount; ++i) {
            int childStart = treeIndexOffsetOfChild(tree, i);
            int childEnd = treeIndexOfBranchKey(tree, i);
            if(childStart <= treeEnd && childEnd >= treeStart) {
               newTargetOffset += toArray((Object[])((Object[])tree[childOffset + i]), Math.max(0, treeStart - childStart), Math.min(childEnd, treeEnd) - childStart, target, newTargetOffset);
               if(treeStart <= childEnd && treeEnd > childEnd) {
                  target[newTargetOffset++] = tree[i];
               }
            }
         }

         return newTargetOffset - targetOffset;
      }
   }

   public static <V> Object[] transformAndFilter(Object[] btree, Function<? super V, ? extends V> function) {
      if(isEmpty(btree)) {
         return btree;
      } else {
         BTree.FiltrationTracker<V> wrapped = new BTree.FiltrationTracker(function);
         Object[] result = transformAndFilter(btree, wrapped);
         if(!wrapped.failed) {
            return result;
         } else {
            Iterable<V> head = iterable(result, 0, wrapped.index - 1, BTree.Dir.ASC);
            Iterable<V> remainder = iterable(btree, wrapped.index + 1, size(btree) - 1, BTree.Dir.ASC);
            remainder = Iterables.filter(Iterables.transform(remainder, function), (x) -> {
               return x != null;
            });
            Iterable<V> build = Iterables.concat(head, remainder);
            return buildInternal(build, -1, UpdateFunction.noOp());
         }
      }
   }

   private static <V> Object[] transformAndFilter(Object[] btree, BTree.FiltrationTracker<V> function) {
      Object[] result = btree;
      boolean isLeaf = isLeaf(btree);
      int childOffset = isLeaf?2147483647:getChildStart(btree);
      int limit = isLeaf?getLeafKeyEnd(btree):btree.length - 1;

      for(int i = 0; i < limit; ++i) {
         int idx = isLeaf?i:i / 2 + (i % 2 == 0?childOffset:0);
         Object current = btree[idx];
         Object updated = idx < childOffset?function.apply((V)current):transformAndFilter((Object[])((Object[])current), function);
         if(updated != current) {
            if(result == btree) {
               result = (Object[])btree.clone();
            }

            result[idx] = updated;
         }

         if(function.failed) {
            return result;
         }
      }

      return result;
   }

   public static boolean equals(Object[] a, Object[] b) {
      return size(a) == size(b) && Iterators.elementsEqual(iterator(a), iterator(b));
   }

   public static int hashCode(Object[] btree) {
      int result = 1;

      Object v;
      for(Iterator var2 = iterable(btree).iterator(); var2.hasNext(); result = 31 * result + Objects.hashCode(v)) {
         v = var2.next();
      }

      return result;
   }

   public static int treeIndexOfKey(Object[] root, int keyIndex) {
      if(isLeaf(root)) {
         return keyIndex;
      } else {
         int[] sizeMap = getSizeMap(root);
         return keyIndex >= 0 & keyIndex < sizeMap.length?sizeMap[keyIndex]:(keyIndex < 0?-1:sizeMap[keyIndex - 1] + 1);
      }
   }

   public static int treeIndexOfLeafKey(int keyIndex) {
      return keyIndex;
   }

   public static int treeIndexOfBranchKey(Object[] root, int keyIndex) {
      return lookupSizeMap(root, keyIndex);
   }

   public static int treeIndexOffsetOfChild(Object[] root, int childIndex) {
      return childIndex == 0?0:1 + lookupSizeMap(root, childIndex - 1);
   }

   public static <V> BTree.Builder<V> builder(Comparator<? super V> comparator) {
      BTree.Builder<V> builder = (BTree.Builder)builderRecycler.get();
      builder.reuse(comparator);
      return builder;
   }

   public static <V> BTree.Builder<V> builder(Comparator<? super V> comparator, int initialCapacity) {
      return builder(comparator);
   }

   static <V> int compare(Comparator<V> cmp, Object a, Object b) {
      return a == b?0:(a == NEGATIVE_INFINITY | b == POSITIVE_INFINITY?-1:(b == NEGATIVE_INFINITY | a == POSITIVE_INFINITY?1:cmp.compare((V)a, (V)b)));
   }

   public static boolean isWellFormed(Object[] btree, Comparator<? extends Object> cmp) {
      return isWellFormed(cmp, btree, true, NEGATIVE_INFINITY, POSITIVE_INFINITY);
   }

   private static boolean isWellFormed(Comparator<?> cmp, Object[] node, boolean isRoot, Object min, Object max) {
      if(cmp != null && !isNodeWellFormed(cmp, node, min, max)) {
         return false;
      } else if(isLeaf(node)) {
         return isRoot?node.length <= FAN_FACTOR + 1:node.length >= FAN_FACTOR / 2 && node.length <= FAN_FACTOR + 1;
      } else {
         int keyCount = getBranchKeyEnd(node);
         if((isRoot || keyCount >= FAN_FACTOR / 2) && keyCount <= FAN_FACTOR + 1) {
            int type = 0;
            int size = -1;
            int[] sizeMap = getSizeMap(node);

            for(int i = getChildStart(node); i < getChildEnd(node); ++i) {
               Object[] child = (Object[])((Object[])node[i]);
               size += size(child) + 1;
               if(sizeMap[i - getChildStart(node)] != size) {
                  return false;
               }

               Object localmax = i < node.length - 2?node[i - getChildStart(node)]:max;
               if(!isWellFormed(cmp, child, false, min, localmax)) {
                  return false;
               }

               type |= isLeaf(child)?1:2;
               min = localmax;
            }

            return type < 3;
         } else {
            return false;
         }
      }
   }

   private static boolean isNodeWellFormed(Comparator<?> cmp, Object[] node, Object min, Object max) {
      Object previous = min;
      int end = getKeyEnd(node);

      for(int i = 0; i < end; ++i) {
         Object current = node[i];
         if(compare(cmp, previous, current) >= 0) {
            return false;
         }

         previous = current;
      }

      return compare(cmp, previous, max) < 0;
   }

   public static <V> void apply(Object[] btree, Consumer<V> function, boolean reversed) {
      if(reversed) {
         applyReverse(btree, function, (Predicate)null);
      } else {
         applyForwards(btree, function, (Predicate)null);
      }

   }

   public static <V> boolean apply(Object[] btree, Consumer<V> function, Predicate<V> stopCondition, boolean reversed) {
      return reversed?applyReverse(btree, function, stopCondition):applyForwards(btree, function, stopCondition);
   }

   private static <V> boolean applyForwards(Object[] btree, Consumer<V> function, Predicate<V> stopCondition) {
      boolean isLeaf = isLeaf(btree);
      int childOffset = isLeaf?2147483647:getChildStart(btree);
      int limit = isLeaf?getLeafKeyEnd(btree):btree.length - 1;

      for(int i = 0; i < limit; ++i) {
         int idx = isLeaf?i:i / 2 + (i % 2 == 0?childOffset:0);
         Object current = btree[idx];
         if(idx < childOffset) {
            if(stopCondition != null && stopCondition.apply((V)current)) {
               return true;
            }

            function.accept((V)current);
         } else if(applyForwards((Object[])((Object[])current), function, stopCondition)) {
            return true;
         }
      }

      return false;
   }

   private static <V> boolean applyReverse(Object[] btree, Consumer<V> function, Predicate<V> stopCondition) {
      boolean isLeaf = isLeaf(btree);
      int childOffset = isLeaf?0:getChildStart(btree);
      int limit = isLeaf?getLeafKeyEnd(btree):btree.length - 1;
      int i = limit - 1;

      for(int visited = 0; i >= 0; ++visited) {
         int idx = i;
         if(!isLeaf) {
            int typeOffset = visited / 2;
            if(i % 2 == 0) {
               idx = i + typeOffset;
            } else {
               idx = i - childOffset + typeOffset;
            }
         }

         Object current = btree[idx];
         if(!isLeaf && idx >= childOffset) {
            if(applyReverse((Object[])((Object[])current), function, stopCondition)) {
               return true;
            }
         } else {
            if(stopCondition != null && stopCondition.apply((V)current)) {
               return true;
            }

            function.accept((V)current);
         }

         --i;
      }

      return false;
   }

   public static <R, V> R reduce(Object[] btree, R seed, BTree.ReduceFunction<R, V> function) {
      boolean isLeaf = isLeaf(btree);
      int childOffset = isLeaf?2147483647:getChildStart(btree);
      int limit = isLeaf?getLeafKeyEnd(btree):btree.length - 1;

      for(int i = 0; i < limit; ++i) {
         int idx = isLeaf?i:i / 2 + (i % 2 == 0?childOffset:0);
         Object current = btree[idx];
         if(idx < childOffset) {
            seed = function.apply(seed, (V)current);
         } else {
            seed = reduce((Object[])((Object[])current), seed, function);
         }

         if(function.stop(seed)) {
            break;
         }
      }

      return seed;
   }

   static {
      int fanfactor = 32;
      if(System.getProperty("cassandra.btree.fanfactor") != null) {
         fanfactor = Integer.parseInt(System.getProperty("cassandra.btree.fanfactor"));
      }

      int shift;
      for(shift = 1; 1 << shift < fanfactor; ++shift) {
         ;
      }

      FAN_SHIFT = shift;
      FAN_FACTOR = 1 << FAN_SHIFT;
      MINIMAL_NODE_SIZE = FAN_FACTOR >> 1;
      EMPTY_LEAF = new Object[1];
      EMPTY_BRANCH = new Object[]{null, new int[0]};
      builderRecycler = new Recycler<BTree.Builder>() {
         protected BTree.Builder newObject(Handle handle) {
            return new BTree.Builder(handle);
         }
      };
      POSITIVE_INFINITY = new Object();
      NEGATIVE_INFINITY = new Object();
   }

   public interface ReduceFunction<ACC, I> extends BiFunction<ACC, I, ACC> {
      default boolean stop(ACC res) {
         return false;
      }
   }

   public static class Builder<V> {
      Comparator<? super V> comparator;
      Object[] values;
      int count;
      boolean detected;
      boolean auto;
      BTree.Builder.QuickResolver<V> quickResolver;
      final Handle recycleHandle;

      private Builder(Handle handle) {
         this.detected = true;
         this.auto = true;
         this.recycleHandle = handle;
         this.values = new Object[16];
      }

      private Builder(BTree.Builder<V> builder) {
         this.detected = true;
         this.auto = true;
         this.comparator = builder.comparator;
         this.values = Arrays.copyOf(builder.values, builder.values.length);
         this.count = builder.count;
         this.detected = builder.detected;
         this.auto = builder.auto;
         this.quickResolver = builder.quickResolver;
         this.recycleHandle = null;
      }

      public BTree.Builder<V> copy() {
         return new BTree.Builder(this);
      }

      public BTree.Builder<V> setQuickResolver(BTree.Builder.QuickResolver<V> quickResolver) {
         this.quickResolver = quickResolver;
         return this;
      }

      public void recycle() {
         if(this.recycleHandle != null) {
            this.cleanup();
            BTree.builderRecycler.recycle(this, this.recycleHandle);
         }

      }

      private void cleanup() {
         this.quickResolver = null;
         Arrays.fill(this.values, (Object)null);
         this.count = 0;
         this.detected = true;
         this.auto = true;
      }

      private void reuse(Comparator<? super V> comparator) {
         this.comparator = comparator;
      }

      public BTree.Builder<V> auto(boolean auto) {
         this.auto = auto;
         return this;
      }

      public BTree.Builder<V> add(V v) {
         if(this.count == this.values.length) {
            this.values = Arrays.copyOf(this.values, this.count * 2);
         }

         Object[] values = this.values;
         int prevCount = this.count++;
         values[prevCount] = v;
         if(this.auto && this.detected && prevCount > 0) {
            V prev = (V)values[prevCount - 1];
            int c = this.comparator.compare(prev, v);
            if(c == 0 && this.auto) {
               this.count = prevCount;
               if(this.quickResolver != null) {
                  values[prevCount - 1] = this.quickResolver.resolve(prev, v);
               }
            } else if(c > 0) {
               this.detected = false;
            }
         }

         return this;
      }

      public BTree.Builder<V> addAll(Collection<V> add) {
         if(this.auto && add instanceof SortedSet && equalComparators(this.comparator, ((SortedSet)add).comparator())) {
            return this.mergeAll(add, add.size());
         } else {
            this.detected = false;
            if(this.values.length < this.count + add.size()) {
               this.values = Arrays.copyOf(this.values, Math.max(this.count + add.size(), this.count * 2));
            }

            Object v;
            for(Iterator var2 = add.iterator(); var2.hasNext(); this.values[this.count++] = v) {
               v = var2.next();
            }

            return this;
         }
      }

      private static boolean equalComparators(Comparator<?> a, Comparator<?> b) {
         return a == b || isNaturalComparator(a) && isNaturalComparator(b);
      }

      private static boolean isNaturalComparator(Comparator<?> a) {
         return a == null || a == Comparator.naturalOrder() || a == Ordering.natural();
      }

      private BTree.Builder<V> mergeAll(Iterable<V> add, int addCount) {
         assert this.auto;

         this.autoEnforce();
         int curCount = this.count;
         if(this.values.length < curCount * 2 + addCount) {
            this.values = Arrays.copyOf(this.values, Math.max(curCount * 2 + addCount, curCount * 3));
         }

         if(add instanceof BTreeSet) {
            ((BTreeSet)add).toArray(this.values, curCount);
         } else {
            int i = curCount;

            Object v;
            for(Iterator var5 = add.iterator(); var5.hasNext(); this.values[i++] = v) {
               v = var5.next();
            }
         }

         return this.mergeAll(addCount);
      }

      private Builder<V> mergeAll(int addCount) {
         int i;
         Object[] a = this.values;
         int addOffset = this.count;
         int j = addOffset;
         int curEnd = addOffset;
         int addEnd = addOffset + addCount;
         for (i = 0; i < curEnd && j < addEnd; ++i) {
            int c;
            V ai = (V)a[i];
            V aj = (V)a[j];
            int n = c = ai == aj ? 0 : this.comparator.compare(ai, aj);
            if (c > 0) break;
            if (c != 0) continue;
            if (this.quickResolver != null) {
               a[i] = this.quickResolver.resolve(ai, aj);
            }
            ++j;
         }
         if (j == addEnd) {
            return this;
         }
         int newCount = i;
         System.arraycopy(a, i, a, addEnd, this.count - i);
         curEnd = addEnd + (this.count - i);
         i = addEnd;
         while (i < curEnd && j < addEnd) {
            V ai = (V)a[i];
            V aj = (V)a[j];
            int c = this.comparator.compare(ai, aj);
            if (c == 0) {
               Object newValue = this.quickResolver == null ? ai : this.quickResolver.resolve(ai, aj);
               a[newCount++] = newValue;
               ++i;
               ++j;
               continue;
            }
            a[newCount++] = c < 0 ? a[i++] : a[j++];
         }
         if (i < curEnd) {
            System.arraycopy(a, i, a, newCount, curEnd - i);
            newCount += curEnd - i;
         } else if (j < addEnd) {
            if (j != newCount) {
               System.arraycopy(a, j, a, newCount, addEnd - j);
            }
            newCount += addEnd - j;
         }
         this.count = newCount;
         return this;
      }

      public boolean isEmpty() {
         return this.count == 0;
      }

      public BTree.Builder<V> reverse() {
         assert !this.auto;

         int mid = this.count / 2;

         for(int i = 0; i < mid; ++i) {
            Object t = this.values[i];
            this.values[i] = this.values[this.count - (1 + i)];
            this.values[this.count - (1 + i)] = t;
         }

         return this;
      }

      public BTree.Builder<V> sort() {
         Arrays.sort((V[])this.values, 0, this.count, this.comparator);
         return this;
      }

      private void autoEnforce() {
         if (!this.detected && this.count > 1) {
            this.sort();
            int prevIdx = 0;
            V prev = (V)this.values[0];
            for (int i = 1; i < this.count; ++i) {
               final V next = (V)this.values[i];
               if (this.comparator.compare((V)prev, (V)next) != 0) {
                  prev = (V)(this.values[++prevIdx] = next);
               }
               else if (this.quickResolver != null) {
                  prev = (V)(this.values[prevIdx] = this.quickResolver.resolve(prev, next));
               }
            }
            this.count = prevIdx + 1;
         }
         this.detected = true;
      }

      public Builder<V> resolve(final Resolver resolver) {
         if (this.count > 0) {
            int c = 0;
            int prev = 0;
            for (int i = 1; i < this.count; ++i) {
               if (this.comparator.compare((V)this.values[i], (V)this.values[prev]) != 0) {
                  this.values[c++] = resolver.resolve(this.values, prev, i);
                  prev = i;
               }
            }
            this.values[c++] = resolver.resolve(this.values, prev, this.count);
            this.count = c;
         }
         return this;
      }

      public Object[] build() {
         Object[] var1;
         try {
            if(this.auto) {
               this.autoEnforce();
            }

            var1 = BTree.build((Collection)Arrays.asList(this.values).subList(0, this.count), UpdateFunction.noOp());
         } finally {
            this.recycle();
         }

         return var1;
      }

      public interface QuickResolver<V> {
         V resolve(V var1, V var2);
      }

      public interface Resolver {
         Object resolve(Object[] var1, int var2, int var3);
      }
   }

   private static class FiltrationTracker<V> implements Function<V, V> {
      final Function<? super V, ? extends V> wrapped;
      int index;
      boolean failed;

      private FiltrationTracker(Function<? super V, ? extends V> wrapped) {
         this.wrapped = wrapped;
      }

      public V apply(V i) {
         V o = this.wrapped.apply(i);
         if(o != null) {
            ++this.index;
         } else {
            this.failed = true;
         }

         return o;
      }
   }

   public static enum Dir {
      ASC,
      DESC;

      private Dir() {
      }

      public BTree.Dir invert() {
         return this == ASC?DESC:ASC;
      }

      public static BTree.Dir asc(boolean asc) {
         return asc?ASC:DESC;
      }

      public static BTree.Dir desc(boolean desc) {
         return desc?DESC:ASC;
      }
   }
}
