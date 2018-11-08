package com.datastax.bdp.dht;

import com.datastax.bdp.dht.endpoint.SeededComparator;
import com.datastax.bdp.util.IntSetArray;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

public class SetCoverFinder<T, S> {
   private final Function<T, Collection<S>> itemElementsFn;
   private final Collection<T> items;
   private final ArrayList<S> universe;
   private final Map<S, Integer> universeMap;
   private final ArrayList<int[]> itemElements;
   private final IntSetArray elementIdToItemId;

   public SetCoverFinder(Set<S> universe, Collection<T> items, Function<T, Collection<S>> itemElementsFn) {
      this.universe = new ArrayList(universe);
      this.universeMap = this.createUniverseMap(this.universe);
      this.items = items;
      this.itemElementsFn = itemElementsFn;
      this.itemElements = this.indexesOfAll(items);
      this.elementIdToItemId = this.indexItemsByElements(this.itemElements);
   }

   private Map<S, Integer> createUniverseMap(ArrayList<S> universe) {
      Map<S, Integer> universeMap = Maps.newHashMapWithExpectedSize(universe.size() * 2);

      for(int i = 0; i < universe.size(); ++i) {
         universeMap.put(universe.get(i), Integer.valueOf(i));
      }

      return universeMap;
   }

   private ArrayList<int[]> indexesOfAll(Collection<T> sourceItems) {
      ArrayList<int[]> result = Lists.newArrayListWithExpectedSize(sourceItems.size());
      Iterator var3 = sourceItems.iterator();

      while(var3.hasNext()) {
         T item = var3.next();
         result.add(this.indexesOf((Collection)this.itemElementsFn.apply(item)));
      }

      return result;
   }

   private int[] indexesOf(Collection<S> elements) {
      ArrayList<Integer> indexes = new ArrayList(elements.size());
      Iterator var3 = elements.iterator();

      while(var3.hasNext()) {
         S element = var3.next();
         Integer elementIndex = (Integer)this.universeMap.get(element);
         if(elementIndex != null) {
            indexes.add(elementIndex);
         }
      }

      return toArray(indexes);
   }

   private static int[] toArray(List<Integer> integers) {
      int[] ret = new int[integers.size()];
      Iterator<Integer> iterator = integers.iterator();

      for(int i = 0; i < ret.length; ++i) {
         ret[i] = ((Integer)iterator.next()).intValue();
      }

      return ret;
   }

   private IntSetArray indexItemsByElements(ArrayList<int[]> elementSets) {
      int replicationFactor = this.replicationFactor(elementSets);
      IntSetArray itemsToElements = new IntSetArray(this.universe.size(), replicationFactor, elementSets.size());

      for(int itemId = 0; itemId < elementSets.size(); ++itemId) {
         int[] var5 = (int[])elementSets.get(itemId);
         int var6 = var5.length;

         for(int var7 = 0; var7 < var6; ++var7) {
            int elementId = var5[var7];
            itemsToElements.add(elementId, itemId);
         }
      }

      return itemsToElements;
   }

   private int replicationFactor(ArrayList<int[]> elementSets) {
      short[] counters = new short[this.universe.size()];
      Iterator var3 = elementSets.iterator();

      while(var3.hasNext()) {
         int[] elementSet = (int[])var3.next();
         int[] var5 = elementSet;
         int var6 = elementSet.length;

         for(int var7 = 0; var7 < var6; ++var7) {
            int elementId = var5[var7];
            ++counters[elementId];
         }
      }

      return this.maxArrayElement(counters);
   }

   private short maxArrayElement(short[] array) {
      short max = -32768;
      short[] var3 = array;
      int var4 = array.length;

      for(int var5 = 0; var5 < var4; ++var5) {
         short element = var3[var5];
         if(max < element) {
            max = element;
         }
      }

      return max;
   }

   public SetCoverResult<T, S> findSetCover(SetCoverFinder.SetCoverOptimizationStrategy<T> strategy, RefiningFilter<S> elementFilter) {
      return (new SetCoverFinder.SetCoverProblem(strategy)).solve(elementFilter);
   }

   private static class StaticOptimizationStrategy<T> implements SetCoverFinder.SetCoverOptimizationStrategy<T> {
      private final Comparator<SetCoverFinder.Item<T>> comparator;

      public StaticOptimizationStrategy(Comparator<T> comparator) {
         this.comparator = (item1, item2) -> {
            if(item1 == item2) {
               return 0;
            } else {
               int priorityDelta = comparator.compare(item1.item, item2.item);
               return priorityDelta != 0?priorityDelta:item1.itemId - item2.itemId;
            }
         };
      }

      public SetCoverFinder.Item<T> createDecoratedItem(int itemId, T item, int[] elements) {
         return new SetCoverFinder.Item(itemId, item, elements);
      }

      public SetCoverFinder.ItemQueue<T> createItemQueue(Collection<SetCoverFinder.Item<T>> items) {
         return new SetCoverFinder.StaticItemQueue(items, this.comparator);
      }
   }

   private static class DynamicOptimizationStrategy<T> implements SetCoverFinder.SetCoverOptimizationStrategy<T> {
      private final Random random;
      private final Comparator<SetCoverFinder.Item<T>> comparator;

      public DynamicOptimizationStrategy(Comparator<T> comparator, int randomSeed) {
         this.random = new Random((long)randomSeed);
         this.comparator = (item1, item2) -> {
            if(item1 == item2) {
               return 0;
            } else {
               int priorityDelta = comparator.compare(item1.item, item2.item);
               return priorityDelta != 0?priorityDelta:(item2.size() < item1.size()?-1:(item2.size() > item1.size()?1:(item1.randomValue < item2.randomValue?-1:(item1.randomValue > item2.randomValue?1:item1.itemId - item2.itemId))));
            }
         };
      }

      public SetCoverFinder.Item<T> createDecoratedItem(int itemId, T item, int[] elements) {
         return new SetCoverFinder.Item(itemId, item, elements, this.random.nextInt());
      }

      public SetCoverFinder.ItemQueue<T> createItemQueue(Collection<SetCoverFinder.Item<T>> items) {
         return new SetCoverFinder.DynamicItemQueue(items, this.comparator);
      }
   }

   interface SetCoverOptimizationStrategy<T> {
      SetCoverFinder.Item<T> createDecoratedItem(int var1, T var2, int[] var3);

      SetCoverFinder.ItemQueue<T> createItemQueue(Collection<SetCoverFinder.Item<T>> var1);
   }

   private static class StaticItemQueue<T> implements SetCoverFinder.ItemQueue<T> {
      private final List<SetCoverFinder.Item<T>> items;

      private StaticItemQueue(Collection<SetCoverFinder.Item<T>> items, Comparator<SetCoverFinder.Item<T>> comparator) {
         this.items = new ArrayList(items.size());
         Iterator var3 = items.iterator();

         while(var3.hasNext()) {
            SetCoverFinder.Item<T> item = (SetCoverFinder.Item)var3.next();
            if(item.size() > 0) {
               this.items.add(item);
            }
         }

         this.items.sort(comparator);
      }

      public SetCoverFinder.Item<T> pop() {
         int maxSize = 0;
         SetCoverFinder.Item<T> biggestItem = null;
         Iterator var3 = this.items.iterator();

         while(var3.hasNext()) {
            SetCoverFinder.Item<T> item = (SetCoverFinder.Item)var3.next();
            if(item.size() > maxSize) {
               biggestItem = item;
               maxSize = item.size();
            }
         }

         this.items.remove(biggestItem);
         return biggestItem;
      }

      public boolean isEmpty() {
         return this.items.isEmpty();
      }

      public void itemChanging(SetCoverFinder.Item<T> item) {
      }

      public void updateOrder() {
      }

      public boolean isStatic() {
         return false;
      }
   }

   private static class DynamicItemQueue<T> implements SetCoverFinder.ItemQueue<T> {
      private final TreeSet<SetCoverFinder.Item<T>> itemTree;
      private final BitSet changedItemIds;
      private final List<SetCoverFinder.Item<T>> changedItems;

      private DynamicItemQueue(Collection<SetCoverFinder.Item<T>> items, Comparator<SetCoverFinder.Item<T>> comparator) {
         this.changedItemIds = new BitSet();
         this.changedItems = new ArrayList();
         this.itemTree = new TreeSet(comparator);
         Iterator var3 = items.iterator();

         while(var3.hasNext()) {
            SetCoverFinder.Item<T> item = (SetCoverFinder.Item)var3.next();
            if(item.size() > 0) {
               this.itemTree.add(item);
            }
         }

      }

      public SetCoverFinder.Item<T> pop() {
         SetCoverFinder.Item<T> result = (SetCoverFinder.Item)this.itemTree.first();
         this.itemTree.remove(result);
         return result;
      }

      public boolean isEmpty() {
         return this.itemTree.isEmpty();
      }

      public void itemChanging(SetCoverFinder.Item<T> item) {
         if(!this.changedItemIds.get(item.itemId)) {
            this.itemTree.remove(item);
            this.changedItems.add(item);
            this.changedItemIds.set(item.itemId);
         }

      }

      public void updateOrder() {
         Iterator var1 = this.changedItems.iterator();

         while(var1.hasNext()) {
            SetCoverFinder.Item<T> item = (SetCoverFinder.Item)var1.next();
            if(item.size() > 0) {
               this.itemTree.add(item);
            }
         }

         this.changedItems.clear();
         this.changedItemIds.clear();
      }

      public boolean isStatic() {
         return false;
      }
   }

   private interface ItemQueue<T> {
      SetCoverFinder.Item<T> pop();

      boolean isEmpty();

      void itemChanging(SetCoverFinder.Item<T> var1);

      void updateOrder();

      boolean isStatic();
   }

   private static final class Item<T> {
      final T item;
      final int randomValue;
      final int itemId;
      private int removedElementCount;
      private final int[] elements;

      private Item(int itemId, T item, int[] elements) {
         this(itemId, item, elements, 0);
      }

      private Item(int itemId, T item, int[] elements, int randomValue) {
         this.removedElementCount = 0;
         this.itemId = itemId;
         this.item = item;
         this.elements = elements;
         this.randomValue = randomValue;
      }

      private void elementRemoved() {
         ++this.removedElementCount;
      }

      public int size() {
         return this.elements.length - this.removedElementCount;
      }

      public String toString() {
         return "Item { " + this.item.toString() + " " + Arrays.toString(this.elements) + " - " + this.removedElementCount + " element(s) }";
      }
   }

   private class SetCoverProblem {
      private final ArrayList<SetCoverFinder.Item<T>> itemById;
      private final SetCoverFinder.ItemQueue<T> itemQueue;
      private final int[] itemIdsTempBuffer;
      private final BitSet removedElements;
      private final BitSet removedItems;

      private SetCoverProblem(SetCoverFinder.SetCoverOptimizationStrategy<T> var1) {
         this.removedElements = new BitSet(SetCoverFinder.this.universe.size());
         this.removedItems = new BitSet();
         this.itemById = this.decorateItems(SetCoverFinder.this.items, strategy);
         this.itemQueue = strategy.createItemQueue(this.itemById);
         this.itemIdsTempBuffer = new int[SetCoverFinder.this.items.size()];
      }

      private ArrayList<SetCoverFinder.Item<T>> decorateItems(Collection<T> items, SetCoverFinder.SetCoverOptimizationStrategy<T> strategy) {
         ArrayList<SetCoverFinder.Item<T>> result = Lists.newArrayListWithExpectedSize(items.size());
         int id = 0;

         for(Iterator var5 = items.iterator(); var5.hasNext(); ++id) {
            T item = var5.next();
            result.add(strategy.createDecoratedItem(id, item, (int[])SetCoverFinder.this.itemElements.get(id)));
         }

         return result;
      }

      private SetCoverResult<T, S> solve(RefiningFilter<S> elementFilter) {
         LinkedHashMap<T, List<S>> result = Maps.newLinkedHashMap();
         int uncoveredCount = SetCoverFinder.this.universe.size();

         while(!this.itemQueue.isEmpty()) {
            SetCoverFinder.Item<T> resultItem = this.itemQueue.pop();
            if(resultItem == null) {
               break;
            }

            this.removedItems.set(resultItem.itemId);
            List<S> coveringElements = this.removeElementsOf(resultItem);
            uncoveredCount -= coveringElements.size();
            this.itemQueue.updateOrder();
            if(!coveringElements.isEmpty()) {
               if(elementFilter != null) {
                  List<S> filteredElements = Lists.newArrayListWithExpectedSize(coveringElements.size());
                  Iterator var7 = coveringElements.iterator();

                  while(var7.hasNext()) {
                     S candidate = var7.next();
                     if(elementFilter.apply(candidate)) {
                        filteredElements.add(elementFilter.refine(candidate));
                     }
                  }

                  if(!filteredElements.isEmpty()) {
                     result.put(resultItem.item, filteredElements);
                  }
               } else {
                  result.put(resultItem.item, coveringElements);
               }
            }

            if(uncoveredCount == 0) {
               break;
            }
         }

         if(elementFilter != null) {
            if(result.isEmpty()) {
               return new SetCoverResult(result);
            }
         } else {
            if(uncoveredCount > 0) {
               return new SetCoverResult(result, this.notCoveredElements(result));
            }

            assert uncoveredCount == 0 : "Bug detected: Some input elements have been covered by more than one result subset.";
         }

         return new SetCoverResult(result);
      }

      private Set<S> notCoveredElements(LinkedHashMap<T, List<S>> result) {
         Set<S> remainingElements = Sets.newHashSet(SetCoverFinder.this.universe);
         Iterator var3 = result.values().iterator();

         while(var3.hasNext()) {
            List<S> elements = (List)var3.next();
            remainingElements.removeAll(elements);
         }

         return remainingElements;
      }

      private List<S> removeElementsOf(SetCoverFinder.Item<T> item) {
         List<S> result = Lists.newArrayListWithCapacity(item.size());
         int[] var3 = item.elements;
         int var4 = var3.length;

         for(int var5 = 0; var5 < var4; ++var5) {
            int element = var3[var5];
            if(!this.removedElements.get(element)) {
               this.removedElements.set(element);
               result.add(SetCoverFinder.this.universe.get(element));
               if(!this.itemQueue.isStatic()) {
                  this.removeElementFromAllItems(element);
               }
            }
         }

         return result;
      }

      private void removeElementFromAllItems(int elementId) {
         int count = SetCoverFinder.this.elementIdToItemId.get(elementId, this.itemIdsTempBuffer);

         for(int i = 0; i < count; ++i) {
            if(!this.removedItems.get(this.itemIdsTempBuffer[i])) {
               this.removeElementFromItem(this.itemIdsTempBuffer[i]);
            }
         }

      }

      private void removeElementFromItem(int itemId) {
         SetCoverFinder.Item<T> item = (SetCoverFinder.Item)this.itemById.get(itemId);
         this.itemQueue.itemChanging(item);
         item.elementRemoved();
      }
   }

   public static enum Kind {
      STATIC(false),
      DYNAMIC(true);

      private final boolean random;

      private Kind(boolean random) {
         this.random = random;
      }

      public SetCoverFinder.SetCoverOptimizationStrategy strategy(SeededComparator endpointComparator) {
         return (SetCoverFinder.SetCoverOptimizationStrategy)(this.random?new SetCoverFinder.DynamicOptimizationStrategy(endpointComparator, endpointComparator.getSeed()):new SetCoverFinder.StaticOptimizationStrategy(endpointComparator));
      }
   }
}
