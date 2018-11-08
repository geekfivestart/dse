package org.apache.cassandra.utils;

import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;
import java.util.Comparator;
import java.util.SortedMap;
import java.util.TreeMap;

public class SortedBiMultiValMap<K, V> extends BiMultiValMap<K, V> {
   protected SortedBiMultiValMap(SortedMap<K, V> forwardMap, SortedSetMultimap<V, K> reverseMap) {
      super(forwardMap, reverseMap);
   }

   public static <K extends Comparable<K>, V extends Comparable<V>> SortedBiMultiValMap<K, V> create() {
      return new SortedBiMultiValMap(new TreeMap(), TreeMultimap.create());
   }

   public static <K, V> SortedBiMultiValMap<K, V> create(Comparator<K> keyComparator, Comparator<V> valueComparator) {
      if(keyComparator == null) {
         keyComparator = defaultComparator();
      }

      if(valueComparator == null) {
         valueComparator = defaultComparator();
      }

      return new SortedBiMultiValMap(new TreeMap(keyComparator), TreeMultimap.create(valueComparator, keyComparator));
   }

   public static <K extends Comparable<K>, V extends Comparable<V>> SortedBiMultiValMap<K, V> create(BiMultiValMap<K, V> map) {
      SortedBiMultiValMap<K, V> newMap = create();
      newMap.forwardMap.putAll(map);
      newMap.reverseMap.putAll(map.inverse());
      return newMap;
   }

   public static <K, V> SortedBiMultiValMap<K, V> create(BiMultiValMap<K, V> map, Comparator<K> keyComparator, Comparator<V> valueComparator) {
      SortedBiMultiValMap<K, V> newMap = create(keyComparator, valueComparator);
      newMap.forwardMap.putAll(map);
      newMap.reverseMap.putAll(map.inverse());
      return newMap;
   }

   private static <T> Comparator<T> defaultComparator() {
      return new Comparator<T>() {
         public int compare(T o1, T o2) {
            return ((Comparable)o1).compareTo(o2);
         }
      };
   }
}
