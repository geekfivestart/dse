package org.apache.cassandra.locator;

import com.google.common.collect.Iterators;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PendingRangeMaps implements Iterable<Entry<Range<Token>, List<InetAddress>>> {
   private static final Logger logger = LoggerFactory.getLogger(PendingRangeMaps.class);
   final NavigableMap<Range<Token>, List<InetAddress>> ascendingMap;
   static final Comparator<Range<Token>> ascendingComparator = new Comparator<Range<Token>>() {
      public int compare(Range<Token> o1, Range<Token> o2) {
         int res = ((Token)o1.right).compareTo(o2.right);
         return res != 0?res:((Token)o2.left).compareTo(o1.left);
      }
   };
   final NavigableMap<Range<Token>, List<InetAddress>> descendingMap;
   static final Comparator<Range<Token>> descendingComparator = new Comparator<Range<Token>>() {
      public int compare(Range<Token> o1, Range<Token> o2) {
         int res = ((Token)o2.left).compareTo(o1.left);
         return res != 0?res:((Token)o2.right).compareTo(o1.right);
      }
   };
   final NavigableMap<Range<Token>, List<InetAddress>> ascendingMapForWrapAround;
   static final Comparator<Range<Token>> ascendingComparatorForWrapAround = new Comparator<Range<Token>>() {
      public int compare(Range<Token> o1, Range<Token> o2) {
         int res = ((Token)o1.right).compareTo(o2.right);
         return res != 0?res:((Token)o1.left).compareTo(o2.left);
      }
   };
   final NavigableMap<Range<Token>, List<InetAddress>> descendingMapForWrapAround;
   static final Comparator<Range<Token>> descendingComparatorForWrapAround = new Comparator<Range<Token>>() {
      public int compare(Range<Token> o1, Range<Token> o2) {
         int res = ((Token)o2.left).compareTo(o1.left);
         return res != 0?res:((Token)o1.right).compareTo(o2.right);
      }
   };

   public PendingRangeMaps() {
      this.ascendingMap = new TreeMap(ascendingComparator);
      this.descendingMap = new TreeMap(descendingComparator);
      this.ascendingMapForWrapAround = new TreeMap(ascendingComparatorForWrapAround);
      this.descendingMapForWrapAround = new TreeMap(descendingComparatorForWrapAround);
   }

   static final void addToMap(Range<Token> range, InetAddress address, NavigableMap<Range<Token>, List<InetAddress>> ascendingMap, NavigableMap<Range<Token>, List<InetAddress>> descendingMap) {
      List<InetAddress> addresses = (List)ascendingMap.get(range);
      if(addresses == null) {
         addresses = new ArrayList(1);
         ascendingMap.put(range, addresses);
         descendingMap.put(range, addresses);
      }

      ((List)addresses).add(address);
   }

   public void addPendingRange(Range<Token> range, InetAddress address) {
      if(range.isWrapAround()) {
         addToMap(range, address, this.ascendingMapForWrapAround, this.descendingMapForWrapAround);
      } else {
         addToMap(range, address, this.ascendingMap, this.descendingMap);
      }

   }

   static final void addIntersections(Set<InetAddress> endpointsToAdd, NavigableMap<Range<Token>, List<InetAddress>> smallerMap, NavigableMap<Range<Token>, List<InetAddress>> biggerMap) {
      Iterator var3 = smallerMap.keySet().iterator();

      while(var3.hasNext()) {
         Range<Token> range = (Range)var3.next();
         List<InetAddress> addresses = (List)biggerMap.get(range);
         if(addresses != null) {
            endpointsToAdd.addAll(addresses);
         }
      }

   }

   public Collection<InetAddress> pendingEndpointsFor(Token token) {
      Set<InetAddress> endpoints = new HashSet();
      Range searchRange = new Range(token, token);
      NavigableMap<Range<Token>, List<InetAddress>> ascendingTailMap = this.ascendingMap.tailMap(searchRange, true);
      NavigableMap<Range<Token>, List<InetAddress>> descendingTailMap = this.descendingMap.tailMap(searchRange, false);
      if(ascendingTailMap.size() < descendingTailMap.size()) {
         addIntersections(endpoints, ascendingTailMap, descendingTailMap);
      } else {
         addIntersections(endpoints, descendingTailMap, ascendingTailMap);
      }

      ascendingTailMap = this.ascendingMapForWrapAround.tailMap(searchRange, true);
      descendingTailMap = this.descendingMapForWrapAround.tailMap(searchRange, false);
      Iterator var6 = ascendingTailMap.entrySet().iterator();

      Entry entry;
      while(var6.hasNext()) {
         entry = (Entry)var6.next();
         endpoints.addAll((Collection)entry.getValue());
      }

      var6 = descendingTailMap.entrySet().iterator();

      while(var6.hasNext()) {
         entry = (Entry)var6.next();
         endpoints.addAll((Collection)entry.getValue());
      }

      return endpoints;
   }

   public String printPendingRanges() {
      StringBuilder sb = new StringBuilder();
      Iterator var2 = this.iterator();

      while(var2.hasNext()) {
         Entry<Range<Token>, List<InetAddress>> entry = (Entry)var2.next();
         Range<Token> range = (Range)entry.getKey();
         Iterator var5 = ((List)entry.getValue()).iterator();

         while(var5.hasNext()) {
            InetAddress address = (InetAddress)var5.next();
            sb.append(address).append(':').append(range);
            sb.append(System.getProperty("line.separator"));
         }
      }

      return sb.toString();
   }

   public Iterator<Entry<Range<Token>, List<InetAddress>>> iterator() {
      return Iterators.concat(this.ascendingMap.entrySet().iterator(), this.ascendingMapForWrapAround.entrySet().iterator());
   }
}
