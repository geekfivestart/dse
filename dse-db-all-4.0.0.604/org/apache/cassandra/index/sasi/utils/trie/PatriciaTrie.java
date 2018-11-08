package org.apache.cassandra.index.sasi.utils.trie;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;
import java.util.Map.Entry;

public class PatriciaTrie<K, V> extends AbstractPatriciaTrie<K, V> implements Serializable {
   private static final long serialVersionUID = -2246014692353432660L;

   public PatriciaTrie(KeyAnalyzer<? super K> keyAnalyzer) {
      super(keyAnalyzer);
   }

   public PatriciaTrie(KeyAnalyzer<? super K> keyAnalyzer, Map<? extends K, ? extends V> m) {
      super(keyAnalyzer, m);
   }

   public Comparator<? super K> comparator() {
      return this.keyAnalyzer;
   }

   public SortedMap<K, V> prefixMap(K prefix) {
      return (SortedMap)(this.lengthInBits(prefix) == 0?this:new PatriciaTrie.PrefixRangeMap(prefix));
   }

   public K firstKey() {
      return this.firstEntry().getKey();
   }

   public K lastKey() {
      AbstractPatriciaTrie.TrieEntry<K, V> entry = this.lastEntry();
      return entry != null?entry.getKey():null;
   }

   public SortedMap<K, V> headMap(K toKey) {
      return new PatriciaTrie.RangeEntryMap((Object)null, toKey);
   }

   public SortedMap<K, V> subMap(K fromKey, K toKey) {
      return new PatriciaTrie.RangeEntryMap(fromKey, toKey);
   }

   public SortedMap<K, V> tailMap(K fromKey) {
      return new PatriciaTrie.RangeEntryMap(fromKey, (Object)null);
   }

   private AbstractPatriciaTrie.TrieEntry<K, V> higherEntry(K key) {
      int lengthInBits = this.lengthInBits(key);
      if(lengthInBits == 0) {
         return !this.root.isEmpty()?(this.size() > 1?this.nextEntry(this.root):null):this.firstEntry();
      } else {
         AbstractPatriciaTrie.TrieEntry<K, V> found = this.getNearestEntryForKey(key);
         if(this.compareKeys(key, found.key)) {
            return this.nextEntry(found);
         } else {
            int bitIndex = this.bitIndex(key, found.key);
            if(Tries.isValidBitIndex(bitIndex)) {
               return this.replaceCeil(key, bitIndex);
            } else if(Tries.isNullBitKey(bitIndex)) {
               return !this.root.isEmpty()?this.firstEntry():(this.size() > 1?this.nextEntry(this.firstEntry()):null);
            } else if(Tries.isEqualBitKey(bitIndex)) {
               return this.nextEntry(found);
            } else {
               throw new IllegalStateException("invalid lookup: " + key);
            }
         }
      }
   }

   AbstractPatriciaTrie.TrieEntry<K, V> ceilingEntry(K key) {
      int lengthInBits = this.lengthInBits(key);
      if(lengthInBits == 0) {
         return !this.root.isEmpty()?this.root:this.firstEntry();
      } else {
         AbstractPatriciaTrie.TrieEntry<K, V> found = this.getNearestEntryForKey(key);
         if(this.compareKeys(key, found.key)) {
            return found;
         } else {
            int bitIndex = this.bitIndex(key, found.key);
            if(Tries.isValidBitIndex(bitIndex)) {
               return this.replaceCeil(key, bitIndex);
            } else if(Tries.isNullBitKey(bitIndex)) {
               return !this.root.isEmpty()?this.root:this.firstEntry();
            } else if(Tries.isEqualBitKey(bitIndex)) {
               return found;
            } else {
               throw new IllegalStateException("invalid lookup: " + key);
            }
         }
      }
   }

   private AbstractPatriciaTrie.TrieEntry<K, V> replaceCeil(K key, int bitIndex) {
      AbstractPatriciaTrie.TrieEntry<K, V> added = new AbstractPatriciaTrie.TrieEntry(key, (Object)null, bitIndex);
      this.addEntry(added);
      this.incrementSize();
      AbstractPatriciaTrie.TrieEntry<K, V> ceil = this.nextEntry(added);
      this.removeEntry(added);
      this.modCount -= 2;
      return ceil;
   }

   private AbstractPatriciaTrie.TrieEntry<K, V> replaceLower(K key, int bitIndex) {
      AbstractPatriciaTrie.TrieEntry<K, V> added = new AbstractPatriciaTrie.TrieEntry(key, (Object)null, bitIndex);
      this.addEntry(added);
      this.incrementSize();
      AbstractPatriciaTrie.TrieEntry<K, V> prior = this.previousEntry(added);
      this.removeEntry(added);
      this.modCount -= 2;
      return prior;
   }

   AbstractPatriciaTrie.TrieEntry<K, V> lowerEntry(K key) {
      int lengthInBits = this.lengthInBits(key);
      if(lengthInBits == 0) {
         return null;
      } else {
         AbstractPatriciaTrie.TrieEntry<K, V> found = this.getNearestEntryForKey(key);
         if(this.compareKeys(key, found.key)) {
            return this.previousEntry(found);
         } else {
            int bitIndex = this.bitIndex(key, found.key);
            if(Tries.isValidBitIndex(bitIndex)) {
               return this.replaceLower(key, bitIndex);
            } else if(Tries.isNullBitKey(bitIndex)) {
               return null;
            } else if(Tries.isEqualBitKey(bitIndex)) {
               return this.previousEntry(found);
            } else {
               throw new IllegalStateException("invalid lookup: " + key);
            }
         }
      }
   }

   AbstractPatriciaTrie.TrieEntry<K, V> floorEntry(K key) {
      int lengthInBits = this.lengthInBits(key);
      if(lengthInBits == 0) {
         return !this.root.isEmpty()?this.root:null;
      } else {
         AbstractPatriciaTrie.TrieEntry<K, V> found = this.getNearestEntryForKey(key);
         if(this.compareKeys(key, found.key)) {
            return found;
         } else {
            int bitIndex = this.bitIndex(key, found.key);
            if(Tries.isValidBitIndex(bitIndex)) {
               return this.replaceLower(key, bitIndex);
            } else if(Tries.isNullBitKey(bitIndex)) {
               return !this.root.isEmpty()?this.root:null;
            } else if(Tries.isEqualBitKey(bitIndex)) {
               return found;
            } else {
               throw new IllegalStateException("invalid lookup: " + key);
            }
         }
      }
   }

   private AbstractPatriciaTrie.TrieEntry<K, V> subtree(K prefix) {
      int lengthInBits = this.lengthInBits(prefix);
      AbstractPatriciaTrie.TrieEntry<K, V> current = this.root.left;

      AbstractPatriciaTrie.TrieEntry path;
      for(path = this.root; current.bitIndex > path.bitIndex && lengthInBits >= current.bitIndex; current = !this.isBitSet(prefix, current.bitIndex)?current.left:current.right) {
         path = current;
      }

      AbstractPatriciaTrie.TrieEntry<K, V> entry = current.isEmpty()?path:current;
      if(entry.isEmpty()) {
         return null;
      } else if(entry == this.root && this.lengthInBits(entry.getKey()) < lengthInBits) {
         return null;
      } else if(this.isBitSet(prefix, lengthInBits) != this.isBitSet(entry.key, lengthInBits)) {
         return null;
      } else {
         int bitIndex = this.bitIndex(prefix, entry.key);
         return bitIndex >= 0 && bitIndex < lengthInBits?null:entry;
      }
   }

   private AbstractPatriciaTrie.TrieEntry<K, V> lastEntry() {
      return this.followRight(this.root.left);
   }

   private AbstractPatriciaTrie.TrieEntry<K, V> followRight(AbstractPatriciaTrie.TrieEntry<K, V> node) {
      if(node.right == null) {
         return null;
      } else {
         while(node.right.bitIndex > node.bitIndex) {
            node = node.right;
         }

         return node.right;
      }
   }

   private AbstractPatriciaTrie.TrieEntry<K, V> previousEntry(AbstractPatriciaTrie.TrieEntry<K, V> start) {
      if(start.predecessor == null) {
         throw new IllegalArgumentException("must have come from somewhere!");
      } else if(start.predecessor.right == start) {
         return isValidUplink(start.predecessor.left, start.predecessor)?start.predecessor.left:this.followRight(start.predecessor.left);
      } else {
         AbstractPatriciaTrie.TrieEntry node;
         for(node = start.predecessor; node.parent != null && node == node.parent.left; node = node.parent) {
            ;
         }

         return node.parent == null?null:(isValidUplink(node.parent.left, node.parent)?(node.parent.left == this.root?(this.root.isEmpty()?null:this.root):node.parent.left):this.followRight(node.parent.left));
      }
   }

   private AbstractPatriciaTrie.TrieEntry<K, V> nextEntryInSubtree(AbstractPatriciaTrie.TrieEntry<K, V> node, AbstractPatriciaTrie.TrieEntry<K, V> parentOfSubtree) {
      return node == null?this.firstEntry():this.nextEntryImpl(node.predecessor, node, parentOfSubtree);
   }

   private boolean isPrefix(K key, K prefix) {
      return this.keyAnalyzer.isPrefix(key, prefix);
   }

   private final class PrefixRangeEntrySet extends PatriciaTrie<K, V>.RangeEntrySet {
      private final PatriciaTrie<K, V>.PrefixRangeMap delegate;
      private AbstractPatriciaTrie.TrieEntry<K, V> prefixStart;
      private int expectedModCount = -1;

      public PrefixRangeEntrySet(PatriciaTrie<K, V>.PrefixRangeMap var1) {
         super(delegate);
         this.delegate = delegate;
      }

      public int size() {
         return this.delegate.fixup();
      }

      public Iterator<Entry<K, V>> iterator() {
         if(PatriciaTrie.this.modCount != this.expectedModCount) {
            this.prefixStart = PatriciaTrie.this.subtree(this.delegate.prefix);
            this.expectedModCount = PatriciaTrie.this.modCount;
         }

         if(this.prefixStart == null) {
            Set<Entry<K, V>> empty = Collections.emptySet();
            return empty.iterator();
         } else {
            return (Iterator)(PatriciaTrie.this.lengthInBits(this.delegate.prefix) >= this.prefixStart.bitIndex?new PatriciaTrie.PrefixRangeEntrySet.SingletonIterator(this.prefixStart):new PatriciaTrie.PrefixRangeEntrySet.EntryIterator(this.prefixStart, this.delegate.prefix));
         }
      }

      private final class EntryIterator extends AbstractPatriciaTrie<K, V>.TrieIterator<Entry<K, V>> {
         protected final K prefix;
         protected boolean lastOne;
         protected AbstractPatriciaTrie.TrieEntry<K, V> subtree;

         EntryIterator(AbstractPatriciaTrie.TrieEntry<K, V> var1, K startScan) {
            super();
            this.subtree = startScan;
            this.next = PatriciaTrie.this.followLeft(startScan);
            this.prefix = prefix;
         }

         public Entry<K, V> next() {
            Entry<K, V> entry = this.nextEntry();
            if(this.lastOne) {
               this.next = null;
            }

            return entry;
         }

         protected AbstractPatriciaTrie.TrieEntry<K, V> findNext(AbstractPatriciaTrie.TrieEntry<K, V> prior) {
            return PatriciaTrie.this.nextEntryInSubtree(prior, this.subtree);
         }

         public void remove() {
            boolean needsFixing = false;
            int bitIdx = this.subtree.bitIndex;
            if(this.current == this.subtree) {
               needsFixing = true;
            }

            super.remove();
            if(bitIdx != this.subtree.bitIndex || needsFixing) {
               this.subtree = PatriciaTrie.this.subtree(this.prefix);
            }

            if(PatriciaTrie.this.lengthInBits(this.prefix) >= this.subtree.bitIndex) {
               this.lastOne = true;
            }

         }
      }

      private final class SingletonIterator implements Iterator<Entry<K, V>> {
         private final AbstractPatriciaTrie.TrieEntry<K, V> entry;
         private int hit = 0;

         public SingletonIterator(AbstractPatriciaTrie.TrieEntry<K, V> var1) {
            this.entry = entry;
         }

         public boolean hasNext() {
            return this.hit == 0;
         }

         public Entry<K, V> next() {
            if(this.hit != 0) {
               throw new NoSuchElementException();
            } else {
               ++this.hit;
               return this.entry;
            }
         }

         public void remove() {
            if(this.hit != 1) {
               throw new IllegalStateException();
            } else {
               ++this.hit;
               PatriciaTrie.this.removeEntry(this.entry);
            }
         }
      }
   }

   private class PrefixRangeMap extends PatriciaTrie<K, V>.RangeMap {
      private final K prefix;
      private K fromKey;
      private K toKey;
      private int expectedModCount;
      private int size;

      private PrefixRangeMap(K var1) {
         super(null);
         this.fromKey = null;
         this.toKey = null;
         this.expectedModCount = -1;
         this.size = -1;
         this.prefix = prefix;
      }

      private int fixup() {
         if(this.size == -1 || PatriciaTrie.this.modCount != this.expectedModCount) {
            Iterator<Entry<K, V>> it = this.entrySet().iterator();
            this.size = 0;
            Entry<K, V> entryx = null;
            if(it.hasNext()) {
               entryx = (Entry)it.next();
               this.size = 1;
            }

            this.fromKey = entryx == null?null:entryx.getKey();
            if(this.fromKey != null) {
               AbstractPatriciaTrie.TrieEntry<K, V> prior = PatriciaTrie.this.previousEntry((AbstractPatriciaTrie.TrieEntry)entryx);
               this.fromKey = prior == null?null:prior.getKey();
            }

            for(this.toKey = this.fromKey; it.hasNext(); entryx = (Entry)it.next()) {
               ++this.size;
            }

            this.toKey = entryx == null?null:entryx.getKey();
            if(this.toKey != null) {
               Entry<K, V> entry = PatriciaTrie.this.nextEntry((AbstractPatriciaTrie.TrieEntry)entryx);
               this.toKey = entry == null?null:entry.getKey();
            }

            this.expectedModCount = PatriciaTrie.this.modCount;
         }

         return this.size;
      }

      public K firstKey() {
         this.fixup();
         Entry<K, V> e = this.fromKey == null?PatriciaTrie.this.firstEntry():PatriciaTrie.this.higherEntry(this.fromKey);
         K first = e != null?e.getKey():null;
         if(e != null && PatriciaTrie.this.isPrefix(first, this.prefix)) {
            return first;
         } else {
            throw new NoSuchElementException();
         }
      }

      public K lastKey() {
         this.fixup();
         Entry<K, V> e = this.toKey == null?PatriciaTrie.this.lastEntry():PatriciaTrie.this.lowerEntry(this.toKey);
         K last = e != null?e.getKey():null;
         if(e != null && PatriciaTrie.this.isPrefix(last, this.prefix)) {
            return last;
         } else {
            throw new NoSuchElementException();
         }
      }

      protected boolean inRange(K key) {
         return PatriciaTrie.this.isPrefix(key, this.prefix);
      }

      protected boolean inRange2(K key) {
         return this.inRange(key);
      }

      protected boolean inFromRange(K key, boolean forceInclusive) {
         return PatriciaTrie.this.isPrefix(key, this.prefix);
      }

      protected boolean inToRange(K key, boolean forceInclusive) {
         return PatriciaTrie.this.isPrefix(key, this.prefix);
      }

      protected Set<Entry<K, V>> createEntrySet() {
         return PatriciaTrie.this.new PrefixRangeEntrySet(this);
      }

      public K getFromKey() {
         return this.fromKey;
      }

      public K getToKey() {
         return this.toKey;
      }

      public boolean isFromInclusive() {
         return false;
      }

      public boolean isToInclusive() {
         return false;
      }

      protected SortedMap<K, V> createRangeMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
         return PatriciaTrie.this.new RangeEntryMap(fromKey, fromInclusive, toKey, toInclusive);
      }
   }

   private class RangeEntrySet extends AbstractSet<Entry<K, V>> {
      private final PatriciaTrie<K, V>.RangeMap delegate;
      private int size = -1;
      private int expectedModCount = -1;

      public RangeEntrySet(PatriciaTrie<K, V>.RangeMap var1) {
         if(delegate == null) {
            throw new NullPointerException("delegate");
         } else {
            this.delegate = delegate;
         }
      }

      public Iterator<Entry<K, V>> iterator() {
         K fromKey = this.delegate.getFromKey();
         K toKey = this.delegate.getToKey();
         AbstractPatriciaTrie.TrieEntry<K, V> first = fromKey == null?PatriciaTrie.this.firstEntry():PatriciaTrie.this.ceilingEntry(fromKey);
         AbstractPatriciaTrie.TrieEntry<K, V> last = null;
         if(toKey != null) {
            last = PatriciaTrie.this.ceilingEntry(toKey);
         }

         return new PatriciaTrie.RangeEntrySet.EntryIterator(first, last);
      }

      public int size() {
         if(this.size == -1 || this.expectedModCount != PatriciaTrie.this.modCount) {
            this.size = 0;
            Iterator it = this.iterator();

            while(it.hasNext()) {
               ++this.size;
               it.next();
            }

            this.expectedModCount = PatriciaTrie.this.modCount;
         }

         return this.size;
      }

      public boolean isEmpty() {
         return !this.iterator().hasNext();
      }

      public boolean contains(Object o) {
         if(!(o instanceof Entry)) {
            return false;
         } else {
            Entry<K, V> entry = (Entry)o;
            K key = entry.getKey();
            if(!this.delegate.inRange(key)) {
               return false;
            } else {
               AbstractPatriciaTrie.TrieEntry<K, V> node = PatriciaTrie.this.getEntry(key);
               return node != null && Tries.areEqual(node.getValue(), entry.getValue());
            }
         }
      }

      public boolean remove(Object o) {
         if(!(o instanceof Entry)) {
            return false;
         } else {
            Entry<K, V> entry = (Entry)o;
            K key = entry.getKey();
            if(!this.delegate.inRange(key)) {
               return false;
            } else {
               AbstractPatriciaTrie.TrieEntry<K, V> node = PatriciaTrie.this.getEntry(key);
               if(node != null && Tries.areEqual(node.getValue(), entry.getValue())) {
                  PatriciaTrie.this.removeEntry(node);
                  return true;
               } else {
                  return false;
               }
            }
         }
      }

      private final class EntryIterator extends AbstractPatriciaTrie<K, V>.TrieIterator<Entry<K, V>> {
         private final K excludedKey;

         private EntryIterator(AbstractPatriciaTrie.TrieEntry<K, V> var1, AbstractPatriciaTrie.TrieEntry<K, V> first) {
            super(first);
            this.excludedKey = last != null?last.getKey():null;
         }

         public boolean hasNext() {
            return this.next != null && !Tries.areEqual(this.next.key, this.excludedKey);
         }

         public Entry<K, V> next() {
            if(this.next != null && !Tries.areEqual(this.next.key, this.excludedKey)) {
               return this.nextEntry();
            } else {
               throw new NoSuchElementException();
            }
         }
      }
   }

   private class RangeEntryMap extends PatriciaTrie<K, V>.RangeMap {
      protected final K fromKey;
      protected final K toKey;
      protected final boolean fromInclusive;
      protected final boolean toInclusive;

      protected RangeEntryMap(K var1, K fromKey) {
         this(fromKey, true, toKey, false);
      }

      protected RangeEntryMap(K var1, boolean fromKey, K fromInclusive, boolean toKey) {
         super(null);
         if(fromKey == null && toKey == null) {
            throw new IllegalArgumentException("must have a from or to!");
         } else if(fromKey != null && toKey != null && PatriciaTrie.this.keyAnalyzer.compare(fromKey, toKey) > 0) {
            throw new IllegalArgumentException("fromKey > toKey");
         } else {
            this.fromKey = fromKey;
            this.fromInclusive = fromInclusive;
            this.toKey = toKey;
            this.toInclusive = toInclusive;
         }
      }

      public K firstKey() {
         Entry<K, V> e = this.fromKey == null?PatriciaTrie.this.firstEntry():(this.fromInclusive?PatriciaTrie.this.ceilingEntry(this.fromKey):PatriciaTrie.this.higherEntry(this.fromKey));
         K first = e != null?e.getKey():null;
         if(e != null && (this.toKey == null || this.inToRange(first, false))) {
            return first;
         } else {
            throw new NoSuchElementException();
         }
      }

      public K lastKey() {
         Entry<K, V> e = this.toKey == null?PatriciaTrie.this.lastEntry():(this.toInclusive?PatriciaTrie.this.floorEntry(this.toKey):PatriciaTrie.this.lowerEntry(this.toKey));
         K last = e != null?e.getKey():null;
         if(e != null && (this.fromKey == null || this.inFromRange(last, false))) {
            return last;
         } else {
            throw new NoSuchElementException();
         }
      }

      protected Set<Entry<K, V>> createEntrySet() {
         return PatriciaTrie.this.new RangeEntrySet(this);
      }

      public K getFromKey() {
         return this.fromKey;
      }

      public K getToKey() {
         return this.toKey;
      }

      public boolean isFromInclusive() {
         return this.fromInclusive;
      }

      public boolean isToInclusive() {
         return this.toInclusive;
      }

      protected SortedMap<K, V> createRangeMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
         return PatriciaTrie.this.new RangeEntryMap(fromKey, fromInclusive, toKey, toInclusive);
      }
   }

   private abstract class RangeMap extends AbstractMap<K, V> implements SortedMap<K, V> {
      private transient volatile Set<Entry<K, V>> entrySet;

      private RangeMap() {
      }

      protected abstract Set<Entry<K, V>> createEntrySet();

      protected abstract K getFromKey();

      protected abstract boolean isFromInclusive();

      protected abstract K getToKey();

      protected abstract boolean isToInclusive();

      public Comparator<? super K> comparator() {
         return PatriciaTrie.this.comparator();
      }

      public boolean containsKey(Object key) {
         return this.inRange(Tries.cast(key)) && PatriciaTrie.this.containsKey(key);
      }

      public V remove(Object key) {
         return !this.inRange(Tries.cast(key))?null:PatriciaTrie.this.remove(key);
      }

      public V get(Object key) {
         return !this.inRange(Tries.cast(key))?null:PatriciaTrie.this.get(key);
      }

      public V put(K key, V value) {
         if(!this.inRange(key)) {
            throw new IllegalArgumentException("Key is out of range: " + key);
         } else {
            return PatriciaTrie.this.put(key, value);
         }
      }

      public Set<Entry<K, V>> entrySet() {
         if(this.entrySet == null) {
            this.entrySet = this.createEntrySet();
         }

         return this.entrySet;
      }

      public SortedMap<K, V> subMap(K fromKey, K toKey) {
         if(!this.inRange2(fromKey)) {
            throw new IllegalArgumentException("FromKey is out of range: " + fromKey);
         } else if(!this.inRange2(toKey)) {
            throw new IllegalArgumentException("ToKey is out of range: " + toKey);
         } else {
            return this.createRangeMap(fromKey, this.isFromInclusive(), toKey, this.isToInclusive());
         }
      }

      public SortedMap<K, V> headMap(K toKey) {
         if(!this.inRange2(toKey)) {
            throw new IllegalArgumentException("ToKey is out of range: " + toKey);
         } else {
            return this.createRangeMap(this.getFromKey(), this.isFromInclusive(), toKey, this.isToInclusive());
         }
      }

      public SortedMap<K, V> tailMap(K fromKey) {
         if(!this.inRange2(fromKey)) {
            throw new IllegalArgumentException("FromKey is out of range: " + fromKey);
         } else {
            return this.createRangeMap(fromKey, this.isFromInclusive(), this.getToKey(), this.isToInclusive());
         }
      }

      protected boolean inRange(K key) {
         K fromKey = this.getFromKey();
         K toKey = this.getToKey();
         return (fromKey == null || this.inFromRange(key, false)) && (toKey == null || this.inToRange(key, false));
      }

      protected boolean inRange2(K key) {
         K fromKey = this.getFromKey();
         K toKey = this.getToKey();
         return (fromKey == null || this.inFromRange(key, false)) && (toKey == null || this.inToRange(key, true));
      }

      protected boolean inFromRange(K key, boolean forceInclusive) {
         K fromKey = this.getFromKey();
         boolean fromInclusive = this.isFromInclusive();
         int ret = PatriciaTrie.this.keyAnalyzer.compare(key, fromKey);
         return !fromInclusive && !forceInclusive?ret > 0:ret >= 0;
      }

      protected boolean inToRange(K key, boolean forceInclusive) {
         K toKey = this.getToKey();
         boolean toInclusive = this.isToInclusive();
         int ret = PatriciaTrie.this.keyAnalyzer.compare(key, toKey);
         return !toInclusive && !forceInclusive?ret < 0:ret <= 0;
      }

      protected abstract SortedMap<K, V> createRangeMap(K var1, boolean var2, K var3, boolean var4);
   }
}
