package org.apache.cassandra.index.sasi.utils.trie;

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.Map.Entry;

abstract class AbstractPatriciaTrie<K, V> extends AbstractTrie<K, V> {
   private static final long serialVersionUID = -2303909182832019043L;
   final AbstractPatriciaTrie.TrieEntry<K, V> root = new AbstractPatriciaTrie.TrieEntry((Object)null, (Object)null, -1);
   private transient volatile Set<K> keySet;
   private transient volatile Collection<V> values;
   private transient volatile Set<Entry<K, V>> entrySet;
   private int size = 0;
   transient int modCount = 0;

   public AbstractPatriciaTrie(KeyAnalyzer<? super K> keyAnalyzer) {
      super(keyAnalyzer);
   }

   public AbstractPatriciaTrie(KeyAnalyzer<? super K> keyAnalyzer, Map<? extends K, ? extends V> m) {
      super(keyAnalyzer);
      this.putAll(m);
   }

   public void clear() {
      this.root.key = null;
      this.root.bitIndex = -1;
      this.root.value = null;
      this.root.parent = null;
      this.root.left = this.root;
      this.root.right = null;
      this.root.predecessor = this.root;
      this.size = 0;
      this.incrementModCount();
   }

   public int size() {
      return this.size;
   }

   void incrementSize() {
      ++this.size;
      this.incrementModCount();
   }

   void decrementSize() {
      --this.size;
      this.incrementModCount();
   }

   private void incrementModCount() {
      ++this.modCount;
   }

   public V put(K key, V value) {
      if(key == null) {
         throw new NullPointerException("Key cannot be null");
      } else {
         int lengthInBits = this.lengthInBits(key);
         if(lengthInBits == 0) {
            if(this.root.isEmpty()) {
               this.incrementSize();
            } else {
               this.incrementModCount();
            }

            return this.root.setKeyValue(key, value);
         } else {
            AbstractPatriciaTrie.TrieEntry<K, V> found = this.getNearestEntryForKey(key);
            if(this.compareKeys(key, found.key)) {
               if(found.isEmpty()) {
                  this.incrementSize();
               } else {
                  this.incrementModCount();
               }

               return found.setKeyValue(key, value);
            } else {
               int bitIndex = this.bitIndex(key, found.key);
               if(!Tries.isOutOfBoundsIndex(bitIndex)) {
                  if(Tries.isValidBitIndex(bitIndex)) {
                     AbstractPatriciaTrie.TrieEntry<K, V> t = new AbstractPatriciaTrie.TrieEntry(key, value, bitIndex);
                     this.addEntry(t);
                     this.incrementSize();
                     return null;
                  }

                  if(Tries.isNullBitKey(bitIndex)) {
                     if(this.root.isEmpty()) {
                        this.incrementSize();
                     } else {
                        this.incrementModCount();
                     }

                     return this.root.setKeyValue(key, value);
                  }

                  if(Tries.isEqualBitKey(bitIndex) && found != this.root) {
                     this.incrementModCount();
                     return found.setKeyValue(key, value);
                  }
               }

               throw new IndexOutOfBoundsException("Failed to put: " + key + " -> " + value + ", " + bitIndex);
            }
         }
      }
   }

   AbstractPatriciaTrie.TrieEntry<K, V> addEntry(AbstractPatriciaTrie.TrieEntry<K, V> entry) {
      AbstractPatriciaTrie.TrieEntry<K, V> current = this.root.left;

      AbstractPatriciaTrie.TrieEntry path;
      for(path = this.root; current.bitIndex < entry.bitIndex && current.bitIndex > path.bitIndex; current = !this.isBitSet(entry.key, current.bitIndex)?current.left:current.right) {
         path = current;
      }

      entry.predecessor = entry;
      if(!this.isBitSet(entry.key, entry.bitIndex)) {
         entry.left = entry;
         entry.right = current;
      } else {
         entry.left = current;
         entry.right = entry;
      }

      entry.parent = path;
      if(current.bitIndex >= entry.bitIndex) {
         current.parent = entry;
      }

      if(current.bitIndex <= path.bitIndex) {
         current.predecessor = entry;
      }

      if(path != this.root && this.isBitSet(entry.key, path.bitIndex)) {
         path.right = entry;
      } else {
         path.left = entry;
      }

      return entry;
   }

   public V get(Object k) {
      AbstractPatriciaTrie.TrieEntry<K, V> entry = this.getEntry(k);
      return entry != null?entry.getValue():null;
   }

   AbstractPatriciaTrie.TrieEntry<K, V> getEntry(Object k) {
      K key = Tries.cast(k);
      if(key == null) {
         return null;
      } else {
         AbstractPatriciaTrie.TrieEntry<K, V> entry = this.getNearestEntryForKey(key);
         return !entry.isEmpty() && this.compareKeys(key, entry.key)?entry:null;
      }
   }

   public Entry<K, V> select(K key) {
      AbstractPatriciaTrie.Reference<Entry<K, V>> reference = new AbstractPatriciaTrie.Reference(null);
      return !this.selectR(this.root.left, -1, key, reference)?(Entry)reference.get():null;
   }

   public Entry<K, V> select(K key, Cursor<? super K, ? super V> cursor) {
      AbstractPatriciaTrie.Reference<Entry<K, V>> reference = new AbstractPatriciaTrie.Reference(null);
      this.selectR(this.root.left, -1, key, cursor, reference);
      return (Entry)reference.get();
   }

   private boolean selectR(AbstractPatriciaTrie.TrieEntry<K, V> h, int bitIndex, K key, AbstractPatriciaTrie.Reference<Entry<K, V>> reference) {
      if(h.bitIndex <= bitIndex) {
         if(!h.isEmpty()) {
            reference.set(h);
            return false;
         } else {
            return true;
         }
      } else {
         if(!this.isBitSet(key, h.bitIndex)) {
            if(this.selectR(h.left, h.bitIndex, key, reference)) {
               return this.selectR(h.right, h.bitIndex, key, reference);
            }
         } else if(this.selectR(h.right, h.bitIndex, key, reference)) {
            return this.selectR(h.left, h.bitIndex, key, reference);
         }

         return false;
      }
   }

   private boolean selectR(AbstractPatriciaTrie.TrieEntry<K, V> h, int bitIndex, K key, Cursor<? super K, ? super V> cursor, AbstractPatriciaTrie.Reference<Entry<K, V>> reference) {
      if(h.bitIndex <= bitIndex) {
         if(!h.isEmpty()) {
            Cursor.Decision decision = cursor.select(h);
            switch(null.$SwitchMap$org$apache$cassandra$index$sasi$utils$trie$Cursor$Decision[decision.ordinal()]) {
            case 1:
               throw new UnsupportedOperationException("Cannot remove during select");
            case 2:
               reference.set(h);
               return false;
            case 3:
               AbstractPatriciaTrie.TrieEntry<K, V> entry = new AbstractPatriciaTrie.TrieEntry(h.getKey(), h.getValue(), -1);
               reference.set(entry);
               this.removeEntry(h);
               return false;
            case 4:
            }
         }

         return true;
      } else {
         if(!this.isBitSet(key, h.bitIndex)) {
            if(this.selectR(h.left, h.bitIndex, key, cursor, reference)) {
               return this.selectR(h.right, h.bitIndex, key, cursor, reference);
            }
         } else if(this.selectR(h.right, h.bitIndex, key, cursor, reference)) {
            return this.selectR(h.left, h.bitIndex, key, cursor, reference);
         }

         return false;
      }
   }

   public Entry<K, V> traverse(Cursor<? super K, ? super V> cursor) {
      AbstractPatriciaTrie.TrieEntry entry = this.nextEntry((AbstractPatriciaTrie.TrieEntry)null);

      while(entry != null) {
         AbstractPatriciaTrie.TrieEntry<K, V> current = entry;
         Cursor.Decision decision = cursor.select(entry);
         entry = this.nextEntry(entry);
         switch(null.$SwitchMap$org$apache$cassandra$index$sasi$utils$trie$Cursor$Decision[decision.ordinal()]) {
         case 1:
            this.removeEntry(current);
            break;
         case 2:
            return current;
         case 3:
            Entry<K, V> value = new AbstractPatriciaTrie.TrieEntry(current.getKey(), current.getValue(), -1);
            this.removeEntry(current);
            return value;
         case 4:
         }
      }

      return null;
   }

   public boolean containsKey(Object k) {
      if(k == null) {
         return false;
      } else {
         K key = Tries.cast(k);
         AbstractPatriciaTrie.TrieEntry<K, V> entry = this.getNearestEntryForKey(key);
         return !entry.isEmpty() && this.compareKeys(key, entry.key);
      }
   }

   public Set<Entry<K, V>> entrySet() {
      if(this.entrySet == null) {
         this.entrySet = new AbstractPatriciaTrie.EntrySet(null);
      }

      return this.entrySet;
   }

   public Set<K> keySet() {
      if(this.keySet == null) {
         this.keySet = new AbstractPatriciaTrie.KeySet(null);
      }

      return this.keySet;
   }

   public Collection<V> values() {
      if(this.values == null) {
         this.values = new AbstractPatriciaTrie.Values(null);
      }

      return this.values;
   }

   public V remove(Object k) {
      if(k == null) {
         return null;
      } else {
         K key = Tries.cast(k);
         AbstractPatriciaTrie.TrieEntry<K, V> current = this.root.left;

         for(AbstractPatriciaTrie.TrieEntry path = this.root; current.bitIndex > path.bitIndex; current = !this.isBitSet(key, current.bitIndex)?current.left:current.right) {
            path = current;
         }

         if(!current.isEmpty() && this.compareKeys(key, current.key)) {
            return this.removeEntry(current);
         } else {
            return null;
         }
      }
   }

   AbstractPatriciaTrie.TrieEntry<K, V> getNearestEntryForKey(K key) {
      AbstractPatriciaTrie.TrieEntry<K, V> current = this.root.left;

      for(AbstractPatriciaTrie.TrieEntry path = this.root; current.bitIndex > path.bitIndex; current = !this.isBitSet(key, current.bitIndex)?current.left:current.right) {
         path = current;
      }

      return current;
   }

   V removeEntry(AbstractPatriciaTrie.TrieEntry<K, V> h) {
      if(h != this.root) {
         if(h.isInternalNode()) {
            this.removeInternalEntry(h);
         } else {
            this.removeExternalEntry(h);
         }
      }

      this.decrementSize();
      return h.setKeyValue((Object)null, (Object)null);
   }

   private void removeExternalEntry(AbstractPatriciaTrie.TrieEntry<K, V> h) {
      if(h == this.root) {
         throw new IllegalArgumentException("Cannot delete root Entry!");
      } else if(!h.isExternalNode()) {
         throw new IllegalArgumentException(h + " is not an external Entry!");
      } else {
         AbstractPatriciaTrie.TrieEntry<K, V> parent = h.parent;
         AbstractPatriciaTrie.TrieEntry<K, V> child = h.left == h?h.right:h.left;
         if(parent.left == h) {
            parent.left = child;
         } else {
            parent.right = child;
         }

         if(child.bitIndex > parent.bitIndex) {
            child.parent = parent;
         } else {
            child.predecessor = parent;
         }

      }
   }

   private void removeInternalEntry(AbstractPatriciaTrie.TrieEntry<K, V> h) {
      if(h == this.root) {
         throw new IllegalArgumentException("Cannot delete root Entry!");
      } else if(!h.isInternalNode()) {
         throw new IllegalArgumentException(h + " is not an internal Entry!");
      } else {
         AbstractPatriciaTrie.TrieEntry<K, V> p = h.predecessor;
         p.bitIndex = h.bitIndex;
         AbstractPatriciaTrie.TrieEntry<K, V> parent = p.parent;
         AbstractPatriciaTrie.TrieEntry<K, V> child = p.left == h?p.right:p.left;
         if(p.predecessor == p && p.parent != h) {
            p.predecessor = p.parent;
         }

         if(parent.left == p) {
            parent.left = child;
         } else {
            parent.right = child;
         }

         if(child.bitIndex > parent.bitIndex) {
            child.parent = parent;
         }

         if(h.left.parent == h) {
            h.left.parent = p;
         }

         if(h.right.parent == h) {
            h.right.parent = p;
         }

         if(h.parent.left == h) {
            h.parent.left = p;
         } else {
            h.parent.right = p;
         }

         p.parent = h.parent;
         p.left = h.left;
         p.right = h.right;
         if(isValidUplink(p.left, p)) {
            p.left.predecessor = p;
         }

         if(isValidUplink(p.right, p)) {
            p.right.predecessor = p;
         }

      }
   }

   AbstractPatriciaTrie.TrieEntry<K, V> nextEntry(AbstractPatriciaTrie.TrieEntry<K, V> node) {
      return node == null?this.firstEntry():this.nextEntryImpl(node.predecessor, node, (AbstractPatriciaTrie.TrieEntry)null);
   }

   AbstractPatriciaTrie.TrieEntry<K, V> nextEntryImpl(AbstractPatriciaTrie.TrieEntry<K, V> start, AbstractPatriciaTrie.TrieEntry<K, V> previous, AbstractPatriciaTrie.TrieEntry<K, V> tree) {
      AbstractPatriciaTrie.TrieEntry<K, V> current = start;
      if(previous == null || start != previous.predecessor) {
         while(!current.left.isEmpty() && previous != current.left) {
            if(isValidUplink(current.left, current)) {
               return current.left;
            }

            current = current.left;
         }
      }

      if(current.isEmpty()) {
         return null;
      } else if(current.right == null) {
         return null;
      } else if(previous != current.right) {
         return isValidUplink(current.right, current)?current.right:this.nextEntryImpl(current.right, previous, tree);
      } else {
         while(current == current.parent.right) {
            if(current == tree) {
               return null;
            }

            current = current.parent;
         }

         if(current == tree) {
            return null;
         } else if(current.parent.right == null) {
            return null;
         } else if(previous != current.parent.right && isValidUplink(current.parent.right, current.parent)) {
            return current.parent.right;
         } else if(current.parent.right == current.parent) {
            return null;
         } else {
            return this.nextEntryImpl(current.parent.right, previous, tree);
         }
      }
   }

   AbstractPatriciaTrie.TrieEntry<K, V> firstEntry() {
      return this.isEmpty()?null:this.followLeft(this.root);
   }

   AbstractPatriciaTrie.TrieEntry<K, V> followLeft(AbstractPatriciaTrie.TrieEntry<K, V> node) {
      while(true) {
         AbstractPatriciaTrie.TrieEntry<K, V> child = node.left;
         if(child.isEmpty()) {
            child = node.right;
         }

         if(child.bitIndex <= node.bitIndex) {
            return child;
         }

         node = child;
      }
   }

   static boolean isValidUplink(AbstractPatriciaTrie.TrieEntry<?, ?> next, AbstractPatriciaTrie.TrieEntry<?, ?> from) {
      return next != null && next.bitIndex <= from.bitIndex && !next.isEmpty();
   }

   abstract class TrieIterator<E> implements Iterator<E> {
      protected int expectedModCount;
      protected AbstractPatriciaTrie.TrieEntry<K, V> next;
      protected AbstractPatriciaTrie.TrieEntry<K, V> current;

      protected TrieIterator() {
         this.expectedModCount = AbstractPatriciaTrie.this.modCount;
         this.next = AbstractPatriciaTrie.this.nextEntry((AbstractPatriciaTrie.TrieEntry)null);
      }

      protected TrieIterator(AbstractPatriciaTrie.TrieEntry<K, V> this$0) {
         this.expectedModCount = AbstractPatriciaTrie.this.modCount;
         this.next = firstEntry;
      }

      protected AbstractPatriciaTrie.TrieEntry<K, V> nextEntry() {
         if(this.expectedModCount != AbstractPatriciaTrie.this.modCount) {
            throw new ConcurrentModificationException();
         } else {
            AbstractPatriciaTrie.TrieEntry<K, V> e = this.next;
            if(e == null) {
               throw new NoSuchElementException();
            } else {
               this.next = this.findNext(e);
               this.current = e;
               return e;
            }
         }
      }

      protected AbstractPatriciaTrie.TrieEntry<K, V> findNext(AbstractPatriciaTrie.TrieEntry<K, V> prior) {
         return AbstractPatriciaTrie.this.nextEntry(prior);
      }

      public boolean hasNext() {
         return this.next != null;
      }

      public void remove() {
         if(this.current == null) {
            throw new IllegalStateException();
         } else if(this.expectedModCount != AbstractPatriciaTrie.this.modCount) {
            throw new ConcurrentModificationException();
         } else {
            AbstractPatriciaTrie.TrieEntry<K, V> node = this.current;
            this.current = null;
            AbstractPatriciaTrie.this.removeEntry(node);
            this.expectedModCount = AbstractPatriciaTrie.this.modCount;
         }
      }
   }

   private class Values extends AbstractCollection<V> {
      private Values() {
      }

      public Iterator<V> iterator() {
         return new AbstractPatriciaTrie.Values.ValueIterator(null);
      }

      public int size() {
         return AbstractPatriciaTrie.this.size();
      }

      public boolean contains(Object o) {
         return AbstractPatriciaTrie.this.containsValue(o);
      }

      public void clear() {
         AbstractPatriciaTrie.this.clear();
      }

      public boolean remove(Object o) {
         Iterator it = this.iterator();

         Object value;
         do {
            if(!it.hasNext()) {
               return false;
            }

            value = it.next();
         } while(!Tries.areEqual(value, o));

         it.remove();
         return true;
      }

      private class ValueIterator extends AbstractPatriciaTrie<K, V>.TrieIterator<V> {
         private ValueIterator() {
            super();
         }

         public V next() {
            return this.nextEntry().getValue();
         }
      }
   }

   private class KeySet extends AbstractSet<K> {
      private KeySet() {
      }

      public Iterator<K> iterator() {
         return new AbstractPatriciaTrie.KeySet.KeyIterator(null);
      }

      public int size() {
         return AbstractPatriciaTrie.this.size();
      }

      public boolean contains(Object o) {
         return AbstractPatriciaTrie.this.containsKey(o);
      }

      public boolean remove(Object o) {
         int size = this.size();
         AbstractPatriciaTrie.this.remove(o);
         return size != this.size();
      }

      public void clear() {
         AbstractPatriciaTrie.this.clear();
      }

      private class KeyIterator extends AbstractPatriciaTrie<K, V>.TrieIterator<K> {
         private KeyIterator() {
            super();
         }

         public K next() {
            return this.nextEntry().getKey();
         }
      }
   }

   private class EntrySet extends AbstractSet<Entry<K, V>> {
      private EntrySet() {
      }

      public Iterator<Entry<K, V>> iterator() {
         return new AbstractPatriciaTrie.EntrySet.EntryIterator(null);
      }

      public boolean contains(Object o) {
         if(!(o instanceof Entry)) {
            return false;
         } else {
            AbstractPatriciaTrie.TrieEntry<K, V> candidate = AbstractPatriciaTrie.this.getEntry(((Entry)o).getKey());
            return candidate != null && candidate.equals(o);
         }
      }

      public boolean remove(Object o) {
         int size = this.size();
         AbstractPatriciaTrie.this.remove(o);
         return size != this.size();
      }

      public int size() {
         return AbstractPatriciaTrie.this.size();
      }

      public void clear() {
         AbstractPatriciaTrie.this.clear();
      }

      private class EntryIterator extends AbstractPatriciaTrie<K, V>.TrieIterator<Entry<K, V>> {
         private EntryIterator() {
            super();
         }

         public Entry<K, V> next() {
            return this.nextEntry();
         }
      }
   }

   static class TrieEntry<K, V> extends AbstractTrie.BasicEntry<K, V> {
      private static final long serialVersionUID = 4596023148184140013L;
      protected int bitIndex;
      protected AbstractPatriciaTrie.TrieEntry<K, V> parent;
      protected AbstractPatriciaTrie.TrieEntry<K, V> left;
      protected AbstractPatriciaTrie.TrieEntry<K, V> right;
      protected AbstractPatriciaTrie.TrieEntry<K, V> predecessor;

      public TrieEntry(K key, V value, int bitIndex) {
         super(key, value);
         this.bitIndex = bitIndex;
         this.parent = null;
         this.left = this;
         this.right = null;
         this.predecessor = this;
      }

      public boolean isEmpty() {
         return this.key == null;
      }

      public boolean isInternalNode() {
         return this.left != this && this.right != this;
      }

      public boolean isExternalNode() {
         return !this.isInternalNode();
      }
   }

   private static class Reference<E> {
      private E item;

      private Reference() {
      }

      public void set(E item) {
         this.item = item;
      }

      public E get() {
         return this.item;
      }
   }
}
