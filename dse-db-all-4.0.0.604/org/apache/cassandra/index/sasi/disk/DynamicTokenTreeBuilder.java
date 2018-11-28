package org.apache.cassandra.index.sasi.disk;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
import com.carrotsearch.hppc.cursors.LongCursor;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map.Entry;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.Pair;

public class DynamicTokenTreeBuilder extends AbstractTokenTreeBuilder {
   private final SortedMap<Long, LongSet> tokens = new TreeMap();

   public DynamicTokenTreeBuilder() {
   }

   public DynamicTokenTreeBuilder(TokenTreeBuilder data) {
      this.add((TokenTreeBuilder)data);
   }

   public DynamicTokenTreeBuilder(SortedMap<Long, LongSet> data) {
      this.add(data);
   }

   public void add(Long token, long keyPosition) {
      LongSet found = (LongSet)this.tokens.get(token);
      if(found == null) {
         this.tokens.put(token, found = new LongHashSet(2));
      }

      ((LongSet)found).add(keyPosition);
   }

   public void add(Iterator<Pair<Long, LongSet>> data) {
      label16:
      while(true) {
         if(data.hasNext()) {
            Pair<Long, LongSet> entry = (Pair)data.next();
            Iterator var3 = ((LongSet)entry.right).iterator();

            while(true) {
               if(!var3.hasNext()) {
                  continue label16;
               }

               LongCursor l = (LongCursor)var3.next();
               this.add((Long)entry.left, l.value);
            }
         }

         return;
      }
   }

   public void add(SortedMap<Long, LongSet> data) {
      Iterator var2 = data.entrySet().iterator();

      while(var2.hasNext()) {
         Entry<Long, LongSet> newEntry = (Entry)var2.next();
         LongSet found = (LongSet)this.tokens.get(newEntry.getKey());
         if(found == null) {
            this.tokens.put(newEntry.getKey(), found = new LongHashSet(4));
         }

         Iterator var5 = ((LongSet)newEntry.getValue()).iterator();

         while(var5.hasNext()) {
            LongCursor offset = (LongCursor)var5.next();
            ((LongSet)found).add(offset.value);
         }
      }

   }

   public Iterator<Pair<Long, LongSet>> iterator() {
      final Iterator<Entry<Long, LongSet>> iterator = this.tokens.entrySet().iterator();
      return new AbstractIterator<Pair<Long, LongSet>>() {
         protected Pair<Long, LongSet> computeNext() {
            if(!iterator.hasNext()) {
               return (Pair)this.endOfData();
            } else {
               Entry<Long, LongSet> entry = (Entry)iterator.next();
               return Pair.create(entry.getKey(), entry.getValue());
            }
         }
      };
   }

   public boolean isEmpty() {
      return this.tokens.size() == 0;
   }

   protected void constructTree() {
      this.tokenCount = (long)this.tokens.size();
      this.treeMinToken = ((Long)this.tokens.firstKey()).longValue();
      this.treeMaxToken = ((Long)this.tokens.lastKey()).longValue();
      this.numBlocks = 1;
      if(this.tokenCount <= 248L) {
         this.leftmostLeaf = new DynamicTokenTreeBuilder.DynamicLeaf(this.tokens);
         this.rightmostLeaf = this.leftmostLeaf;
         this.root = this.leftmostLeaf;
      } else {
         this.root = new AbstractTokenTreeBuilder.InteriorNode();
         this.rightmostParent = (AbstractTokenTreeBuilder.InteriorNode)this.root;
         int i = 0;
         AbstractTokenTreeBuilder.Leaf lastLeaf = null;
         Long firstToken = (Long)this.tokens.firstKey();
         Long finalToken = (Long)this.tokens.lastKey();
         Iterator var6 = this.tokens.keySet().iterator();

         while(true) {
            while(var6.hasNext()) {
               Long token = (Long)var6.next();
               if(i != 0 && (i % 248 == 0 || (long)i == this.tokenCount - 1L)) {
                  AbstractTokenTreeBuilder.Leaf leaf = (long)i == this.tokenCount - 1L && !token.equals(finalToken)?new DynamicTokenTreeBuilder.DynamicLeaf(this.tokens.tailMap(firstToken)):new DynamicTokenTreeBuilder.DynamicLeaf(this.tokens.subMap(firstToken, token));
                  if(i == 248) {
                     this.leftmostLeaf = leaf;
                  } else {
                     lastLeaf.next = leaf;
                  }

                  this.rightmostParent.add(leaf);
                  lastLeaf = leaf;
                  this.rightmostLeaf = leaf;
                  firstToken = token;
                  ++i;
                  ++this.numBlocks;
                  if(token.equals(finalToken)) {
                     AbstractTokenTreeBuilder.Leaf finalLeaf = new DynamicTokenTreeBuilder.DynamicLeaf(this.tokens.tailMap(token));
                     leaf.next = finalLeaf;
                     this.rightmostParent.add(finalLeaf);
                     this.rightmostLeaf = finalLeaf;
                     ++this.numBlocks;
                  }
               } else {
                  ++i;
               }
            }

            return;
         }
      }
   }

   private class DynamicLeaf extends AbstractTokenTreeBuilder.Leaf {
      private final SortedMap<Long, LongSet> tokens;

      DynamicLeaf(SortedMap<Long, LongSet> data) {
         super((Long)data.firstKey(), (Long)data.lastKey());
         this.tokens = data;
      }

      public int tokenCount() {
         return this.tokens.size();
      }

      public boolean isSerializable() {
         return true;
      }

      protected void serializeData(ByteBuffer buf) {
         Iterator var2 = this.tokens.entrySet().iterator();

         while(var2.hasNext()) {
            Entry<Long, LongSet> entry = (Entry)var2.next();
            this.createEntry(((Long)entry.getKey()).longValue(), (LongSet)entry.getValue()).serialize(buf);
         }

      }
   }
}
