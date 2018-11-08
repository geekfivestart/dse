package org.apache.cassandra.utils.btree;

import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import java.util.Comparator;
import java.util.Iterator;

final class TreeBuilder {
   private static final Recycler<TreeBuilder> builderRecycler = new Recycler<TreeBuilder>() {
      protected TreeBuilder newObject(Handle handle) {
         return new TreeBuilder(handle, null);
      }
   };
   private final Handle recycleHandle;
   private final NodeBuilder rootBuilder;

   public static TreeBuilder newInstance() {
      return (TreeBuilder)builderRecycler.get();
   }

   private TreeBuilder(Handle handle) {
      this.rootBuilder = new NodeBuilder();
      this.recycleHandle = handle;
   }

   public <C, K extends C, V extends C> Object[] update(Object[] btree, Comparator<C> comparator, Iterable<K> source, UpdateFunction<K, V> updateF) {
      assert updateF != null;

      NodeBuilder current = this.rootBuilder;
      current.reset(btree, BTree.POSITIVE_INFINITY, updateF, comparator);
      Iterator var6 = source.iterator();

      label43:
      while(var6.hasNext()) {
         NodeBuilder next;
         for(Object key = var6.next(); !updateF.abortEarly(); current = next) {
            next = current.update(key);
            if(next == null) {
               continue label43;
            }
         }

         this.rootBuilder.clear();
         return null;
      }

      while(true) {
         NodeBuilder next = current.finish();
         if(next == null) {
            assert current.isRoot();

            Object[] r = current.toNode();
            current.clear();
            builderRecycler.recycle(this, this.recycleHandle);
            return r;
         }

         current = next;
      }
   }

   public <C, K extends C, V extends C> Object[] build(Iterable<K> source, UpdateFunction<K, V> updateF, int size) {
      assert updateF != null;

      NodeBuilder current;
      for(current = this.rootBuilder; (size >>= BTree.FAN_SHIFT) > 0; current = current.ensureChild()) {
         ;
      }

      current.reset(BTree.EMPTY_LEAF, BTree.POSITIVE_INFINITY, updateF, (Comparator)null);
      Iterator var5 = source.iterator();

      while(var5.hasNext()) {
         K key = var5.next();
         current.addNewKey(key);
      }

      current = current.ascendToRoot();
      Object[] r = current.toNode();
      current.clear();
      builderRecycler.recycle(this, this.recycleHandle);
      return r;
   }
}
