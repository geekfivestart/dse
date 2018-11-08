package org.apache.cassandra.utils.btree;

import java.util.Arrays;
import java.util.Comparator;

class NodeCursor<K> {
   final NodeCursor<K> parent;
   final NodeCursor<K> child;
   final Comparator<? super K> comparator;
   boolean inChild;
   int position;
   Object[] node;
   int nodeOffset;

   NodeCursor(Object[] node, NodeCursor<K> parent, Comparator<? super K> comparator) {
      this.node = node;
      this.parent = parent;
      this.comparator = comparator;
      this.child = BTree.isLeaf(node)?null:new NodeCursor((Object[])((Object[])node[BTree.getChildStart(node)]), this, comparator);
   }

   void resetNode(Object[] node, int nodeOffset) {
      this.node = node;
      this.nodeOffset = nodeOffset;
   }

   void safeAdvanceIntoBranchFromChild(boolean forwards) {
      if(!forwards) {
         --this.position;
      }

   }

   boolean advanceIntoBranchFromChild(boolean forwards) {
      return forwards?this.position < BTree.getBranchKeyEnd(this.node):--this.position >= 0;
   }

   boolean advanceLeafNode(boolean forwards) {
      return forwards?++this.position < BTree.getLeafKeyEnd(this.node):--this.position >= 0;
   }

   K bound(boolean upper) {
      return this.node[this.position - (upper?0:1)];
   }

   NodeCursor<K> boundIterator(boolean upper) {
      NodeCursor bound;
      for(bound = this.parent; bound != null; bound = bound.parent) {
         if(upper) {
            if(bound.position < BTree.getChildCount(bound.node) - 1) {
               break;
            }
         } else if(bound.position > 0) {
            break;
         }
      }

      return bound;
   }

   boolean seekInNode(K key, boolean forwards) {
      int position = this.position;
      int lb;
      int ub;
      if(forwards) {
         lb = position + 1;
         ub = BTree.getKeyEnd(this.node);
      } else {
         ub = position;
         lb = 0;
      }

      int find = Arrays.binarySearch((Object[])this.node, lb, ub, key, this.comparator);
      if(find >= 0) {
         this.position = find;
         this.inChild = false;
         return true;
      } else {
         int delta = this.isLeaf() & !forwards?-1:0;
         this.position = delta - 1 - find;
         return false;
      }
   }

   NodeCursor<K> descendToFirstChild(boolean forwards) {
      if(this.isLeaf()) {
         this.position = forwards?0:BTree.getLeafKeyEnd(this.node) - 1;
         return null;
      } else {
         this.inChild = true;
         this.position = forwards?0:BTree.getChildCount(this.node) - 1;
         return this.descend();
      }
   }

   NodeCursor<K> descend() {
      Object[] childNode = (Object[])((Object[])this.node[this.position + BTree.getChildStart(this.node)]);
      int childOffset = this.nodeOffset + BTree.treeIndexOffsetOfChild(this.node, this.position);
      this.child.resetNode(childNode, childOffset);
      this.inChild = true;
      return this.child;
   }

   boolean isLeaf() {
      return this.child == null;
   }

   int globalIndex() {
      return this.nodeOffset + BTree.treeIndexOfKey(this.node, this.position);
   }

   int globalLeafIndex() {
      return this.nodeOffset + BTree.treeIndexOfLeafKey(this.position);
   }

   int globalBranchIndex() {
      return this.nodeOffset + BTree.treeIndexOfBranchKey(this.node, this.position);
   }

   K value() {
      return this.node[this.position];
   }
}
