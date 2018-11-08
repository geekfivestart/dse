package org.apache.cassandra.utils.btree;

import java.util.Arrays;
import java.util.Comparator;

class TreeCursor<K> extends NodeCursor<K> {
   NodeCursor<K> cur;

   TreeCursor(Comparator<? super K> comparator, Object[] node) {
      super(node, (NodeCursor)null, comparator);
   }

   void reset(boolean start) {
      this.cur = this.root();
      this.root().inChild = false;
      this.root().position = start?-1:BTree.getKeyEnd(this.root().node);
   }

   int moveOne(boolean forwards) {
      NodeCursor<K> cur = this.cur;
      if(cur.isLeaf()) {
         if(cur.advanceLeafNode(forwards)) {
            return cur.globalLeafIndex();
         } else {
            this.cur = cur = this.moveOutOfLeaf(forwards, cur, this.root());
            return cur.globalIndex();
         }
      } else {
         if(forwards) {
            ++cur.position;
         }

         NodeCursor next;
         for(cur = cur.descend(); null != (next = cur.descendToFirstChild(forwards)); cur = next) {
            ;
         }

         this.cur = cur;
         return cur.globalLeafIndex();
      }
   }

   boolean seekTo(K key, boolean forwards, boolean skipOne) {
      NodeCursor<K> cur = this.cur;
      boolean tryOne = !skipOne;
      if(!tryOne & cur.isLeaf() && !(tryOne = cur.advanceLeafNode(forwards) || (cur = this.moveOutOfLeaf(forwards, cur, (NodeCursor)null)) != null)) {
         this.cur = this.root();
         return false;
      } else {
         int cmpbound;
         if(tryOne) {
            label111: {
               K test = cur.value();
               if(key == test) {
                  cmpbound = 0;
               } else {
                  cmpbound = this.comparator.compare(test, key);
               }

               if(forwards) {
                  if(cmpbound < 0) {
                     break label111;
                  }
               } else if(cmpbound > 0) {
                  break label111;
               }

               this.cur = cur;
               return cmpbound == 0;
            }
         }

         while(cur != this.root()) {
            NodeCursor<K> bound = cur.boundIterator(forwards);
            if(bound == null) {
               break;
            }

            cmpbound = this.comparator.compare(bound.bound(forwards), key);
            if(forwards) {
               if(cmpbound > 0) {
                  break;
               }
            } else if(cmpbound < 0) {
               break;
            }

            cur = bound;
            bound.safeAdvanceIntoBranchFromChild(forwards);
            if(cmpbound == 0) {
               this.cur = bound;
               return true;
            }
         }

         boolean match;
         while(!(match = cur.seekInNode(key, forwards)) && !cur.isLeaf()) {
            cur = cur.descend();
            cur.position = forwards?-1:BTree.getKeyEnd(cur.node);
         }

         if(!match) {
            cur = this.ensureValidLocation(forwards, cur);
         }

         this.cur = cur;

         assert !cur.inChild;

         return match;
      }
   }

   private NodeCursor<K> ensureValidLocation(boolean forwards, NodeCursor<K> cur) {
      assert cur.isLeaf();

      int position = cur.position;
      if(position < 0 | position >= BTree.getLeafKeyEnd(cur.node)) {
         cur = this.moveOutOfLeaf(forwards, cur, this.root());
      }

      return cur;
   }

   private <K> NodeCursor<K> moveOutOfLeaf(boolean forwards, NodeCursor<K> cur, NodeCursor<K> ifFail) {
      do {
         cur = cur.parent;
         if(cur == null) {
            this.root().inChild = false;
            return ifFail;
         }
      } while(!cur.advanceIntoBranchFromChild(forwards));

      cur.inChild = false;
      return cur;
   }

   void seekTo(int index) {
      if(index < 0 | index >= BTree.size(this.rootNode())) {
         if(index < -1 | index > BTree.size(this.rootNode())) {
            throw new IndexOutOfBoundsException(index + " not in range [0.." + BTree.size(this.rootNode()) + ")");
         } else {
            this.reset(index == -1);
         }
      } else {
         NodeCursor<K> cur = this.root();

         assert cur.nodeOffset == 0;

         while(true) {
            int relativeIndex = index - cur.nodeOffset;
            Object[] node = cur.node;
            if(cur.isLeaf()) {
               assert relativeIndex < BTree.getLeafKeyEnd(node);

               cur.position = relativeIndex;
               this.cur = cur;
               return;
            }

            int[] sizeMap = BTree.getSizeMap(node);
            int boundary = Arrays.binarySearch(sizeMap, relativeIndex);
            if(boundary >= 0) {
               assert boundary < sizeMap.length - 1;

               cur.position = boundary;
               cur.inChild = false;
               this.cur = cur;
               return;
            }

            cur.inChild = true;
            cur.position = -1 - boundary;
            cur = cur.descend();
         }
      }
   }

   private NodeCursor<K> root() {
      return this;
   }

   Object[] rootNode() {
      return this.node;
   }

   K currentValue() {
      return this.cur.value();
   }
}
