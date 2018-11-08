package org.apache.cassandra.utils.btree;

import java.util.Arrays;
import java.util.Comparator;
import org.apache.cassandra.utils.ObjectSizes;

final class NodeBuilder {
   private static final int MAX_KEYS;
   private NodeBuilder parent;
   private NodeBuilder child;
   private Object[] buildKeys;
   private Object[] buildChildren;
   private int buildKeyPosition;
   private int buildChildPosition;
   private int maxBuildKeyPosition;
   private Object[] copyFrom;
   private int copyFromKeyPosition;
   private int copyFromChildPosition;
   private UpdateFunction updateFunction;
   private Comparator comparator;
   private Object upperBound;

   NodeBuilder() {
      this.buildKeys = new Object[MAX_KEYS];
      this.buildChildren = new Object[1 + MAX_KEYS];
   }

   void clear() {
      NodeBuilder current;
      for(current = this; current != null && current.upperBound != null; current = current.child) {
         current.clearSelf();
      }

      for(current = this.parent; current != null && current.upperBound != null; current = current.parent) {
         current.clearSelf();
      }

   }

   void clearSelf() {
      this.reset((Object[])null, (Object)null, (UpdateFunction)null, (Comparator)null);
      Arrays.fill(this.buildKeys, 0, this.maxBuildKeyPosition, (Object)null);
      Arrays.fill(this.buildChildren, 0, this.maxBuildKeyPosition + 1, (Object)null);
      this.maxBuildKeyPosition = 0;
   }

   void reset(Object[] copyFrom, Object upperBound, UpdateFunction updateFunction, Comparator comparator) {
      this.copyFrom = copyFrom;
      this.upperBound = upperBound;
      this.updateFunction = updateFunction;
      this.comparator = comparator;
      this.maxBuildKeyPosition = Math.max(this.maxBuildKeyPosition, this.buildKeyPosition);
      this.buildKeyPosition = 0;
      this.buildChildPosition = 0;
      this.copyFromKeyPosition = 0;
      this.copyFromChildPosition = 0;
   }

   NodeBuilder finish() {
      assert this.copyFrom != null;

      int copyFromKeyEnd = BTree.getKeyEnd(this.copyFrom);
      if(this.buildKeyPosition + this.buildChildPosition > 0) {
         this.copyKeys(copyFromKeyEnd);
         if(!BTree.isLeaf(this.copyFrom)) {
            this.copyChildren(copyFromKeyEnd + 1);
         }
      }

      return this.isRoot()?null:this.ascend();
   }

   NodeBuilder update(Object key) {
      assert this.copyFrom != null;

      int copyFromKeyEnd = BTree.getKeyEnd(this.copyFrom);
      int i = this.copyFromKeyPosition;
      boolean owns = true;
      boolean found;
      if(i == copyFromKeyEnd) {
         found = false;
      } else {
         int c = -this.comparator.compare(key, this.copyFrom[i]);
         if(c >= 0) {
            found = c == 0;
         } else {
            i = Arrays.binarySearch(this.copyFrom, i + 1, copyFromKeyEnd, key, this.comparator);
            found = i >= 0;
            if(!found) {
               i = -i - 1;
            }
         }
      }

      Object newUpperBound;
      if(found) {
         newUpperBound = this.copyFrom[i];
         Object next = this.updateFunction.apply(newUpperBound, key);
         if(newUpperBound == next) {
            return null;
         }

         key = next;
      } else if(i == copyFromKeyEnd && compareUpperBound(this.comparator, key, this.upperBound) >= 0) {
         owns = false;
      }

      if(BTree.isLeaf(this.copyFrom)) {
         if(owns) {
            this.copyKeys(i);
            if(found) {
               this.replaceNextKey(key);
            } else {
               this.addNewKey(key);
            }

            return null;
         }

         if(this.buildKeyPosition > 0) {
            this.copyKeys(i);
         }
      } else {
         if(found) {
            this.copyKeys(i);
            this.replaceNextKey(key);
            this.copyChildren(i + 1);
            return null;
         }

         if(owns) {
            this.copyKeys(i);
            this.copyChildren(i);
            newUpperBound = i < copyFromKeyEnd?this.copyFrom[i]:this.upperBound;
            Object[] descendInto = (Object[])((Object[])this.copyFrom[copyFromKeyEnd + i]);
            this.ensureChild().reset(descendInto, newUpperBound, this.updateFunction, this.comparator);
            return this.child;
         }

         if(this.buildKeyPosition > 0 || this.buildChildPosition > 0) {
            this.copyKeys(copyFromKeyEnd);
            this.copyChildren(copyFromKeyEnd + 1);
         }
      }

      return this.ascend();
   }

   private static <V> int compareUpperBound(Comparator<V> comparator, Object value, Object upperBound) {
      return upperBound == BTree.POSITIVE_INFINITY?-1:comparator.compare(value, upperBound);
   }

   boolean isRoot() {
      return (this.parent == null || this.parent.upperBound == null) && this.buildKeyPosition <= BTree.FAN_FACTOR;
   }

   NodeBuilder ascendToRoot() {
      NodeBuilder current;
      for(current = this; !current.isRoot(); current = current.ascend()) {
         ;
      }

      return current;
   }

   Object[] toNode() {
      assert this.buildKeyPosition <= BTree.FAN_FACTOR : this.buildKeyPosition;

      return this.buildFromRange(0, this.buildKeyPosition, BTree.isLeaf(this.copyFrom), false);
   }

   private NodeBuilder ascend() {
      this.ensureParent();
      boolean isLeaf = BTree.isLeaf(this.copyFrom);
      if(this.buildKeyPosition > BTree.FAN_FACTOR) {
         int mid = this.buildKeyPosition / 2;
         this.parent.addExtraChild(this.buildFromRange(0, mid, isLeaf, true), this.buildKeys[mid]);
         this.parent.finishChild(this.buildFromRange(mid + 1, this.buildKeyPosition - (mid + 1), isLeaf, false));
      } else {
         this.parent.finishChild(this.buildFromRange(0, this.buildKeyPosition, isLeaf, false));
      }

      return this.parent;
   }

   private void copyKeys(int upToKeyPosition) {
      if(this.copyFromKeyPosition < upToKeyPosition) {
         int len = upToKeyPosition - this.copyFromKeyPosition;

         assert len <= BTree.FAN_FACTOR : upToKeyPosition + "," + this.copyFromKeyPosition;

         this.ensureRoom(this.buildKeyPosition + len);
         if(len > 0) {
            System.arraycopy(this.copyFrom, this.copyFromKeyPosition, this.buildKeys, this.buildKeyPosition, len);
            this.copyFromKeyPosition = upToKeyPosition;
            this.buildKeyPosition += len;
         }

      }
   }

   private void replaceNextKey(Object with) {
      this.ensureRoom(this.buildKeyPosition + 1);
      this.buildKeys[this.buildKeyPosition++] = with;
      ++this.copyFromKeyPosition;
   }

   void addNewKey(Object key) {
      this.ensureRoom(this.buildKeyPosition + 1);
      this.buildKeys[this.buildKeyPosition++] = this.updateFunction.apply(key);
   }

   private void copyChildren(int upToChildPosition) {
      if(this.copyFromChildPosition < upToChildPosition) {
         int len = upToChildPosition - this.copyFromChildPosition;
         if(len > 0) {
            System.arraycopy(this.copyFrom, BTree.getKeyEnd(this.copyFrom) + this.copyFromChildPosition, this.buildChildren, this.buildChildPosition, len);
            this.copyFromChildPosition = upToChildPosition;
            this.buildChildPosition += len;
         }

      }
   }

   private void addExtraChild(Object[] child, Object upperBound) {
      this.ensureRoom(this.buildKeyPosition + 1);
      this.buildKeys[this.buildKeyPosition++] = upperBound;
      this.buildChildren[this.buildChildPosition++] = child;
   }

   private void finishChild(Object[] child) {
      this.buildChildren[this.buildChildPosition++] = child;
      ++this.copyFromChildPosition;
   }

   private void ensureRoom(int nextBuildKeyPosition) {
      if(nextBuildKeyPosition >= MAX_KEYS) {
         Object[] flushUp = this.buildFromRange(0, BTree.FAN_FACTOR, BTree.isLeaf(this.copyFrom), true);
         this.ensureParent().addExtraChild(flushUp, this.buildKeys[BTree.FAN_FACTOR]);
         int size = BTree.FAN_FACTOR + 1;

         assert size <= this.buildKeyPosition : this.buildKeyPosition + "," + nextBuildKeyPosition;

         System.arraycopy(this.buildKeys, size, this.buildKeys, 0, this.buildKeyPosition - size);
         this.buildKeyPosition -= size;
         this.maxBuildKeyPosition = this.buildKeys.length;
         if(this.buildChildPosition > 0) {
            System.arraycopy(this.buildChildren, size, this.buildChildren, 0, this.buildChildPosition - size);
            this.buildChildPosition -= size;
         }

      }
   }

   private Object[] buildFromRange(int offset, int keyLength, boolean isLeaf, boolean isExtra) {
      if(keyLength == 0) {
         return this.copyFrom;
      } else {
         Object[] a;
         if(isLeaf) {
            a = new Object[keyLength | 1];
            System.arraycopy(this.buildKeys, offset, a, 0, keyLength);
         } else {
            a = new Object[2 + keyLength * 2];
            System.arraycopy(this.buildKeys, offset, a, 0, keyLength);
            System.arraycopy(this.buildChildren, offset, a, keyLength, keyLength + 1);
            int[] indexOffsets = new int[keyLength + 1];
            int size = BTree.size((Object[])((Object[])a[keyLength]));

            for(int i = 0; i < keyLength; ++i) {
               indexOffsets[i] = size;
               size += 1 + BTree.size((Object[])((Object[])a[keyLength + 1 + i]));
            }

            indexOffsets[keyLength] = size;
            a[a.length - 1] = indexOffsets;
         }

         if(this.updateFunction != UpdateFunction.noOp()) {
            if(isExtra) {
               this.updateFunction.allocated(ObjectSizes.sizeOfArray(a));
            } else if(a.length != this.copyFrom.length) {
               this.updateFunction.allocated(ObjectSizes.sizeOfArray(a) - (this.copyFrom.length == 0?0L:ObjectSizes.sizeOfArray(this.copyFrom)));
            }
         }

         return a;
      }
   }

   private NodeBuilder ensureParent() {
      if(this.parent == null) {
         this.parent = new NodeBuilder();
         this.parent.child = this;
      }

      if(this.parent.upperBound == null) {
         this.parent.reset(BTree.EMPTY_BRANCH, this.upperBound, this.updateFunction, this.comparator);
      }

      return this.parent;
   }

   NodeBuilder ensureChild() {
      if(this.child == null) {
         this.child = new NodeBuilder();
         this.child.parent = this;
      }

      return this.child;
   }

   static {
      MAX_KEYS = 1 + BTree.FAN_FACTOR * 2;
   }
}
