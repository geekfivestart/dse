package org.apache.cassandra.utils.btree;

import java.util.Arrays;
import java.util.Comparator;

public class BTreeRemoval {
   public BTreeRemoval() {
   }

   public static <V> Object[] remove(Object[] btree, Comparator<? super V> comparator, V elem) {
      if(BTree.isEmpty(btree)) {
         return btree;
      } else {
         int index = true;
         V elemToSwap = null;
         int lb = 0;
         Object[] node = btree;

         while(true) {
            int keyEnd = BTree.getKeyEnd(node);
            int i = Arrays.binarySearch((Object[])node, 0, keyEnd, elem, comparator);
            if(i >= 0) {
               int index;
               if(BTree.isLeaf(node)) {
                  index = lb + i;
               } else {
                  int indexInNode = BTree.getSizeMap(node)[i];
                  index = lb + indexInNode - 1;
                  elemToSwap = BTree.findByIndex(node, indexInNode - 1);
               }

               if(BTree.size(btree) == 1) {
                  return BTree.empty();
               } else {
                  Object[] result = removeFromLeaf(btree, index);
                  if(elemToSwap != null) {
                     BTree.replaceInSitu(result, index, elemToSwap);
                  }

                  return result;
               }
            }

            if(BTree.isLeaf(node)) {
               return btree;
            }

            i = -1 - i;
            if(i > 0) {
               lb += BTree.getSizeMap(node)[i - 1] + 1;
            }

            node = (Object[])((Object[])node[keyEnd + i]);
         }
      }
   }

   private static Object[] removeFromLeaf(Object[] node, int index) {
      Object[] result = null;
      Object[] prevNode = null;
      int prevI = -1;

      int keyEnd;
      boolean nextNodeNeedsCopy;
      for(boolean needsCopy = true; !BTree.isLeaf(node); needsCopy = nextNodeNeedsCopy) {
         keyEnd = BTree.getBranchKeyEnd(node);
         int i = -1 - Arrays.binarySearch(BTree.getSizeMap(node), index);
         if(i > 0) {
            index -= 1 + BTree.getSizeMap(node)[i - 1];
         }

         Object[] nextNode = (Object[])((Object[])node[keyEnd + i]);
         nextNodeNeedsCopy = true;
         if(BTree.getKeyEnd(nextNode) > BTree.MINIMAL_NODE_SIZE) {
            node = copyIfNeeded(node, needsCopy);
         } else {
            Object[] leftNeighbour;
            if(i > 0 && BTree.getKeyEnd((Object[])((Object[])node[keyEnd + i - 1])) > BTree.MINIMAL_NODE_SIZE) {
               node = copyIfNeeded(node, needsCopy);
               leftNeighbour = (Object[])((Object[])node[keyEnd + i - 1]);
               ++index;
               if(!BTree.isLeaf(leftNeighbour)) {
                  index += BTree.size((Object[])((Object[])leftNeighbour[BTree.getChildEnd(leftNeighbour) - 1]));
               }

               nextNode = rotateLeft(node, i);
            } else if(i < keyEnd && BTree.getKeyEnd((Object[])((Object[])node[keyEnd + i + 1])) > BTree.MINIMAL_NODE_SIZE) {
               node = copyIfNeeded(node, needsCopy);
               nextNode = rotateRight(node, i);
            } else {
               nextNodeNeedsCopy = false;
               Object nodeKey;
               if(i > 0) {
                  leftNeighbour = (Object[])((Object[])node[keyEnd + i - 1]);
                  nodeKey = node[i - 1];
                  node = keyEnd == 1?null:copyWithKeyAndChildRemoved(node, i - 1, i - 1, false);
                  nextNode = merge(leftNeighbour, nextNode, nodeKey);
                  --i;
                  index += BTree.size(leftNeighbour) + 1;
               } else {
                  leftNeighbour = (Object[])((Object[])node[keyEnd + i + 1]);
                  nodeKey = node[i];
                  node = keyEnd == 1?null:copyWithKeyAndChildRemoved(node, i, i, false);
                  nextNode = merge(nextNode, leftNeighbour, nodeKey);
               }
            }
         }

         if(node != null) {
            int[] sizeMap = BTree.getSizeMap(node);

            for(int j = i; j < sizeMap.length; ++j) {
               --sizeMap[j];
            }

            if(prevNode != null) {
               prevNode[prevI] = node;
            } else {
               result = node;
            }

            prevNode = node;
            prevI = BTree.getChildStart(node) + i;
         }

         node = nextNode;
      }

      keyEnd = BTree.getLeafKeyEnd(node);
      Object[] newLeaf = new Object[(keyEnd & 1) == 1?keyEnd:keyEnd - 1];
      copyKeys(node, newLeaf, 0, index);
      if(prevNode != null) {
         prevNode[prevI] = newLeaf;
      } else {
         result = newLeaf;
      }

      return result;
   }

   private static Object[] rotateRight(Object[] node, int i) {
      int keyEnd = BTree.getBranchKeyEnd(node);
      Object[] nextNode = (Object[])((Object[])node[keyEnd + i]);
      Object[] rightNeighbour = (Object[])((Object[])node[keyEnd + i + 1]);
      boolean leaves = BTree.isLeaf(nextNode);
      int nextKeyEnd = BTree.getKeyEnd(nextNode);
      Object[] newChild = leaves?null:(Object[])((Object[])rightNeighbour[BTree.getChildStart(rightNeighbour)]);
      Object[] newNextNode = copyWithKeyAndChildInserted(nextNode, nextKeyEnd, node[i], BTree.getChildCount(nextNode), newChild);
      node[i] = rightNeighbour[0];
      node[keyEnd + i + 1] = copyWithKeyAndChildRemoved(rightNeighbour, 0, 0, true);
      int[] var10000 = BTree.getSizeMap(node);
      var10000[i] += leaves?1:1 + BTree.size((Object[])((Object[])newNextNode[BTree.getChildEnd(newNextNode) - 1]));
      return newNextNode;
   }

   private static Object[] rotateLeft(Object[] node, int i) {
      int keyEnd = BTree.getBranchKeyEnd(node);
      Object[] nextNode = (Object[])((Object[])node[keyEnd + i]);
      Object[] leftNeighbour = (Object[])((Object[])node[keyEnd + i - 1]);
      int leftNeighbourEndKey = BTree.getKeyEnd(leftNeighbour);
      boolean leaves = BTree.isLeaf(nextNode);
      Object[] newChild = leaves?null:(Object[])((Object[])leftNeighbour[BTree.getChildEnd(leftNeighbour) - 1]);
      Object[] newNextNode = copyWithKeyAndChildInserted(nextNode, 0, node[i - 1], 0, newChild);
      node[i - 1] = leftNeighbour[leftNeighbourEndKey - 1];
      node[keyEnd + i - 1] = copyWithKeyAndChildRemoved(leftNeighbour, leftNeighbourEndKey - 1, leftNeighbourEndKey, true);
      int[] var10000 = BTree.getSizeMap(node);
      var10000[i - 1] -= leaves?1:1 + BTree.getSizeMap(newNextNode)[0];
      return newNextNode;
   }

   private static <V> Object[] copyWithKeyAndChildInserted(Object[] node, int keyIndex, V key, int childIndex, Object[] child) {
      boolean leaf = BTree.isLeaf(node);
      int keyEnd = BTree.getKeyEnd(node);
      Object[] copy;
      if(leaf) {
         copy = new Object[keyEnd + ((keyEnd & 1) == 1?2:1)];
      } else {
         copy = new Object[node.length + 2];
      }

      if(keyIndex > 0) {
         System.arraycopy(node, 0, copy, 0, keyIndex);
      }

      copy[keyIndex] = key;
      if(keyIndex < keyEnd) {
         System.arraycopy(node, keyIndex, copy, keyIndex + 1, keyEnd - keyIndex);
      }

      if(!leaf) {
         if(childIndex > 0) {
            System.arraycopy(node, BTree.getChildStart(node), copy, keyEnd + 1, childIndex);
         }

         copy[keyEnd + 1 + childIndex] = child;
         if(childIndex <= keyEnd) {
            System.arraycopy(node, BTree.getChildStart(node) + childIndex, copy, keyEnd + childIndex + 2, keyEnd - childIndex + 1);
         }

         int[] sizeMap = BTree.getSizeMap(node);
         int[] newSizeMap = new int[sizeMap.length + 1];
         if(childIndex > 0) {
            System.arraycopy(sizeMap, 0, newSizeMap, 0, childIndex);
         }

         int childSize = BTree.size(child);
         newSizeMap[childIndex] = childSize + (childIndex == 0?0:newSizeMap[childIndex - 1] + 1);

         for(int i = childIndex + 1; i < newSizeMap.length; ++i) {
            newSizeMap[i] = sizeMap[i - 1] + childSize + 1;
         }

         copy[copy.length - 1] = newSizeMap;
      }

      return copy;
   }

   private static Object[] copyWithKeyAndChildRemoved(Object[] node, int keyIndex, int childIndex, boolean substractSize) {
      boolean leaf = BTree.isLeaf(node);
      Object[] newNode;
      int offset;
      if(leaf) {
         offset = BTree.getKeyEnd(node);
         newNode = new Object[offset - ((offset & 1) == 1?0:1)];
      } else {
         newNode = new Object[node.length - 2];
      }

      offset = copyKeys(node, newNode, 0, keyIndex);
      if(!leaf) {
         offset = copyChildren(node, newNode, offset, childIndex);
         int[] nodeSizeMap = BTree.getSizeMap(node);
         int[] newNodeSizeMap = new int[nodeSizeMap.length - 1];
         int pos = 0;
         int sizeToRemove = BTree.size((Object[])((Object[])node[BTree.getChildStart(node) + childIndex])) + 1;

         for(int i = 0; i < nodeSizeMap.length; ++i) {
            if(i != childIndex) {
               newNodeSizeMap[pos++] = nodeSizeMap[i] - (substractSize && i > childIndex?sizeToRemove:0);
            }
         }

         newNode[offset] = newNodeSizeMap;
      }

      return newNode;
   }

   private static <V> Object[] merge(Object[] left, Object[] right, V nodeKey) {
      assert BTree.getKeyEnd(left) == BTree.MINIMAL_NODE_SIZE;

      assert BTree.getKeyEnd(right) == BTree.MINIMAL_NODE_SIZE;

      boolean leaves = BTree.isLeaf(left);
      Object[] result;
      if(leaves) {
         result = new Object[BTree.MINIMAL_NODE_SIZE * 2 + 1];
      } else {
         result = new Object[left.length + right.length];
      }

      int offset = 0;
      int offset = copyKeys(left, result, offset);
      result[offset++] = nodeKey;
      offset = copyKeys(right, result, offset);
      if(!leaves) {
         offset = copyChildren(left, result, offset);
         copyChildren(right, result, offset);
         int[] leftSizeMap = BTree.getSizeMap(left);
         int[] rightSizeMap = BTree.getSizeMap(right);
         int[] newSizeMap = new int[leftSizeMap.length + rightSizeMap.length];
         offset = 0;
         offset = copySizeMap(leftSizeMap, newSizeMap, offset, 0);
         copySizeMap(rightSizeMap, newSizeMap, offset, leftSizeMap[leftSizeMap.length - 1] + 1);
         result[result.length - 1] = newSizeMap;
      }

      return result;
   }

   private static int copyKeys(Object[] from, Object[] to, int offset) {
      int keysCount = BTree.getKeyEnd(from);
      System.arraycopy(from, 0, to, offset, keysCount);
      return offset + keysCount;
   }

   private static int copyKeys(Object[] from, Object[] to, int offset, int skipIndex) {
      int keysCount = BTree.getKeyEnd(from);
      if(skipIndex > 0) {
         System.arraycopy(from, 0, to, offset, skipIndex);
      }

      if(skipIndex + 1 < keysCount) {
         System.arraycopy(from, skipIndex + 1, to, offset + skipIndex, keysCount - skipIndex - 1);
      }

      return offset + keysCount - 1;
   }

   private static int copyChildren(Object[] from, Object[] to, int offset) {
      assert !BTree.isLeaf(from);

      int start = BTree.getChildStart(from);
      int childCount = BTree.getChildCount(from);
      System.arraycopy(from, start, to, offset, childCount);
      return offset + childCount;
   }

   private static int copyChildren(Object[] from, Object[] to, int offset, int skipIndex) {
      assert !BTree.isLeaf(from);

      int start = BTree.getChildStart(from);
      int childCount = BTree.getChildCount(from);
      if(skipIndex > 0) {
         System.arraycopy(from, start, to, offset, skipIndex);
      }

      if(skipIndex + 1 <= childCount) {
         System.arraycopy(from, start + skipIndex + 1, to, offset + skipIndex, childCount - skipIndex - 1);
      }

      return offset + childCount - 1;
   }

   private static int copySizeMap(int[] from, int[] to, int offset, int extra) {
      for(int i = 0; i < from.length; ++i) {
         to[offset + i] = from[i] + extra;
      }

      return offset + from.length;
   }

   private static Object[] copyIfNeeded(Object[] node, boolean needCopy) {
      if(!needCopy) {
         return node;
      } else {
         Object[] copy = Arrays.copyOf(node, node.length);
         if(!BTree.isLeaf(node)) {
            int[] sizeMap = BTree.getSizeMap(node);
            int[] copySizeMap = Arrays.copyOf(sizeMap, sizeMap.length);
            copy[copy.length - 1] = copySizeMap;
         }

         return copy;
      }
   }
}
