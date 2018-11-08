package org.apache.cassandra.io.tries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.PageAware;

public class IncrementalTrieWriterPageAware<Value> extends IncrementalTrieWriterBase<Value, DataOutputPlus, IncrementalTrieWriterPageAware.Node<Value>> implements IncrementalTrieWriter<Value> {
   private static final Comparator<IncrementalTrieWriterPageAware.Node<?>> branchSizeComparator = (l, r) -> {
      int c = Integer.compare(l.branchSize + l.nodeSize, r.branchSize + r.nodeSize);
      if(c != 0) {
         return c;
      } else {
         c = Integer.compare(l.transition, r.transition);

         assert c != 0 || l == r;

         return c;
      }
   };

   public IncrementalTrieWriterPageAware(TrieSerializer<Value, ? super DataOutputPlus> serializer, DataOutputPlus dest) {
      super(serializer, dest, new IncrementalTrieWriterPageAware.Node(0));
   }

   IncrementalTrieWriterPageAware.Node<Value> performCompletion() throws IOException {
      IncrementalTrieWriterPageAware.Node<Value> root = (IncrementalTrieWriterPageAware.Node)super.performCompletion();
      int actualSize = this.recalcTotalSize(root, ((DataOutputPlus)this.dest).position());
      int bytesLeft = this.bytesLeftInPage();
      if(actualSize > bytesLeft) {
         if(actualSize <= 4096) {
            PageAware.pad((DataOutputPlus)this.dest);
            bytesLeft = 4096;
            actualSize = this.recalcTotalSize(root, ((DataOutputPlus)this.dest).position());
         }

         if(actualSize > bytesLeft) {
            this.layoutChildren(root);
            if(root.nodeSize > this.bytesLeftInPage()) {
               PageAware.pad((DataOutputPlus)this.dest);
               this.recalcTotalSize(root, ((DataOutputPlus)this.dest).position());
            }
         }
      }

      root.finalizeWithPosition(this.writeRecursive(root));
      return root;
   }

   void complete(IncrementalTrieWriterPageAware.Node<Value> node) throws IOException {
      assert node.filePos == -1L;

      int branchSize = 0;

      IncrementalTrieWriterPageAware.Node child;
      for(Iterator var3 = node.children.iterator(); var3.hasNext(); branchSize += child.branchSize + child.nodeSize) {
         child = (IncrementalTrieWriterPageAware.Node)var3.next();
      }

      node.branchSize = branchSize;
      int nodeSize = this.serializer.sizeofNode(node, ((DataOutputPlus)this.dest).position());
      if(nodeSize + branchSize >= 4096) {
         this.layoutChildren(node);
      } else {
         node.nodeSize = nodeSize;
         node.hasOutOfPageChildren = false;
         node.hasOutOfPageInBranch = false;
         Iterator var7 = node.children.iterator();

         while(true) {
            while(var7.hasNext()) {
               IncrementalTrieWriterPageAware.Node<Value> child = (IncrementalTrieWriterPageAware.Node)var7.next();
               if(child.filePos != -1L) {
                  node.hasOutOfPageChildren = true;
               } else if(child.hasOutOfPageChildren || child.hasOutOfPageInBranch) {
                  node.hasOutOfPageInBranch = true;
               }
            }

            return;
         }
      }
   }

   void layoutChildren(IncrementalTrieWriterPageAware.Node<Value> node) throws IOException {
      assert node.filePos == -1L;

      NavigableSet<IncrementalTrieWriterPageAware.Node<Value>> children = new TreeSet(branchSizeComparator);
      Iterator var3 = node.children.iterator();

      IncrementalTrieWriterPageAware.Node cmp;
      while(var3.hasNext()) {
         cmp = (IncrementalTrieWriterPageAware.Node)var3.next();
         if(cmp.filePos == -1L) {
            children.add(cmp);
         }
      }

      int bytesLeft = this.bytesLeftInPage();
      cmp = new IncrementalTrieWriterPageAware.Node(256);
      cmp.nodeSize = 0;

      while(true) {
         while(!children.isEmpty()) {
            cmp.branchSize = bytesLeft;
            IncrementalTrieWriterPageAware.Node<Value> child = (IncrementalTrieWriterPageAware.Node)children.headSet(cmp, true).pollLast();
            if(child == null) {
               PageAware.pad((DataOutputPlus)this.dest);
               bytesLeft = 4096;
               child = (IncrementalTrieWriterPageAware.Node)children.pollLast();
            }

            if(child.hasOutOfPageChildren || child.hasOutOfPageInBranch) {
               int actualSize = this.recalcTotalSize(child, ((DataOutputPlus)this.dest).position());
               if(actualSize > bytesLeft) {
                  if(bytesLeft == 4096) {
                     this.layoutChildren(child);
                     bytesLeft = this.bytesLeftInPage();

                     assert child.filePos == -1L;
                  }

                  children.add(child);
                  continue;
               }
            }

            child.finalizeWithPosition(this.writeRecursive(child));
            bytesLeft = this.bytesLeftInPage();
         }

         node.branchSize = 0;
         node.hasOutOfPageChildren = true;
         node.hasOutOfPageInBranch = false;
         node.nodeSize = this.serializer.sizeofNode(node, ((DataOutputPlus)this.dest).position());
         return;
      }
   }

   private int recalcTotalSize(IncrementalTrieWriterPageAware.Node<Value> node, long nodePosition) throws IOException {
      if(node.hasOutOfPageInBranch) {
         (new IncrementalTrieWriterPageAware.RecalcTotalSizeRecursion(node, (IncrementalTrieWriterPageAware.Recursion)null, nodePosition)).process();
      }

      if(node.hasOutOfPageChildren || node.hasOutOfPageInBranch) {
         node.nodeSize = this.serializer.sizeofNode(node, nodePosition + (long)node.branchSize);
      }

      return node.branchSize + node.nodeSize;
   }

   private long writeRecursive(IncrementalTrieWriterPageAware.Node<Value> node) throws IOException {
      return ((IncrementalTrieWriterPageAware.Node)(new IncrementalTrieWriterPageAware.WriteRecursion(node, (IncrementalTrieWriterPageAware.Recursion)null)).process().node).filePos;
   }

   String dumpNode(IncrementalTrieWriterPageAware.Node<Value> node, long nodePosition) {
      String res = String.format("At %,d(%x) type %s child count %s nodeSize %,d branchSize %,d %s%s\n", new Object[]{Long.valueOf(nodePosition), Long.valueOf(nodePosition), TrieNode.typeFor(node, nodePosition), Integer.valueOf(node.childCount()), Integer.valueOf(node.nodeSize), Integer.valueOf(node.branchSize), node.hasOutOfPageChildren?"C":"", node.hasOutOfPageInBranch?"B":""});

      IncrementalTrieWriterPageAware.Node child;
      for(Iterator var5 = node.children.iterator(); var5.hasNext(); res = res + String.format("Child %2x at %,d(%x) type %s child count %s size %s nodeSize %,d branchSize %,d %s%s\n", new Object[]{Integer.valueOf(child.transition & 255), Long.valueOf(child.filePos), Long.valueOf(child.filePos), child.children != null?TrieNode.typeFor(child, child.filePos):"n/a", child.children != null?Integer.valueOf(child.childCount()):"n/a", child.children != null?Integer.valueOf(this.serializer.sizeofNode(child, child.filePos)):"n/a", Integer.valueOf(child.nodeSize), Integer.valueOf(child.branchSize), child.hasOutOfPageChildren?"C":"", child.hasOutOfPageInBranch?"B":""})) {
         child = (IncrementalTrieWriterPageAware.Node)var5.next();
      }

      return res;
   }

   int bytesLeftInPage() {
      long position = ((DataOutputPlus)this.dest).position();
      long bytesLeft = PageAware.pageLimit(position) - position;
      return (int)bytesLeft;
   }

   public IncrementalTrieWriter.PartialTail makePartialRoot() throws IOException {
      DataOutputBuffer buf = new DataOutputBuffer();
      Throwable var2 = null;

      IncrementalTrieWriterBase.PTail var4;
      try {
         IncrementalTrieWriterBase.PTail tail = new IncrementalTrieWriterBase.PTail();
         tail.cutoff = PageAware.padded(((DataOutputPlus)this.dest).position());
         tail.count = this.count;
         tail.root = this.writePartialRecursive((IncrementalTrieWriterPageAware.Node)this.stack.getFirst(), buf, tail.cutoff);
         tail.tail = buf.trimmedBuffer();
         var4 = tail;
      } catch (Throwable var13) {
         var2 = var13;
         throw var13;
      } finally {
         if(buf != null) {
            if(var2 != null) {
               try {
                  buf.close();
               } catch (Throwable var12) {
                  var2.addSuppressed(var12);
               }
            } else {
               buf.close();
            }
         }

      }

      return var4;
   }

   private long writePartialRecursive(IncrementalTrieWriterPageAware.Node<Value> node, DataOutputPlus dest, long baseOffset) throws IOException {
      (new IncrementalTrieWriterPageAware.WritePartialRecursion(node, dest, baseOffset)).process();
      long pos = node.filePos;
      node.filePos = -1L;
      return pos;
   }

   static class Node<Value> extends IncrementalTrieWriterBase.BaseNode<Value, IncrementalTrieWriterPageAware.Node<Value>> {
      int branchSize = -1;
      int nodeSize = -1;
      boolean hasOutOfPageInBranch = false;
      boolean hasOutOfPageChildren = true;

      Node(int transition) {
         super(transition);
      }

      IncrementalTrieWriterPageAware.Node<Value> newNode(byte transition) {
         return new IncrementalTrieWriterPageAware.Node(transition & 255);
      }

      public long serializedPositionDelta(int i, long nodePosition) {
         assert ((IncrementalTrieWriterPageAware.Node)this.children.get(i)).filePos != -1L;

         return ((IncrementalTrieWriterPageAware.Node)this.children.get(i)).filePos - nodePosition;
      }

      public long maxPositionDelta(long nodePosition) {
         assert this.childCount() > 0;

         if(!this.hasOutOfPageChildren) {
            return (long)(-(this.branchSize - ((IncrementalTrieWriterPageAware.Node)this.children.get(0)).branchSize));
         } else {
            long minPlaced = 0L;
            long minUnplaced = 1L;
            Iterator var7 = this.children.iterator();

            while(var7.hasNext()) {
               IncrementalTrieWriterPageAware.Node<Value> child = (IncrementalTrieWriterPageAware.Node)var7.next();
               if(child.filePos != -1L) {
                  minPlaced = Math.min(minPlaced, child.filePos - nodePosition);
               } else if(minUnplaced > 0L) {
                  minUnplaced = (long)(-(this.branchSize - child.branchSize));
               }
            }

            return Math.min(minPlaced, minUnplaced);
         }
      }

      void finalizeWithPosition(long position) {
         this.branchSize = 0;
         this.nodeSize = 0;
         this.hasOutOfPageInBranch = false;
         this.hasOutOfPageChildren = false;
         super.finalizeWithPosition(position);
      }

      public String toString() {
         return String.format("%02x branchSize=%04x nodeSize=%04x %s%s", new Object[]{Integer.valueOf(this.transition), Integer.valueOf(this.branchSize), Integer.valueOf(this.nodeSize), this.hasOutOfPageInBranch?"B":"", this.hasOutOfPageChildren?"C":""});
      }
   }

   class WritePartialRecursion extends IncrementalTrieWriterPageAware.Recursion<IncrementalTrieWriterPageAware.Node<Value>> {
      final DataOutputPlus dest;
      final long baseOffset;
      final long startPosition;
      final List<IncrementalTrieWriterPageAware.Node<Value>> childrenToClear;

      WritePartialRecursion(IncrementalTrieWriterPageAware.Node<Value> this$0, IncrementalTrieWriterPageAware<Value>.WritePartialRecursion node) {
         super(node, node.children.iterator(), parent);
         this.dest = parent.dest;
         this.baseOffset = parent.baseOffset;
         this.startPosition = this.dest.position() + this.baseOffset;
         this.childrenToClear = new ArrayList();
      }

      WritePartialRecursion(IncrementalTrieWriterPageAware.Node<Value> this$0, DataOutputPlus node, long dest) {
         super(node, node.children.iterator(), (IncrementalTrieWriterPageAware.Recursion)null);
         this.dest = dest;
         this.baseOffset = baseOffset;
         this.startPosition = dest.position() + baseOffset;
         this.childrenToClear = new ArrayList();
      }

      IncrementalTrieWriterPageAware.Recursion<IncrementalTrieWriterPageAware.Node<Value>> makeChild(IncrementalTrieWriterPageAware.Node<Value> child) {
         if(child.filePos == -1L) {
            this.childrenToClear.add(child);
            return IncrementalTrieWriterPageAware.this.new WritePartialRecursion(child, this);
         } else {
            return null;
         }
      }

      void complete() throws IOException {
         long nodePosition = this.dest.position() + this.baseOffset;
         if(((IncrementalTrieWriterPageAware.Node)this.node).hasOutOfPageInBranch) {
            ((IncrementalTrieWriterPageAware.Node)this.node).branchSize = (int)(nodePosition - this.startPosition);
         }

         IncrementalTrieWriterPageAware.this.serializer.write(this.dest, (SerializationNode)this.node, nodePosition);
         if(((IncrementalTrieWriterPageAware.Node)this.node).hasOutOfPageChildren || ((IncrementalTrieWriterPageAware.Node)this.node).hasOutOfPageInBranch) {
            long endPosition = this.dest.position() + this.baseOffset;
            ((IncrementalTrieWriterPageAware.Node)this.node).nodeSize = (int)(endPosition - nodePosition);
         }

         IncrementalTrieWriterPageAware.Node child;
         for(Iterator var5 = this.childrenToClear.iterator(); var5.hasNext(); child.filePos = -1L) {
            child = (IncrementalTrieWriterPageAware.Node)var5.next();
         }

         ((IncrementalTrieWriterPageAware.Node)this.node).filePos = nodePosition;
      }
   }

   class WriteRecursion extends IncrementalTrieWriterPageAware.Recursion<IncrementalTrieWriterPageAware.Node<Value>> {
      long nodePosition;

      WriteRecursion(IncrementalTrieWriterPageAware.Node<Value> this$0, IncrementalTrieWriterPageAware.Recursion<IncrementalTrieWriterPageAware.Node<Value>> node) {
         super(node, node.children.iterator(), parent);
         this.nodePosition = ((DataOutputPlus)IncrementalTrieWriterPageAware.this.dest).position();
      }

      IncrementalTrieWriterPageAware.Recursion<IncrementalTrieWriterPageAware.Node<Value>> makeChild(IncrementalTrieWriterPageAware.Node<Value> child) {
         return child.filePos == -1L?IncrementalTrieWriterPageAware.this.new WriteRecursion(child, this):null;
      }

      void complete() throws IOException {
         this.nodePosition += (long)((IncrementalTrieWriterPageAware.Node)this.node).branchSize;

         assert ((DataOutputPlus)IncrementalTrieWriterPageAware.this.dest).position() == this.nodePosition : "Expected node position to be " + this.nodePosition + " but got " + ((DataOutputPlus)IncrementalTrieWriterPageAware.this.dest).position() + " after writing children.\n" + IncrementalTrieWriterPageAware.this.dumpNode((IncrementalTrieWriterPageAware.Node)this.node, ((DataOutputPlus)IncrementalTrieWriterPageAware.this.dest).position());

         IncrementalTrieWriterPageAware.this.serializer.write(IncrementalTrieWriterPageAware.this.dest, (SerializationNode)this.node, this.nodePosition);

         assert ((DataOutputPlus)IncrementalTrieWriterPageAware.this.dest).position() == this.nodePosition + (long)((IncrementalTrieWriterPageAware.Node)this.node).nodeSize || PageAware.padded(((DataOutputPlus)IncrementalTrieWriterPageAware.this.dest).position()) == ((DataOutputPlus)IncrementalTrieWriterPageAware.this.dest).position() : "Expected node position to be " + (this.nodePosition + (long)((IncrementalTrieWriterPageAware.Node)this.node).nodeSize) + " but got " + ((DataOutputPlus)IncrementalTrieWriterPageAware.this.dest).position() + " after writing node, nodeSize " + ((IncrementalTrieWriterPageAware.Node)this.node).nodeSize + ".\n" + IncrementalTrieWriterPageAware.this.dumpNode((IncrementalTrieWriterPageAware.Node)this.node, this.nodePosition);

         ((IncrementalTrieWriterPageAware.Node)this.node).filePos = this.nodePosition;
      }
   }

   class RecalcTotalSizeRecursion extends IncrementalTrieWriterPageAware.Recursion<IncrementalTrieWriterPageAware.Node<Value>> {
      final long nodePosition;
      int sz = 0;

      RecalcTotalSizeRecursion(IncrementalTrieWriterPageAware.Node<Value> this$0, IncrementalTrieWriterPageAware.Recursion<IncrementalTrieWriterPageAware.Node<Value>> node, long parent) {
         super(node, node.children.iterator(), parent);
         this.nodePosition = nodePosition;
      }

      IncrementalTrieWriterPageAware.Recursion<IncrementalTrieWriterPageAware.Node<Value>> makeChild(IncrementalTrieWriterPageAware.Node<Value> child) {
         return child.hasOutOfPageInBranch?IncrementalTrieWriterPageAware.this.new RecalcTotalSizeRecursion(child, this, this.nodePosition + (long)this.sz):null;
      }

      void complete() {
         ((IncrementalTrieWriterPageAware.Node)this.node).branchSize = this.sz;
      }

      void completeChild(IncrementalTrieWriterPageAware.Node<Value> child) {
         if(child.hasOutOfPageChildren || child.hasOutOfPageInBranch) {
            long childPosition = this.nodePosition + (long)this.sz;
            child.nodeSize = IncrementalTrieWriterPageAware.this.serializer.sizeofNode(child, childPosition + (long)child.branchSize);
         }

         this.sz += child.branchSize + child.nodeSize;
      }
   }

   abstract static class Recursion<NodeType> {
      final IncrementalTrieWriterPageAware.Recursion<NodeType> parent;
      final NodeType node;
      final Iterator<NodeType> childIterator;

      Recursion(NodeType node, Iterator<NodeType> childIterator, IncrementalTrieWriterPageAware.Recursion<NodeType> parent) {
         this.parent = parent;
         this.node = node;
         this.childIterator = childIterator;
      }

      abstract IncrementalTrieWriterPageAware.Recursion<NodeType> makeChild(NodeType var1);

      abstract void complete() throws IOException;

      void completeChild(NodeType child) {
      }

      IncrementalTrieWriterPageAware.Recursion<NodeType> process() throws IOException {
         IncrementalTrieWriterPageAware.Recursion curr = this;

         while(true) {
            while(!curr.childIterator.hasNext()) {
               curr.complete();
               IncrementalTrieWriterPageAware.Recursion<NodeType> parent = curr.parent;
               if(parent == null) {
                  return curr;
               }

               parent.completeChild(curr.node);
               curr = parent;
            }

            NodeType child = curr.childIterator.next();
            IncrementalTrieWriterPageAware.Recursion<NodeType> childRec = curr.makeChild(child);
            if(childRec != null) {
               curr = childRec;
            } else {
               curr.completeChild(child);
            }
         }
      }
   }
}
