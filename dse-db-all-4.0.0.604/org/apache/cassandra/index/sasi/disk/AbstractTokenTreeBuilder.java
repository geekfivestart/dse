package org.apache.cassandra.index.sasi.disk;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongSet;
import com.carrotsearch.hppc.cursors.LongCursor;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

public abstract class AbstractTokenTreeBuilder implements TokenTreeBuilder {
   protected int numBlocks;
   protected AbstractTokenTreeBuilder.Node root;
   protected AbstractTokenTreeBuilder.InteriorNode rightmostParent;
   protected AbstractTokenTreeBuilder.Leaf leftmostLeaf;
   protected AbstractTokenTreeBuilder.Leaf rightmostLeaf;
   protected long tokenCount = 0L;
   protected long treeMinToken;
   protected long treeMaxToken;

   public AbstractTokenTreeBuilder() {
   }

   public void add(TokenTreeBuilder other) {
      this.add(other.iterator());
   }

   public TokenTreeBuilder finish() {
      if(this.root == null) {
         this.constructTree();
      }

      return this;
   }

   public long getTokenCount() {
      return this.tokenCount;
   }

   public int serializedSize() {
      return this.numBlocks == 1?64 + (int)this.tokenCount * 16 + ((AbstractTokenTreeBuilder.Leaf)this.root).overflowCollisionCount() * 8:this.numBlocks * 4096;
   }

   public void write(DataOutputPlus out) throws IOException {
      ByteBuffer blockBuffer = ByteBuffer.allocate(4096);
      Iterator<AbstractTokenTreeBuilder.Node> levelIterator = this.root.levelIterator();

      AbstractTokenTreeBuilder.Node firstChild;
      AbstractTokenTreeBuilder.Node block;
      for(long childBlockIndex = 1L; levelIterator != null; levelIterator = firstChild == null?null:firstChild.levelIterator()) {
         for(firstChild = null; levelIterator.hasNext(); childBlockIndex += (long)block.childCount()) {
            block = (AbstractTokenTreeBuilder.Node)levelIterator.next();
            if(firstChild == null && !block.isLeaf()) {
               firstChild = (AbstractTokenTreeBuilder.Node)((AbstractTokenTreeBuilder.InteriorNode)block).children.get(0);
            }

            if(block.isSerializable()) {
               block.serialize(childBlockIndex, blockBuffer);
               this.flushBuffer(blockBuffer, out, this.numBlocks != 1);
            }
         }
      }

   }

   protected abstract void constructTree();

   protected void flushBuffer(ByteBuffer buffer, DataOutputPlus o, boolean align) throws IOException {
      if(align) {
         alignBuffer(buffer, 4096);
      }

      buffer.flip();
      o.write(buffer);
      buffer.clear();
   }

   protected static void alignBuffer(ByteBuffer buffer, int blockSize) {
      long curPos = (long)buffer.position();
      if((curPos & (long)(blockSize - 1)) != 0L) {
         buffer.position((int)FBUtilities.align(curPos, blockSize));
      }

   }

   public static class LevelIterator extends AbstractIterator<AbstractTokenTreeBuilder.Node> {
      private AbstractTokenTreeBuilder.Node currentNode;

      LevelIterator(AbstractTokenTreeBuilder.Node first) {
         this.currentNode = first;
      }

      public AbstractTokenTreeBuilder.Node computeNext() {
         if(this.currentNode == null) {
            return (AbstractTokenTreeBuilder.Node)this.endOfData();
         } else {
            AbstractTokenTreeBuilder.Node returnNode = this.currentNode;
            this.currentNode = returnNode.next;
            return returnNode;
         }
      }
   }

   protected class InteriorNode extends AbstractTokenTreeBuilder.Node {
      protected List<Long> tokens = new ArrayList(248);
      protected List<AbstractTokenTreeBuilder.Node> children = new ArrayList(249);
      protected int position = 0;

      public InteriorNode() {
         super((Long)null, (Long)null);
      }

      public boolean isSerializable() {
         return true;
      }

      public void serialize(long childBlockIndex, ByteBuffer buf) {
         this.serializeHeader(buf);
         this.serializeTokens(buf);
         this.serializeChildOffsets(childBlockIndex, buf);
      }

      public int childCount() {
         return this.children.size();
      }

      public int tokenCount() {
         return this.tokens.size();
      }

      public Long smallestToken() {
         return (Long)this.tokens.get(0);
      }

      protected void add(Long token, AbstractTokenTreeBuilder.InteriorNode leftChild, AbstractTokenTreeBuilder.InteriorNode rightChild) {
         int pos = this.tokens.size();
         if(pos == 248) {
            AbstractTokenTreeBuilder.InteriorNode sibling = this.split();
            sibling.add(token, leftChild, rightChild);
         } else {
            if(leftChild != null) {
               this.children.add(pos, leftChild);
            }

            if(rightChild != null) {
               this.children.add(pos + 1, rightChild);
               rightChild.parent = this;
            }

            this.updateTokenRange(token.longValue());
            this.tokens.add(pos, token);
         }

      }

      protected void add(AbstractTokenTreeBuilder.Leaf node) {
         if(this.position == 249) {
            AbstractTokenTreeBuilder.this.rightmostParent = this.split();
            AbstractTokenTreeBuilder.this.rightmostParent.add(node);
         } else {
            node.parent = this;
            this.children.add(this.position, node);
            ++this.position;
            if(this.position - 1 == 0) {
               return;
            }

            Long smallestToken = node.smallestToken();
            this.updateTokenRange(smallestToken.longValue());
            this.tokens.add(this.position - 2, smallestToken);
         }

      }

      protected AbstractTokenTreeBuilder.InteriorNode split() {
         Pair<Long, AbstractTokenTreeBuilder.InteriorNode> splitResult = this.splitBlock();
         Long middleValue = (Long)splitResult.left;
         AbstractTokenTreeBuilder.InteriorNode sibling = (AbstractTokenTreeBuilder.InteriorNode)splitResult.right;
         AbstractTokenTreeBuilder.InteriorNode leftChild = null;
         if(this.parent == null) {
            this.parent = AbstractTokenTreeBuilder.this.new InteriorNode();
            AbstractTokenTreeBuilder.this.root = this.parent;
            sibling.parent = this.parent;
            leftChild = this;
            ++AbstractTokenTreeBuilder.this.numBlocks;
         }

         this.parent.add(middleValue, leftChild, sibling);
         return sibling;
      }

      protected Pair<Long, AbstractTokenTreeBuilder.InteriorNode> splitBlock() {
         int splitPosition = 246;
         AbstractTokenTreeBuilder.InteriorNode sibling = AbstractTokenTreeBuilder.this.new InteriorNode();
         sibling.parent = this.parent;
         this.next = sibling;
         Long middleValue = (Long)this.tokens.get(246);

         int i;
         for(i = 246; i < 248; ++i) {
            if(i != 248 && i != 246) {
               long token = ((Long)this.tokens.get(i)).longValue();
               sibling.updateTokenRange(token);
               sibling.tokens.add(Long.valueOf(token));
            }

            AbstractTokenTreeBuilder.Node child = (AbstractTokenTreeBuilder.Node)this.children.get(i + 1);
            child.parent = sibling;
            sibling.children.add(child);
            ++sibling.position;
         }

         for(i = 248; i >= 246; --i) {
            if(i != 248) {
               this.tokens.remove(i);
            }

            if(i != 246) {
               this.children.remove(i);
            }
         }

         this.nodeMinToken = this.smallestToken();
         this.nodeMaxToken = (Long)this.tokens.get(this.tokens.size() - 1);
         ++AbstractTokenTreeBuilder.this.numBlocks;
         return Pair.create(middleValue, sibling);
      }

      protected boolean isFull() {
         return this.position >= 249;
      }

      private void serializeTokens(ByteBuffer buf) {
         this.tokens.forEach(buf::putLong);
      }

      private void serializeChildOffsets(long childBlockIndex, ByteBuffer buf) {
         for(int i = 0; i < this.children.size(); ++i) {
            buf.putLong((childBlockIndex + (long)i) * 4096L);
         }

      }
   }

   protected abstract class Leaf extends AbstractTokenTreeBuilder.Node {
      protected LongArrayList overflowCollisions;

      public Leaf(Long minToken, Long maxToken) {
         super(minToken, maxToken);
      }

      public int childCount() {
         return 0;
      }

      public int overflowCollisionCount() {
         return this.overflowCollisions == null?0:this.overflowCollisions.size();
      }

      protected void serializeOverflowCollisions(ByteBuffer buf) {
         if(this.overflowCollisions != null) {
            Iterator var2 = this.overflowCollisions.iterator();

            while(var2.hasNext()) {
               LongCursor offset = (LongCursor)var2.next();
               buf.putLong(offset.value);
            }
         }

      }

      public void serialize(long childBlockIndex, ByteBuffer buf) {
         this.serializeHeader(buf);
         this.serializeData(buf);
         this.serializeOverflowCollisions(buf);
      }

      protected abstract void serializeData(ByteBuffer var1);

      protected AbstractTokenTreeBuilder.Leaf.LeafEntry createEntry(long tok, LongSet offsets) {
         int offsetCount = offsets.size();
         switch(offsetCount) {
         case 0:
            throw new AssertionError("no offsets for token " + tok);
         case 1:
            long offset = offsets.toArray()[0];
            if(offset > 140737488355327L) {
               throw new AssertionError("offset " + offset + " cannot be greater than " + 140737488355327L);
            } else {
               if(offset <= 2147483647L) {
                  return new AbstractTokenTreeBuilder.Leaf.SimpleLeafEntry(tok, offset);
               }

               return new AbstractTokenTreeBuilder.Leaf.FactoredOffsetLeafEntry(tok, offset);
            }
         case 2:
            long[] rawOffsets = offsets.toArray();
            return (AbstractTokenTreeBuilder.Leaf.LeafEntry)(rawOffsets[0] > 2147483647L || rawOffsets[1] > 2147483647L || rawOffsets[0] > 32767L && rawOffsets[1] > 32767L?this.createOverflowEntry(tok, offsetCount, offsets):new AbstractTokenTreeBuilder.Leaf.PackedCollisionLeafEntry(tok, rawOffsets));
         default:
            return this.createOverflowEntry(tok, offsetCount, offsets);
         }
      }

      private AbstractTokenTreeBuilder.Leaf.LeafEntry createOverflowEntry(long tok, int offsetCount, LongSet offsets) {
         if(this.overflowCollisions == null) {
            this.overflowCollisions = new LongArrayList();
         }

         AbstractTokenTreeBuilder.Leaf.LeafEntry entry = new AbstractTokenTreeBuilder.Leaf.OverflowCollisionLeafEntry(tok, (short)this.overflowCollisions.size(), (short)offsetCount);
         Iterator var6 = offsets.iterator();

         while(var6.hasNext()) {
            LongCursor o = (LongCursor)var6.next();
            if(this.overflowCollisions.size() == 8) {
               throw new AssertionError("cannot have more than 8 overflow collisions per leaf");
            }

            this.overflowCollisions.add(o.value);
         }

         return entry;
      }

      private class OverflowCollisionLeafEntry extends AbstractTokenTreeBuilder.Leaf.LeafEntry {
         private final short startIndex;
         private final short count;

         public OverflowCollisionLeafEntry(long tok, short collisionStartIndex, short collisionCount) {
            super(tok);
            this.startIndex = collisionStartIndex;
            this.count = collisionCount;
         }

         public TokenTreeBuilder.EntryType type() {
            return TokenTreeBuilder.EntryType.OVERFLOW;
         }

         public int offsetData() {
            return this.startIndex;
         }

         public short offsetExtra() {
            return this.count;
         }
      }

      private class PackedCollisionLeafEntry extends AbstractTokenTreeBuilder.Leaf.LeafEntry {
         private short smallerOffset;
         private int largerOffset;

         public PackedCollisionLeafEntry(long tok, long[] offs) {
            super(tok);
            this.smallerOffset = (short)((int)Math.min(offs[0], offs[1]));
            this.largerOffset = (int)Math.max(offs[0], offs[1]);
         }

         public TokenTreeBuilder.EntryType type() {
            return TokenTreeBuilder.EntryType.PACKED;
         }

         public int offsetData() {
            return this.largerOffset;
         }

         public short offsetExtra() {
            return this.smallerOffset;
         }
      }

      private class FactoredOffsetLeafEntry extends AbstractTokenTreeBuilder.Leaf.LeafEntry {
         private final long offset;

         public FactoredOffsetLeafEntry(long tok, long off) {
            super(tok);
            this.offset = off;
         }

         public TokenTreeBuilder.EntryType type() {
            return TokenTreeBuilder.EntryType.FACTORED;
         }

         public int offsetData() {
            return (int)(this.offset >>> 16);
         }

         public short offsetExtra() {
            return (short)((int)this.offset);
         }
      }

      protected class SimpleLeafEntry extends AbstractTokenTreeBuilder.Leaf.LeafEntry {
         private final long offset;

         public SimpleLeafEntry(long tok, long off) {
            super(tok);
            this.offset = off;
         }

         public TokenTreeBuilder.EntryType type() {
            return TokenTreeBuilder.EntryType.SIMPLE;
         }

         public int offsetData() {
            return (int)this.offset;
         }

         public short offsetExtra() {
            return 0;
         }
      }

      protected abstract class LeafEntry {
         protected final long token;

         public abstract TokenTreeBuilder.EntryType type();

         public abstract int offsetData();

         public abstract short offsetExtra();

         public LeafEntry(long tok) {
            this.token = tok;
         }

         public void serialize(ByteBuffer buf) {
            buf.putShort((short)this.type().ordinal()).putShort(this.offsetExtra()).putLong(this.token).putInt(this.offsetData());
         }
      }
   }

   protected abstract class Node {
      protected AbstractTokenTreeBuilder.InteriorNode parent;
      protected AbstractTokenTreeBuilder.Node next;
      protected Long nodeMinToken;
      protected Long nodeMaxToken;

      public Node(Long minToken, Long maxToken) {
         this.nodeMinToken = minToken;
         this.nodeMaxToken = maxToken;
      }

      public abstract boolean isSerializable();

      public abstract void serialize(long var1, ByteBuffer var3);

      public abstract int childCount();

      public abstract int tokenCount();

      public Long smallestToken() {
         return this.nodeMinToken;
      }

      public Long largestToken() {
         return this.nodeMaxToken;
      }

      public Iterator<AbstractTokenTreeBuilder.Node> levelIterator() {
         return new AbstractTokenTreeBuilder.LevelIterator(this);
      }

      public boolean isLeaf() {
         return this instanceof AbstractTokenTreeBuilder.Leaf;
      }

      protected boolean isLastLeaf() {
         return this == AbstractTokenTreeBuilder.this.rightmostLeaf;
      }

      protected boolean isRoot() {
         return this == AbstractTokenTreeBuilder.this.root;
      }

      protected void updateTokenRange(long token) {
         this.nodeMinToken = Long.valueOf(this.nodeMinToken == null?token:Math.min(this.nodeMinToken.longValue(), token));
         this.nodeMaxToken = Long.valueOf(this.nodeMaxToken == null?token:Math.max(this.nodeMaxToken.longValue(), token));
      }

      protected void serializeHeader(ByteBuffer buf) {
         Object header;
         if(this.isRoot()) {
            header = new AbstractTokenTreeBuilder.Node.RootHeader();
         } else if(!this.isLeaf()) {
            header = new AbstractTokenTreeBuilder.Node.InteriorNodeHeader();
         } else {
            header = new AbstractTokenTreeBuilder.Node.LeafHeader();
         }

         ((AbstractTokenTreeBuilder.Node.Header)header).serialize(buf);
         AbstractTokenTreeBuilder.alignBuffer(buf, 64);
      }

      private class LeafHeader extends AbstractTokenTreeBuilder.Node.Header {
         private LeafHeader() {
            super();
         }

         protected byte infoByte() {
            byte infoByte = 1;
            byte infoBytex = (byte)(infoByte | (Node.this.isLastLeaf()?2:0));
            return infoBytex;
         }
      }

      private class InteriorNodeHeader extends AbstractTokenTreeBuilder.Node.Header {
         private InteriorNodeHeader() {
            super();
         }

         protected byte infoByte() {
            return 0;
         }
      }

      private class RootHeader extends AbstractTokenTreeBuilder.Node.Header {
         private RootHeader() {
            super();
         }

         public void serialize(ByteBuffer buf) {
            super.serialize(buf);
            this.writeMagic(buf);
            buf.putLong(AbstractTokenTreeBuilder.this.tokenCount).putLong(AbstractTokenTreeBuilder.this.treeMinToken).putLong(AbstractTokenTreeBuilder.this.treeMaxToken);
         }

         protected byte infoByte() {
            return (byte)(Node.this.isLeaf()?3:0);
         }

         protected void writeMagic(ByteBuffer buf) {
            switch ("ab") {
               case "ab": {
                  buf.putShort((short)23121);
                  break;
               }
            }
         }
      }

      private abstract class Header {
         private Header() {
         }

         public void serialize(ByteBuffer buf) {
            buf.put(this.infoByte()).putShort((short)Node.this.tokenCount()).putLong(Node.this.nodeMinToken.longValue()).putLong(Node.this.nodeMaxToken.longValue());
         }

         protected abstract byte infoByte();
      }
   }
}
