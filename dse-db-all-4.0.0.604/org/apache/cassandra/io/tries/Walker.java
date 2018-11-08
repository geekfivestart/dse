package org.apache.cassandra.io.tries;

import java.io.PrintStream;
import java.nio.ByteBuffer;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.utils.ByteSource;
import org.apache.cassandra.utils.PageAware;

public class Walker<Concrete extends Walker<Concrete>> implements AutoCloseable {
   private final Rebufferer source;
   protected final long root;
   private Rebufferer.BufferHolder bh;
   private int offset;
   protected TrieNode nodeType;
   protected ByteBuffer buf;
   protected long position;
   protected long greaterBranch;
   protected long lesserBranch;
   protected final Rebufferer.ReaderConstraint rc;

   public Walker(Rebufferer source, long root, Rebufferer.ReaderConstraint rc) {
      this.source = source;
      this.root = root;
      this.rc = rc;

      try {
         this.bh = source.rebuffer(PageAware.pageStart(root), rc);
         this.buf = this.bh.buffer();
      } catch (Throwable var6) {
         source.closeReader();
         throw var6;
      }
   }

   public void close() {
      this.bh.release();
      this.source.closeReader();
   }

   protected final void go(long position) {
      long offset = position - this.bh.offset();
      if(offset < 0L || offset >= (long)this.buf.limit()) {
         Rebufferer.BufferHolder currentBh = this.bh;
         this.bh = this.source.rebuffer(PageAware.pageStart(position), this.rc);
         currentBh.release();
         this.buf = this.bh.buffer();
         offset = position - this.bh.offset();

         assert offset >= 0L && offset < (long)this.buf.limit() : String.format("Invalid offset: %d, buf: %s, bh: %s", new Object[]{Long.valueOf(offset), this.buf, this.bh});
      }

      this.offset = (int)offset;
      this.position = position;
      this.nodeType = TrieNode.at(this.buf, (int)offset);
   }

   protected final int payloadFlags() {
      return this.nodeType.payloadFlags(this.buf, this.offset);
   }

   protected final int payloadPosition() {
      return this.nodeType.payloadPosition(this.buf, this.offset);
   }

   protected final int search(int transitionByte) {
      return this.nodeType.search(this.buf, this.offset, transitionByte);
   }

   protected final long transition(int childIndex) {
      return this.nodeType.transition(this.buf, this.offset, this.position, childIndex);
   }

   protected final long lastTransition() {
      return this.nodeType.lastTransition(this.buf, this.offset, this.position);
   }

   protected final long greaterTransition(int searchIndex, long defaultValue) {
      return this.nodeType.greaterTransition(this.buf, this.offset, this.position, searchIndex, defaultValue);
   }

   protected final long lesserTransition(int searchIndex) {
      return this.nodeType.lesserTransition(this.buf, this.offset, this.position, searchIndex);
   }

   protected final int transitionByte(int childIndex) {
      return this.nodeType.transitionByte(this.buf, this.offset, childIndex);
   }

   protected final int transitionRange() {
      return this.nodeType.transitionRange(this.buf, this.offset);
   }

   protected final boolean hasChildren() {
      return this.transitionRange() > 0;
   }

   protected final void goMax(long pos) {
      this.go(pos);

      while(true) {
         long lastChild = this.lastTransition();
         if(lastChild == -1L) {
            return;
         }

         this.go(lastChild);
      }
   }

   protected final void goMin(long pos) {
      this.go(pos);

      while(true) {
         int payloadBits = this.payloadFlags();
         if(payloadBits > 0) {
            return;
         }

         this.go(this.transition(0));
      }
   }

   public int follow(ByteSource key) {
      key.reset();
      this.go(this.root);

      while(true) {
         int b = key.next();
         int childIndex = this.search(b);
         if(childIndex < 0) {
            return b;
         }

         this.go(this.transition(childIndex));
      }
   }

   public int followWithGreater(ByteSource key) {
      this.greaterBranch = -1L;
      key.reset();
      this.go(this.root);

      while(true) {
         int b = key.next();
         int searchIndex = this.search(b);
         this.greaterBranch = this.greaterTransition(searchIndex, this.greaterBranch);
         if(searchIndex < 0) {
            return b;
         }

         this.go(this.transition(searchIndex));
      }
   }

   public <ResType> ResType prefix(ByteSource key, Walker.Extractor<ResType, Concrete> extractor) {
      ResType payload = null;
      key.reset();
      this.go(this.root);

      while(true) {
         int b = key.next();
         int childIndex = this.search(b);
         if(childIndex > 0) {
            payload = null;
         } else {
            int payloadBits = this.payloadFlags();
            if(payloadBits > 0) {
               payload = extractor.extract(this, this.payloadPosition(), payloadBits);
            }

            if(childIndex < 0) {
               return payload;
            }
         }

         this.go(this.transition(childIndex));
      }
   }

   public <ResType> ResType prefixAndNeighbours(ByteSource key, Walker.Extractor<ResType, Concrete> extractor) {
      ResType payload = null;
      this.greaterBranch = -1L;
      this.lesserBranch = -1L;
      key.reset();
      this.go(this.root);

      while(true) {
         int b = key.next();
         int searchIndex = this.search(b);
         this.greaterBranch = this.greaterTransition(searchIndex, this.greaterBranch);
         if(searchIndex != -1 && searchIndex != 0) {
            this.lesserBranch = this.lesserTransition(searchIndex);
            payload = null;
         } else {
            int payloadBits = this.payloadFlags();
            if(payloadBits > 0) {
               payload = extractor.extract(this, this.payloadPosition(), payloadBits);
            }
         }

         if(searchIndex < 0) {
            return payload;
         }

         this.go(this.transition(searchIndex));
      }
   }

   protected int nodeTypeOrdinal() {
      return this.nodeType.ordinal;
   }

   protected int nodeSize() {
      return this.payloadPosition() - this.offset;
   }

   public void dumpTrie(PrintStream out, Walker.PayloadToString payloadReader) {
      out.print("ROOT");
      this.dumpTrie(out, payloadReader, this.root, "");
   }

   private void dumpTrie(PrintStream out, Walker.PayloadToString payloadReader, long node, String indent) {
      this.go(node);
      int bits = this.payloadFlags();
      out.format(" %s@%x %s\n", new Object[]{this.nodeType.toString(), Long.valueOf(node), bits == 0?"":payloadReader.payloadAsString(this.buf, this.payloadPosition(), bits)});
      int range = this.transitionRange();

      for(int i = 0; i < range; ++i) {
         long child = this.transition(i);
         if(child != -1L) {
            out.format("%s%02x %s>", new Object[]{indent, Integer.valueOf(this.transitionByte(i)), PageAware.pageStart(this.position) == PageAware.pageStart(child)?"--":"=="});
            this.dumpTrie(out, payloadReader, child, indent + "  ");
            this.go(node);
         }
      }

   }

   public String toString() {
      return String.format("[Trie Walker - NodeType: %s, source: %s, buffer: %s, buffer file offset: %d, Node buffer offset: %d, Node file position: %d]", new Object[]{this.nodeType, this.source, this.buf, Long.valueOf(this.bh.offset()), Integer.valueOf(this.offset), Long.valueOf(this.position)});
   }

   public interface PayloadToString {
      String payloadAsString(ByteBuffer var1, int var2, int var3);
   }

   public interface Extractor<ResType, Concrete> {
      ResType extract(Concrete var1, int var2, int var3);
   }
}
