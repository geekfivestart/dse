package org.apache.cassandra.io.tries;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Deque;
import java.util.Iterator;
import org.apache.cassandra.io.util.DataOutputBuffer;

public class IncrementalTrieWriterSimple<Value> extends IncrementalTrieWriterBase<Value, DataOutput, IncrementalTrieWriterSimple.Node<Value>> implements IncrementalTrieWriter<Value> {
   private long position = 0L;

   public IncrementalTrieWriterSimple(TrieSerializer<Value, DataOutput> serializer, DataOutput dest) {
      super(serializer, dest, new IncrementalTrieWriterSimple.Node(0));
   }

   protected void complete(IncrementalTrieWriterSimple.Node<Value> node) throws IOException {
      long nodePos = this.position;
      this.position += this.write(node, (DataOutput)this.dest, this.position);
      node.finalizeWithPosition(nodePos);
   }

   public IncrementalTrieWriter.PartialTail makePartialRoot() throws IOException {
      DataOutputBuffer buf = new DataOutputBuffer();
      Throwable var2 = null;

      try {
         IncrementalTrieWriterBase.PTail tail = new IncrementalTrieWriterBase.PTail();
         tail.cutoff = this.position;
         tail.count = this.count;
         long nodePos = this.position;
         Deque var10000 = this.stack;
         this.stack.getClass();

         IncrementalTrieWriterSimple.Node node;
         for(Iterator var6 = (var10000.descendingIterator()); var6.hasNext(); nodePos += this.write(node, buf, nodePos)) {
            node = (IncrementalTrieWriterSimple.Node)var6.next();
            node.filePos = nodePos;
         }

         tail.tail = buf.trimmedBuffer();
         tail.root = ((IncrementalTrieWriterSimple.Node)this.stack.getFirst()).filePos;
         IncrementalTrieWriterBase.PTail var17 = tail;
         return var17;
      } catch (Throwable var15) {
         var2 = var15;
         throw var15;
      } finally {
         if(buf != null) {
            if(var2 != null) {
               try {
                  buf.close();
               } catch (Throwable var14) {
                  var2.addSuppressed(var14);
               }
            } else {
               buf.close();
            }
         }

      }
   }

   private long write(IncrementalTrieWriterSimple.Node<Value> node, DataOutput dest, long nodePosition) throws IOException {
      long size = (long)this.serializer.sizeofNode(node, nodePosition);
      this.serializer.write(dest, node, nodePosition);
      return size;
   }

   static class Node<Value> extends IncrementalTrieWriterBase.BaseNode<Value, IncrementalTrieWriterSimple.Node<Value>> {
      Node(int transition) {
         super(transition);
      }

      IncrementalTrieWriterSimple.Node<Value> newNode(byte transition) {
         return new IncrementalTrieWriterSimple.Node(transition & 255);
      }

      public long serializedPositionDelta(int i, long nodePosition) {
         assert ((IncrementalTrieWriterSimple.Node)this.children.get(i)).filePos != -1L;

         return ((IncrementalTrieWriterSimple.Node)this.children.get(i)).filePos - nodePosition;
      }

      public long maxPositionDelta(long nodePosition) {
         long min = 0L;
         Iterator var5 = this.children.iterator();

         while(var5.hasNext()) {
            IncrementalTrieWriterSimple.Node<Value> child = (IncrementalTrieWriterSimple.Node)var5.next();
            if(child.filePos != -1L) {
               min = Math.min(min, child.filePos - nodePosition);
            }
         }

         return min;
      }
   }
}
