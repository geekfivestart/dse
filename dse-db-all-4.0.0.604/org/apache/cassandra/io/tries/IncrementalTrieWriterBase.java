package org.apache.cassandra.io.tries;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import org.apache.cassandra.utils.ByteSource;

public abstract class IncrementalTrieWriterBase<Value, Dest, Node extends IncrementalTrieWriterBase.BaseNode<Value, Node>> implements IncrementalTrieWriter<Value> {
   protected final TrieSerializer<Value, ? super Dest> serializer;
   protected final Dest dest;
   protected ByteSource prev = null;
   protected Deque<Node> stack = new ArrayDeque();
   long count = 0L;

   public IncrementalTrieWriterBase(TrieSerializer<Value, ? super Dest> serializer, Dest dest, Node root) {
      this.serializer = serializer;
      this.dest = dest;
      this.stack.addLast(root);
   }

   public void add(final ByteSource next, final Value value) throws IOException {
      ++this.count;
      int stackpos = 0;
      next.reset();
      int n = next.next();
      if (this.prev != null) {
         this.prev.reset();
         int p;
         for (p = this.prev.next(); n == p; n = next.next(), p = this.prev.next()) {
            assert n != -1 : String.format("Incremental trie requires unique sorted keys, got equal %s after %s.", next, this.prev);
            ++stackpos;
         }
         assert p < n : String.format("Incremental trie requires sorted keys, got %s after %s.", next, this.prev);
      }
      this.prev = next;
      while (this.stack.size() > stackpos + 1) {
         this.completeLast();
      }
      Node node = this.stack.getLast();
      while (n != -1) {
         this.stack.addLast(node = ((BaseNode<Value, Node>)node).addChild((byte)n));
         ++stackpos;
         n = next.next();
      }
      final Value existingPayload = ((BaseNode<Value, Node>)node).setPayload(value);
      assert existingPayload == null;
   }

   public long complete() throws IOException {
      Node root = this.stack.getFirst();
      return root.filePos != -1L?root.filePos:this.performCompletion().filePos;
   }

   Node performCompletion() throws IOException {
      Node root = null;
      while (!this.stack.isEmpty()) {
         root = this.completeLast();
      }
      this.stack.addLast(root);
      return root;
   }

   public long count() {
      return this.count;
   }

   protected Node completeLast() throws IOException {
      Node node = this.stack.removeLast();
      this.complete(node);
      return node;
   }

   abstract void complete(Node var1) throws IOException;

   public abstract IncrementalTrieWriter.PartialTail makePartialRoot() throws IOException;

   abstract static class BaseNode<Value, Node extends IncrementalTrieWriterBase.BaseNode<Value, Node>> implements SerializationNode<Value> {
      Value payload;
      List<Node> children = new ArrayList();
      final int transition;
      long filePos = -1L;

      BaseNode(int transition) {
         this.transition = transition;
      }

      public Value payload() {
         return this.payload;
      }

      public Value setPayload(Value newPayload) {
         Value p = this.payload;
         this.payload = newPayload;
         return p;
      }

      public Node addChild(byte b) {
         assert this.children.isEmpty() || (((IncrementalTrieWriterBase.BaseNode)this.children.get(this.children.size() - 1)).transition & 255) < (b & 255);

         Node node = this.newNode(b);
         this.children.add(node);
         return node;
      }

      public int childCount() {
         return this.children.size();
      }

      void finalizeWithPosition(long position) {
         this.filePos = position;
         this.children = null;
         this.payload = null;
      }

      public int transition(int i) {
         return ((IncrementalTrieWriterBase.BaseNode)this.children.get(i)).transition;
      }

      public String toString() {
         return String.format("%02x", new Object[]{Integer.valueOf(this.transition)});
      }

      abstract Node newNode(byte var1);
   }

   static class PTail implements IncrementalTrieWriter.PartialTail {
      long root;
      long cutoff;
      long count;
      ByteBuffer tail;

      PTail() {
      }

      public long root() {
         return this.root;
      }

      public long cutoff() {
         return this.cutoff;
      }

      public ByteBuffer tail() {
         return this.tail;
      }

      public long count() {
         return this.count;
      }
   }
}
