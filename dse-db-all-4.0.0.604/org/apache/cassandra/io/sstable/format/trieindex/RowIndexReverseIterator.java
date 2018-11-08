package org.apache.cassandra.io.sstable.format.trieindex;

import java.io.PrintStream;
import org.apache.cassandra.io.tries.ReverseValueIterator;
import org.apache.cassandra.io.tries.Walker;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.utils.ByteSource;

class RowIndexReverseIterator extends ReverseValueIterator<RowIndexReverseIterator> {
   private long currentNode;

   public RowIndexReverseIterator(FileHandle file, long root, ByteSource start, ByteSource end, Rebufferer.ReaderConstraint rc) {
      super(file.rebuffererFactory().instantiateRebufferer(), root, start, end, true, rc);
      this.currentNode = -1L;
   }

   public RowIndexReverseIterator(FileHandle file, TrieIndexEntry entry, ByteSource end, Rebufferer.ReaderConstraint rc) {
      this(file, entry.indexTrieRoot, ByteSource.empty(), end, rc);
   }

   public RowIndexReader.IndexInfo nextIndexInfo() {
      if(this.currentNode == -1L) {
         this.currentNode = this.nextPayloadedNode();
         if(this.currentNode == -1L) {
            return null;
         }
      }

      this.go(this.currentNode);
      RowIndexReader.IndexInfo info = RowIndexReader.readPayload(this.buf, this.payloadPosition(), this.payloadFlags());
      this.currentNode = -1L;
      return info;
   }

   public void dumpTrie(PrintStream out) {
      this.dumpTrie(out, (buf, ppos, bits) -> {
         RowIndexReader.IndexInfo ii = RowIndexReader.readPayload(buf, ppos, bits);
         return String.format("pos %x %s", new Object[]{Long.valueOf(ii.offset), ii.openDeletion == null?"":ii.openDeletion});
      });
   }
}
