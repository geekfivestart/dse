package org.apache.cassandra.io.sstable.format.trieindex;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.io.tries.SerializationNode;
import org.apache.cassandra.io.tries.TrieNode;
import org.apache.cassandra.io.tries.TrieSerializer;
import org.apache.cassandra.io.tries.Walker;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.utils.ByteSource;
import org.apache.cassandra.utils.SizedInts;

class RowIndexReader extends Walker<RowIndexReader> {
   private static final int FLAG_OPEN_MARKER = 8;
   static final TrieSerializer<RowIndexReader.IndexInfo, DataOutputPlus> trieSerializer = new TrieSerializer<RowIndexReader.IndexInfo, DataOutputPlus>() {
      public int sizeofNode(SerializationNode<RowIndexReader.IndexInfo> node, long nodePosition) {
         return TrieNode.typeFor(node, nodePosition).sizeofNode(node) + this.sizeof((RowIndexReader.IndexInfo)node.payload());
      }

      public void write(DataOutputPlus dest, SerializationNode<RowIndexReader.IndexInfo> node, long nodePosition) throws IOException {
         this.write(dest, TrieNode.typeFor(node, nodePosition), node, nodePosition);
      }

      public int sizeof(RowIndexReader.IndexInfo payload) {
         int size = 0;
         if(payload != null) {
            size += SizedInts.nonZeroSize(payload.offset);
            if(!payload.openDeletion.isLive()) {
               size = (int)((long)size + DeletionTime.serializer.serializedSize(payload.openDeletion));
            }
         }

         return size;
      }

      public void write(DataOutputPlus dest, TrieNode type, SerializationNode<RowIndexReader.IndexInfo> node, long nodePosition) throws IOException {
         RowIndexReader.IndexInfo payload = (RowIndexReader.IndexInfo)node.payload();
         int bytes = 0;
         int hasOpenMarker = 0;
         if(payload != null) {
            bytes = SizedInts.nonZeroSize(payload.offset);
            if(!payload.openDeletion.isLive()) {
               hasOpenMarker = 8;
            }
         }

         type.serialize(dest, node, bytes | hasOpenMarker, nodePosition);
         if(payload != null) {
            SizedInts.write(dest, payload.offset, bytes);
            if(hasOpenMarker != 0) {
               DeletionTime.serializer.serialize(payload.openDeletion, dest);
            }
         }

      }
   };

   public RowIndexReader(FileHandle file, long root, Rebufferer.ReaderConstraint rc) {
      super(file.rebuffererFactory().instantiateRebufferer(), root, rc);
   }

   public RowIndexReader(FileHandle file, TrieIndexEntry entry, Rebufferer.ReaderConstraint rc) {
      this(file, entry.indexTrieRoot, rc);
   }

   public RowIndexReader.IndexInfo separatorFloor(ByteSource source) throws IOException {
      RowIndexReader.IndexInfo res = (RowIndexReader.IndexInfo)this.prefixAndNeighbours(source, RowIndexReader::readPayload);
      if(res != null) {
         return res;
      } else if(this.lesserBranch == -1L) {
         return null;
      } else {
         this.goMax(this.lesserBranch);
         return this.getCurrentIndexInfo();
      }
   }

   public RowIndexReader.IndexInfo min() {
      this.goMin(this.root);
      return this.getCurrentIndexInfo();
   }

   protected RowIndexReader.IndexInfo getCurrentIndexInfo() {
      return this.readPayload(this.payloadPosition(), this.payloadFlags());
   }

   protected RowIndexReader.IndexInfo readPayload(int ppos, int bits) {
      return readPayload(this.buf, ppos, bits);
   }

   static RowIndexReader.IndexInfo readPayload(ByteBuffer buf, int ppos, int bits) {
      if(bits == 0) {
         return null;
      } else {
         int bytes = bits & -9;
         long dataOffset = SizedInts.read(buf, ppos, bytes);
         ppos += bytes;
         DeletionTime deletion = (bits & 8) != 0?DeletionTime.serializer.deserialize(buf, ppos):null;
         return new RowIndexReader.IndexInfo(dataOffset, deletion);
      }
   }

   public void dumpTrie(PrintStream out) {
      this.dumpTrie(out, (buf, ppos, bits) -> {
         RowIndexReader.IndexInfo ii = readPayload(buf, ppos, bits);
         return String.format("pos %x %s", new Object[]{Long.valueOf(ii.offset), ii.openDeletion == null?"":ii.openDeletion});
      });
   }

   static class IndexInfo {
      long offset;
      DeletionTime openDeletion;

      IndexInfo(long offset, DeletionTime openDeletion) {
         this.offset = offset;
         this.openDeletion = openDeletion;
      }
   }
}
