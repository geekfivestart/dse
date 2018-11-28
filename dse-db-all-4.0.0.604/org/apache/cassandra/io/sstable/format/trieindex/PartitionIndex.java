package org.apache.cassandra.io.sstable.format.trieindex;

import java.io.Closeable;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.tries.SerializationNode;
import org.apache.cassandra.io.tries.TrieNode;
import org.apache.cassandra.io.tries.TrieSerializer;
import org.apache.cassandra.io.tries.ValueIterator;
import org.apache.cassandra.io.tries.Walker;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ByteSource;
import org.apache.cassandra.utils.SizedInts;
import org.apache.cassandra.utils.concurrent.Ref;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PartitionIndex implements Closeable {
   private static final Logger logger = LoggerFactory.getLogger(PartitionIndex.class);
   private final FileHandle fh;
   private final long keyCount;
   private final DecoratedKey first;
   private final DecoratedKey last;
   private final long root;
   public static final long NOT_FOUND = -9223372036854775808L;
   public static PartitionIndex.PartitionIndexSerializer trieSerializer = new PartitionIndex.PartitionIndexSerializer();

   public PartitionIndex(FileHandle fh, long trieRoot, long keyCount, DecoratedKey first, DecoratedKey last) {
      this.keyCount = keyCount;
      this.fh = fh.sharedCopy();
      this.first = first;
      this.last = last;
      this.root = trieRoot;
   }

   private PartitionIndex(PartitionIndex src) {
      this(src.fh, src.root, src.keyCount, src.first, src.last);
   }

   public long size() {
      return this.keyCount;
   }

   public DecoratedKey firstKey() {
      return this.first;
   }

   public DecoratedKey lastKey() {
      return this.last;
   }

   public PartitionIndex sharedCopy() {
      return new PartitionIndex(this);
   }

   public void addTo(Ref.IdentityCollection identities) {
      this.fh.addTo(identities);
   }

   public static PartitionIndex load(FileHandle.Builder fhBuilder, IPartitioner partitioner, boolean preload) throws IOException {
      FileHandle fh = fhBuilder.complete();
      Throwable var4 = null;

      PartitionIndex var43;
      try {
         FileDataInput rdr = fh.createReader(fh.dataLength() - 24L, Rebufferer.ReaderConstraint.NONE);
         Throwable var6 = null;

         try {
            long firstPos = rdr.readLong();
            long keyCount = rdr.readLong();
            long root = rdr.readLong();
            rdr.seek(firstPos);
            DecoratedKey first = partitioner != null?partitioner.decorateKey(ByteBufferUtil.readWithShortLength(rdr)):null;
            DecoratedKey last = partitioner != null?partitioner.decorateKey(ByteBufferUtil.readWithShortLength(rdr)):null;
            if(preload) {
               int csum = 0;

               for(long pos = 0L; pos < fh.dataLength(); pos += 4096L) {
                  rdr.seek(pos);
                  csum += rdr.readByte();
               }

               logger.trace("Checksum {}", Integer.valueOf(csum));
            }

            var43 = new PartitionIndex(fh, root, keyCount, first, last);
         } catch (Throwable var39) {
            var6 = var39;
            throw var39;
         } finally {
            if(rdr != null) {
               if(var6 != null) {
                  try {
                     rdr.close();
                  } catch (Throwable var38) {
                     var6.addSuppressed(var38);
                  }
               } else {
                  rdr.close();
               }
            }

         }
      } catch (Throwable var41) {
         var4 = var41;
         throw var41;
      } finally {
         if(fh != null) {
            if(var4 != null) {
               try {
                  fh.close();
               } catch (Throwable var37) {
                  var4.addSuppressed(var37);
               }
            } else {
               fh.close();
            }
         }

      }

      return var43;
   }

   public void close() {
      this.fh.close();
   }

   static ByteSource source(PartitionPosition key) {
      ByteSource s = key.asByteComparableSource();
      return s;
   }

   public PartitionIndex.Reader openReader(Rebufferer.ReaderConstraint rc) {
      return new PartitionIndex.Reader(this, rc);
   }

   protected PartitionIndex.IndexPosIterator allKeysIterator(Rebufferer.ReaderConstraint rc) {
      return new PartitionIndex.IndexPosIterator(this, rc);
   }

   protected Rebufferer instantiateRebufferer() {
      return this.fh.rebuffererFactory().instantiateRebufferer();
   }

   FileHandle getFileHandle() {
      return this.fh;
   }

   private static long getIndexPos(ByteBuffer contents, int payloadPos, int bytes) {
      if(bytes > 7) {
         ++payloadPos;
         bytes -= 7;
      }

      return bytes == 0?-9223372036854775808L:SizedInts.read(contents, payloadPos, bytes);
   }

   public void dumpTrie(String fileName) {
      try {
         PrintStream ps = new PrintStream(new File(fileName));
         Throwable var3 = null;

         try {
            this.dumpTrie(ps);
         } catch (Throwable var13) {
            var3 = var13;
            throw var13;
         } finally {
            if(ps != null) {
               if(var3 != null) {
                  try {
                     ps.close();
                  } catch (Throwable var12) {
                     var3.addSuppressed(var12);
                  }
               } else {
                  ps.close();
               }
            }

         }
      } catch (Throwable var15) {
         logger.warn("Failed to dump trie to {} due to exception {}", fileName, var15);
      }

   }

   public void dumpTrie(PrintStream out) {
      PartitionIndex.Reader rdr = this.openReader(Rebufferer.ReaderConstraint.NONE);
      Throwable var3 = null;

      try {
         rdr.dumpTrie(out, (buf, ppos, pbits) -> {
            return Long.toString(getIndexPos(buf, ppos, pbits));
         });
      } catch (Throwable var12) {
         var3 = var12;
         throw var12;
      } finally {
         if(rdr != null) {
            if(var3 != null) {
               try {
                  rdr.close();
               } catch (Throwable var11) {
                  var3.addSuppressed(var11);
               }
            } else {
               rdr.close();
            }
         }

      }

   }

   public static class IndexPosIterator extends ValueIterator<PartitionIndex.IndexPosIterator> {
      static final long INVALID = -1L;
      long pos = -1L;

      public IndexPosIterator(PartitionIndex index, Rebufferer.ReaderConstraint rc) {
         super(index.instantiateRebufferer(), index.root, rc);
      }

      public IndexPosIterator(PartitionIndex summary, PartitionPosition start, PartitionPosition end, Rebufferer.ReaderConstraint rc) {
         super(summary.instantiateRebufferer(), summary.root, PartitionIndex.source(start), PartitionIndex.source(end), true, rc);
      }

      protected long nextIndexPos() throws IOException {
         if(this.pos == -1L) {
            this.pos = this.nextPayloadedNode();
            if(this.pos == -1L) {
               return -9223372036854775808L;
            }
         }

         this.go(this.pos);
         this.pos = -1L;
         return PartitionIndex.getIndexPos(this.buf, this.payloadPosition(), this.payloadFlags());
      }
   }

   public static class Reader extends Walker<PartitionIndex.Reader> {
      protected Reader(PartitionIndex summary, Rebufferer.ReaderConstraint rc) {
         super(summary.instantiateRebufferer(), summary.root, rc);
      }

      public long exactCandidate(DecoratedKey key) {
         int b = this.follow(PartitionIndex.source(key));
         return b != -1 && this.hasChildren()?-9223372036854775808L:(!this.checkHashBits(key.filterHashLowerBits())?-9223372036854775808L:this.getCurrentIndexPos());
      }

      final boolean checkHashBits(short hashBits) {
         int bytes = this.payloadFlags();
         return bytes <= 7?bytes > 0:this.buf.get(this.payloadPosition()) == (byte)hashBits;
      }

      public <ResultType> ResultType ceiling(PartitionPosition key, PartitionIndex.Acceptor<PartitionPosition, ResultType> acceptor) throws IOException {
         int b = this.followWithGreater(PartitionIndex.source(key));
         long indexPos;
         if(!this.hasChildren() || b == -1) {
            indexPos = this.getCurrentIndexPos();
            if(indexPos != -9223372036854775808L) {
               ResultType res = acceptor.accept(indexPos, false, key);
               if(res != null) {
                  return res;
               }
            }
         }

         if(this.greaterBranch == -1L) {
            return null;
         } else {
            this.goMin(this.greaterBranch);
            indexPos = this.getCurrentIndexPos();
            return acceptor.accept(indexPos, true, key);
         }
      }

      public long getCurrentIndexPos() {
         return PartitionIndex.getIndexPos(this.buf, this.payloadPosition(), this.payloadFlags());
      }

      public long getLastIndexPosition() {
         this.goMax(this.root);
         return this.getCurrentIndexPos();
      }

      protected int payloadSize() {
         int bytes = this.payloadFlags();
         return bytes > 7?bytes - 6:bytes;
      }
   }

   public interface Acceptor<ArgType, ResultType> {
      ResultType accept(long var1, boolean var3, ArgType var4) throws IOException;
   }

   static class PartitionIndexSerializer implements TrieSerializer<PartitionIndex.Payload, DataOutput> {
      PartitionIndexSerializer() {
      }

      public int sizeofNode(SerializationNode<PartitionIndex.Payload> node, long nodePosition) {
         return TrieNode.typeFor(node, nodePosition).sizeofNode(node) + (node.payload() != null?1 + SizedInts.nonZeroSize(((PartitionIndex.Payload)node.payload()).position):0);
      }

      public void write(DataOutput dest, SerializationNode<PartitionIndex.Payload> node, long nodePosition) throws IOException {
         this.write(dest, TrieNode.typeFor(node, nodePosition), node, nodePosition);
      }

      public void write(DataOutput dest, TrieNode type, SerializationNode<PartitionIndex.Payload> node, long nodePosition) throws IOException {
         PartitionIndex.Payload payload = (PartitionIndex.Payload)node.payload();
         if(payload != null) {
            int size = SizedInts.nonZeroSize(payload.position);
            int payloadBits = 7 + size;
            type.serialize(dest, node, payloadBits, nodePosition);
            dest.writeByte(payload.hashBits);
            SizedInts.write(dest, payload.position, size);
         } else {
            type.serialize(dest, node, 0, nodePosition);
         }

      }
   }

   static class Payload {
      final long position;
      final short hashBits;

      public Payload(long position, short hashBits) {
         this.position = position;

         assert this.position != -9223372036854775808L;

         this.hashBits = hashBits;
      }
   }
}
