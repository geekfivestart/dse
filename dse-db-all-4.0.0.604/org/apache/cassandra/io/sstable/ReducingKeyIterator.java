package org.apache.cassandra.io.sstable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.sstable.format.PartitionIndexIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.IMergeIterator;
import org.apache.cassandra.utils.MergeIterator;
import org.apache.cassandra.utils.Reducer;

public class ReducingKeyIterator implements KeyIterator {
   private IMergeIterator<DecoratedKey, DecoratedKey> mi;
   long bytesRead;
   long bytesTotal;

   public ReducingKeyIterator(Collection<SSTableReader> sstables) {
      this(sstables, (AbstractBounds)null);
   }

   ReducingKeyIterator(Collection<SSTableReader> sstables, AbstractBounds<Token> range) {
      List<ReducingKeyIterator.Iter> iters = new ArrayList(sstables.size());
      Iterator var4 = sstables.iterator();

      while(var4.hasNext()) {
         SSTableReader sstable = (SSTableReader)var4.next();
         iters.add(new ReducingKeyIterator.Iter(sstable, range));
      }

      this.mi = MergeIterator.get(iters, DecoratedKey.comparator, new Reducer<DecoratedKey, DecoratedKey>() {
         DecoratedKey reduced = null;

         public boolean trivialReduceIsTrivial() {
            return true;
         }

         public void reduce(int idx, DecoratedKey current) {
            this.reduced = current;
         }

         public DecoratedKey getReduced() {
            return this.reduced;
         }
      });
   }

   public void close() {
      this.mi.close();
   }

   public long getTotalBytes() {
      return this.bytesTotal;
   }

   public long getBytesRead() {
      return this.bytesRead;
   }

   public boolean hasNext() {
      return this.mi.hasNext();
   }

   public DecoratedKey next() {
      return (DecoratedKey)this.mi.next();
   }

   class Iter implements CloseableIterator<DecoratedKey> {
      PartitionIndexIterator source;
      SSTableReader sstable;
      AbstractBounds<Token> range;
      final long total;

      public Iter(SSTableReader this$0, AbstractBounds<Token> sstable) {
         this.sstable = sstable;
         this.range = range;
         ReducingKeyIterator.this.bytesTotal += this.total = sstable.uncompressedLength();
      }

      public void close() {
         if(this.source != null) {
            this.source.close();
         }

      }

      public boolean hasNext() {
         if(this.source == null) {
            try {
               if(this.range == null) {
                  this.source = this.sstable.allKeysIterator();
               } else {
                  this.source = this.sstable.coveredKeysIterator(((Token)this.range.left).minKeyBound(), this.range.inclusiveLeft(), this.range.inclusiveRight()?((Token)this.range.right).maxKeyBound():((Token)this.range.right).minKeyBound(), this.range.inclusiveRight());
               }
            } catch (IOException var2) {
               throw new FSReadError(var2, this.sstable.getFilename());
            }
         }

         return this.source.key() != null;
      }

      public DecoratedKey next() {
         if(!this.hasNext()) {
            throw new AssertionError();
         } else {
            try {
               DecoratedKey key = this.source.key();
               long prevPos = this.source.dataPosition();
               this.source.advance();
               long pos = this.source.key() != null?this.source.dataPosition():this.computeEndPosition(key);
               ReducingKeyIterator.this.bytesRead += pos - prevPos;
               return key;
            } catch (IOException var6) {
               throw new FSReadError(var6, this.sstable.getFilename());
            }
         }
      }

      private long computeEndPosition(DecoratedKey lastKey) {
         if(this.range != null) {
            RowIndexEntry entry = this.sstable.getPosition(lastKey, SSTableReader.Operator.GT);
            if(entry != null) {
               return entry.position;
            }
         }

         return this.total;
      }
   }
}
