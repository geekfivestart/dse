package org.apache.cassandra.io.sstable;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.Collection;
import java.util.Iterator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;

public class PerRangeReducingKeyIterator implements KeyIterator {
   private final ReducingKeyIterator[] iterators;
   private final Collection<? extends AbstractBounds<Token>> bounds;
   private int current = -1;
   private int next = -1;

   public PerRangeReducingKeyIterator(Collection<SSTableReader> sstables, Collection<? extends AbstractBounds<Token>> bounds) {
      Preconditions.checkArgument(!sstables.isEmpty(), "There must be at least one sstable to iterate.");
      Preconditions.checkArgument(!bounds.isEmpty(), "There must be at least one bound to iterate through.");
      this.bounds = ImmutableList.copyOf(bounds);
      this.iterators = new ReducingKeyIterator[bounds.size()];
      this.initIterators(sstables, bounds);
   }

   public void close() {
      ReducingKeyIterator[] var1 = this.iterators;
      int var2 = var1.length;

      for(int var3 = 0; var3 < var2; ++var3) {
         ReducingKeyIterator it = var1[var3];
         it.close();
      }

   }

   public long getTotalBytes() {
      return this.iterators.length == 0?0L:this.iterators[0].bytesTotal;
   }

   public long getBytesRead() {
      long bytesRead = 0L;
      ReducingKeyIterator[] var3 = this.iterators;
      int var4 = var3.length;

      for(int var5 = 0; var5 < var4; ++var5) {
         ReducingKeyIterator it = var3[var5];
         bytesRead += it.bytesRead;
      }

      return bytesRead;
   }

   public boolean hasNext() {
      return this.computeNext();
   }

   public DecoratedKey next() {
      boolean hasNext = this.computeNext();
      if(hasNext) {
         this.current = this.next;
         return this.iterators[this.current].next();
      } else {
         throw new IllegalStateException("This iterator has no next element!");
      }
   }

   public Collection<? extends AbstractBounds<Token>> getBounds() {
      return this.bounds;
   }

   private void initIterators(Collection<SSTableReader> sstables, Collection<? extends AbstractBounds<Token>> bounds) {
      Iterator<? extends AbstractBounds<Token>> boundsIt = bounds.iterator();

      AbstractBounds bound;
      for(int var4 = 0; boundsIt.hasNext(); this.iterators[var4++] = new ReducingKeyIterator(sstables, bound)) {
         bound = (AbstractBounds)boundsIt.next();
      }

   }

   private boolean computeNext() {
      boolean hasNext = false;
      int remaining = this.iterators.length;
      this.next = this.current;

      do {
         this.next = ++this.next % this.iterators.length;
         hasNext = this.iterators[this.next].hasNext();
         if(hasNext) {
            break;
         }

         --remaining;
      } while(remaining > 0);

      return hasNext;
   }
}
