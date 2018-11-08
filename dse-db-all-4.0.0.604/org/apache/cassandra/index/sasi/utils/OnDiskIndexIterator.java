package org.apache.cassandra.index.sasi.utils;

import java.io.IOException;
import java.util.Iterator;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sasi.disk.OnDiskIndex;

public class OnDiskIndexIterator extends RangeIterator<OnDiskIndex.DataTerm, CombinedTerm> {
   private final AbstractType<?> comparator;
   private final Iterator<OnDiskIndex.DataTerm> terms;

   public OnDiskIndexIterator(OnDiskIndex index) {
      super(index.min(), index.max(), 9223372036854775807L);
      this.comparator = index.getComparator();
      this.terms = index.iterator();
   }

   public static RangeIterator<OnDiskIndex.DataTerm, CombinedTerm> union(OnDiskIndex... union) {
      RangeUnionIterator.Builder<OnDiskIndex.DataTerm, CombinedTerm> builder = RangeUnionIterator.builder();
      OnDiskIndex[] var2 = union;
      int var3 = union.length;

      for(int var4 = 0; var4 < var3; ++var4) {
         OnDiskIndex e = var2[var4];
         if(e != null) {
            builder.add(new OnDiskIndexIterator(e));
         }
      }

      return builder.build();
   }

   protected CombinedTerm computeNext() {
      return this.terms.hasNext()?new CombinedTerm(this.comparator, (OnDiskIndex.DataTerm)this.terms.next()):(CombinedTerm)this.endOfData();
   }

   protected void performSkipTo(OnDiskIndex.DataTerm nextToken) {
      throw new UnsupportedOperationException();
   }

   public void close() throws IOException {
   }
}
