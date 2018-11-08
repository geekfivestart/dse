package org.apache.cassandra.index.sasi.conf.view;

import java.nio.ByteBuffer;
import java.util.Set;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sasi.SSTableIndex;
import org.apache.cassandra.index.sasi.disk.OnDiskIndexBuilder;
import org.apache.cassandra.index.sasi.plan.Expression;

public interface TermTree {
   Set<SSTableIndex> search(Expression var1);

   int intervalCount();

   public abstract static class Builder {
      protected final OnDiskIndexBuilder.Mode mode;
      protected final AbstractType<?> comparator;
      protected ByteBuffer min;
      protected ByteBuffer max;

      protected Builder(OnDiskIndexBuilder.Mode mode, AbstractType<?> comparator) {
         this.mode = mode;
         this.comparator = comparator;
      }

      public final void add(SSTableIndex index) {
         this.addIndex(index);
         this.min = this.min != null && this.comparator.compare(this.min, index.minTerm()) <= 0?this.min:index.minTerm();
         this.max = this.max != null && this.comparator.compare(this.max, index.maxTerm()) >= 0?this.max:index.maxTerm();
      }

      protected abstract void addIndex(SSTableIndex var1);

      public abstract TermTree build();
   }
}
