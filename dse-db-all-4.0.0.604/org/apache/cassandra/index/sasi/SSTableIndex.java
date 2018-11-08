package org.apache.cassandra.index.sasi;

import com.google.common.base.Function;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sasi.conf.ColumnIndex;
import org.apache.cassandra.index.sasi.disk.OnDiskIndex;
import org.apache.cassandra.index.sasi.disk.OnDiskIndexBuilder;
import org.apache.cassandra.index.sasi.disk.Token;
import org.apache.cassandra.index.sasi.plan.Expression;
import org.apache.cassandra.index.sasi.utils.RangeIterator;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class SSTableIndex {
   private final ColumnIndex columnIndex;
   private final Ref<SSTableReader> sstableRef;
   private final SSTableReader sstable;
   private final OnDiskIndex index;
   private final AtomicInteger references = new AtomicInteger(1);
   private final AtomicBoolean obsolete = new AtomicBoolean(false);

   public SSTableIndex(ColumnIndex index, File indexFile, SSTableReader referent) {
      this.columnIndex = index;
      this.sstableRef = referent.tryRef();
      this.sstable = (SSTableReader)this.sstableRef.get();
      if(this.sstable == null) {
         throw new IllegalStateException("Couldn't acquire reference to the sstable: " + referent);
      } else {
         AbstractType<?> validator = this.columnIndex.getValidator();

         assert validator != null;

         assert indexFile.exists() : String.format("SSTable %s should have index %s.", new Object[]{this.sstable.getFilename(), this.columnIndex.getIndexName()});

         this.index = new OnDiskIndex(indexFile, validator, new SSTableIndex.DecoratedKeyFetcher(this.sstable));
      }
   }

   public OnDiskIndexBuilder.Mode mode() {
      return this.index.mode();
   }

   public boolean hasMarkedPartials() {
      return this.index.hasMarkedPartials();
   }

   public ByteBuffer minTerm() {
      return this.index.minTerm();
   }

   public ByteBuffer maxTerm() {
      return this.index.maxTerm();
   }

   public ByteBuffer minKey() {
      return this.index.minKey();
   }

   public ByteBuffer maxKey() {
      return this.index.maxKey();
   }

   public RangeIterator<Long, Token> search(Expression expression) {
      return this.index.search(expression);
   }

   public SSTableReader getSSTable() {
      return this.sstable;
   }

   public String getPath() {
      return this.index.getIndexPath();
   }

   public boolean reference() {
      int n;
      do {
         n = this.references.get();
         if(n <= 0) {
            return false;
         }
      } while(!this.references.compareAndSet(n, n + 1));

      return true;
   }

   public void release() {
      int n = this.references.decrementAndGet();
      if(n == 0) {
         FileUtils.closeQuietly((Closeable)this.index);
         this.sstableRef.release();
         if(this.obsolete.get() || this.sstableRef.globalCount() == 0) {
            FileUtils.delete(this.index.getIndexPath());
         }
      }

   }

   public void markObsolete() {
      this.obsolete.getAndSet(true);
      this.release();
   }

   public boolean isObsolete() {
      return this.obsolete.get();
   }

   public boolean equals(Object o) {
      return o instanceof SSTableIndex && this.index.getIndexPath().equals(((SSTableIndex)o).index.getIndexPath());
   }

   public int hashCode() {
      return (new HashCodeBuilder()).append(this.index.getIndexPath()).build().intValue();
   }

   public String toString() {
      return String.format("SSTableIndex(column: %s, SSTable: %s)", new Object[]{this.columnIndex.getColumnName(), this.sstable.descriptor});
   }

   private static class DecoratedKeyFetcher implements Function<Long, DecoratedKey> {
      private final SSTableReader sstable;

      DecoratedKeyFetcher(SSTableReader reader) {
         this.sstable = reader;
      }

      public DecoratedKey apply(Long offset) {
         try {
            return this.sstable.keyAt(offset.longValue(), Rebufferer.ReaderConstraint.NONE);
         } catch (IOException var3) {
            throw new FSReadError(new IOException("Failed to read key from " + this.sstable.descriptor, var3), this.sstable.getFilename());
         }
      }

      public int hashCode() {
         return this.sstable.descriptor.hashCode();
      }

      public boolean equals(Object other) {
         return other instanceof SSTableIndex.DecoratedKeyFetcher && this.sstable.descriptor.equals(((SSTableIndex.DecoratedKeyFetcher)other).sstable.descriptor);
      }
   }
}
