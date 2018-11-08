package org.apache.cassandra.io.sstable;

import java.io.Closeable;
import java.io.IOError;
import java.io.IOException;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

public class SSTableIdentityIterator implements Comparable<SSTableIdentityIterator>, UnfilteredRowIterator {
   private final SSTableReader sstable;
   private DecoratedKey key;
   private DeletionTime partitionLevelDeletion;
   private final FileDataInput dfile;
   private final String filename;
   private boolean shouldClose;
   protected final SSTableSimpleIterator iterator;
   private Row staticRow;

   public SSTableIdentityIterator(SSTableReader sstable, DecoratedKey key, DeletionTime partitionLevelDeletion, FileDataInput dfile, boolean shouldClose, SSTableSimpleIterator iterator) throws IOException {
      this.sstable = sstable;
      this.key = key;
      this.partitionLevelDeletion = partitionLevelDeletion;
      this.dfile = dfile;
      this.filename = dfile.getPath();
      this.iterator = iterator;
      this.staticRow = iterator.readStaticRow();
      this.shouldClose = shouldClose;
   }

   public static SSTableIdentityIterator create(SSTableReader sstable, long partitionStartPosition, DecoratedKey key) {
      FileDataInput file = sstable.getFileDataInput(partitionStartPosition, Rebufferer.ReaderConstraint.NONE);

      try {
         if(key != null) {
            ByteBufferUtil.skipShortLength(file);
         } else {
            key = sstable.decorateKey(ByteBufferUtil.readWithShortLength(file));
         }
      } catch (IOException var7) {
         sstable.markSuspect();
         String filePath = file.getPath();
         FileUtils.closeQuietly((Closeable)file);
         throw new CorruptSSTableException(var7, filePath);
      }

      return create(sstable, file, key, true);
   }

   public static SSTableIdentityIterator create(SSTableReader sstable, FileDataInput file, DecoratedKey key) {
      return create(sstable, file, key, false);
   }

   static SSTableIdentityIterator create(SSTableReader sstable, FileDataInput file, DecoratedKey key, boolean shouldClose) {
      try {
         DeletionTime partitionLevelDeletion = DeletionTime.serializer.deserialize(file);
         SerializationHelper helper = new SerializationHelper(sstable.metadata(), sstable.descriptor.version.encodingVersion(), SerializationHelper.Flag.LOCAL);
         SSTableSimpleIterator iterator = SSTableSimpleIterator.create(sstable.metadata(), file, sstable.header, helper);
         return new SSTableIdentityIterator(sstable, key, partitionLevelDeletion, file, shouldClose, iterator);
      } catch (IOException var7) {
         sstable.markSuspect();
         throw new CorruptSSTableException(var7, file.getPath());
      }
   }

   public static SSTableIdentityIterator create(SSTableReader sstable, FileDataInput dfile, RowIndexEntry indexEntry, DecoratedKey key, boolean tombstoneOnly) {
      try {
         dfile.seek(indexEntry.position);
         ByteBufferUtil.skipShortLength(dfile);
         DeletionTime partitionLevelDeletion = DeletionTime.serializer.deserialize(dfile);
         SerializationHelper helper = new SerializationHelper(sstable.metadata(), sstable.descriptor.version.encodingVersion(), SerializationHelper.Flag.LOCAL);
         SSTableSimpleIterator iterator = tombstoneOnly?SSTableSimpleIterator.createTombstoneOnly(sstable.metadata(), dfile, sstable.header, helper):SSTableSimpleIterator.create(sstable.metadata(), dfile, sstable.header, helper);
         return new SSTableIdentityIterator(sstable, key, partitionLevelDeletion, dfile, false, iterator);
      } catch (IOException var8) {
         sstable.markSuspect();
         throw new CorruptSSTableException(var8, dfile.getPath());
      }
   }

   public TableMetadata metadata() {
      return this.iterator.metadata;
   }

   public RegularAndStaticColumns columns() {
      return this.metadata().regularAndStaticColumns();
   }

   public boolean isReverseOrder() {
      return false;
   }

   public DecoratedKey partitionKey() {
      return this.key;
   }

   public DeletionTime partitionLevelDeletion() {
      return this.partitionLevelDeletion;
   }

   public Row staticRow() {
      return this.staticRow;
   }

   public boolean hasNext() {
      try {
         return this.iterator.hasNext();
      } catch (IndexOutOfBoundsException var2) {
         this.sstable.markSuspect();
         throw new CorruptSSTableException(var2, this.filename);
      } catch (IOError var3) {
         if(var3.getCause() instanceof IOException) {
            this.sstable.markSuspect();
            throw new CorruptSSTableException((Exception)var3.getCause(), this.filename);
         } else {
            throw var3;
         }
      }
   }

   public Unfiltered next() {
      try {
         return this.doCompute();
      } catch (IndexOutOfBoundsException var2) {
         this.sstable.markSuspect();
         throw new CorruptSSTableException(var2, this.filename);
      } catch (IOError var3) {
         if(var3.getCause() instanceof IOException) {
            this.sstable.markSuspect();
            throw new CorruptSSTableException((Exception)var3.getCause(), this.filename);
         } else {
            throw var3;
         }
      }
   }

   protected Unfiltered doCompute() {
      return (Unfiltered)this.iterator.next();
   }

   public void close() {
      if(this.shouldClose) {
         FileUtils.closeQuietly((Closeable)this.dfile);
         this.shouldClose = false;
      }

   }

   public String getPath() {
      return this.filename;
   }

   public EncodingStats stats() {
      return this.sstable.stats();
   }

   public int compareTo(SSTableIdentityIterator o) {
      return this.key.compareTo((PartitionPosition)o.key);
   }
}
