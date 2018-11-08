package org.apache.cassandra.io.sstable;

import java.io.Closeable;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.service.ActiveRepairService;

abstract class AbstractSSTableSimpleWriter implements Closeable {
   protected final File directory;
   protected final TableMetadataRef metadata;
   protected final RegularAndStaticColumns columns;
   protected SSTableFormat.Type formatType = SSTableFormat.Type.current();
   protected static AtomicInteger generation = new AtomicInteger(0);
   protected boolean makeRangeAware = false;

   protected AbstractSSTableSimpleWriter(File directory, TableMetadataRef metadata, RegularAndStaticColumns columns) {
      this.metadata = metadata;
      this.directory = directory;
      this.columns = columns;
   }

   protected void setSSTableFormatType(SSTableFormat.Type type) {
      this.formatType = type;
   }

   protected void setRangeAwareWriting(boolean makeRangeAware) {
      this.makeRangeAware = makeRangeAware;
   }

   protected SSTableTxnWriter createWriter() {
      SerializationHeader header = new SerializationHeader(true, this.metadata.get(), this.columns, EncodingStats.NO_STATS);
      return this.makeRangeAware?SSTableTxnWriter.createRangeAware(this.metadata, 0L, 0L, ActiveRepairService.NO_PENDING_REPAIR, this.formatType, 0, header):SSTableTxnWriter.create(this.metadata, createDescriptor(this.directory, this.metadata.keyspace, this.metadata.name, this.formatType), 0L, 0L, ActiveRepairService.NO_PENDING_REPAIR, 0, header, Collections.emptySet());
   }

   private static Descriptor createDescriptor(File directory, String keyspace, String columnFamily, SSTableFormat.Type fmt) {
      int maxGen = getNextGeneration(directory, columnFamily);
      return new Descriptor(directory, keyspace, columnFamily, maxGen + 1, fmt);
   }

   private static int getNextGeneration(File directory, final String columnFamily) {
      final Set<Descriptor> existing = new HashSet();
      directory.listFiles(new FileFilter() {
         public boolean accept(File file) {
            Descriptor desc = SSTable.tryDescriptorFromFilename(file);
            if(desc == null) {
               return false;
            } else {
               if(desc.cfname.equals(columnFamily)) {
                  existing.add(desc);
               }

               return false;
            }
         }
      });
      int maxGen = generation.getAndIncrement();
      Iterator var4 = existing.iterator();

      while(var4.hasNext()) {
         for(Descriptor desc = (Descriptor)var4.next(); desc.generation > maxGen; maxGen = generation.getAndIncrement()) {
            ;
         }
      }

      return maxGen;
   }

   PartitionUpdate getUpdateFor(ByteBuffer key) throws IOException {
      return this.getUpdateFor(this.metadata.get().partitioner.decorateKey(key));
   }

   abstract PartitionUpdate getUpdateFor(DecoratedKey var1) throws IOException;
}
