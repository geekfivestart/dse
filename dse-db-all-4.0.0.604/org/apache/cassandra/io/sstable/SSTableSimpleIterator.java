package org.apache.cassandra.io.sstable;

import java.io.IOError;
import java.io.IOException;
import java.util.Iterator;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.rows.ArrayBackedRow;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractIterator;

public abstract class SSTableSimpleIterator extends AbstractIterator<Unfiltered> implements Iterator<Unfiltered> {
   final TableMetadata metadata;
   protected final DataInputPlus in;
   protected final SerializationHelper helper;

   private SSTableSimpleIterator(TableMetadata metadata, DataInputPlus in, SerializationHelper helper) {
      this.metadata = metadata;
      this.in = in;
      this.helper = helper;
   }

   public static SSTableSimpleIterator create(TableMetadata metadata, DataInputPlus in, SerializationHeader header, SerializationHelper helper) {
      return new SSTableSimpleIterator.CurrentFormatIterator(metadata, in, header, helper);
   }

   public static SSTableSimpleIterator createTombstoneOnly(TableMetadata metadata, DataInputPlus in, SerializationHeader header, SerializationHelper helper) {
      return new SSTableSimpleIterator.CurrentFormatTombstoneIterator(metadata, in, header, helper);
   }

   public abstract Row readStaticRow() throws IOException;

   private static class CurrentFormatTombstoneIterator extends SSTableSimpleIterator {
      private final SerializationHeader header;

      private CurrentFormatTombstoneIterator(TableMetadata metadata, DataInputPlus in, SerializationHeader header, SerializationHelper helper) {
         super(metadata, in, helper);
         this.header = header;
      }

      public Row readStaticRow() throws IOException {
         if(this.header.hasStatic()) {
            Row staticRow = ((UnfilteredSerializer)UnfilteredSerializer.serializers.get(this.helper.version)).deserializeStaticRow(this.in, this.header, this.helper);
            if(!staticRow.deletion().isLive()) {
               return ArrayBackedRow.emptyDeletedRow(staticRow.clustering(), staticRow.deletion());
            }
         }

         return Rows.EMPTY_STATIC_ROW;
      }

      protected Unfiltered computeNext() {
         try {
            Unfiltered unfiltered = ((UnfilteredSerializer)UnfilteredSerializer.serializers.get(this.helper.version)).deserializeTombstonesOnly((FileDataInput)this.in, this.header, this.helper);
            return unfiltered == null?(Unfiltered)this.endOfData():unfiltered;
         } catch (IOException var2) {
            throw new IOError(var2);
         }
      }
   }

   private static class CurrentFormatIterator extends SSTableSimpleIterator {
      private final SerializationHeader header;
      private final Row.Builder builder;

      private CurrentFormatIterator(TableMetadata metadata, DataInputPlus in, SerializationHeader header, SerializationHelper helper) {
         super(metadata, in, helper);
         this.header = header;
         this.builder = Row.Builder.sorted();
      }

      public Row readStaticRow() throws IOException {
         return this.header.hasStatic()?((UnfilteredSerializer)UnfilteredSerializer.serializers.get(this.helper.version)).deserializeStaticRow(this.in, this.header, this.helper):Rows.EMPTY_STATIC_ROW;
      }

      protected Unfiltered computeNext() {
         try {
            Unfiltered unfiltered = ((UnfilteredSerializer)UnfilteredSerializer.serializers.get(this.helper.version)).deserialize(this.in, this.header, this.helper, this.builder);
            return unfiltered == null?(Unfiltered)this.endOfData():unfiltered;
         } catch (IOException var2) {
            throw new IOError(var2);
         }
      }
   }
}
