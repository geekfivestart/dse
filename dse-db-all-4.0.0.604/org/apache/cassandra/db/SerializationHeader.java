package org.apache.cassandra.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Map.Entry;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.metadata.IMetadataComponentSerializer;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.SearchIterator;

public class SerializationHeader {
   public static final SerializationHeader.Serializer serializer = new SerializationHeader.Serializer();
   private final boolean isForSSTable;
   private final AbstractType<?> keyType;
   private final List<AbstractType<?>> clusteringTypes;
   private final RegularAndStaticColumns columns;
   private final Columns.ColumnSearchIterator staticColumnIterator;
   private final Columns.ColumnSearchIterator regularColumnIterator;
   private final EncodingStats stats;
   private final Map<ByteBuffer, AbstractType<?>> typeMap;

   private SerializationHeader(boolean isForSSTable, AbstractType<?> keyType, List<AbstractType<?>> clusteringTypes, RegularAndStaticColumns columns, EncodingStats stats, Map<ByteBuffer, AbstractType<?>> typeMap) {
      this.isForSSTable = isForSSTable;
      this.keyType = keyType;
      this.clusteringTypes = clusteringTypes;
      this.columns = columns;
      this.staticColumnIterator = columns.statics.isEmpty()?null:columns.statics.searchIterator();
      this.regularColumnIterator = columns.regulars.searchIterator();
      this.stats = stats;
      this.typeMap = typeMap;
   }

   public static SerializationHeader makeWithoutStats(TableMetadata metadata) {
      return new SerializationHeader(true, metadata, metadata.regularAndStaticColumns(), EncodingStats.NO_STATS);
   }

   public static SerializationHeader make(TableMetadata metadata, Collection<SSTableReader> sstables) {
      EncodingStats.Collector stats = new EncodingStats.Collector();
      RegularAndStaticColumns.Builder columns = RegularAndStaticColumns.builder();
      columns.addAll(metadata.regularAndStaticColumns());
      Iterator var4 = sstables.iterator();

      while(var4.hasNext()) {
         SSTableReader sstable = (SSTableReader)var4.next();
         stats.updateTimestamp(sstable.getMinTimestamp());
         stats.updateLocalDeletionTime(sstable.getMinLocalDeletionTime());
         stats.updateTTL(sstable.getMinTTL());
      }

      return new SerializationHeader(true, metadata, columns.build(), stats.get());
   }

   public SerializationHeader(boolean isForSSTable, TableMetadata metadata, RegularAndStaticColumns columns, EncodingStats stats) {
      this(isForSSTable, metadata.partitionKeyType, metadata.comparator.subtypes(), columns, stats, (Map)null);
   }

   public RegularAndStaticColumns columns() {
      return this.columns;
   }

   public SearchIterator<ColumnMetadata, ColumnMetadata> columnsIterator(boolean isStatic) {
      Columns.ColumnSearchIterator iterator = isStatic?this.staticColumnIterator:this.regularColumnIterator;

      assert iterator != null;

      iterator.rewind();
      return iterator;
   }

   public boolean hasStatic() {
      return !this.columns.statics.isEmpty();
   }

   public boolean isForSSTable() {
      return this.isForSSTable;
   }

   public EncodingStats stats() {
      return this.stats;
   }

   public AbstractType<?> keyType() {
      return this.keyType;
   }

   public List<AbstractType<?>> clusteringTypes() {
      return this.clusteringTypes;
   }

   public Columns columns(boolean isStatic) {
      return isStatic?this.columns.statics:this.columns.regulars;
   }

   public AbstractType<?> getType(ColumnMetadata column) {
      return this.typeMap == null?column.type:(AbstractType)this.typeMap.get(column.name.bytes);
   }

   public void writeTimestamp(long timestamp, DataOutputPlus out) throws IOException {
      out.writeUnsignedVInt(timestamp - this.stats.minTimestamp);
   }

   public void writeLocalDeletionTime(int localDeletionTime, DataOutputPlus out) throws IOException {
      out.writeUnsignedVInt((long)(localDeletionTime - this.stats.minLocalDeletionTime));
   }

   public void writeTTL(int ttl, DataOutputPlus out) throws IOException {
      out.writeUnsignedVInt((long)(ttl - this.stats.minTTL));
   }

   public void writeDeletionTime(DeletionTime dt, DataOutputPlus out) throws IOException {
      this.writeTimestamp(dt.markedForDeleteAt(), out);
      this.writeLocalDeletionTime(dt.localDeletionTime(), out);
   }

   public long readTimestamp(DataInputPlus in) throws IOException {
      return in.readUnsignedVInt() + this.stats.minTimestamp;
   }

   public int readLocalDeletionTime(DataInputPlus in) throws IOException {
      return (int)in.readUnsignedVInt() + this.stats.minLocalDeletionTime;
   }

   public int readTTL(DataInputPlus in) throws IOException {
      return (int)in.readUnsignedVInt() + this.stats.minTTL;
   }

   public DeletionTime readDeletionTime(DataInputPlus in) throws IOException {
      long markedAt = this.readTimestamp(in);
      int localDeletionTime = this.readLocalDeletionTime(in);
      return new DeletionTime(markedAt, localDeletionTime);
   }

   public long timestampSerializedSize(long timestamp) {
      return (long)TypeSizes.sizeofUnsignedVInt(timestamp - this.stats.minTimestamp);
   }

   public long localDeletionTimeSerializedSize(int localDeletionTime) {
      return (long)TypeSizes.sizeofUnsignedVInt((long)(localDeletionTime - this.stats.minLocalDeletionTime));
   }

   public long ttlSerializedSize(int ttl) {
      return (long)TypeSizes.sizeofUnsignedVInt((long)(ttl - this.stats.minTTL));
   }

   public long deletionTimeSerializedSize(DeletionTime dt) {
      return this.timestampSerializedSize(dt.markedForDeleteAt()) + this.localDeletionTimeSerializedSize(dt.localDeletionTime());
   }

   public void skipTimestamp(DataInputPlus in) throws IOException {
      in.readUnsignedVInt();
   }

   public void skipLocalDeletionTime(DataInputPlus in) throws IOException {
      in.readUnsignedVInt();
   }

   public void skipTTL(DataInputPlus in) throws IOException {
      in.readUnsignedVInt();
   }

   public void skipDeletionTime(DataInputPlus in) throws IOException {
      this.skipTimestamp(in);
      this.skipLocalDeletionTime(in);
   }

   public SerializationHeader.Component toComponent() {
      Map<ByteBuffer, AbstractType<?>> staticColumns = new LinkedHashMap();
      Map<ByteBuffer, AbstractType<?>> regularColumns = new LinkedHashMap();
      Iterator var3 = this.columns.statics.iterator();

      ColumnMetadata column;
      while(var3.hasNext()) {
         column = (ColumnMetadata)var3.next();
         staticColumns.put(column.name.bytes, column.type);
      }

      var3 = this.columns.regulars.iterator();

      while(var3.hasNext()) {
         column = (ColumnMetadata)var3.next();
         regularColumns.put(column.name.bytes, column.type);
      }

      return new SerializationHeader.Component(this.keyType, this.clusteringTypes, staticColumns, regularColumns, this.stats);
   }

   public String toString() {
      return String.format("SerializationHeader[key=%s, cks=%s, columns=%s, stats=%s, typeMap=%s]", new Object[]{this.keyType, this.clusteringTypes, this.columns, this.stats, this.typeMap});
   }

   public static class Serializer implements IMetadataComponentSerializer<SerializationHeader.Component> {
      public Serializer() {
      }

      public void serializeForMessaging(SerializationHeader header, ColumnFilter selection, DataOutputPlus out, boolean hasStatic) throws IOException {
         EncodingStats.serializer.serialize(header.stats, out);
         if(selection == null) {
            if(hasStatic) {
               Columns.serializer.serialize(header.columns.statics, out);
            }

            Columns.serializer.serialize(header.columns.regulars, out);
         } else {
            if(hasStatic) {
               Columns.serializer.serializeSubset(header.columns.statics, selection.fetchedColumns().statics, out);
            }

            Columns.serializer.serializeSubset(header.columns.regulars, selection.fetchedColumns().regulars, out);
         }

      }

      public SerializationHeader deserializeForMessaging(DataInputPlus in, TableMetadata metadata, ColumnFilter selection, boolean hasStatic) throws IOException {
         EncodingStats stats = EncodingStats.serializer.deserialize(in);
         AbstractType<?> keyType = metadata.partitionKeyType;
         List<AbstractType<?>> clusteringTypes = metadata.comparator.subtypes();
         Columns statics;
         Columns regulars;
         if(selection == null) {
            statics = hasStatic?Columns.serializer.deserialize(in, metadata):Columns.NONE;
            regulars = Columns.serializer.deserialize(in, metadata);
         } else {
            statics = hasStatic?Columns.serializer.deserializeSubset(selection.fetchedColumns().statics, in):Columns.NONE;
            regulars = Columns.serializer.deserializeSubset(selection.fetchedColumns().regulars, in);
         }

         return new SerializationHeader(false, keyType, clusteringTypes, new RegularAndStaticColumns(statics, regulars), stats, (Map)null);
      }

      public long serializedSizeForMessaging(SerializationHeader header, ColumnFilter selection, boolean hasStatic) {
         long size = (long)EncodingStats.serializer.serializedSize(header.stats);
         if(selection == null) {
            if(hasStatic) {
               size += Columns.serializer.serializedSize(header.columns.statics);
            }

            size += Columns.serializer.serializedSize(header.columns.regulars);
         } else {
            if(hasStatic) {
               size += Columns.serializer.serializedSubsetSize(header.columns.statics, selection.fetchedColumns().statics);
            }

            size += Columns.serializer.serializedSubsetSize(header.columns.regulars, selection.fetchedColumns().regulars);
         }

         return size;
      }

      public void serialize(Version version, SerializationHeader.Component header, DataOutputPlus out) throws IOException {
         EncodingStats.serializer.serialize(header.stats, out);
         this.writeType(header.keyType, out);
         out.writeUnsignedVInt((long)header.clusteringTypes.size());
         Iterator var4 = header.clusteringTypes.iterator();

         while(var4.hasNext()) {
            AbstractType<?> type = (AbstractType)var4.next();
            this.writeType(type, out);
         }

         this.writeColumnsWithTypes(header.staticColumns, out);
         this.writeColumnsWithTypes(header.regularColumns, out);
      }

      public SerializationHeader.Component deserialize(Version version, DataInputPlus in) throws IOException {
         EncodingStats stats = EncodingStats.serializer.deserialize(in);
         AbstractType<?> keyType = this.readType(in);
         int size = (int)in.readUnsignedVInt();
         List<AbstractType<?>> clusteringTypes = new ArrayList(size);

         for(int i = 0; i < size; ++i) {
            clusteringTypes.add(this.readType(in));
         }

         Map<ByteBuffer, AbstractType<?>> staticColumns = new LinkedHashMap();
         Map<ByteBuffer, AbstractType<?>> regularColumns = new LinkedHashMap();
         this.readColumnsWithType(in, staticColumns);
         this.readColumnsWithType(in, regularColumns);
         return new SerializationHeader.Component(keyType, clusteringTypes, staticColumns, regularColumns, stats);
      }

      public int serializedSize(Version version, SerializationHeader.Component header) {
         int size = EncodingStats.serializer.serializedSize(header.stats);
         size += this.sizeofType(header.keyType);
         size += TypeSizes.sizeofUnsignedVInt((long)header.clusteringTypes.size());

         AbstractType type;
         for(Iterator var4 = header.clusteringTypes.iterator(); var4.hasNext(); size += this.sizeofType(type)) {
            type = (AbstractType)var4.next();
         }

         size = (int)((long)size + this.sizeofColumnsWithTypes(header.staticColumns));
         size = (int)((long)size + this.sizeofColumnsWithTypes(header.regularColumns));
         return size;
      }

      private void writeColumnsWithTypes(Map<ByteBuffer, AbstractType<?>> columns, DataOutputPlus out) throws IOException {
         out.writeUnsignedVInt((long)columns.size());
         Iterator var3 = columns.entrySet().iterator();

         while(var3.hasNext()) {
            Entry<ByteBuffer, AbstractType<?>> entry = (Entry)var3.next();
            ByteBufferUtil.writeWithVIntLength((ByteBuffer)entry.getKey(), out);
            this.writeType((AbstractType)entry.getValue(), out);
         }

      }

      private long sizeofColumnsWithTypes(Map<ByteBuffer, AbstractType<?>> columns) {
         long size = (long)TypeSizes.sizeofUnsignedVInt((long)columns.size());

         Entry entry;
         for(Iterator var4 = columns.entrySet().iterator(); var4.hasNext(); size += (long)this.sizeofType((AbstractType)entry.getValue())) {
            entry = (Entry)var4.next();
            size += (long)ByteBufferUtil.serializedSizeWithVIntLength((ByteBuffer)entry.getKey());
         }

         return size;
      }

      private void readColumnsWithType(DataInputPlus in, Map<ByteBuffer, AbstractType<?>> typeMap) throws IOException {
         int length = (int)in.readUnsignedVInt();

         for(int i = 0; i < length; ++i) {
            ByteBuffer name = ByteBufferUtil.readWithVIntLength(in);
            typeMap.put(name, this.readType(in));
         }

      }

      private void writeType(AbstractType<?> type, DataOutputPlus out) throws IOException {
         ByteBufferUtil.writeWithVIntLength(UTF8Type.instance.decompose(type.toString()), out);
      }

      private AbstractType<?> readType(DataInputPlus in) throws IOException {
         ByteBuffer raw = ByteBufferUtil.readWithVIntLength(in);
         return TypeParser.parse((String)UTF8Type.instance.compose(raw));
      }

      private int sizeofType(AbstractType<?> type) {
         return ByteBufferUtil.serializedSizeWithVIntLength(UTF8Type.instance.decompose(type.toString()));
      }
   }

   public static class Component extends MetadataComponent {
      private final AbstractType<?> keyType;
      private final List<AbstractType<?>> clusteringTypes;
      private final Map<ByteBuffer, AbstractType<?>> staticColumns;
      private final Map<ByteBuffer, AbstractType<?>> regularColumns;
      private final EncodingStats stats;

      private Component(AbstractType<?> keyType, List<AbstractType<?>> clusteringTypes, Map<ByteBuffer, AbstractType<?>> staticColumns, Map<ByteBuffer, AbstractType<?>> regularColumns, EncodingStats stats) {
         this.keyType = keyType;
         this.clusteringTypes = clusteringTypes;
         this.staticColumns = staticColumns;
         this.regularColumns = regularColumns;
         this.stats = stats;
      }

      public MetadataType getType() {
         return MetadataType.HEADER;
      }

      public SerializationHeader toHeader(TableMetadata metadata) {
         Map<ByteBuffer, AbstractType<?>> typeMap = new HashMap(this.staticColumns.size() + this.regularColumns.size());
         typeMap.putAll(this.staticColumns);
         typeMap.putAll(this.regularColumns);
         RegularAndStaticColumns.Builder builder = RegularAndStaticColumns.builder();
         Iterator var4 = this.staticColumns.entrySet().iterator();

         Entry regularEntry;
         while(var4.hasNext()) {
            regularEntry = (Entry)var4.next();
            builder.add(ColumnMetadata.staticColumn(metadata, (ByteBuffer)regularEntry.getKey(), (AbstractType)regularEntry.getValue()));
         }

         var4 = this.regularColumns.entrySet().iterator();

         while(var4.hasNext()) {
            regularEntry = (Entry)var4.next();
            builder.add(ColumnMetadata.regularColumn(metadata, (ByteBuffer)regularEntry.getKey(), (AbstractType)regularEntry.getValue()));
         }

         return new SerializationHeader(true, this.keyType, this.clusteringTypes, builder.build(), this.stats, typeMap);
      }

      public boolean equals(Object o) {
         if(!(o instanceof SerializationHeader.Component)) {
            return false;
         } else {
            SerializationHeader.Component that = (SerializationHeader.Component)o;
            return Objects.equals(this.keyType, that.keyType) && Objects.equals(this.clusteringTypes, that.clusteringTypes) && Objects.equals(this.staticColumns, that.staticColumns) && Objects.equals(this.regularColumns, that.regularColumns) && Objects.equals(this.stats, that.stats);
         }
      }

      public int hashCode() {
         return Objects.hash(new Object[]{this.keyType, this.clusteringTypes, this.staticColumns, this.regularColumns, this.stats});
      }

      public String toString() {
         return String.format("SerializationHeader.Component[key=%s, cks=%s, statics=%s, regulars=%s, stats=%s]", new Object[]{this.keyType, this.clusteringTypes, this.staticColumns, this.regularColumns, this.stats});
      }

      public AbstractType<?> getKeyType() {
         return this.keyType;
      }

      public List<AbstractType<?>> getClusteringTypes() {
         return this.clusteringTypes;
      }

      public Map<ByteBuffer, AbstractType<?>> getStaticColumns() {
         return this.staticColumns;
      }

      public Map<ByteBuffer, AbstractType<?>> getRegularColumns() {
         return this.regularColumns;
      }

      public EncodingStats getEncodingStats() {
         return this.stats;
      }
   }
}
