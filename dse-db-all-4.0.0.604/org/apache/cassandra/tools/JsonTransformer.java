package org.apache.cassandra.tools;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneBoundaryMarker;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.util.DefaultPrettyPrinter;
import org.codehaus.jackson.util.MinimalPrettyPrinter;
import org.codehaus.jackson.util.DefaultPrettyPrinter.NopIndenter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JsonTransformer {
   private static final Logger logger = LoggerFactory.getLogger(JsonTransformer.class);
   private static final JsonFactory jsonFactory = new JsonFactory();
   private final JsonGenerator json;
   private final JsonTransformer.CompactIndenter objectIndenter = new JsonTransformer.CompactIndenter();
   private final JsonTransformer.CompactIndenter arrayIndenter = new JsonTransformer.CompactIndenter();
   private final TableMetadata metadata;
   private final ISSTableScanner currentScanner;
   private boolean rawTime = false;
   private long currentPosition = 0L;

   private JsonTransformer(JsonGenerator json, ISSTableScanner currentScanner, boolean rawTime, TableMetadata metadata, boolean isJsonLines) {
      this.json = json;
      this.metadata = metadata;
      this.currentScanner = currentScanner;
      this.rawTime = rawTime;
      if(isJsonLines) {
         MinimalPrettyPrinter minimalPrettyPrinter = new MinimalPrettyPrinter();
         minimalPrettyPrinter.setRootValueSeparator("\n");
         json.setPrettyPrinter(minimalPrettyPrinter);
      } else {
         DefaultPrettyPrinter prettyPrinter = new DefaultPrettyPrinter();
         prettyPrinter.indentObjectsWith(this.objectIndenter);
         prettyPrinter.indentArraysWith(this.arrayIndenter);
         json.setPrettyPrinter(prettyPrinter);
      }

   }

   public static void toJson(ISSTableScanner currentScanner, Stream<UnfilteredRowIterator> partitions, boolean rawTime, TableMetadata metadata, OutputStream out) throws IOException {
      JsonGenerator json = jsonFactory.createJsonGenerator(new OutputStreamWriter(out, StandardCharsets.UTF_8));
      Throwable var6 = null;

      try {
         JsonTransformer transformer = new JsonTransformer(json, currentScanner, rawTime, metadata, false);
         json.writeStartArray();
         partitions.forEach(transformer::serializePartition);
         json.writeEndArray();
      } catch (Throwable var15) {
         var6 = var15;
         throw var15;
      } finally {
         if(json != null) {
            if(var6 != null) {
               try {
                  json.close();
               } catch (Throwable var14) {
                  var6.addSuppressed(var14);
               }
            } else {
               json.close();
            }
         }

      }

   }

   public static void toJsonLines(ISSTableScanner currentScanner, Stream<UnfilteredRowIterator> partitions, boolean rawTime, TableMetadata metadata, OutputStream out) throws IOException {
      JsonGenerator json = jsonFactory.createJsonGenerator(new OutputStreamWriter(out, StandardCharsets.UTF_8));
      Throwable var6 = null;

      try {
         JsonTransformer transformer = new JsonTransformer(json, currentScanner, rawTime, metadata, true);
         partitions.forEach(transformer::serializePartition);
      } catch (Throwable var15) {
         var6 = var15;
         throw var15;
      } finally {
         if(json != null) {
            if(var6 != null) {
               try {
                  json.close();
               } catch (Throwable var14) {
                  var6.addSuppressed(var14);
               }
            } else {
               json.close();
            }
         }

      }

   }

   public static void keysToJson(ISSTableScanner currentScanner, Stream<DecoratedKey> keys, boolean rawTime, TableMetadata metadata, OutputStream out) throws IOException {
      JsonGenerator json = jsonFactory.createJsonGenerator(new OutputStreamWriter(out, StandardCharsets.UTF_8));
      Throwable var6 = null;

      try {
         JsonTransformer transformer = new JsonTransformer(json, currentScanner, rawTime, metadata, false);
         json.writeStartArray();
         keys.forEach(transformer::serializePartitionKey);
         json.writeEndArray();
      } catch (Throwable var15) {
         var6 = var15;
         throw var15;
      } finally {
         if(json != null) {
            if(var6 != null) {
               try {
                  json.close();
               } catch (Throwable var14) {
                  var6.addSuppressed(var14);
               }
            } else {
               json.close();
            }
         }

      }

   }

   private void updatePosition() {
      this.currentPosition = this.currentScanner.getCurrentPosition();
   }

   private void serializePartitionKey(DecoratedKey key) {
      AbstractType<?> keyValidator = this.metadata.partitionKeyType;
      this.objectIndenter.setCompact(true);

      try {
         this.arrayIndenter.setCompact(true);
         this.json.writeStartArray();
         if(keyValidator instanceof CompositeType) {
            CompositeType compositeType = (CompositeType)keyValidator;
            ByteBuffer keyBytes = key.getKey().duplicate();
            int i;
            if(keyBytes.remaining() >= 2) {
               i = ByteBufferUtil.getShortLength(keyBytes, keyBytes.position());
               if((i & '\uffff') == '\uffff') {
                  ByteBufferUtil.readShortLength(keyBytes);
               }
            }

            for(i = 0; keyBytes.remaining() > 0 && i < compositeType.getComponents().size(); ++i) {
               AbstractType<?> colType = (AbstractType)compositeType.getComponents().get(i);
               ByteBuffer value = ByteBufferUtil.readBytesWithShortLength(keyBytes);
               String colValue = colType.getString(value);
               this.json.writeString(colValue);
               byte b = keyBytes.get();
               if(b != 0) {
                  break;
               }
            }
         } else {
            assert this.metadata.partitionKeyColumns().size() == 1;

            this.json.writeString(keyValidator.getString(key.getKey()));
         }

         this.json.writeEndArray();
         this.objectIndenter.setCompact(false);
         this.arrayIndenter.setCompact(false);
      } catch (IOException var10) {
         logger.error("Failure serializing partition key.", var10);
      }

   }

   private void serializePartition(UnfilteredRowIterator partition) {
      try {
         this.json.writeStartObject();
         this.json.writeFieldName("partition");
         this.json.writeStartObject();
         this.json.writeFieldName("key");
         this.serializePartitionKey(partition.partitionKey());
         this.json.writeNumberField("position", this.currentScanner.getCurrentPosition());
         if(!partition.partitionLevelDeletion().isLive()) {
            this.serializeDeletion(partition.partitionLevelDeletion());
         }

         this.json.writeEndObject();
         if(partition.hasNext() || partition.staticRow() != null) {
            this.json.writeFieldName("rows");
            this.json.writeStartArray();
            this.updatePosition();
            if(!partition.staticRow().isEmpty()) {
               this.serializeRow(partition.staticRow());
            }

            this.updatePosition();

            for(; partition.hasNext(); this.updatePosition()) {
               Unfiltered unfiltered = (Unfiltered)partition.next();
               if(unfiltered instanceof Row) {
                  this.serializeRow((Row)unfiltered);
               } else if(unfiltered instanceof RangeTombstoneMarker) {
                  this.serializeTombstone((RangeTombstoneMarker)unfiltered);
               }
            }

            this.json.writeEndArray();
            this.json.writeEndObject();
         }
      } catch (IOException var4) {
         String key = this.metadata.partitionKeyType.getString(partition.partitionKey().getKey());
         logger.error("Fatal error parsing partition: {}", key, var4);
      }

   }

   private void serializeRow(Row row) {
      try {
         this.json.writeStartObject();
         String rowType = row.isStatic()?"static_block":"row";
         this.json.writeFieldName("type");
         this.json.writeString(rowType);
         this.json.writeNumberField("position", this.currentPosition);
         if(!row.isStatic()) {
            this.serializeClustering(row.clustering());
         }

         LivenessInfo liveInfo = row.primaryKeyLivenessInfo();
         if(!liveInfo.isEmpty()) {
            this.objectIndenter.setCompact(false);
            this.json.writeFieldName("liveness_info");
            this.objectIndenter.setCompact(true);
            this.json.writeStartObject();
            this.json.writeFieldName("tstamp");
            this.json.writeString(this.dateString(TimeUnit.MICROSECONDS, liveInfo.timestamp()));
            if(liveInfo.isExpiring()) {
               this.json.writeNumberField("ttl", liveInfo.ttl());
               this.json.writeFieldName("expires_at");
               this.json.writeString(this.dateString(TimeUnit.SECONDS, (long)liveInfo.localExpirationTime()));
               this.json.writeFieldName("expired");
               this.json.writeBoolean((long)liveInfo.localExpirationTime() < System.currentTimeMillis() / 1000L);
            }

            this.json.writeEndObject();
            this.objectIndenter.setCompact(false);
         }

         if(!row.deletion().isLive()) {
            this.serializeDeletion(row.deletion().time());
         }

         this.json.writeFieldName("cells");
         this.json.writeStartArray();
         Iterator var4 = row.iterator();

         while(var4.hasNext()) {
            ColumnData cd = (ColumnData)var4.next();
            this.serializeColumnData(cd, liveInfo);
         }

         this.json.writeEndArray();
         this.json.writeEndObject();
      } catch (IOException var6) {
         logger.error("Fatal error parsing row.", var6);
      }

   }

   private void serializeTombstone(RangeTombstoneMarker tombstone) {
      try {
         this.json.writeStartObject();
         this.json.writeFieldName("type");
         if(tombstone instanceof RangeTombstoneBoundMarker) {
            this.json.writeString("range_tombstone_bound");
            RangeTombstoneBoundMarker bm = (RangeTombstoneBoundMarker)tombstone;
            this.serializeBound((ClusteringBound)bm.clustering(), bm.deletionTime());
         } else {
            assert tombstone instanceof RangeTombstoneBoundaryMarker;

            this.json.writeString("range_tombstone_boundary");
            RangeTombstoneBoundaryMarker bm = (RangeTombstoneBoundaryMarker)tombstone;
            this.serializeBound(bm.openBound(false), bm.openDeletionTime(false));
            this.serializeBound(bm.closeBound(false), bm.closeDeletionTime(false));
         }

         this.json.writeEndObject();
         this.objectIndenter.setCompact(false);
      } catch (IOException var3) {
         logger.error("Failure parsing tombstone.", var3);
      }

   }

   private void serializeBound(ClusteringBound bound, DeletionTime deletionTime) throws IOException {
      this.json.writeFieldName(bound.isStart()?"start":"end");
      this.json.writeStartObject();
      this.json.writeFieldName("type");
      this.json.writeString(bound.isInclusive()?"inclusive":"exclusive");
      this.serializeClustering(bound.clustering());
      this.serializeDeletion(deletionTime);
      this.json.writeEndObject();
   }

   private void serializeClustering(ClusteringPrefix clustering) throws IOException {
      if(clustering.size() > 0) {
         this.json.writeFieldName("clustering");
         this.objectIndenter.setCompact(true);
         this.json.writeStartArray();
         this.arrayIndenter.setCompact(true);
         List<ColumnMetadata> clusteringColumns = this.metadata.clusteringColumns();

         for(int i = 0; i < clusteringColumns.size(); ++i) {
            ColumnMetadata column = (ColumnMetadata)clusteringColumns.get(i);
            if(i >= clustering.size()) {
               this.json.writeString("*");
            } else {
               this.json.writeRawValue(column.cellValueType().toJSONString(clustering.get(i), ProtocolVersion.CURRENT));
            }
         }

         this.json.writeEndArray();
         this.objectIndenter.setCompact(false);
         this.arrayIndenter.setCompact(false);
      }

   }

   private void serializeDeletion(DeletionTime deletion) throws IOException {
      this.json.writeFieldName("deletion_info");
      this.objectIndenter.setCompact(true);
      this.json.writeStartObject();
      this.json.writeFieldName("marked_deleted");
      this.json.writeString(this.dateString(TimeUnit.MICROSECONDS, deletion.markedForDeleteAt()));
      this.json.writeFieldName("local_delete_time");
      this.json.writeString(this.dateString(TimeUnit.SECONDS, (long)deletion.localDeletionTime()));
      this.json.writeEndObject();
      this.objectIndenter.setCompact(false);
   }

   private void serializeColumnData(ColumnData cd, LivenessInfo liveInfo) {
      if(cd.column().isSimple()) {
         this.serializeCell((Cell)cd, liveInfo);
      } else {
         ComplexColumnData complexData = (ComplexColumnData)cd;
         if(!complexData.complexDeletion().isLive()) {
            try {
               this.objectIndenter.setCompact(true);
               this.json.writeStartObject();
               this.json.writeFieldName("name");
               this.json.writeString(cd.column().name.toCQLString());
               this.serializeDeletion(complexData.complexDeletion());
               this.objectIndenter.setCompact(true);
               this.json.writeEndObject();
               this.objectIndenter.setCompact(false);
            } catch (IOException var6) {
               logger.error("Failure parsing ColumnData.", var6);
            }
         }

         Iterator var4 = complexData.iterator();

         while(var4.hasNext()) {
            Cell cell = (Cell)var4.next();
            this.serializeCell(cell, liveInfo);
         }
      }

   }

   private void serializeCell(Cell cell, LivenessInfo liveInfo) {
      try {
         this.json.writeStartObject();
         this.objectIndenter.setCompact(true);
         this.json.writeFieldName("name");
         AbstractType<?> type = cell.column().type;
         AbstractType<?> cellType = null;
         this.json.writeString(cell.column().name.toCQLString());
         int i;
         if(type.isCollection() && type.isMultiCell()) {
            CollectionType ct = (CollectionType)type;
            this.json.writeFieldName("path");
            this.arrayIndenter.setCompact(true);
            this.json.writeStartArray();
            i = 0;

            while(true) {
               if(i >= cell.path().size()) {
                  this.json.writeEndArray();
                  this.arrayIndenter.setCompact(false);
                  cellType = cell.column().cellValueType();
                  break;
               }

               this.json.writeString(ct.nameComparator().getString(cell.path().get(i)));
               ++i;
            }
         } else {
            Short elementIndex;
            if(type.isUDT() && type.isMultiCell()) {
               UserType ut = (UserType)type;
               this.json.writeFieldName("path");
               this.arrayIndenter.setCompact(true);
               this.json.writeStartArray();

               for(i = 0; i < cell.path().size(); ++i) {
                  elementIndex = (Short)ut.nameComparator().compose(cell.path().get(i));
                  this.json.writeString(ut.fieldNameAsString(elementIndex.shortValue()));
               }

               this.json.writeEndArray();
               this.arrayIndenter.setCompact(false);
               elementIndex = (Short)((UserType)type).nameComparator().compose(cell.path().get(0));
               cellType = ((UserType)type).fieldType(elementIndex.shortValue());
            } else if(type.isTuple() && type.isMultiCell()) {
               TupleType tt = (TupleType)type;
               this.json.writeFieldName("path");
               this.arrayIndenter.setCompact(true);
               this.json.writeStartArray();

               for(i = 0; i < cell.path().size(); ++i) {
                  elementIndex = (Short)tt.nameComparator().compose(cell.path().get(i));
                  this.json.writeString(elementIndex.toString());
               }

               this.json.writeEndArray();
               this.arrayIndenter.setCompact(false);
               elementIndex = (Short)((TupleType)type).nameComparator().compose(cell.path().get(0));
               cellType = tt.type(elementIndex.shortValue());
            } else {
               cellType = cell.column().cellValueType();
            }
         }

         if(cell.isTombstone()) {
            this.json.writeFieldName("deletion_info");
            this.objectIndenter.setCompact(true);
            this.json.writeStartObject();
            this.json.writeFieldName("local_delete_time");
            this.json.writeString(this.dateString(TimeUnit.SECONDS, (long)cell.localDeletionTime()));
            this.json.writeEndObject();
            this.objectIndenter.setCompact(false);
         } else {
            this.json.writeFieldName("value");
            this.json.writeRawValue(cellType.toJSONString(cell.value(), ProtocolVersion.CURRENT));
         }

         if(liveInfo.isEmpty() || cell.timestamp() != liveInfo.timestamp()) {
            this.json.writeFieldName("tstamp");
            this.json.writeString(this.dateString(TimeUnit.MICROSECONDS, cell.timestamp()));
         }

         if(cell.isExpiring() && (liveInfo.isEmpty() || cell.ttl() != liveInfo.ttl())) {
            this.json.writeFieldName("ttl");
            this.json.writeNumber(cell.ttl());
            this.json.writeFieldName("expires_at");
            this.json.writeString(this.dateString(TimeUnit.SECONDS, (long)cell.localDeletionTime()));
            this.json.writeFieldName("expired");
            this.json.writeBoolean(!cell.isLive((int)(System.currentTimeMillis() / 1000L)));
         }

         this.json.writeEndObject();
         this.objectIndenter.setCompact(false);
      } catch (IOException var8) {
         logger.error("Failure parsing cell.", var8);
      }

   }

   private String dateString(TimeUnit from, long time) {
      if(this.rawTime) {
         return Long.toString(time);
      } else {
         long secs = from.toSeconds(time);
         long offset = Math.floorMod(from.toNanos(time), 1000000000L);
         return Instant.ofEpochSecond(secs, offset).toString();
      }
   }

   private static final class CompactIndenter extends NopIndenter {
      private static final int INDENT_LEVELS = 16;
      private final char[] indents;
      private final int charsPerLevel;
      private final String eol;
      private static final String space = " ";
      private boolean compact;

      CompactIndenter() {
         this("  ", System.lineSeparator());
      }

      CompactIndenter(String indent, String eol) {
         this.compact = false;
         this.eol = eol;
         this.charsPerLevel = indent.length();
         this.indents = new char[indent.length() * 16];
         int offset = 0;

         for(int i = 0; i < 16; ++i) {
            indent.getChars(0, indent.length(), this.indents, offset);
            offset += indent.length();
         }

      }

      public boolean isInline() {
         return false;
      }

      public void setCompact(boolean compact) {
         this.compact = compact;
      }

      public void writeIndentation(JsonGenerator jg, int level) {
         try {
            if(!this.compact) {
               jg.writeRaw(this.eol);
               if(level > 0) {
                  for(level *= this.charsPerLevel; level > this.indents.length; level -= this.indents.length) {
                     jg.writeRaw(this.indents, 0, this.indents.length);
                  }

                  jg.writeRaw(this.indents, 0, level);
               }
            } else {
               jg.writeRaw(" ");
            }
         } catch (IOException var4) {
            var4.printStackTrace();
            System.exit(1);
         }

      }
   }
}
