package org.apache.cassandra.streaming.messages;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.streaming.compress.CompressionInfo;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.UUIDSerializer;
import org.apache.cassandra.utils.versioning.VersionDependent;
import org.apache.cassandra.utils.versioning.Versioned;

public class FileMessageHeader {
   public static Versioned<StreamMessage.StreamVersion, FileMessageHeader.FileMessageHeaderSerializer> serializers = StreamMessage.StreamVersion.versioned(FileMessageHeader.FileMessageHeaderSerializer::new);
   public final TableId tableId;
   public final int sequenceNumber;
   public final Version version;
   public final SSTableFormat.Type format;
   public final long estimatedKeys;
   public final List<Pair<Long, Long>> sections;
   public final CompressionInfo compressionInfo;
   private final CompressionMetadata compressionMetadata;
   public final long repairedAt;
   public final UUID pendingRepair;
   public final int sstableLevel;
   public final SerializationHeader.Component header;
   private final transient long size;

   public FileMessageHeader(TableId tableId, int sequenceNumber, Version version, SSTableFormat.Type format, long estimatedKeys, List<Pair<Long, Long>> sections, CompressionInfo compressionInfo, long repairedAt, UUID pendingRepair, int sstableLevel, SerializationHeader.Component header) {
      this.tableId = tableId;
      this.sequenceNumber = sequenceNumber;
      this.version = version;
      this.format = format;
      this.estimatedKeys = estimatedKeys;
      this.sections = sections;
      this.compressionInfo = compressionInfo;
      this.compressionMetadata = null;
      this.repairedAt = repairedAt;
      this.pendingRepair = pendingRepair;
      this.sstableLevel = sstableLevel;
      this.header = header;
      this.size = this.calculateSize();
   }

   public FileMessageHeader(TableId tableId, int sequenceNumber, Version version, SSTableFormat.Type format, long estimatedKeys, List<Pair<Long, Long>> sections, CompressionMetadata compressionMetadata, long repairedAt, UUID pendingRepair, int sstableLevel, SerializationHeader.Component header) {
      this.tableId = tableId;
      this.sequenceNumber = sequenceNumber;
      this.version = version;
      this.format = format;
      this.estimatedKeys = estimatedKeys;
      this.sections = sections;
      this.compressionInfo = null;
      this.compressionMetadata = compressionMetadata;
      this.repairedAt = repairedAt;
      this.pendingRepair = pendingRepair;
      this.sstableLevel = sstableLevel;
      this.header = header;
      this.size = this.calculateSize();
   }

   public boolean isCompressed() {
      return this.compressionInfo != null || this.compressionMetadata != null;
   }

   public long size() {
      return this.size;
   }

   private long calculateSize() {
      long transferSize = 0L;
      if(this.compressionInfo != null) {
         CompressionMetadata.Chunk[] var3 = this.compressionInfo.chunks;
         int var4 = var3.length;

         for(int var5 = 0; var5 < var4; ++var5) {
            CompressionMetadata.Chunk chunk = var3[var5];
            transferSize += (long)(chunk.length + 4);
         }
      } else {
         Pair section;
         if(this.compressionMetadata != null) {
            transferSize = this.compressionMetadata.getTotalSizeForSections(this.sections);
         } else {
            for(Iterator var7 = this.sections.iterator(); var7.hasNext(); transferSize += ((Long)section.right).longValue() - ((Long)section.left).longValue()) {
               section = (Pair)var7.next();
            }
         }
      }

      return transferSize;
   }

   public String toString() {
      StringBuilder sb = new StringBuilder("Header (");
      sb.append("tableId: ").append(this.tableId);
      sb.append(", #").append(this.sequenceNumber);
      sb.append(", version: ").append(this.version);
      sb.append(", format: ").append(this.format);
      sb.append(", estimated keys: ").append(this.estimatedKeys);
      sb.append(", transfer size: ").append(this.size());
      sb.append(", compressed?: ").append(this.isCompressed());
      sb.append(", repairedAt: ").append(this.repairedAt);
      sb.append(", pendingRepair: ").append(this.pendingRepair);
      sb.append(", level: ").append(this.sstableLevel);
      sb.append(')');
      return sb.toString();
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(o != null && this.getClass() == o.getClass()) {
         FileMessageHeader that = (FileMessageHeader)o;
         return this.sequenceNumber == that.sequenceNumber && this.tableId.equals(that.tableId);
      } else {
         return false;
      }
   }

   public int hashCode() {
      int result = this.tableId.hashCode();
      result = 31 * result + this.sequenceNumber;
      return result;
   }

   static class FileMessageHeaderSerializer extends VersionDependent<StreamMessage.StreamVersion> {
      FileMessageHeaderSerializer(StreamMessage.StreamVersion version) {
         super(version);
      }

      public CompressionInfo serialize(FileMessageHeader header, DataOutputPlus out) throws IOException {
         header.tableId.serialize(out);
         out.writeInt(header.sequenceNumber);
         out.writeUTF(header.version.toString());
         out.writeUTF(header.format.name);
         out.writeLong(header.estimatedKeys);
         out.writeInt(header.sections.size());
         Iterator var3 = header.sections.iterator();

         while(var3.hasNext()) {
            Pair<Long, Long> section = (Pair)var3.next();
            out.writeLong(((Long)section.left).longValue());
            out.writeLong(((Long)section.right).longValue());
         }

         CompressionInfo compressionInfo = null;
         if(header.compressionMetadata != null) {
            compressionInfo = new CompressionInfo(header.compressionMetadata.getChunksForSections(header.sections), header.compressionMetadata.parameters);
         }

         ((Serializer)CompressionInfo.serializers.get(this.version)).serialize(compressionInfo, out);
         out.writeLong(header.repairedAt);
         out.writeBoolean(header.pendingRepair != null);
         if(header.pendingRepair != null) {
            UUIDSerializer.serializer.serialize(header.pendingRepair, out);
         }

         out.writeInt(header.sstableLevel);
         SerializationHeader.serializer.serialize(header.version, header.header, out);
         return compressionInfo;
      }

      public FileMessageHeader deserialize(DataInputPlus in) throws IOException {
         TableId tableId = TableId.deserialize(in);
         int sequenceNumber = in.readInt();
         String versionString = in.readUTF();
         SSTableFormat.Type format = SSTableFormat.Type.validate(in.readUTF());
         Version sstableVersion = format.info.getVersion(versionString);
         long estimatedKeys = in.readLong();
         int count = in.readInt();
         List<Pair<Long, Long>> sections = new ArrayList(count);

         for(int k = 0; k < count; ++k) {
            sections.add(Pair.create(Long.valueOf(in.readLong()), Long.valueOf(in.readLong())));
         }

         CompressionInfo compressionInfo = (CompressionInfo)((Serializer)CompressionInfo.serializers.get(this.version)).deserialize(in);
         long repairedAt = in.readLong();
         UUID pendingRepair = in.readBoolean()?UUIDSerializer.serializer.deserialize(in):null;
         int sstableLevel = in.readInt();
         SerializationHeader.Component header = SerializationHeader.serializer.deserialize(sstableVersion, in);
         return new FileMessageHeader(tableId, sequenceNumber, sstableVersion, format, estimatedKeys, sections, compressionInfo, repairedAt, pendingRepair, sstableLevel, header);
      }

      public long serializedSize(FileMessageHeader header) {
         long size = (long)header.tableId.serializedSize();
         size += (long)TypeSizes.sizeof(header.sequenceNumber);
         size += (long)TypeSizes.sizeof(header.version.toString());
         size += (long)TypeSizes.sizeof(header.format.name);
         size += (long)TypeSizes.sizeof(header.estimatedKeys);
         size += (long)TypeSizes.sizeof(header.sections.size());

         Pair section;
         for(Iterator var4 = header.sections.iterator(); var4.hasNext(); size += (long)TypeSizes.sizeof(((Long)section.right).longValue())) {
            section = (Pair)var4.next();
            size += (long)TypeSizes.sizeof(((Long)section.left).longValue());
         }

         size += ((Serializer)CompressionInfo.serializers.get(this.version)).serializedSize(header.compressionInfo);
         size += (long)TypeSizes.sizeof(header.repairedAt);
         size += (long)TypeSizes.sizeof(header.pendingRepair != null);
         size += header.pendingRepair != null?UUIDSerializer.serializer.serializedSize(header.pendingRepair):0L;
         size += (long)TypeSizes.sizeof(header.sstableLevel);
         size += (long)SerializationHeader.serializer.serializedSize(header.version, header.header);
         return size;
      }
   }
}
