package org.apache.cassandra.cql3;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import org.apache.cassandra.cql3.selection.ResultBuilder;
import org.apache.cassandra.cql3.selection.Selection;
import org.apache.cassandra.cql3.selection.SelectionColumns;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.aggregation.AggregationSpecification;
import org.apache.cassandra.db.aggregation.GroupMaker;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.transport.CBCodec;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.DataType;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Flags;
import org.apache.cassandra.utils.MD5Digest;

public class ResultSet {
   private static final int INITIAL_ROWS_CAPACITY = 16;
   public static final ResultSet.Codec codec = new ResultSet.Codec();
   public final ResultSet.ResultMetadata metadata;
   public final List<List<ByteBuffer>> rows;

   public ResultSet(ResultSet.ResultMetadata metadata) {
      this(metadata, new ArrayList(16));
   }

   public ResultSet(ResultSet.ResultMetadata metadata, List<List<ByteBuffer>> rows) {
      this.metadata = metadata;
      this.rows = rows;
   }

   public int size() {
      return this.rows.size();
   }

   public boolean isEmpty() {
      return this.size() == 0;
   }

   public void addRow(List<ByteBuffer> row) {
      this.rows.add(row);
   }

   public void addColumnValue(ByteBuffer value) {
      if(this.rows.isEmpty() || this.lastRow().size() == this.metadata.valueCount()) {
         this.rows.add(new ArrayList(this.metadata.valueCount()));
      }

      this.lastRow().add(value);
   }

   private List<ByteBuffer> lastRow() {
      return (List)this.rows.get(this.rows.size() - 1);
   }

   public void reverse() {
      Collections.reverse(this.rows);
   }

   public void trim(int limit) {
      int toRemove = this.rows.size() - limit;
      if(toRemove > 0) {
         for(int i = 0; i < toRemove; ++i) {
            this.rows.remove(this.rows.size() - 1);
         }
      }

   }

   public String toString() {
      try {
         StringBuilder sb = new StringBuilder();
         sb.append(this.metadata).append('\n');
         Iterator var2 = this.rows.iterator();

         while(var2.hasNext()) {
            List<ByteBuffer> row = (List)var2.next();

            for(int i = 0; i < row.size(); ++i) {
               ByteBuffer v = (ByteBuffer)row.get(i);
               if(v == null) {
                  sb.append(" | null");
               } else {
                  sb.append(" | ");
                  if(Flags.contains(this.metadata.flags, 4)) {
                     sb.append("0x").append(ByteBufferUtil.bytesToHex(v));
                  } else {
                     sb.append(((ColumnSpecification)this.metadata.names.get(i)).type.getString(v));
                  }
               }
            }

            sb.append('\n');
         }

         sb.append("---");
         return sb.toString();
      } catch (Exception var6) {
         throw new RuntimeException(var6);
      }
   }

   public static int estimatedRowSizeForColumns(TableMetadata cfm, SelectionColumns columns) {
      return estimatedRowSize(columns.getColumnSpecifications(), cfm);
   }

   public static int estimatedRowSizeForAllColumns(TableMetadata cfm) {
      return estimatedRowSize(cfm.columns(), cfm);
   }

   private static int estimatedRowSize(Iterable<? extends ColumnSpecification> columns, TableMetadata cfm) {
      ColumnFamilyStore cfs = Keyspace.open(cfm.keyspace).getColumnFamilyStore(cfm.name);
      int avgColumnSize = (int)(cfs.getMeanCells() > 0?cfs.getMeanPartitionSize() / (double)cfs.getMeanCells():cfs.getMeanPartitionSize());
      int ret = 0;

      int fixedLength;
      for(Iterator var5 = columns.iterator(); var5.hasNext(); ret += CBUtil.sizeOfValue(fixedLength > 0?fixedLength:avgColumnSize)) {
         ColumnSpecification def = (ColumnSpecification)var5.next();
         fixedLength = def.type.valueLengthIfFixed();
      }

      return ret;
   }

   public static ResultSet.Builder makeBuilder(ResultSet.ResultMetadata resultMetadata, Selection.Selectors selectors) {
      return new ResultSet.Builder(resultMetadata, selectors, (GroupMaker)null);
   }

   public static ResultSet.Builder makeBuilder(ResultSet.ResultMetadata resultMetadata, Selection.Selectors selectors, AggregationSpecification aggregationSpec) {
      return aggregationSpec == null?new ResultSet.Builder(resultMetadata, selectors, (GroupMaker)null):new ResultSet.Builder(resultMetadata, selectors, aggregationSpec.newGroupMaker());
   }

   public static final class Builder extends ResultBuilder {
      private ResultSet resultSet;

      public Builder(ResultSet.ResultMetadata resultMetadata, Selection.Selectors selectors, GroupMaker groupMaker) {
         super(selectors, groupMaker);
         this.resultSet = new ResultSet(resultMetadata, new ArrayList(16));
      }

      public boolean onRowCompleted(List<ByteBuffer> row, boolean nextRowPending) {
         this.resultSet.addRow(row);
         return true;
      }

      public boolean resultIsEmpty() {
         return this.resultSet.isEmpty();
      }

      public ResultSet build() throws InvalidRequestException {
         this.complete();
         return this.resultSet;
      }
   }

   public interface ResultSetFlag {
      int NONE = 0;
      int GLOBAL_TABLES_SPEC = 1;
      int HAS_MORE_PAGES = 2;
      int NO_METADATA = 4;
      int METADATA_CHANGED = 8;
      int CONTINUOUS_PAGING = 1073741824;
      int LAST_CONTINOUS_PAGE = -2147483648;
   }

   public static class PreparedMetadata {
      public static final CBCodec<ResultSet.PreparedMetadata> codec = new ResultSet.PreparedMetadata.Codec();
      private int flags;
      public final List<ColumnSpecification> names;
      private final short[] partitionKeyBindIndexes;

      public PreparedMetadata(List<ColumnSpecification> names, short[] partitionKeyBindIndexes) {
         this(0, names, partitionKeyBindIndexes);
         if(!names.isEmpty() && ColumnSpecification.allInSameTable(names)) {
            this.flags = Flags.add(this.flags, 1);
         }

      }

      private PreparedMetadata(int flags, List<ColumnSpecification> names, short[] partitionKeyBindIndexes) {
         this.flags = flags;
         this.names = names;
         this.partitionKeyBindIndexes = partitionKeyBindIndexes;
      }

      public ResultSet.PreparedMetadata copy() {
         return new ResultSet.PreparedMetadata(this.flags, this.names, this.partitionKeyBindIndexes);
      }

      public boolean equals(Object other) {
         if(this == other) {
            return true;
         } else if(!(other instanceof ResultSet.PreparedMetadata)) {
            return false;
         } else {
            ResultSet.PreparedMetadata that = (ResultSet.PreparedMetadata)other;
            return this.names.equals(that.names) && this.flags == that.flags && Arrays.equals(this.partitionKeyBindIndexes, that.partitionKeyBindIndexes);
         }
      }

      public int hashCode() {
         return Objects.hash(new Object[]{this.names, Integer.valueOf(this.flags)}) + Arrays.hashCode(this.partitionKeyBindIndexes);
      }

      public String toString() {
         StringBuilder sb = new StringBuilder();
         Iterator var2 = this.names.iterator();

         while(var2.hasNext()) {
            ColumnSpecification name = (ColumnSpecification)var2.next();
            sb.append("[").append(name.name);
            sb.append("(").append(name.ksName).append(", ").append(name.cfName).append(")");
            sb.append(", ").append(name.type).append("]");
         }

         sb.append(", bindIndexes=[");
         if(this.partitionKeyBindIndexes != null) {
            for(int i = 0; i < this.partitionKeyBindIndexes.length; ++i) {
               if(i > 0) {
                  sb.append(", ");
               }

               sb.append(this.partitionKeyBindIndexes[i]);
            }
         }

         sb.append("]");
         return sb.toString();
      }

      public static ResultSet.PreparedMetadata fromPrepared(ParsedStatement.Prepared prepared) {
         return new ResultSet.PreparedMetadata(prepared.boundNames, prepared.partitionKeyBindIndexes);
      }

      private static class Codec implements CBCodec<ResultSet.PreparedMetadata> {
         private Codec() {
         }

         public ResultSet.PreparedMetadata decode(ByteBuf body, ProtocolVersion version) {
            int flags = body.readInt();
            int columnCount = body.readInt();
            short[] partitionKeyBindIndexes = null;
            if(version.isGreaterOrEqualTo(ProtocolVersion.V4)) {
               int numPKNames = body.readInt();
               if(numPKNames > 0) {
                  partitionKeyBindIndexes = new short[numPKNames];

                  for(int i = 0; i < numPKNames; ++i) {
                     partitionKeyBindIndexes[i] = body.readShort();
                  }
               }
            }

            boolean globalTablesSpec = Flags.contains(flags, 1);
            String globalKsName = null;
            String globalCfName = null;
            if(globalTablesSpec) {
               globalKsName = CBUtil.readString(body);
               globalCfName = CBUtil.readString(body);
            }

            List<ColumnSpecification> names = new ArrayList(columnCount);

            for(int i = 0; i < columnCount; ++i) {
               String ksName = globalTablesSpec?globalKsName:CBUtil.readString(body);
               String cfName = globalTablesSpec?globalCfName:CBUtil.readString(body);
               ColumnIdentifier colName = new ColumnIdentifier(CBUtil.readString(body), true);
               AbstractType type = DataType.toType(DataType.codec.decodeOne(body, version));
               names.add(new ColumnSpecification(ksName, cfName, colName, type));
            }

            return new ResultSet.PreparedMetadata(flags, names, partitionKeyBindIndexes);
         }

         public void encode(ResultSet.PreparedMetadata m, ByteBuf dest, ProtocolVersion version) {
            boolean globalTablesSpec = Flags.contains(m.flags, 1);
            dest.writeInt(m.flags);
            dest.writeInt(m.names.size());
            if(version.isGreaterOrEqualTo(ProtocolVersion.V4)) {
               if(m.partitionKeyBindIndexes != null && globalTablesSpec) {
                  dest.writeInt(m.partitionKeyBindIndexes.length);
                  short[] var5 = m.partitionKeyBindIndexes;
                  int var6 = var5.length;

                  for(int var7 = 0; var7 < var6; ++var7) {
                     Short bindIndex = Short.valueOf(var5[var7]);
                     dest.writeShort(bindIndex.shortValue());
                  }
               } else {
                  dest.writeInt(0);
               }
            }

            if(globalTablesSpec) {
               CBUtil.writeString(((ColumnSpecification)m.names.get(0)).ksName, dest);
               CBUtil.writeString(((ColumnSpecification)m.names.get(0)).cfName, dest);
            }

            Iterator var9 = m.names.iterator();

            while(var9.hasNext()) {
               ColumnSpecification name = (ColumnSpecification)var9.next();
               if(!globalTablesSpec) {
                  CBUtil.writeString(name.ksName, dest);
                  CBUtil.writeString(name.cfName, dest);
               }

               CBUtil.writeString(name.name.toString(), dest);
               DataType.codec.writeOne(DataType.fromType(name.type, version), dest, version);
            }

         }

         public int encodedSize(ResultSet.PreparedMetadata m, ProtocolVersion version) {
            boolean globalTablesSpec = Flags.contains(m.flags, 1);
            int size = 8;
            if(globalTablesSpec) {
               size += CBUtil.sizeOfString(((ColumnSpecification)m.names.get(0)).ksName);
               size += CBUtil.sizeOfString(((ColumnSpecification)m.names.get(0)).cfName);
            }

            if(m.partitionKeyBindIndexes != null && version.isGreaterOrEqualTo(ProtocolVersion.V4)) {
               size += 4 + 2 * m.partitionKeyBindIndexes.length;
            }

            ColumnSpecification name;
            for(Iterator var5 = m.names.iterator(); var5.hasNext(); size += DataType.codec.oneSerializedSize(DataType.fromType(name.type, version), version)) {
               name = (ColumnSpecification)var5.next();
               if(!globalTablesSpec) {
                  size += CBUtil.sizeOfString(name.ksName);
                  size += CBUtil.sizeOfString(name.cfName);
               }

               size += CBUtil.sizeOfString(name.name.toString());
            }

            return size;
         }
      }
   }

   public static class ResultMetadata {
      public static final CBCodec<ResultSet.ResultMetadata> codec = new ResultSet.ResultMetadata.Codec();
      public static final ResultSet.ResultMetadata EMPTY;
      private int flags;
      public final List<ColumnSpecification> names;
      private final int columnCount;
      private PagingResult pagingResult;
      private final MD5Digest resultMetadataId;

      public ResultMetadata(List<ColumnSpecification> names) {
         this(computeResultMetadataId(names), 0, names, names.size(), PagingResult.NONE);
         if(!names.isEmpty() && ColumnSpecification.allInSameTable(names)) {
            this.flags = Flags.add(this.flags, 1);
         }

      }

      public ResultMetadata(MD5Digest digest, List<ColumnSpecification> names) {
         this(digest, 0, names, names.size(), PagingResult.NONE);
         if(!names.isEmpty() && ColumnSpecification.allInSameTable(names)) {
            this.flags = Flags.add(this.flags, 1);
         }

      }

      private ResultMetadata(MD5Digest digest, int flags, List<ColumnSpecification> names, int columnCount, PagingResult pagingResult) {
         this.resultMetadataId = digest;
         this.flags = flags;
         this.names = names;
         this.columnCount = columnCount;
         this.pagingResult = pagingResult;
      }

      public ResultSet.ResultMetadata copy() {
         return new ResultSet.ResultMetadata(this.resultMetadataId, this.flags, this.names, this.columnCount, this.pagingResult);
      }

      public List<ColumnSpecification> requestNames() {
         return this.names.subList(0, this.columnCount);
      }

      public int valueCount() {
         return this.names == null?this.columnCount:this.names.size();
      }

      @VisibleForTesting
      public int getFlags() {
         return this.flags;
      }

      @VisibleForTesting
      public int getColumnCount() {
         return this.columnCount;
      }

      @VisibleForTesting
      public PagingResult getPagingResult() {
         return this.pagingResult;
      }

      public ResultSet.ResultMetadata addNonSerializedColumns(Collection<? extends ColumnSpecification> columns) {
         this.names.addAll(columns);
         return this;
      }

      public void setPagingResult(PagingResult pagingResult) {
         this.pagingResult = pagingResult;
         if(pagingResult.state != null) {
            this.flags = Flags.add(this.flags, 2);
         } else {
            this.flags = Flags.remove(this.flags, 2);
         }

         boolean continuous = pagingResult.seqNo > 0;
         if(continuous) {
            this.flags = Flags.add(this.flags, 1073741824);
         } else {
            this.flags = Flags.remove(this.flags, 1073741824);
         }

         if(continuous && pagingResult.last) {
            this.flags = Flags.add(this.flags, -2147483648);
         } else {
            this.flags = Flags.remove(this.flags, -2147483648);
         }

      }

      public void setSkipMetadata() {
         this.flags = Flags.add(this.flags, 4);
      }

      public void setMetadataChanged() {
         this.flags = Flags.add(this.flags, 8);
      }

      public MD5Digest getResultMetadataId() {
         return this.resultMetadataId;
      }

      public static ResultSet.ResultMetadata fromPrepared(ParsedStatement.Prepared prepared) {
         CQLStatement statement = prepared.statement;
         return !(statement instanceof SelectStatement)?EMPTY:((SelectStatement)statement).getResultMetadata();
      }

      public boolean equals(Object other) {
         if(this == other) {
            return true;
         } else if(!(other instanceof ResultSet.ResultMetadata)) {
            return false;
         } else {
            ResultSet.ResultMetadata that = (ResultSet.ResultMetadata)other;
            return this.flags == that.flags && Objects.equals(this.names, that.names) && this.columnCount == that.columnCount && Objects.equals(this.pagingResult, that.pagingResult);
         }
      }

      public int hashCode() {
         return Objects.hash(new Object[]{Integer.valueOf(this.flags), this.names, Integer.valueOf(this.columnCount), this.pagingResult});
      }

      public String toString() {
         StringBuilder sb = new StringBuilder();
         if(this.names == null) {
            sb.append("[").append(this.columnCount).append(" columns]");
         } else {
            Iterator var2 = this.names.iterator();

            while(var2.hasNext()) {
               ColumnSpecification name = (ColumnSpecification)var2.next();
               sb.append("[").append(name.name);
               sb.append("(").append(name.ksName).append(", ").append(name.cfName).append(")");
               sb.append(", ").append(name.type).append("]");
            }
         }

         if(this.pagingResult != null) {
            sb.append("[").append(this.pagingResult.toString()).append("]");
         }

         if(Flags.contains(this.flags, 2)) {
            sb.append(" (to be continued)");
         }

         return sb.toString();
      }

      public static MD5Digest computeResultMetadataId(List<ColumnSpecification> columnSpecifications) {
         MessageDigest md = MD5Digest.threadLocalMD5Digest();
         if(columnSpecifications != null) {
            Iterator var2 = columnSpecifications.iterator();

            while(var2.hasNext()) {
               ColumnSpecification cs = (ColumnSpecification)var2.next();
               md.update(cs.name.bytes.duplicate());
               md.update(0);
               md.update(cs.type.toString().getBytes(StandardCharsets.UTF_8));
               md.update(0);
               md.update(0);
            }
         }

         return MD5Digest.wrap(md.digest());
      }

      static {
         EMPTY = new ResultSet.ResultMetadata(MD5Digest.compute(new byte[0]), 4, (List)null, 0, PagingResult.NONE);
      }

      private static class Codec implements CBCodec<ResultSet.ResultMetadata> {
         private Codec() {
         }

         public ResultSet.ResultMetadata decode(ByteBuf body, ProtocolVersion version) {
            int flags = body.readInt();
            int columnCount = body.readInt();
            PagingState state = Flags.contains(flags, 2)?PagingState.deserialize(CBUtil.readValueNoCopy(body), version):null;
            MD5Digest resultMetadataId = null;
            if(Flags.contains(flags, 8)) {
               assert version.isGreaterOrEqualTo(ProtocolVersion.V5, ProtocolVersion.DSE_V2) : "MetadataChanged flag is not supported before native protocol v5";

               assert !Flags.contains(flags, 4) : "MetadataChanged and NoMetadata are mutually exclusive flags";

               resultMetadataId = MD5Digest.wrap(CBUtil.readBytes(body));
            }

            PagingResult pagingResult = Flags.contains(flags, 1073741824)?new PagingResult(state, body.readInt(), Flags.contains(flags, -2147483648)):(state == null?PagingResult.NONE:new PagingResult(state));
            if(Flags.contains(flags, 4)) {
               return new ResultSet.ResultMetadata((MD5Digest)null, flags, (List)null, columnCount, pagingResult);
            } else {
               boolean globalTablesSpec = Flags.contains(flags, 1);
               String globalKsName = null;
               String globalCfName = null;
               if(globalTablesSpec) {
                  globalKsName = CBUtil.readString(body);
                  globalCfName = CBUtil.readString(body);
               }

               List<ColumnSpecification> names = new ArrayList(columnCount);

               for(int i = 0; i < columnCount; ++i) {
                  String ksName = globalTablesSpec?globalKsName:CBUtil.readString(body);
                  String cfName = globalTablesSpec?globalCfName:CBUtil.readString(body);
                  ColumnIdentifier colName = new ColumnIdentifier(CBUtil.readString(body), true);
                  AbstractType type = DataType.toType(DataType.codec.decodeOne(body, version));
                  names.add(new ColumnSpecification(ksName, cfName, colName, type));
               }

               return new ResultSet.ResultMetadata(resultMetadataId, flags, names, names.size(), pagingResult);
            }
         }

         public void encode(ResultSet.ResultMetadata m, ByteBuf dest, ProtocolVersion version) {
            boolean noMetadata = Flags.contains(m.flags, 4);
            boolean globalTablesSpec = Flags.contains(m.flags, 1);
            boolean hasMorePages = Flags.contains(m.flags, 2);
            boolean continuousPaging = Flags.contains(m.flags, 1073741824);
            boolean metadataChanged = Flags.contains(m.flags, 8);

            assert version.isGreaterThan(ProtocolVersion.V1) || !hasMorePages && !noMetadata : "version = " + version + ", flags = " + m.flags;

            dest.writeInt(m.flags);
            dest.writeInt(m.columnCount);
            if(hasMorePages) {
               CBUtil.writeValue(m.pagingResult.state.serialize(version), dest);
            }

            if(version.isGreaterOrEqualTo(ProtocolVersion.V5, ProtocolVersion.DSE_V2) && metadataChanged) {
               assert !noMetadata : "MetadataChanged and NoMetadata are mutually exclusive flags";

               CBUtil.writeBytes(m.getResultMetadataId().bytes, dest);
            }

            if(continuousPaging) {
               assert version.isGreaterOrEqualTo(ProtocolVersion.DSE_V1) : "version = " + version + ", does not support optimized paging.";

               dest.writeInt(m.pagingResult.seqNo);
            }

            if(!noMetadata) {
               if(globalTablesSpec) {
                  CBUtil.writeString(((ColumnSpecification)m.names.get(0)).ksName, dest);
                  CBUtil.writeString(((ColumnSpecification)m.names.get(0)).cfName, dest);
               }

               for(int i = 0; i < m.columnCount; ++i) {
                  ColumnSpecification name = (ColumnSpecification)m.names.get(i);
                  if(!globalTablesSpec) {
                     CBUtil.writeString(name.ksName, dest);
                     CBUtil.writeString(name.cfName, dest);
                  }

                  CBUtil.writeString(name.name.toString(), dest);
                  DataType.codec.writeOne(DataType.fromType(name.type, version), dest, version);
               }
            }

         }

         public int encodedSize(ResultSet.ResultMetadata m, ProtocolVersion version) {
            boolean noMetadata = Flags.contains(m.flags, 4);
            boolean globalTablesSpec = Flags.contains(m.flags, 1);
            boolean hasMorePages = Flags.contains(m.flags, 2);
            boolean continuousPaging = Flags.contains(m.flags, 1073741824);
            boolean metadataChanged = Flags.contains(m.flags, 8);
            int size = 8;
            if(hasMorePages) {
               size += CBUtil.sizeOfValue(m.pagingResult.state.serializedSize(version));
            }

            if(continuousPaging) {
               size += 4;
            }

            if(version.isGreaterOrEqualTo(ProtocolVersion.V5, ProtocolVersion.DSE_V2) && metadataChanged) {
               size += CBUtil.sizeOfBytes(m.getResultMetadataId().bytes);
            }

            if(!noMetadata) {
               if(globalTablesSpec) {
                  size += CBUtil.sizeOfString(((ColumnSpecification)m.names.get(0)).ksName);
                  size += CBUtil.sizeOfString(((ColumnSpecification)m.names.get(0)).cfName);
               }

               for(int i = 0; i < m.columnCount; ++i) {
                  ColumnSpecification name = (ColumnSpecification)m.names.get(i);
                  if(!globalTablesSpec) {
                     size += CBUtil.sizeOfString(name.ksName);
                     size += CBUtil.sizeOfString(name.cfName);
                  }

                  size += CBUtil.sizeOfString(name.name.toString());
                  size += DataType.codec.oneSerializedSize(DataType.fromType(name.type, version), version);
               }
            }

            return size;
         }
      }
   }

   public static class Codec implements CBCodec<ResultSet> {
      public Codec() {
      }

      public ResultSet decode(ByteBuf body, ProtocolVersion version) {
         ResultSet.ResultMetadata m = (ResultSet.ResultMetadata)ResultSet.ResultMetadata.codec.decode(body, version);
         int rowCount = body.readInt();
         ResultSet rs = new ResultSet(m, new ArrayList(rowCount));
         int totalValues = rowCount * m.columnCount;

         for(int i = 0; i < totalValues; ++i) {
            rs.addColumnValue(CBUtil.readValue(body));
         }

         return rs;
      }

      public void encode(ResultSet rs, ByteBuf dest, ProtocolVersion version) {
         this.encodeHeader(rs.metadata, dest, rs.rows.size(), version);
         Iterator var4 = rs.rows.iterator();

         while(var4.hasNext()) {
            List<ByteBuffer> row = (List)var4.next();
            this.encodeRow(row, rs.metadata, dest, false);
         }

      }

      public void encodeHeader(ResultSet.ResultMetadata metadata, ByteBuf dest, int numRows, ProtocolVersion version) {
         ResultSet.ResultMetadata.codec.encode(metadata, dest, version);
         dest.writeInt(numRows);
      }

      public boolean encodeRow(List<ByteBuffer> row, ResultSet.ResultMetadata metadata, ByteBuf dest, boolean checkSpace) {
         for(int i = 0; i < metadata.columnCount; ++i) {
            if(checkSpace && dest.writableBytes() < CBUtil.sizeOfValue((ByteBuffer)row.get(i))) {
               return false;
            }

            CBUtil.writeValue((ByteBuffer)row.get(i), dest);
         }

         return true;
      }

      public int encodedSize(ResultSet rs, ProtocolVersion version) {
         int size = this.encodedHeaderSize(rs.metadata, version);

         List row;
         for(Iterator var4 = rs.rows.iterator(); var4.hasNext(); size += this.encodedRowSize(row, rs.metadata)) {
            row = (List)var4.next();
         }

         return size;
      }

      public int encodedHeaderSize(ResultSet.ResultMetadata metadata, ProtocolVersion version) {
         return ResultSet.ResultMetadata.codec.encodedSize(metadata, version) + 4;
      }

      public int encodedRowSize(List<ByteBuffer> row, ResultSet.ResultMetadata metadata) {
         int size = 0;

         for(int i = 0; i < metadata.columnCount; ++i) {
            size += CBUtil.sizeOfValue((ByteBuffer)row.get(i));
         }

         return size;
      }
   }
}
