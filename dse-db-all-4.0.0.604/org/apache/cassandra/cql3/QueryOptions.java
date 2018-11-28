package org.apache.cassandra.cql3;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.transport.CBCodec;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.ProtocolException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.Flags;
import org.apache.cassandra.utils.Pair;

public abstract class QueryOptions {
   public static final QueryOptions DEFAULT;
   public static final CBCodec<QueryOptions> codec;
   private List<Map<ColumnIdentifier, Term>> jsonValuesCache;

   public QueryOptions() {
   }

   public static QueryOptions forInternalCalls(ConsistencyLevel consistency, List<ByteBuffer> values) {
      return new QueryOptions.DefaultQueryOptions(consistency, values, false, QueryOptions.SpecificOptions.DEFAULT, ProtocolVersion.V3);
   }

   public static QueryOptions forInternalCalls(List<ByteBuffer> values) {
      return new QueryOptions.DefaultQueryOptions(ConsistencyLevel.ONE, values, false, QueryOptions.SpecificOptions.DEFAULT, ProtocolVersion.V3);
   }

   public static QueryOptions forProtocolVersion(ProtocolVersion protocolVersion) {
      return new QueryOptions.DefaultQueryOptions((ConsistencyLevel)null, (List)null, true, (QueryOptions.SpecificOptions)null, protocolVersion);
   }

   public static QueryOptions create(ConsistencyLevel consistency, List<ByteBuffer> values, boolean skipMetadata, int pageSize, PagingState pagingState, ConsistencyLevel serialConsistency, ProtocolVersion version, String keyspace) {
      assert pageSize > 0 || pagingState == null;

      QueryOptions.PagingOptions pagingOptions = pageSize > 0?new QueryOptions.PagingOptions(PageSize.rowsSize(pageSize), QueryOptions.PagingOptions.Mechanism.SINGLE, pagingState):null;
      return create(consistency, values, skipMetadata, pagingOptions, serialConsistency, version, keyspace);
   }

   public static QueryOptions create(ConsistencyLevel consistency, List<ByteBuffer> values, boolean skipMetadata, QueryOptions.PagingOptions pagingOptions, ConsistencyLevel serialConsistency, ProtocolVersion version, String keyspace) {
      return new QueryOptions.DefaultQueryOptions(consistency, values, skipMetadata, new QueryOptions.SpecificOptions(pagingOptions, serialConsistency, -9223372036854775808L, keyspace), version);
   }

   public static QueryOptions addColumnSpecifications(QueryOptions options, List<ColumnSpecification> columnSpecs) {
      return new QueryOptions.OptionsWithColumnSpecifications(options, columnSpecs);
   }

   public abstract ConsistencyLevel getConsistency();

   public abstract List<ByteBuffer> getValues();

   public abstract boolean skipMetadata();

   public Term getJsonColumnValue(int bindIndex, ColumnIdentifier columnName, Collection<ColumnMetadata> expectedReceivers) throws InvalidRequestException {
      if(this.jsonValuesCache == null) {
         this.jsonValuesCache = new ArrayList(Collections.nCopies(this.getValues().size(), (Object)null));
      }

      Map<ColumnIdentifier, Term> jsonValue = (Map)this.jsonValuesCache.get(bindIndex);
      if(jsonValue == null) {
         ByteBuffer value = (ByteBuffer)this.getValues().get(bindIndex);
         if(value == null) {
            throw new InvalidRequestException("Got null for INSERT JSON values");
         }

         jsonValue = Json.parseJson((String)UTF8Type.instance.getSerializer().deserialize(value), expectedReceivers);
         this.jsonValuesCache.set(bindIndex, jsonValue);
      }

      return (Term)jsonValue.get(columnName);
   }

   public boolean hasColumnSpecifications() {
      return false;
   }

   public ImmutableList<ColumnSpecification> getColumnSpecifications() {
      throw new UnsupportedOperationException();
   }

   public ConsistencyLevel getSerialConsistency() {
      return this.getSpecificOptions().serialConsistency;
   }

   public long getTimestamp(QueryState state) {
      long tstamp = this.getSpecificOptions().timestamp;
      return tstamp != -9223372036854775808L?tstamp:state.getTimestamp();
   }

   public String getKeyspace() {
      return this.getSpecificOptions().keyspace;
   }

   public abstract ProtocolVersion getProtocolVersion();

   abstract QueryOptions.SpecificOptions getSpecificOptions();

   public QueryOptions.PagingOptions getPagingOptions() {
      return this.getSpecificOptions().pagingOptions;
   }

   public boolean continuousPagesRequested() {
      QueryOptions.PagingOptions pagingOptions = this.getPagingOptions();
      return pagingOptions != null && pagingOptions.isContinuous();
   }

   public QueryOptions prepare(List<ColumnSpecification> specs) {
      return this;
   }

   static {
      DEFAULT = new QueryOptions.DefaultQueryOptions(ConsistencyLevel.ONE, Collections.emptyList(), false, QueryOptions.SpecificOptions.DEFAULT, ProtocolVersion.CURRENT);
      codec = new QueryOptions.Codec();
   }

   private static class Codec implements CBCodec<QueryOptions> {
      private Codec() {
      }

      public QueryOptions decode(ByteBuf body, ProtocolVersion version) {
         ConsistencyLevel consistency = CBUtil.readConsistencyLevel(body);
         int flags = version.isGreaterOrEqualTo(ProtocolVersion.V5)?(int)body.readUnsignedInt():body.readUnsignedByte();
         List<ByteBuffer> values = Collections.emptyList();
         List<String> names = null;
         if(Flags.contains(flags, 1)) {
            if(Flags.contains(flags, 64)) {
               Pair<List<String>, List<ByteBuffer>> namesAndValues = CBUtil.readNameAndValueList(body, version);
               names = (List)namesAndValues.left;
               values = (List)namesAndValues.right;
            } else {
               values = CBUtil.readValueList(body, version);
            }
         }

         boolean skipMetadata = Flags.contains(flags, 2);
         flags = Flags.remove(flags, 3);
         QueryOptions.SpecificOptions options = QueryOptions.SpecificOptions.DEFAULT;
         if(flags != 0) {
            PageSize pageSize = null;
            if(Flags.contains(flags, 4)) {
               try {
                  PageSize.PageUnit pageUnit = Flags.contains(flags, 1073741824)?PageSize.PageUnit.BYTES:PageSize.PageUnit.ROWS;
                  pageSize = new PageSize(body.readInt(), pageUnit);
               } catch (IllegalArgumentException var20) {
                  throw new ProtocolException(String.format("Invalid page size: " + var20.getMessage(), new Object[0]));
               }
            }

            PagingState pagingState = Flags.contains(flags, 8)?PagingState.deserialize(CBUtil.readValueNoCopy(body), version):null;
            ConsistencyLevel serialConsistency = Flags.contains(flags, 16)?CBUtil.readConsistencyLevel(body):ConsistencyLevel.SERIAL;
            long timestamp = -9223372036854775808L;
            if(Flags.contains(flags, 32)) {
               long ts = body.readLong();
               if(ts == -9223372036854775808L) {
                  throw new ProtocolException(String.format("Out of bound timestamp, must be in [%d, %d] (got %d)", new Object[]{Long.valueOf(-9223372036854775807L), Long.valueOf(9223372036854775807L), Long.valueOf(ts)}));
               }

               timestamp = ts;
            }

            QueryOptions.PagingOptions pagingOptions = null;
            boolean hasContinuousPaging = Flags.contains(flags, -2147483648);
            String keyspace = Flags.contains(flags, 128)?CBUtil.readString(body):null;
            if(pageSize == null) {
               if(hasContinuousPaging) {
                  throw new ProtocolException("Cannot use continuous paging without indicating a positive page size");
               }

               if(pagingState != null) {
                  throw new ProtocolException("Paging state requires a page size");
               }
            } else if(hasContinuousPaging) {
               if(version.isSmallerThan(ProtocolVersion.DSE_V1)) {
                  throw new ProtocolException("Continuous paging requires DSE_V1 or higher");
               }

               int maxPages = body.readInt();
               int maxPagesPerSecond = body.readInt();
               int nextPages = version.isGreaterOrEqualTo(ProtocolVersion.DSE_V2)?body.readInt():0;
               pagingOptions = new QueryOptions.PagingOptions(pageSize, QueryOptions.PagingOptions.Mechanism.CONTINUOUS, pagingState, maxPages, maxPagesPerSecond, nextPages);
            } else {
               if(!pageSize.isInRows()) {
                  throw new ProtocolException("Page size in bytes is only supported with continuous paging");
               }

               pagingOptions = new QueryOptions.PagingOptions(pageSize, QueryOptions.PagingOptions.Mechanism.SINGLE, pagingState);
            }

            options = new QueryOptions.SpecificOptions(pagingOptions, serialConsistency, timestamp, keyspace);
         }

         QueryOptions.DefaultQueryOptions opts = new QueryOptions.DefaultQueryOptions(consistency, values, skipMetadata, options, version);
         return (QueryOptions)(names == null?opts:new QueryOptions.OptionsWithNames(opts, names));
      }

      public void encode(QueryOptions options, ByteBuf dest, ProtocolVersion version) {
         QueryOptions.PagingOptions pagingOptions = options.getPagingOptions();
         CBUtil.writeConsistencyLevel(options.getConsistency(), dest);
         int flags = this.gatherFlags(options);
         if(version.isGreaterOrEqualTo(ProtocolVersion.V5)) {
            dest.writeInt(flags);
         } else {
            dest.writeByte((byte)flags);
         }

         if(Flags.contains(flags, 1)) {
            CBUtil.writeValueList(options.getValues(), dest);
         }

         if(Flags.contains(flags, 4)) {
            dest.writeInt(pagingOptions.pageSize().rawSize());
         }

         if(Flags.contains(flags, 8)) {
            CBUtil.writeValue(pagingOptions.state().serialize(version), dest);
         }

         if(Flags.contains(flags, 16)) {
            CBUtil.writeConsistencyLevel(options.getSerialConsistency(), dest);
         }

         if(Flags.contains(flags, 32)) {
            dest.writeLong(options.getSpecificOptions().timestamp);
         }

         if(Flags.contains(flags, 128)) {
            CBUtil.writeString(options.getSpecificOptions().keyspace, dest);
         }

         if(Flags.contains(flags, -2147483648)) {
            dest.writeInt(pagingOptions.maxPages);
            dest.writeInt(pagingOptions.maxPagesPerSecond);
            if(version.isGreaterOrEqualTo(ProtocolVersion.DSE_V2)) {
               dest.writeInt(pagingOptions.nextPages());
            }
         }

      }

      public int encodedSize(QueryOptions options, ProtocolVersion version) {
         QueryOptions.PagingOptions pagingOptions = options.getPagingOptions();
         int size = CBUtil.sizeOfConsistencyLevel(options.getConsistency());
         int flags = this.gatherFlags(options);
         size += version.isGreaterOrEqualTo(ProtocolVersion.V5)?4:1;
         if(Flags.contains(flags, 1)) {
            size += CBUtil.sizeOfValueList(options.getValues());
         }

         if(Flags.contains(flags, 4)) {
            size += 4;
         }

         if(Flags.contains(flags, 8)) {
            size += CBUtil.sizeOfValue(pagingOptions.state().serializedSize(version));
         }

         if(Flags.contains(flags, 16)) {
            size += CBUtil.sizeOfConsistencyLevel(options.getSerialConsistency());
         }

         if(Flags.contains(flags, 32)) {
            size += 8;
         }

         if(Flags.contains(flags, 128)) {
            size += CBUtil.sizeOfString(options.getSpecificOptions().keyspace);
         }

         if(Flags.contains(flags, -2147483648)) {
            size += version.isGreaterOrEqualTo(ProtocolVersion.DSE_V2)?12:8;
         }

         return size;
      }

      private int gatherFlags(QueryOptions options) {
         int flags = 0;
         if(options.getValues().size() > 0) {
            flags = Flags.add(flags, 1);
         }

         if(options.skipMetadata()) {
            flags = Flags.add(flags, 2);
         }

         QueryOptions.PagingOptions pagingOptions = options.getPagingOptions();
         if(pagingOptions != null) {
            flags = Flags.add(flags, 4);
            if(pagingOptions.pageSize.isInBytes()) {
               flags = Flags.add(flags, 1073741824);
            }

            if(pagingOptions.state() != null) {
               flags = Flags.add(flags, 8);
            }

            if(pagingOptions.isContinuous()) {
               flags = Flags.add(flags, -2147483648);
            }
         }

         if(options.getSerialConsistency() != ConsistencyLevel.SERIAL) {
            flags = Flags.add(flags, 16);
         }

         if(options.getSpecificOptions().timestamp != -9223372036854775808L) {
            flags = Flags.add(flags, 32);
         }

         if(options.getSpecificOptions().keyspace != null) {
            flags = Flags.add(flags, 128);
         }

         return flags;
      }

      private interface CodecFlag {
         int NONE = 0;
         int VALUES = 1;
         int SKIP_METADATA = 2;
         int PAGE_SIZE = 4;
         int PAGING_STATE = 8;
         int SERIAL_CONSISTENCY = 16;
         int TIMESTAMP = 32;
         int NAMES_FOR_VALUES = 64;
         int KEYSPACE = 128;
         int PAGE_SIZE_BYTES = 1073741824;
         int CONTINUOUS_PAGING = -2147483648;
      }
   }

   public static class PagingOptions {
      private final PageSize pageSize;
      private final QueryOptions.PagingOptions.Mechanism mechanism;
      private final PagingState pagingState;
      private final int maxPages;
      private final int maxPagesPerSecond;
      private final int nextPages;

      @VisibleForTesting
      public PagingOptions(PageSize pageSize, QueryOptions.PagingOptions.Mechanism mechanism, PagingState pagingState) {
         this(pageSize, mechanism, pagingState, 0, 0, 0);
      }

      @VisibleForTesting
      public PagingOptions(PageSize pageSize, QueryOptions.PagingOptions.Mechanism mechanism, PagingState pagingState, int maxPages, int maxPagesPerSecond, int nextPages) {
         assert pageSize != null : "pageSize cannot be null";

         this.pageSize = pageSize;
         this.mechanism = mechanism;
         this.pagingState = pagingState;
         this.maxPages = maxPages <= 0?2147483647:maxPages;
         this.maxPagesPerSecond = maxPagesPerSecond;
         this.nextPages = nextPages;
      }

      public boolean isContinuous() {
         return this.mechanism == QueryOptions.PagingOptions.Mechanism.CONTINUOUS;
      }

      public PageSize pageSize() {
         return this.pageSize;
      }

      public PagingState state() {
         return this.pagingState;
      }

      public int maxPages() {
         return this.maxPages;
      }

      public int maxPagesPerSecond() {
         return this.maxPagesPerSecond;
      }

      public int nextPages() {
         return this.nextPages;
      }

      public final int hashCode() {
         return Objects.hash(new Object[]{this.pageSize, this.mechanism, this.pagingState, Integer.valueOf(this.maxPages), Integer.valueOf(this.maxPagesPerSecond), Integer.valueOf(this.nextPages)});
      }

      public final boolean equals(Object o) {
         if(!(o instanceof QueryOptions.PagingOptions)) {
            return false;
         } else {
            QueryOptions.PagingOptions that = (QueryOptions.PagingOptions)o;
            return Objects.equals(this.pageSize, that.pageSize) && Objects.equals(this.mechanism, that.mechanism) && Objects.equals(this.pagingState, that.pagingState) && this.maxPages == that.maxPages && this.maxPagesPerSecond == that.maxPagesPerSecond && this.nextPages == that.nextPages;
         }
      }

      public String toString() {
         return String.format("%s %s (max %d, %d per second, %d next pages) with state %s", new Object[]{this.pageSize, this.mechanism, Integer.valueOf(this.maxPages), Integer.valueOf(this.maxPagesPerSecond), Integer.valueOf(this.nextPages), this.pagingState});
      }

      @VisibleForTesting
      public static enum Mechanism {
         SINGLE,
         CONTINUOUS;

         private Mechanism() {
         }
      }
   }

   static class SpecificOptions {
      private static final QueryOptions.SpecificOptions DEFAULT = new QueryOptions.SpecificOptions((QueryOptions.PagingOptions)null, (ConsistencyLevel)null, -9223372036854775808L, (String)null);
      private final QueryOptions.PagingOptions pagingOptions;
      private final ConsistencyLevel serialConsistency;
      private final long timestamp;
      private final String keyspace;

      private SpecificOptions(QueryOptions.PagingOptions pagingOptions, ConsistencyLevel serialConsistency, long timestamp, String keyspace) {
         this.pagingOptions = pagingOptions;
         this.serialConsistency = serialConsistency == null?ConsistencyLevel.SERIAL:serialConsistency;
         this.timestamp = timestamp;
         this.keyspace = keyspace;
      }
   }

   static class OptionsWithNames extends QueryOptions.QueryOptionsWrapper {
      private final List<String> names;
      private List<ByteBuffer> orderedValues;

      OptionsWithNames(QueryOptions.DefaultQueryOptions wrapped, List<String> names) {
         super(wrapped);
         this.names = names;
      }

      public QueryOptions prepare(List<ColumnSpecification> specs) {
         super.prepare(specs);
         this.orderedValues = new ArrayList(specs.size());

         for(int i = 0; i < specs.size(); ++i) {
            String name = ((ColumnSpecification)specs.get(i)).name.toString();

            for(int j = 0; j < this.names.size(); ++j) {
               if(name.equals(this.names.get(j))) {
                  this.orderedValues.add(this.wrapped.getValues().get(j));
                  break;
               }
            }
         }

         return this;
      }

      public List<ByteBuffer> getValues() {
         assert this.orderedValues != null;

         return this.orderedValues;
      }
   }

   static class OptionsWithColumnSpecifications extends QueryOptions.QueryOptionsWrapper {
      private final ImmutableList<ColumnSpecification> columnSpecs;

      OptionsWithColumnSpecifications(QueryOptions wrapped, List<ColumnSpecification> columnSpecs) {
         super(wrapped);
         this.columnSpecs = ImmutableList.copyOf(columnSpecs);
      }

      public boolean hasColumnSpecifications() {
         return true;
      }

      public ImmutableList<ColumnSpecification> getColumnSpecifications() {
         return this.columnSpecs;
      }
   }

   static class QueryOptionsWrapper extends QueryOptions {
      protected final QueryOptions wrapped;

      QueryOptionsWrapper(QueryOptions wrapped) {
         this.wrapped = wrapped;
      }

      public List<ByteBuffer> getValues() {
         return this.wrapped.getValues();
      }

      public ConsistencyLevel getConsistency() {
         return this.wrapped.getConsistency();
      }

      public boolean skipMetadata() {
         return this.wrapped.skipMetadata();
      }

      public ProtocolVersion getProtocolVersion() {
         return this.wrapped.getProtocolVersion();
      }

      QueryOptions.SpecificOptions getSpecificOptions() {
         return this.wrapped.getSpecificOptions();
      }

      public QueryOptions prepare(List<ColumnSpecification> specs) {
         this.wrapped.prepare(specs);
         return this;
      }
   }

   static class DefaultQueryOptions extends QueryOptions {
      private final ConsistencyLevel consistency;
      private final List<ByteBuffer> values;
      private final boolean skipMetadata;
      private final QueryOptions.SpecificOptions options;
      private final transient ProtocolVersion protocolVersion;

      DefaultQueryOptions(ConsistencyLevel consistency, List<ByteBuffer> values, boolean skipMetadata, QueryOptions.SpecificOptions options, ProtocolVersion protocolVersion) {
         this.consistency = consistency;
         this.values = values;
         this.skipMetadata = skipMetadata;
         this.options = options;
         this.protocolVersion = protocolVersion;
      }

      public ConsistencyLevel getConsistency() {
         return this.consistency;
      }

      public List<ByteBuffer> getValues() {
         return this.values;
      }

      public boolean skipMetadata() {
         return this.skipMetadata;
      }

      public ProtocolVersion getProtocolVersion() {
         return this.protocolVersion;
      }

      QueryOptions.SpecificOptions getSpecificOptions() {
         return this.options;
      }
   }
}
