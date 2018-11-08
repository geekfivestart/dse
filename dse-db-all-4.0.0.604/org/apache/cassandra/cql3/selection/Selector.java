package org.apache.cassandra.cql3.selection;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.RequestValidations;
import org.apache.cassandra.db.ReadVerbs;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.CQLTypeParser;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.versioning.VersionDependent;
import org.apache.cassandra.utils.versioning.Versioned;

public abstract class Selector {
   public static final Versioned<ReadVerbs.ReadVersion, Selector.Serializer> serializers = ReadVerbs.ReadVersion.versioned((x$0) -> {
      return new Selector.Serializer(x$0);
   });
   private final Selector.Kind kind;

   public final Selector.Kind kind() {
      return this.kind;
   }

   protected Selector(Selector.Kind kind) {
      this.kind = kind;
   }

   public abstract void addFetchedColumns(ColumnFilter.Builder var1);

   public abstract void addInput(ProtocolVersion var1, Selector.InputRow var2);

   public abstract ByteBuffer getOutput(ProtocolVersion var1);

   public abstract AbstractType<?> getType();

   public abstract void reset();

   public boolean isTerminal() {
      return false;
   }

   public void validateForGroupBy() {
      throw RequestValidations.invalidRequest("Only column names and monotonic scalar functions are supported in the GROUP BY clause.");
   }

   protected abstract int serializedSize(ReadVerbs.ReadVersion var1);

   protected abstract void serialize(DataOutputPlus var1, ReadVerbs.ReadVersion var2) throws IOException;

   protected static void writeType(DataOutputPlus out, AbstractType<?> type) throws IOException {
      out.writeUTF(type.asCQL3Type().toString());
   }

   protected static int sizeOf(AbstractType<?> type) {
      return TypeSizes.sizeof(type.asCQL3Type().toString());
   }

   public static class Serializer extends VersionDependent<ReadVerbs.ReadVersion> {
      private Serializer(ReadVerbs.ReadVersion version) {
         super(version);
      }

      public void serialize(Selector selector, DataOutputPlus out) throws IOException {
         out.writeByte(selector.kind().ordinal());
         selector.serialize(out, (ReadVerbs.ReadVersion)this.version);
      }

      public Selector deserialize(DataInputPlus in, TableMetadata metadata) throws IOException {
         Selector.Kind kind = Selector.Kind.values()[in.readUnsignedByte()];
         return kind.deserializer.deserialize(in, (ReadVerbs.ReadVersion)this.version, metadata);
      }

      public int serializedSize(Selector selector) {
         return TypeSizes.sizeof((byte)selector.kind().ordinal()) + selector.serializedSize((ReadVerbs.ReadVersion)this.version);
      }
   }

   public static final class InputRow {
      private ByteBuffer[] values;
      private final long[] timestamps;
      private final int[] ttls;
      private int index;

      public InputRow(int size, boolean collectTimestamps, boolean collectTTLs) {
         this.values = new ByteBuffer[size];
         if(collectTimestamps) {
            this.timestamps = new long[size];
            Arrays.fill(this.timestamps, -9223372036854775808L);
         } else {
            this.timestamps = null;
         }

         if(collectTTLs) {
            this.ttls = new int[size];
            Arrays.fill(this.ttls, -1);
         } else {
            this.ttls = null;
         }

      }

      public void add(ByteBuffer v) {
         this.values[this.index++] = v;
      }

      public void add(Cell c, int nowInSec) {
         if(c == null) {
            this.values[this.index] = null;
            if(this.timestamps != null) {
               this.timestamps[this.index] = -9223372036854775808L;
            }

            if(this.ttls != null) {
               this.ttls[this.index] = -1;
            }
         } else {
            this.values[this.index] = this.value(c);
            if(this.timestamps != null) {
               this.timestamps[this.index] = c.timestamp();
            }

            if(this.ttls != null) {
               this.ttls[this.index] = this.remainingTTL(c, nowInSec);
            }
         }

         ++this.index;
      }

      private int remainingTTL(Cell c, int nowInSec) {
         if(!c.isExpiring()) {
            return -1;
         } else {
            int remaining = c.localDeletionTime() - nowInSec;
            return remaining >= 0?remaining:-1;
         }
      }

      private ByteBuffer value(Cell c) {
         return c.isCounterCell()?ByteBufferUtil.bytes(CounterContext.instance().total(c.value())):c.value();
      }

      public ByteBuffer getValue(int index) {
         return this.values[index];
      }

      public void reset(boolean deep) {
         this.index = 0;
         if(deep) {
            this.values = new ByteBuffer[this.values.length];
         }

      }

      public long getTimestamp(int index) {
         return this.timestamps[index];
      }

      public int getTtl(int index) {
         return this.ttls[index];
      }

      public List<ByteBuffer> getValues() {
         return Arrays.asList(this.values);
      }
   }

   public abstract static class Factory {
      public Factory() {
      }

      public void addFunctionsTo(List<org.apache.cassandra.cql3.functions.Function> functions) {
      }

      public ColumnSpecification getColumnSpecification(TableMetadata table) {
         return new ColumnSpecification(table.keyspace, table.name, new ColumnIdentifier(this.getColumnName(), true), this.getReturnType());
      }

      public abstract Selector newInstance(QueryOptions var1);

      public boolean isAggregateSelectorFactory() {
         return false;
      }

      public boolean isWritetimeSelectorFactory() {
         return false;
      }

      public boolean isTTLSelectorFactory() {
         return false;
      }

      public boolean isSimpleSelectorFactory() {
         return false;
      }

      public boolean isSimpleSelectorFactoryFor(int index) {
         return false;
      }

      protected abstract String getColumnName();

      protected abstract AbstractType<?> getReturnType();

      protected abstract void addColumnMapping(SelectionColumnMapping var1, ColumnSpecification var2);

      abstract boolean areAllFetchedColumnsKnown();

      abstract void addFetchedColumns(ColumnFilter.Builder var1);
   }

   public static enum Kind {
      SIMPLE_SELECTOR(SimpleSelector.deserializer),
      TERM_SELECTOR(TermSelector.deserializer),
      WRITETIME_OR_TTL_SELECTOR(WritetimeOrTTLSelector.deserializer),
      LIST_SELECTOR(ListSelector.deserializer),
      SET_SELECTOR(SetSelector.deserializer),
      MAP_SELECTOR(MapSelector.deserializer),
      TUPLE_SELECTOR(TupleSelector.deserializer),
      USER_TYPE_SELECTOR(UserTypeSelector.deserializer),
      FIELD_SELECTOR(FieldSelector.deserializer),
      SCALAR_FUNCTION_SELECTOR(ScalarFunctionSelector.deserializer),
      AGGREGATE_FUNCTION_SELECTOR(AggregateFunctionSelector.deserializer),
      ELEMENT_SELECTOR(ElementsSelector.ElementSelector.deserializer),
      SLICE_SELECTOR(ElementsSelector.SliceSelector.deserializer);

      private final Selector.SelectorDeserializer deserializer;

      private Kind(Selector.SelectorDeserializer deserializer) {
         this.deserializer = deserializer;
      }
   }

   protected abstract static class SelectorDeserializer {
      protected SelectorDeserializer() {
      }

      protected abstract Selector deserialize(DataInputPlus var1, ReadVerbs.ReadVersion var2, TableMetadata var3) throws IOException;

      protected final AbstractType<?> readType(TableMetadata metadata, DataInputPlus in) throws IOException {
         KeyspaceMetadata keyspace = Schema.instance.getKeyspaceMetadata(metadata.keyspace);
         return this.readType(keyspace, in);
      }

      protected final AbstractType<?> readType(KeyspaceMetadata keyspace, DataInputPlus in) throws IOException {
         String cqlType = in.readUTF();
         return CQLTypeParser.parse(keyspace.name, cqlType, keyspace.types);
      }
   }
}
