package org.apache.cassandra.cql3.selection;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.ReadVerbs;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

final class WritetimeOrTTLSelector extends Selector {
   protected static final Selector.SelectorDeserializer deserializer = new Selector.SelectorDeserializer() {
      protected Selector deserialize(DataInputPlus in, ReadVerbs.ReadVersion version, TableMetadata metadata) throws IOException {
         ColumnMetadata column = metadata.getColumn(ByteBufferUtil.readWithVIntLength(in));
         int idx = in.readInt();
         boolean isWritetime = in.readBoolean();
         return new WritetimeOrTTLSelector(column, idx, isWritetime);
      }
   };
   private final ColumnMetadata column;
   private final int idx;
   private final boolean isWritetime;
   private ByteBuffer current;
   private boolean isSet;

   public static Selector.Factory newFactory(final ColumnMetadata def, final int idx, final boolean isWritetime) {
      return new Selector.Factory() {
         protected String getColumnName() {
            return String.format("%s(%s)", new Object[]{isWritetime?"writetime":"ttl", def.name.toString()});
         }

         protected AbstractType<?> getReturnType() {
            return (AbstractType)(isWritetime?LongType.instance:Int32Type.instance);
         }

         protected void addColumnMapping(SelectionColumnMapping mapping, ColumnSpecification resultsColumn) {
            mapping.addMapping(resultsColumn, def);
         }

         public Selector newInstance(QueryOptions options) {
            return new WritetimeOrTTLSelector(def, idx, isWritetime);
         }

         public boolean isWritetimeSelectorFactory() {
            return isWritetime;
         }

         public boolean isTTLSelectorFactory() {
            return !isWritetime;
         }

         public boolean areAllFetchedColumnsKnown() {
            return true;
         }

         public void addFetchedColumns(ColumnFilter.Builder builder) {
            builder.add(def);
         }
      };
   }

   public void addFetchedColumns(ColumnFilter.Builder builder) {
      builder.add(this.column);
   }

   public void addInput(ProtocolVersion protocolVersion, Selector.InputRow input) {
      if(!this.isSet) {
         this.isSet = true;
         if(this.isWritetime) {
            long ts = input.getTimestamp(this.idx);
            this.current = ts != -9223372036854775808L?ByteBufferUtil.bytes(ts):null;
         } else {
            int ttl = input.getTtl(this.idx);
            this.current = ttl > 0?ByteBufferUtil.bytes(ttl):null;
         }

      }
   }

   public ByteBuffer getOutput(ProtocolVersion protocolVersion) {
      return this.current;
   }

   public void reset() {
      this.isSet = false;
      this.current = null;
   }

   public AbstractType<?> getType() {
      return (AbstractType)(this.isWritetime?LongType.instance:Int32Type.instance);
   }

   public String toString() {
      return this.column.name.toString();
   }

   private WritetimeOrTTLSelector(ColumnMetadata column, int idx, boolean isWritetime) {
      super(Selector.Kind.WRITETIME_OR_TTL_SELECTOR);
      this.column = column;
      this.idx = idx;
      this.isWritetime = isWritetime;
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(!(o instanceof WritetimeOrTTLSelector)) {
         return false;
      } else {
         WritetimeOrTTLSelector s = (WritetimeOrTTLSelector)o;
         return Objects.equals(this.column, s.column) && Objects.equals(Integer.valueOf(this.idx), Integer.valueOf(s.idx)) && Objects.equals(Boolean.valueOf(this.isWritetime), Boolean.valueOf(s.isWritetime));
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.column, Integer.valueOf(this.idx), Boolean.valueOf(this.isWritetime)});
   }

   protected int serializedSize(ReadVerbs.ReadVersion version) {
      return TypeSizes.sizeofWithVIntLength(this.column.name.bytes) + TypeSizes.sizeof(this.idx) + TypeSizes.sizeof(this.isWritetime);
   }

   protected void serialize(DataOutputPlus out, ReadVerbs.ReadVersion version) throws IOException {
      ByteBufferUtil.writeWithVIntLength(this.column.name.bytes, out);
      out.writeInt(this.idx);
      out.writeBoolean(this.isWritetime);
   }
}
