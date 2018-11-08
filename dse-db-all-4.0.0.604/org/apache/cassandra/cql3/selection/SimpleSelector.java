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
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

public final class SimpleSelector extends Selector {
   protected static final Selector.SelectorDeserializer deserializer = new Selector.SelectorDeserializer() {
      protected Selector deserialize(DataInputPlus in, ReadVerbs.ReadVersion version, TableMetadata metadata) throws IOException {
         ColumnMetadata column = metadata.getColumn(ByteBufferUtil.readWithVIntLength(in));
         int idx = in.readInt();
         return new SimpleSelector(column, idx, null);
      }
   };
   public final ColumnMetadata column;
   private final int idx;
   private ByteBuffer current;
   private boolean isSet;

   public static Selector.Factory newFactory(ColumnMetadata def, int idx) {
      return new SimpleSelector.SimpleSelectorFactory(idx, def, null);
   }

   public void addFetchedColumns(ColumnFilter.Builder builder) {
      builder.add(this.column);
   }

   public void addInput(ProtocolVersion protocolVersion, Selector.InputRow input) {
      if(!this.isSet) {
         this.isSet = true;
         this.current = input.getValue(this.idx);
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
      return this.column.type;
   }

   public String toString() {
      return this.column.name.toString();
   }

   private SimpleSelector(ColumnMetadata column, int idx) {
      super(Selector.Kind.SIMPLE_SELECTOR);
      this.column = column;
      this.idx = idx;
   }

   public void validateForGroupBy() {
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(!(o instanceof SimpleSelector)) {
         return false;
      } else {
         SimpleSelector s = (SimpleSelector)o;
         return Objects.equals(this.column, s.column) && Objects.equals(Integer.valueOf(this.idx), Integer.valueOf(s.idx));
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.column, Integer.valueOf(this.idx)});
   }

   protected int serializedSize(ReadVerbs.ReadVersion version) {
      return TypeSizes.sizeofWithVIntLength(this.column.name.bytes) + TypeSizes.sizeof(this.idx);
   }

   protected void serialize(DataOutputPlus out, ReadVerbs.ReadVersion version) throws IOException {
      ByteBufferUtil.writeWithVIntLength(this.column.name.bytes, out);
      out.writeInt(this.idx);
   }

   public static final class SimpleSelectorFactory extends Selector.Factory {
      private final int idx;
      private final ColumnMetadata column;

      private SimpleSelectorFactory(int idx, ColumnMetadata def) {
         this.idx = idx;
         this.column = def;
      }

      protected String getColumnName() {
         return this.column.name.toString();
      }

      protected AbstractType<?> getReturnType() {
         return this.column.type;
      }

      protected void addColumnMapping(SelectionColumnMapping mapping, ColumnSpecification resultColumn) {
         mapping.addMapping(resultColumn, this.column);
      }

      public Selector newInstance(QueryOptions options) {
         return new SimpleSelector(this.column, this.idx, null);
      }

      public boolean isSimpleSelectorFactory() {
         return true;
      }

      public boolean isSimpleSelectorFactoryFor(int index) {
         return index == this.idx;
      }

      public boolean areAllFetchedColumnsKnown() {
         return true;
      }

      public void addFetchedColumns(ColumnFilter.Builder builder) {
         builder.add(this.column);
      }

      public ColumnMetadata getColumn() {
         return this.column;
      }
   }
}
