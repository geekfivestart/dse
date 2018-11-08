package org.apache.cassandra.cql3.selection;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.db.ReadVerbs;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

public class TermSelector extends Selector {
   protected static final Selector.SelectorDeserializer deserializer = new Selector.SelectorDeserializer() {
      protected Selector deserialize(DataInputPlus in, ReadVerbs.ReadVersion version, TableMetadata metadata) throws IOException {
         AbstractType<?> type = this.readType(metadata, in);
         ByteBuffer value = ByteBufferUtil.readWithVIntLength(in);
         return new TermSelector(value, type);
      }
   };
   private final ByteBuffer value;
   private final AbstractType<?> type;

   public static Selector.Factory newFactory(final String name, final Term term, final AbstractType<?> type) {
      return new Selector.Factory() {
         protected String getColumnName() {
            return name;
         }

         protected AbstractType<?> getReturnType() {
            return type;
         }

         protected void addColumnMapping(SelectionColumnMapping mapping, ColumnSpecification resultColumn) {
            mapping.addMapping(resultColumn, (ColumnMetadata)null);
         }

         public Selector newInstance(QueryOptions options) {
            return new TermSelector(term.bindAndGet(options), type);
         }

         public void addFetchedColumns(ColumnFilter.Builder builder) {
         }

         public boolean areAllFetchedColumnsKnown() {
            return true;
         }
      };
   }

   TermSelector(ByteBuffer value, AbstractType<?> type) {
      super(Selector.Kind.TERM_SELECTOR);
      this.value = value;
      this.type = type;
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(!(o instanceof TermSelector)) {
         return false;
      } else {
         TermSelector s = (TermSelector)o;
         return Objects.equals(this.value, s.value) && Objects.equals(this.type, s.type);
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.value, this.type});
   }

   public void addFetchedColumns(ColumnFilter.Builder builder) {
   }

   public void addInput(ProtocolVersion protocolVersion, Selector.InputRow input) {
   }

   public ByteBuffer getOutput(ProtocolVersion protocolVersion) {
      return this.value;
   }

   public AbstractType<?> getType() {
      return this.type;
   }

   public void reset() {
   }

   public boolean isTerminal() {
      return true;
   }

   protected int serializedSize(ReadVerbs.ReadVersion version) {
      return sizeOf(this.type) + ByteBufferUtil.serializedSizeWithVIntLength(this.value);
   }

   protected void serialize(DataOutputPlus out, ReadVerbs.ReadVersion version) throws IOException {
      writeType(out, this.type);
      ByteBufferUtil.writeWithVIntLength(this.value, out);
   }
}
