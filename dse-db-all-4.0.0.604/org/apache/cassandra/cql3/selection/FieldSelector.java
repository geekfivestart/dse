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
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.ProtocolVersion;

final class FieldSelector extends Selector {
   protected static final Selector.SelectorDeserializer deserializer = new Selector.SelectorDeserializer() {
      protected Selector deserialize(DataInputPlus in, ReadVerbs.ReadVersion version, TableMetadata metadata) throws IOException {
         UserType type = (UserType)this.readType(metadata, in);
         int field = (int)in.readUnsignedVInt();
         Selector selected = ((Selector.Serializer)Selector.serializers.get(version)).deserialize(in, metadata);
         return new FieldSelector(type, field, selected, null);
      }
   };
   private final UserType type;
   private final int field;
   private final Selector selected;

   public static Selector.Factory newFactory(final UserType type, final int field, final Selector.Factory factory) {
      return new Selector.Factory() {
         protected String getColumnName() {
            return String.format("%s.%s", new Object[]{factory.getColumnName(), type.fieldName(field)});
         }

         protected AbstractType<?> getReturnType() {
            return type.fieldType(field);
         }

         protected void addColumnMapping(SelectionColumnMapping mapping, ColumnSpecification resultsColumn) {
            factory.addColumnMapping(mapping, resultsColumn);
         }

         public Selector newInstance(QueryOptions options) {
            return new FieldSelector(type, field, factory.newInstance(options), null);
         }

         public boolean isAggregateSelectorFactory() {
            return factory.isAggregateSelectorFactory();
         }

         public boolean areAllFetchedColumnsKnown() {
            return factory.areAllFetchedColumnsKnown();
         }

         public void addFetchedColumns(ColumnFilter.Builder builder) {
            factory.addFetchedColumns(builder);
         }
      };
   }

   public void addFetchedColumns(ColumnFilter.Builder builder) {
      this.selected.addFetchedColumns(builder);
   }

   public void addInput(ProtocolVersion protocolVersion, Selector.InputRow input) {
      this.selected.addInput(protocolVersion, input);
   }

   public ByteBuffer getOutput(ProtocolVersion protocolVersion) {
      ByteBuffer value = this.selected.getOutput(protocolVersion);
      if(value == null) {
         return null;
      } else {
         ByteBuffer[] buffers = this.type.split(value);
         return this.field < buffers.length?buffers[this.field]:null;
      }
   }

   public AbstractType<?> getType() {
      return this.type.fieldType(this.field);
   }

   public void reset() {
      this.selected.reset();
   }

   public boolean isTerminal() {
      return this.selected.isTerminal();
   }

   public String toString() {
      return String.format("%s.%s", new Object[]{this.selected, this.type.fieldName(this.field)});
   }

   private FieldSelector(UserType type, int field, Selector selected) {
      super(Selector.Kind.FIELD_SELECTOR);
      this.type = type;
      this.field = field;
      this.selected = selected;
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(!(o instanceof FieldSelector)) {
         return false;
      } else {
         FieldSelector s = (FieldSelector)o;
         return Objects.equals(this.type, s.type) && Objects.equals(Integer.valueOf(this.field), Integer.valueOf(s.field)) && Objects.equals(this.selected, s.selected);
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.type, Integer.valueOf(this.field), this.selected});
   }

   protected int serializedSize(ReadVerbs.ReadVersion version) {
      return sizeOf(this.type) + TypeSizes.sizeofUnsignedVInt((long)this.field) + ((Selector.Serializer)serializers.get(version)).serializedSize(this.selected);
   }

   protected void serialize(DataOutputPlus out, ReadVerbs.ReadVersion version) throws IOException {
      writeType(out, this.type);
      out.writeUnsignedVInt((long)this.field);
      ((Selector.Serializer)serializers.get(version)).serialize(this.selected, out);
   }
}
