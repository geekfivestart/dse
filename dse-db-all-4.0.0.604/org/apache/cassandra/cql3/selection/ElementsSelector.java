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
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

abstract class ElementsSelector extends Selector {
   protected final Selector selected;

   protected ElementsSelector(Selector.Kind kind, Selector selected) {
      super(kind);
      this.selected = selected;
   }

   private static boolean isUnset(ByteBuffer bb) {
      return bb == ByteBufferUtil.UNSET_BYTE_BUFFER;
   }

   private static AbstractType<?> keyType(CollectionType<?> type) {
      return type.nameComparator();
   }

   public static AbstractType<?> valueType(CollectionType<?> type) {
      return type instanceof MapType?type.valueComparator():type.nameComparator();
   }

   public static Selector.Factory newElementFactory(String name, Selector.Factory factory, CollectionType<?> type, final Term key) {
      return new ElementsSelector.AbstractFactory(name, factory, type) {
         protected AbstractType<?> getReturnType() {
            return ElementsSelector.valueType(this.type);
         }

         public Selector newInstance(QueryOptions options) throws InvalidRequestException {
            ByteBuffer keyValue = key.bindAndGet(options);
            if(keyValue == null) {
               throw new InvalidRequestException("Invalid null value for element selection on " + this.factory.getColumnName());
            } else if(keyValue == ByteBufferUtil.UNSET_BYTE_BUFFER) {
               throw new InvalidRequestException("Invalid unset value for element selection on " + this.factory.getColumnName());
            } else {
               return new ElementsSelector.ElementSelector(this.factory.newInstance(options), keyValue, null);
            }
         }

         public boolean areAllFetchedColumnsKnown() {
            return this.factory.areAllFetchedColumnsKnown() && (!this.type.isMultiCell() || !this.factory.isSimpleSelectorFactory() || key.isTerminal());
         }

         public void addFetchedColumns(ColumnFilter.Builder builder) {
            if(this.type.isMultiCell() && this.factory.isSimpleSelectorFactory()) {
               ColumnMetadata column = ((SimpleSelector.SimpleSelectorFactory)this.factory).getColumn();
               builder.select(column, CellPath.create(((Term.Terminal)key).get(ProtocolVersion.V3)));
            } else {
               this.factory.addFetchedColumns(builder);
            }
         }
      };
   }

   public static Selector.Factory newSliceFactory(String name, Selector.Factory factory, CollectionType<?> type, final Term from, final Term to) {
      return new ElementsSelector.AbstractFactory(name, factory, type) {
         protected AbstractType<?> getReturnType() {
            return this.type;
         }

         public Selector newInstance(QueryOptions options) throws InvalidRequestException {
            ByteBuffer fromValue = from.bindAndGet(options);
            ByteBuffer toValue = to.bindAndGet(options);
            if(fromValue != null && toValue != null) {
               return new ElementsSelector.SliceSelector(this.factory.newInstance(options), from.bindAndGet(options), to.bindAndGet(options), null);
            } else {
               throw new InvalidRequestException("Invalid null value for slice selection on " + this.factory.getColumnName());
            }
         }

         public boolean areAllFetchedColumnsKnown() {
            return this.factory.areAllFetchedColumnsKnown() && (!this.type.isMultiCell() || !this.factory.isSimpleSelectorFactory() || from.isTerminal() && to.isTerminal());
         }

         public void addFetchedColumns(ColumnFilter.Builder builder) {
            if(this.type.isMultiCell() && this.factory.isSimpleSelectorFactory()) {
               ColumnMetadata column = ((SimpleSelector.SimpleSelectorFactory)this.factory).getColumn();
               ByteBuffer fromBB = ((Term.Terminal)from).get(ProtocolVersion.V3);
               ByteBuffer toBB = ((Term.Terminal)to).get(ProtocolVersion.V3);
               builder.slice(column, ElementsSelector.isUnset(fromBB)?CellPath.BOTTOM:CellPath.create(fromBB), ElementsSelector.isUnset(toBB)?CellPath.TOP:CellPath.create(toBB));
            } else {
               this.factory.addFetchedColumns(builder);
            }
         }
      };
   }

   public ByteBuffer getOutput(ProtocolVersion protocolVersion) throws InvalidRequestException {
      ByteBuffer value = this.selected.getOutput(protocolVersion);
      return value == null?null:this.extractSelection(value);
   }

   protected abstract ByteBuffer extractSelection(ByteBuffer var1);

   public void addInput(ProtocolVersion protocolVersion, Selector.InputRow input) {
      this.selected.addInput(protocolVersion, input);
   }

   public void reset() {
      this.selected.reset();
   }

   static final class SliceSelector extends ElementsSelector {
      protected static final Selector.SelectorDeserializer deserializer = new Selector.SelectorDeserializer() {
         protected Selector deserialize(DataInputPlus in, ReadVerbs.ReadVersion version, TableMetadata metadata) throws IOException {
            Selector.Serializer serializer = (Selector.Serializer)Selector.serializers.get(version);
            Selector selected = serializer.deserialize(in, metadata);
            ByteBuffer from = ByteBufferUtil.readWithVIntLength(in);
            ByteBuffer to = ByteBufferUtil.readWithVIntLength(in);
            return new ElementsSelector.SliceSelector(selected, from, to, null);
         }
      };
      private final CollectionType<?> type;
      private final ByteBuffer from;
      private final ByteBuffer to;

      private SliceSelector(Selector selected, ByteBuffer from, ByteBuffer to) {
         super(Selector.Kind.SLICE_SELECTOR, selected);

         assert selected.getType() instanceof MapType || selected.getType() instanceof SetType : "this shouldn't have passed validation in Selectable";

         assert from != null && to != null : "We can have unset buffers, but not nulls";

         this.type = (CollectionType)selected.getType();
         this.from = from;
         this.to = to;
      }

      public void addFetchedColumns(ColumnFilter.Builder builder) {
         if(this.type.isMultiCell() && this.selected instanceof SimpleSelector) {
            ColumnMetadata column = ((SimpleSelector)this.selected).column;
            builder.slice(column, ElementsSelector.isUnset(this.from)?CellPath.BOTTOM:CellPath.create(this.from), ElementsSelector.isUnset(this.to)?CellPath.TOP:CellPath.create(this.to));
         } else {
            this.selected.addFetchedColumns(builder);
         }

      }

      protected ByteBuffer extractSelection(ByteBuffer collection) {
         return this.type.getSerializer().getSliceFromSerialized(collection, this.from, this.to, this.type.nameComparator(), this.type.isFrozenCollection());
      }

      public AbstractType<?> getType() {
         return this.type;
      }

      public String toString() {
         boolean fromUnset = ElementsSelector.isUnset(this.from);
         boolean toUnset = ElementsSelector.isUnset(this.to);
         return fromUnset && toUnset?this.selected.toString():String.format("%s[%s..%s]", new Object[]{this.selected, fromUnset?"":this.type.nameComparator().getString(this.from), toUnset?"":this.type.nameComparator().getString(this.to)});
      }

      public boolean isTerminal() {
         return this.selected.isTerminal();
      }

      public boolean equals(Object o) {
         if(this == o) {
            return true;
         } else if(!(o instanceof ElementsSelector.SliceSelector)) {
            return false;
         } else {
            ElementsSelector.SliceSelector s = (ElementsSelector.SliceSelector)o;
            return Objects.equals(this.selected, s.selected) && Objects.equals(this.from, s.from) && Objects.equals(this.to, s.to);
         }
      }

      public int hashCode() {
         return Objects.hash(new Object[]{this.selected, this.from, this.to});
      }

      protected int serializedSize(ReadVerbs.ReadVersion version) {
         Selector.Serializer serializer = (Selector.Serializer)serializers.get(version);
         return serializer.serializedSize(this.selected) + ByteBufferUtil.serializedSizeWithVIntLength(this.from) + ByteBufferUtil.serializedSizeWithVIntLength(this.to);
      }

      protected void serialize(DataOutputPlus out, ReadVerbs.ReadVersion version) throws IOException {
         Selector.Serializer serializer = (Selector.Serializer)serializers.get(version);
         serializer.serialize(this.selected, out);
         ByteBufferUtil.writeWithVIntLength(this.from, out);
         ByteBufferUtil.writeWithVIntLength(this.to, out);
      }
   }

   static final class ElementSelector extends ElementsSelector {
      protected static final Selector.SelectorDeserializer deserializer = new Selector.SelectorDeserializer() {
         protected Selector deserialize(DataInputPlus in, ReadVerbs.ReadVersion version, TableMetadata metadata) throws IOException {
            Selector.Serializer serializer = (Selector.Serializer)Selector.serializers.get(version);
            Selector selected = serializer.deserialize(in, metadata);
            ByteBuffer key = ByteBufferUtil.readWithVIntLength(in);
            return new ElementsSelector.ElementSelector(selected, key, null);
         }
      };
      private final CollectionType<?> type;
      private final ByteBuffer key;

      private ElementSelector(Selector selected, ByteBuffer key) {
         super(Selector.Kind.ELEMENT_SELECTOR, selected);

         assert selected.getType() instanceof MapType || selected.getType() instanceof SetType : "this shouldn't have passed validation in Selectable";

         this.type = (CollectionType)selected.getType();
         this.key = key;
      }

      public void addFetchedColumns(ColumnFilter.Builder builder) {
         if(this.type.isMultiCell() && this.selected instanceof SimpleSelector) {
            ColumnMetadata column = ((SimpleSelector)this.selected).column;
            builder.select(column, CellPath.create(this.key));
         } else {
            this.selected.addFetchedColumns(builder);
         }

      }

      protected ByteBuffer extractSelection(ByteBuffer collection) {
         return this.type.getSerializer().getSerializedValue(collection, this.key, this.type.nameComparator());
      }

      public AbstractType<?> getType() {
         return valueType(this.type);
      }

      public String toString() {
         return String.format("%s[%s]", new Object[]{this.selected, this.type.nameComparator().getString(this.key)});
      }

      public boolean isTerminal() {
         return this.selected.isTerminal();
      }

      protected int serializedSize(ReadVerbs.ReadVersion version) {
         Selector.Serializer serializer = (Selector.Serializer)serializers.get(version);
         return serializer.serializedSize(this.selected) + ByteBufferUtil.serializedSizeWithVIntLength(this.key);
      }

      public boolean equals(Object o) {
         if(this == o) {
            return true;
         } else if(!(o instanceof ElementsSelector.ElementSelector)) {
            return false;
         } else {
            ElementsSelector.ElementSelector s = (ElementsSelector.ElementSelector)o;
            return Objects.equals(this.selected, s.selected) && Objects.equals(this.key, s.key);
         }
      }

      public int hashCode() {
         return Objects.hash(new Object[]{this.selected, this.key});
      }

      protected void serialize(DataOutputPlus out, ReadVerbs.ReadVersion version) throws IOException {
         Selector.Serializer serializer = (Selector.Serializer)serializers.get(version);
         serializer.serialize(this.selected, out);
         ByteBufferUtil.writeWithVIntLength(this.key, out);
      }
   }

   private abstract static class AbstractFactory extends Selector.Factory {
      protected final String name;
      protected final Selector.Factory factory;
      protected final CollectionType<?> type;

      protected AbstractFactory(String name, Selector.Factory factory, CollectionType<?> type) {
         this.name = name;
         this.factory = factory;
         this.type = type;
      }

      protected String getColumnName() {
         return this.name;
      }

      protected void addColumnMapping(SelectionColumnMapping mapping, ColumnSpecification resultsColumn) {
         this.factory.addColumnMapping(mapping, resultsColumn);
      }

      public boolean isAggregateSelectorFactory() {
         return this.factory.isAggregateSelectorFactory();
      }
   }
}
