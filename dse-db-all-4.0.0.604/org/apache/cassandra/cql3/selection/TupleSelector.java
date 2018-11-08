package org.apache.cassandra.cql3.selection;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.Tuples;
import org.apache.cassandra.db.ReadVerbs;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.ProtocolVersion;

final class TupleSelector extends Selector {
   protected static final Selector.SelectorDeserializer deserializer = new Selector.SelectorDeserializer() {
      protected Selector deserialize(DataInputPlus in, ReadVerbs.ReadVersion version, TableMetadata metadata) throws IOException {
         AbstractType<?> type = this.readType(metadata, in);
         int size = (int)in.readUnsignedVInt();
         List<Selector> elements = new ArrayList(size);
         Selector.Serializer serializer = (Selector.Serializer)Selector.serializers.get(version);

         for(int i = 0; i < size; ++i) {
            elements.add(serializer.deserialize(in, metadata));
         }

         return new TupleSelector(type, elements, null);
      }
   };
   private final AbstractType<?> type;
   private final List<Selector> elements;

   public static Selector.Factory newFactory(final AbstractType<?> type, final SelectorFactories factories) {
      return new CollectionFactory(type, factories) {
         protected String getColumnName() {
            return Tuples.tupleToString(factories, Selector.Factory::getColumnName);
         }

         public Selector newInstance(QueryOptions options) {
            return new TupleSelector(type, factories.newInstances(options), null);
         }
      };
   }

   public void addFetchedColumns(ColumnFilter.Builder builder) {
      int i = 0;

      for(int m = this.elements.size(); i < m; ++i) {
         ((Selector)this.elements.get(i)).addFetchedColumns(builder);
      }

   }

   public void addInput(ProtocolVersion protocolVersion, Selector.InputRow input) {
      int i = 0;

      for(int m = this.elements.size(); i < m; ++i) {
         ((Selector)this.elements.get(i)).addInput(protocolVersion, input);
      }

   }

   public ByteBuffer getOutput(ProtocolVersion protocolVersion) {
      ByteBuffer[] buffers = new ByteBuffer[this.elements.size()];
      int i = 0;

      for(int m = this.elements.size(); i < m; ++i) {
         buffers[i] = ((Selector)this.elements.get(i)).getOutput(protocolVersion);
      }

      return TupleType.buildValue(buffers);
   }

   public void reset() {
      int i = 0;

      for(int m = this.elements.size(); i < m; ++i) {
         ((Selector)this.elements.get(i)).reset();
      }

   }

   public AbstractType<?> getType() {
      return this.type;
   }

   public boolean isTerminal() {
      int i = 0;

      for(int m = this.elements.size(); i < m; ++i) {
         if(!((Selector)this.elements.get(i)).isTerminal()) {
            return false;
         }
      }

      return true;
   }

   public String toString() {
      return Tuples.tupleToString(this.elements);
   }

   private TupleSelector(AbstractType<?> type, List<Selector> elements) {
      super(Selector.Kind.TUPLE_SELECTOR);
      this.type = type;
      this.elements = elements;
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(!(o instanceof TupleSelector)) {
         return false;
      } else {
         TupleSelector s = (TupleSelector)o;
         return Objects.equals(this.type, s.type) && Objects.equals(this.elements, s.elements);
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.type, this.elements});
   }

   protected int serializedSize(ReadVerbs.ReadVersion version) {
      int size = sizeOf(this.type) + TypeSizes.sizeofUnsignedVInt((long)this.elements.size());
      Selector.Serializer serializer = (Selector.Serializer)serializers.get(version);
      int i = 0;

      for(int m = this.elements.size(); i < m; ++i) {
         size += serializer.serializedSize((Selector)this.elements.get(i));
      }

      return size;
   }

   protected void serialize(DataOutputPlus out, ReadVerbs.ReadVersion version) throws IOException {
      writeType(out, this.type);
      out.writeUnsignedVInt((long)this.elements.size());
      Selector.Serializer serializer = (Selector.Serializer)serializers.get(version);
      int i = 0;

      for(int m = this.elements.size(); i < m; ++i) {
         serializer.serialize((Selector)this.elements.get(i), out);
      }

   }
}
