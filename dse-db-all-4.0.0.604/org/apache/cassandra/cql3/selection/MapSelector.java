package org.apache.cassandra.cql3.selection;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.Maps;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.ReadVerbs;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.Pair;

final class MapSelector extends Selector {
   protected static final Selector.SelectorDeserializer deserializer = new Selector.SelectorDeserializer() {
      protected Selector deserialize(DataInputPlus in, ReadVerbs.ReadVersion version, TableMetadata metadata) throws IOException {
         MapType<?, ?> type = (MapType)this.readType(metadata, in);
         int size = (int)in.readUnsignedVInt();
         List<Pair<Selector, Selector>> entries = new ArrayList(size);
         Selector.Serializer serializer = (Selector.Serializer)Selector.serializers.get(version);

         for(int i = 0; i < size; ++i) {
            entries.add(Pair.create(serializer.deserialize(in, metadata), serializer.deserialize(in, metadata)));
         }

         return new MapSelector(type, entries);
      }
   };
   private final MapType<?, ?> type;
   private final List<Pair<Selector, Selector>> elements;

   public static Selector.Factory newFactory(final AbstractType<?> type, final List<Pair<Selector.Factory, Selector.Factory>> factories) {
      return new Selector.Factory() {
         protected String getColumnName() {
            return Maps.mapToString(factories, Selector.Factory::getColumnName);
         }

         protected AbstractType<?> getReturnType() {
            return type;
         }

         protected final void addColumnMapping(SelectionColumnMapping mapping, ColumnSpecification resultsColumn) {
            SelectionColumnMapping tmpMapping = SelectionColumnMapping.newMapping();
            Iterator var4 = factories.iterator();

            while(var4.hasNext()) {
               Pair<Selector.Factory, Selector.Factory> entry = (Pair)var4.next();
               ((Selector.Factory)entry.left).addColumnMapping(tmpMapping, resultsColumn);
               ((Selector.Factory)entry.right).addColumnMapping(tmpMapping, resultsColumn);
            }

            if(tmpMapping.getMappings().get(resultsColumn).isEmpty()) {
               mapping.addMapping(resultsColumn, (ColumnMetadata)null);
            } else {
               mapping.addMapping(resultsColumn, (Iterable)tmpMapping.getMappings().values());
            }

         }

         public Selector newInstance(QueryOptions options) {
            return new MapSelector(type, (List)factories.stream().map((p) -> {
               return Pair.create(((Selector.Factory)p.left).newInstance(options), ((Selector.Factory)p.right).newInstance(options));
            }).collect(Collectors.toList()));
         }

         public boolean isAggregateSelectorFactory() {
            Iterator var1 = factories.iterator();

            Pair entry;
            do {
               if(!var1.hasNext()) {
                  return false;
               }

               entry = (Pair)var1.next();
            } while(!((Selector.Factory)entry.left).isAggregateSelectorFactory() && !((Selector.Factory)entry.right).isAggregateSelectorFactory());

            return true;
         }

         public void addFunctionsTo(List<org.apache.cassandra.cql3.functions.Function> functions) {
            Iterator var2 = factories.iterator();

            while(var2.hasNext()) {
               Pair<Selector.Factory, Selector.Factory> entry = (Pair)var2.next();
               ((Selector.Factory)entry.left).addFunctionsTo(functions);
               ((Selector.Factory)entry.right).addFunctionsTo(functions);
            }

         }

         public boolean isWritetimeSelectorFactory() {
            Iterator var1 = factories.iterator();

            Pair entry;
            do {
               if(!var1.hasNext()) {
                  return false;
               }

               entry = (Pair)var1.next();
            } while(!((Selector.Factory)entry.left).isWritetimeSelectorFactory() && !((Selector.Factory)entry.right).isWritetimeSelectorFactory());

            return true;
         }

         public boolean isTTLSelectorFactory() {
            Iterator var1 = factories.iterator();

            Pair entry;
            do {
               if(!var1.hasNext()) {
                  return false;
               }

               entry = (Pair)var1.next();
            } while(!((Selector.Factory)entry.left).isTTLSelectorFactory() && !((Selector.Factory)entry.right).isTTLSelectorFactory());

            return true;
         }

         boolean areAllFetchedColumnsKnown() {
            Iterator var1 = factories.iterator();

            Pair entry;
            do {
               if(!var1.hasNext()) {
                  return true;
               }

               entry = (Pair)var1.next();
            } while(((Selector.Factory)entry.left).areAllFetchedColumnsKnown() && ((Selector.Factory)entry.right).areAllFetchedColumnsKnown());

            return false;
         }

         void addFetchedColumns(ColumnFilter.Builder builder) {
            Iterator var2 = factories.iterator();

            while(var2.hasNext()) {
               Pair<Selector.Factory, Selector.Factory> entry = (Pair)var2.next();
               ((Selector.Factory)entry.left).addFetchedColumns(builder);
               ((Selector.Factory)entry.right).addFetchedColumns(builder);
            }

         }
      };
   }

   public void addFetchedColumns(ColumnFilter.Builder builder) {
      int i = 0;

      for(int m = this.elements.size(); i < m; ++i) {
         Pair<Selector, Selector> pair = (Pair)this.elements.get(i);
         ((Selector)pair.left).addFetchedColumns(builder);
         ((Selector)pair.right).addFetchedColumns(builder);
      }

   }

   public void addInput(ProtocolVersion protocolVersion, Selector.InputRow input) {
      int i = 0;

      for(int m = this.elements.size(); i < m; ++i) {
         Pair<Selector, Selector> pair = (Pair)this.elements.get(i);
         ((Selector)pair.left).addInput(protocolVersion, input);
         ((Selector)pair.right).addInput(protocolVersion, input);
      }

   }

   public ByteBuffer getOutput(ProtocolVersion protocolVersion) {
      Map<ByteBuffer, ByteBuffer> map = new TreeMap(this.type.getKeysType());
      int i = 0;

      for(int m = this.elements.size(); i < m; ++i) {
         Pair<Selector, Selector> pair = (Pair)this.elements.get(i);
         map.put(((Selector)pair.left).getOutput(protocolVersion), ((Selector)pair.right).getOutput(protocolVersion));
      }

      List<ByteBuffer> buffers = new ArrayList(this.elements.size() * 2);
      Iterator var7 = map.entrySet().iterator();

      while(var7.hasNext()) {
         Entry<ByteBuffer, ByteBuffer> entry = (Entry)var7.next();
         buffers.add(entry.getKey());
         buffers.add(entry.getValue());
      }

      return CollectionSerializer.pack(buffers, this.elements.size(), protocolVersion);
   }

   public void reset() {
      int i = 0;

      for(int m = this.elements.size(); i < m; ++i) {
         Pair<Selector, Selector> pair = (Pair)this.elements.get(i);
         ((Selector)pair.left).reset();
         ((Selector)pair.right).reset();
      }

   }

   public AbstractType<?> getType() {
      return this.type;
   }

   public String toString() {
      return Maps.mapToString(this.elements);
   }

   public boolean isTerminal() {
      int i = 0;

      for(int m = this.elements.size(); i < m; ++i) {
         Pair<Selector, Selector> pair = (Pair)this.elements.get(i);
         if(!((Selector)pair.left).isTerminal() || !((Selector)pair.right).isTerminal()) {
            return false;
         }
      }

      return true;
   }

   private MapSelector(AbstractType<?> type, List<Pair<Selector, Selector>> elements) {
      super(Selector.Kind.MAP_SELECTOR);
      this.type = (MapType)type;
      this.elements = elements;
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(!(o instanceof MapSelector)) {
         return false;
      } else {
         MapSelector s = (MapSelector)o;
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
         Pair<Selector, Selector> entry = (Pair)this.elements.get(i);
         size += serializer.serializedSize((Selector)entry.left) + serializer.serializedSize((Selector)entry.right);
      }

      return size;
   }

   protected void serialize(DataOutputPlus out, ReadVerbs.ReadVersion version) throws IOException {
      writeType(out, this.type);
      out.writeUnsignedVInt((long)this.elements.size());
      Selector.Serializer serializer = (Selector.Serializer)serializers.get(version);
      int i = 0;

      for(int m = this.elements.size(); i < m; ++i) {
         Pair<Selector, Selector> entry = (Pair)this.elements.get(i);
         serializer.serialize((Selector)entry.left, out);
         serializer.serialize((Selector)entry.right, out);
      }

   }
}
