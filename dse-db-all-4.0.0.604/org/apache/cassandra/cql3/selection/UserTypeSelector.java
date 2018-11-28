package org.apache.cassandra.cql3.selection;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Map.Entry;
import java.util.function.Function;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.UserTypes;
import org.apache.cassandra.db.ReadVerbs;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

final class UserTypeSelector extends Selector {
   protected static final Selector.SelectorDeserializer deserializer = new Selector.SelectorDeserializer() {
      protected Selector deserialize(DataInputPlus in, ReadVerbs.ReadVersion version, TableMetadata metadata) throws IOException {
         UserType type = (UserType)this.readType(metadata, in);
         int size = (int)in.readUnsignedVInt();
         Map<FieldIdentifier, Selector> fields = new HashMap(size);
         Selector.Serializer serializer = (Selector.Serializer)Selector.serializers.get(version);

         for(int i = 0; i < size; ++i) {
            FieldIdentifier identifier = new FieldIdentifier(ByteBufferUtil.readWithVIntLength(in));
            Selector selector = serializer.deserialize(in, metadata);
            fields.put(identifier, selector);
         }

         return new UserTypeSelector(type, fields);
      }
   };
   private final AbstractType<?> type;
   private final Map<FieldIdentifier, Selector> fields;

   public static Selector.Factory newFactory(final AbstractType<?> type, final Map<FieldIdentifier, Selector.Factory> factories) {
      return new Selector.Factory() {
         protected String getColumnName() {
            return UserTypes.userTypeToString(factories, Selector.Factory::getColumnName);
         }

         protected AbstractType<?> getReturnType() {
            return type;
         }

         protected final void addColumnMapping(SelectionColumnMapping mapping, ColumnSpecification resultsColumn) {
            SelectionColumnMapping tmpMapping = SelectionColumnMapping.newMapping();
            Iterator var4 = factories.values().iterator();

            while(var4.hasNext()) {
               Selector.Factory factory = (Selector.Factory)var4.next();
               factory.addColumnMapping(tmpMapping, resultsColumn);
            }

            if(tmpMapping.getMappings().get(resultsColumn).isEmpty()) {
               mapping.addMapping(resultsColumn, (ColumnMetadata)null);
            } else {
               mapping.addMapping(resultsColumn, (Iterable)tmpMapping.getMappings().values());
            }

         }

         public Selector newInstance(QueryOptions options) {
            Map<FieldIdentifier, Selector> fields = new HashMap(factories.size());
            Iterator var3 = factories.entrySet().iterator();

            while(var3.hasNext()) {
               Entry<FieldIdentifier, Selector.Factory> factory = (Entry)var3.next();
               fields.put(factory.getKey(), ((Selector.Factory)factory.getValue()).newInstance(options));
            }

            return new UserTypeSelector(type, fields);
         }

         public boolean isAggregateSelectorFactory() {
            Iterator var1 = factories.values().iterator();

            Selector.Factory factory;
            do {
               if(!var1.hasNext()) {
                  return false;
               }

               factory = (Selector.Factory)var1.next();
            } while(!factory.isAggregateSelectorFactory());

            return true;
         }

         public void addFunctionsTo(List<org.apache.cassandra.cql3.functions.Function> functions) {
            Iterator var2 = factories.values().iterator();

            while(var2.hasNext()) {
               Selector.Factory factory = (Selector.Factory)var2.next();
               factory.addFunctionsTo(functions);
            }

         }

         public boolean isWritetimeSelectorFactory() {
            Iterator var1 = factories.values().iterator();

            Selector.Factory factory;
            do {
               if(!var1.hasNext()) {
                  return false;
               }

               factory = (Selector.Factory)var1.next();
            } while(!factory.isWritetimeSelectorFactory());

            return true;
         }

         public boolean isTTLSelectorFactory() {
            Iterator var1 = factories.values().iterator();

            Selector.Factory factory;
            do {
               if(!var1.hasNext()) {
                  return false;
               }

               factory = (Selector.Factory)var1.next();
            } while(!factory.isTTLSelectorFactory());

            return true;
         }

         boolean areAllFetchedColumnsKnown() {
            Iterator var1 = factories.values().iterator();

            Selector.Factory factory;
            do {
               if(!var1.hasNext()) {
                  return true;
               }

               factory = (Selector.Factory)var1.next();
            } while(factory.areAllFetchedColumnsKnown());

            return false;
         }

         void addFetchedColumns(ColumnFilter.Builder builder) {
            Iterator var2 = factories.values().iterator();

            while(var2.hasNext()) {
               Selector.Factory factory = (Selector.Factory)var2.next();
               factory.addFetchedColumns(builder);
            }

         }
      };
   }

   public void addFetchedColumns(ColumnFilter.Builder builder) {
      Iterator var2 = this.fields.values().iterator();

      while(var2.hasNext()) {
         Selector field = (Selector)var2.next();
         field.addFetchedColumns(builder);
      }

   }

   public void addInput(ProtocolVersion protocolVersion, Selector.InputRow input) {
      Iterator var3 = this.fields.values().iterator();

      while(var3.hasNext()) {
         Selector field = (Selector)var3.next();
         field.addInput(protocolVersion, input);
      }

   }

   public ByteBuffer getOutput(ProtocolVersion protocolVersion) {
      UserType userType = (UserType)this.type;
      ByteBuffer[] buffers = new ByteBuffer[userType.size()];
      int i = 0;

      for(int m = userType.size(); i < m; ++i) {
         Selector selector = (Selector)this.fields.get(userType.fieldName(i));
         if(selector != null) {
            buffers[i] = selector.getOutput(protocolVersion);
         }
      }

      return TupleType.buildValue(buffers);
   }

   public void reset() {
      Iterator var1 = this.fields.values().iterator();

      while(var1.hasNext()) {
         Selector field = (Selector)var1.next();
         field.reset();
      }

   }

   public boolean isTerminal() {
      Iterator var1 = this.fields.values().iterator();

      Selector field;
      do {
         if(!var1.hasNext()) {
            return true;
         }

         field = (Selector)var1.next();
      } while(field.isTerminal());

      return false;
   }

   public AbstractType<?> getType() {
      return this.type;
   }

   public String toString() {
      return UserTypes.userTypeToString(this.fields);
   }

   private UserTypeSelector(AbstractType<?> type, Map<FieldIdentifier, Selector> fields) {
      super(Selector.Kind.USER_TYPE_SELECTOR);
      this.type = type;
      this.fields = fields;
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(!(o instanceof UserTypeSelector)) {
         return false;
      } else {
         UserTypeSelector s = (UserTypeSelector)o;
         return Objects.equals(this.type, s.type) && Objects.equals(this.fields, s.fields);
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.type, this.fields});
   }

   protected int serializedSize(ReadVerbs.ReadVersion version) {
      int size = sizeOf(this.type) + TypeSizes.sizeofUnsignedVInt((long)this.fields.size());
      Selector.Serializer serializer = (Selector.Serializer)serializers.get(version);

      Entry field;
      for(Iterator var4 = this.fields.entrySet().iterator(); var4.hasNext(); size += ByteBufferUtil.serializedSizeWithVIntLength(((FieldIdentifier)field.getKey()).bytes) + serializer.serializedSize((Selector)field.getValue())) {
         field = (Entry)var4.next();
      }

      return size;
   }

   protected void serialize(DataOutputPlus out, ReadVerbs.ReadVersion version) throws IOException {
      writeType(out, this.type);
      out.writeUnsignedVInt((long)this.fields.size());
      Selector.Serializer serializer = (Selector.Serializer)serializers.get(version);
      Iterator var4 = this.fields.entrySet().iterator();

      while(var4.hasNext()) {
         Entry<FieldIdentifier, Selector> field = (Entry)var4.next();
         ByteBufferUtil.writeWithVIntLength(((FieldIdentifier)field.getKey()).bytes, out);
         serializer.serialize((Selector)field.getValue(), out);
      }

   }
}
