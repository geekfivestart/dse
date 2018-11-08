package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.DurationType;
import org.apache.cassandra.db.marshal.EmptyType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.TimeType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface CQL3Type {
   Logger logger = LoggerFactory.getLogger(CQL3Type.class);

   default boolean isCollection() {
      return false;
   }

   default boolean isUDT() {
      return false;
   }

   AbstractType<?> getType();

   String toCQLLiteral(ByteBuffer var1, ProtocolVersion var2);

   public abstract static class Raw {
      protected boolean frozen = false;

      public Raw() {
      }

      public abstract boolean supportsFreezing();

      public boolean isFrozen() {
         return this.frozen;
      }

      public boolean canBeNonFrozen() {
         return true;
      }

      public boolean isDuration() {
         return false;
      }

      public boolean isCounter() {
         return false;
      }

      public boolean isUDT() {
         return false;
      }

      public String keyspace() {
         return null;
      }

      public void freeze() throws InvalidRequestException {
         String message = String.format("frozen<> is only allowed on collections, tuples, and user-defined types (got %s)", new Object[]{this});
         throw new InvalidRequestException(message);
      }

      public CQL3Type prepare(String keyspace) {
         KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(keyspace);
         if(ksm == null) {
            throw new ConfigurationException(String.format("Keyspace %s doesn't exist", new Object[]{keyspace}));
         } else {
            return this.prepare(keyspace, ksm.types);
         }
      }

      public abstract CQL3Type prepare(String var1, Types var2) throws InvalidRequestException;

      public CQL3Type prepareInternal(String keyspace, Types udts) throws InvalidRequestException {
         return this.prepare(keyspace, udts);
      }

      public boolean referencesUserType(String name) {
         return false;
      }

      public static CQL3Type.Raw from(CQL3Type type) {
         return new CQL3Type.Raw.RawType(type, null);
      }

      public static CQL3Type.Raw userType(UTName name) {
         return new CQL3Type.Raw.RawUT(name, null);
      }

      public static CQL3Type.Raw map(CQL3Type.Raw t1, CQL3Type.Raw t2) {
         return new CQL3Type.Raw.RawCollection(CollectionType.Kind.MAP, t1, t2, null);
      }

      public static CQL3Type.Raw list(CQL3Type.Raw t) {
         return new CQL3Type.Raw.RawCollection(CollectionType.Kind.LIST, (CQL3Type.Raw)null, t, null);
      }

      public static CQL3Type.Raw set(CQL3Type.Raw t) {
         return new CQL3Type.Raw.RawCollection(CollectionType.Kind.SET, (CQL3Type.Raw)null, t, null);
      }

      public static CQL3Type.Raw tuple(List<CQL3Type.Raw> ts) {
         return new CQL3Type.Raw.RawTuple(ts, null);
      }

      public static CQL3Type.Raw tuple(List<CQL3Type.Raw> ts, boolean frozenDefault) {
         return new CQL3Type.Raw.RawTuple(ts, frozenDefault, null);
      }

      public static CQL3Type.Raw frozen(CQL3Type.Raw t) throws InvalidRequestException {
         t.freeze();
         return t;
      }

      private static class RawTuple extends CQL3Type.Raw {
         private final List<CQL3Type.Raw> types;
         private boolean frozenByDefault;

         private RawTuple(List<CQL3Type.Raw> types, boolean frozenByDefault) {
            this.types = types;
            this.frozenByDefault = frozenByDefault;
         }

         private RawTuple(List<CQL3Type.Raw> types) {
            this.types = types;
            this.frozenByDefault = true;
         }

         public boolean supportsFreezing() {
            return true;
         }

         public void freeze() throws InvalidRequestException {
            this.freezeInner();
            this.frozen = true;
         }

         private void freezeInner() {
            Iterator var1 = this.types.iterator();

            while(var1.hasNext()) {
               CQL3Type.Raw t = (CQL3Type.Raw)var1.next();
               if(t.supportsFreezing()) {
                  t.freeze();
               }
            }

         }

         public CQL3Type prepare(String keyspace, Types udts) throws InvalidRequestException {
            if(!this.frozen && this.frozenByDefault) {
               this.freeze();
            } else {
               this.freezeInner();
            }

            List<AbstractType<?>> ts = new ArrayList(this.types.size());
            Iterator var4 = this.types.iterator();

            while(var4.hasNext()) {
               CQL3Type.Raw t = (CQL3Type.Raw)var4.next();
               if(t.isCounter()) {
                  throw new InvalidRequestException("Counters are not allowed inside tuples");
               }

               ts.add(t.prepare(keyspace, udts).getType());
            }

            return new CQL3Type.Tuple(new TupleType(ts, !this.frozen), null);
         }

         public boolean referencesUserType(String name) {
            return this.types.stream().anyMatch((t) -> {
               return t.referencesUserType(name);
            });
         }

         public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("tuple<");

            for(int i = 0; i < this.types.size(); ++i) {
               if(i > 0) {
                  sb.append(", ");
               }

               sb.append(this.types.get(i));
            }

            sb.append('>');
            return sb.toString();
         }
      }

      private static class RawUT extends CQL3Type.Raw {
         private final UTName name;

         private RawUT(UTName name) {
            this.name = name;
         }

         public String keyspace() {
            return this.name.getKeyspace();
         }

         public void freeze() {
            this.frozen = true;
         }

         public boolean canBeNonFrozen() {
            return true;
         }

         public CQL3Type prepare(String keyspace, Types udts) throws InvalidRequestException {
            if(this.name.hasKeyspace()) {
               if(!keyspace.equals(this.name.getKeyspace())) {
                  throw new InvalidRequestException(String.format("Statement on keyspace %s cannot refer to a user type in keyspace %s; user types can only be used in the keyspace they are defined in", new Object[]{keyspace, this.name.getKeyspace()}));
               }
            } else {
               this.name.setKeyspace(keyspace);
            }

            UserType type = udts.getNullable(this.name.getUserTypeName());
            if(type == null) {
               throw new InvalidRequestException("Unknown type " + this.name);
            } else {
               if(this.frozen) {
                  type = type.freeze();
               }

               return new CQL3Type.UserDefined(this.name.toString(), type, null);
            }
         }

         public boolean referencesUserType(String name) {
            return this.name.getStringTypeName().equals(name);
         }

         public boolean supportsFreezing() {
            return true;
         }

         public boolean isUDT() {
            return true;
         }

         public String toString() {
            return this.frozen?"frozen<" + this.name.toString() + '>':this.name.toString();
         }
      }

      private static class RawCollection extends CQL3Type.Raw {
         private final CollectionType.Kind kind;
         private final CQL3Type.Raw keys;
         private final CQL3Type.Raw values;

         private RawCollection(CollectionType.Kind kind, CQL3Type.Raw keys, CQL3Type.Raw values) {
            this.kind = kind;
            this.keys = keys;
            this.values = values;
         }

         public void freeze() throws InvalidRequestException {
            if(this.keys != null && this.keys.supportsFreezing()) {
               this.keys.freeze();
            }

            if(this.values != null && this.values.supportsFreezing()) {
               this.values.freeze();
            }

            this.frozen = true;
         }

         public boolean supportsFreezing() {
            return true;
         }

         public CQL3Type prepare(String keyspace, Types udts) throws InvalidRequestException {
            return this.prepare(keyspace, udts, false);
         }

         public CQL3Type prepareInternal(String keyspace, Types udts) {
            return this.prepare(keyspace, udts, true);
         }

         public CQL3Type prepare(String keyspace, Types udts, boolean isInternal) throws InvalidRequestException {
            assert this.values != null : "Got null values type for a collection";

            if(!this.frozen && this.values.supportsFreezing() && !this.values.frozen) {
               this.throwNestedNonFrozenError(this.values);
            }

            if(this.values.isCounter() && !isInternal) {
               throw new InvalidRequestException("Counters are not allowed inside collections: " + this);
            } else if(this.values.isDuration() && this.kind == CollectionType.Kind.SET) {
               throw new InvalidRequestException("Durations are not allowed inside sets: " + this);
            } else {
               if(this.keys != null) {
                  if(this.keys.isCounter()) {
                     throw new InvalidRequestException("Counters are not allowed inside collections: " + this);
                  }

                  if(this.keys.isDuration()) {
                     throw new InvalidRequestException("Durations are not allowed as map keys: " + this);
                  }

                  if(!this.frozen && this.keys.supportsFreezing() && !this.keys.frozen) {
                     this.throwNestedNonFrozenError(this.keys);
                  }
               }

               AbstractType<?> valueType = this.values.prepare(keyspace, udts).getType();
               switch(null.$SwitchMap$org$apache$cassandra$db$marshal$CollectionType$Kind[this.kind.ordinal()]) {
               case 1:
                  return new CQL3Type.Collection(ListType.getInstance(valueType, !this.frozen));
               case 2:
                  return new CQL3Type.Collection(SetType.getInstance(valueType, !this.frozen));
               case 3:
                  assert this.keys != null : "Got null keys type for a collection";

                  return new CQL3Type.Collection(MapType.getInstance(this.keys.prepare(keyspace, udts).getType(), valueType, !this.frozen));
               default:
                  throw new AssertionError();
               }
            }
         }

         private void throwNestedNonFrozenError(CQL3Type.Raw innerType) {
            if(innerType instanceof CQL3Type.Raw.RawCollection) {
               throw new InvalidRequestException("Non-frozen collections are not allowed inside collections: " + this);
            } else {
               throw new InvalidRequestException("Non-frozen UDTs are not allowed inside collections: " + this);
            }
         }

         public boolean referencesUserType(String name) {
            return this.keys != null && this.keys.referencesUserType(name) || this.values.referencesUserType(name);
         }

         public String toString() {
            String start = this.frozen?"frozen<":"";
            String end = this.frozen?">":"";
            switch(null.$SwitchMap$org$apache$cassandra$db$marshal$CollectionType$Kind[this.kind.ordinal()]) {
            case 1:
               return start + "list<" + this.values + '>' + end;
            case 2:
               return start + "set<" + this.values + '>' + end;
            case 3:
               return start + "map<" + this.keys + ", " + this.values + '>' + end;
            default:
               throw new AssertionError();
            }
         }
      }

      private static class RawType extends CQL3Type.Raw {
         private final CQL3Type type;

         private RawType(CQL3Type type) {
            this.type = type;
         }

         public CQL3Type prepare(String keyspace, Types udts) throws InvalidRequestException {
            return this.type;
         }

         public boolean supportsFreezing() {
            return false;
         }

         public boolean isCounter() {
            return this.type == CQL3Type.Native.COUNTER;
         }

         public boolean isDuration() {
            return this.type == CQL3Type.Native.DURATION;
         }

         public String toString() {
            return this.type.toString();
         }
      }
   }

   public static class Tuple implements CQL3Type {
      private final TupleType type;

      private Tuple(TupleType type) {
         this.type = type;
      }

      public static CQL3Type.Tuple create(TupleType type) {
         return new CQL3Type.Tuple(type);
      }

      public AbstractType<?> getType() {
         return this.type;
      }

      public String toCQLLiteral(ByteBuffer buffer, ProtocolVersion version) {
         if(buffer == null) {
            return "null";
         } else {
            StringBuilder target = new StringBuilder();
            buffer = buffer.duplicate();
            target.append('(');
            boolean first = true;

            for(int i = 0; i < this.type.size() && buffer.hasRemaining(); ++i) {
               if(buffer.remaining() < 4) {
                  throw new MarshalException(String.format("Not enough bytes to read size of %dth component", new Object[]{Integer.valueOf(i)}));
               }

               int size = buffer.getInt();
               if(first) {
                  first = false;
               } else {
                  target.append(", ");
               }

               if(size < 0) {
                  target.append("null");
               } else {
                  if(buffer.remaining() < size) {
                     throw new MarshalException(String.format("Not enough bytes to read %dth component", new Object[]{Integer.valueOf(i)}));
                  }

                  ByteBuffer field = ByteBufferUtil.readBytes(buffer, size);
                  target.append(this.type.type(i).asCQL3Type().toCQLLiteral(field, version));
               }
            }

            target.append(')');
            return target.toString();
         }
      }

      public final boolean equals(Object o) {
         if(!(o instanceof CQL3Type.Tuple)) {
            return false;
         } else {
            CQL3Type.Tuple that = (CQL3Type.Tuple)o;
            return this.type.equals(that.type);
         }
      }

      public final int hashCode() {
         return this.type.hashCode();
      }

      public String toString() {
         StringBuilder sb = new StringBuilder();
         if(this.type.frozen()) {
            sb.append("frozen<");
         }

         sb.append("tuple<");

         for(int i = 0; i < this.type.size(); ++i) {
            if(i > 0) {
               sb.append(", ");
            }

            sb.append(this.type.type(i).asCQL3Type());
         }

         sb.append(">");
         if(this.type.frozen()) {
            sb.append(">");
         }

         return sb.toString();
      }
   }

   public static class UserDefined implements CQL3Type {
      private final String name;
      private final UserType type;

      private UserDefined(String name, UserType type) {
         this.name = name;
         this.type = type;
      }

      public static CQL3Type.UserDefined create(UserType type) {
         return new CQL3Type.UserDefined((String)UTF8Type.instance.compose(type.name), type);
      }

      public boolean isUDT() {
         return true;
      }

      public AbstractType<?> getType() {
         return this.type;
      }

      public String toCQLLiteral(ByteBuffer buffer, ProtocolVersion version) {
         if(buffer == null) {
            return "null";
         } else {
            StringBuilder target = new StringBuilder();
            buffer = buffer.duplicate();
            target.append('{');

            for(int i = 0; i < this.type.size() && buffer.hasRemaining(); ++i) {
               if(buffer.remaining() < 4) {
                  throw new MarshalException(String.format("Not enough bytes to read size of %dth field %s", new Object[]{Integer.valueOf(i), this.type.fieldName(i)}));
               }

               int size = buffer.getInt();
               if(i > 0) {
                  target.append(", ");
               }

               target.append(ColumnIdentifier.maybeQuote(this.type.fieldNameAsString(i)));
               target.append(": ");
               if(size < 0) {
                  target.append("null");
               } else {
                  if(buffer.remaining() < size) {
                     throw new MarshalException(String.format("Not enough bytes to read %dth field %s", new Object[]{Integer.valueOf(i), this.type.fieldName(i)}));
                  }

                  ByteBuffer field = ByteBufferUtil.readBytes(buffer, size);
                  target.append(this.type.fieldType(i).asCQL3Type().toCQLLiteral(field, version));
               }
            }

            target.append('}');
            return target.toString();
         }
      }

      public final boolean equals(Object o) {
         if(!(o instanceof CQL3Type.UserDefined)) {
            return false;
         } else {
            CQL3Type.UserDefined that = (CQL3Type.UserDefined)o;
            return this.type.equals(that.type);
         }
      }

      public final int hashCode() {
         return this.type.hashCode();
      }

      public String toString() {
         return this.type.isMultiCell()?ColumnIdentifier.maybeQuote(this.name):"frozen<" + ColumnIdentifier.maybeQuote(this.name) + '>';
      }
   }

   public static class Collection implements CQL3Type {
      private final CollectionType type;

      public Collection(CollectionType type) {
         this.type = type;
      }

      public AbstractType<?> getType() {
         return this.type;
      }

      public boolean isCollection() {
         return true;
      }

      public String toCQLLiteral(ByteBuffer buffer, ProtocolVersion version) {
         if(buffer == null) {
            return "null";
         } else {
            StringBuilder target = new StringBuilder();
            buffer = buffer.duplicate();
            int size = CollectionSerializer.readCollectionSize(buffer, version);
            CQL3Type elements;
            switch(null.$SwitchMap$org$apache$cassandra$db$marshal$CollectionType$Kind[this.type.kind.ordinal()]) {
            case 1:
               elements = ((ListType)this.type).getElementsType().asCQL3Type();
               target.append('[');
               generateSetOrListCQLLiteral(buffer, version, target, size, elements);
               target.append(']');
               break;
            case 2:
               elements = ((SetType)this.type).getElementsType().asCQL3Type();
               target.append('{');
               generateSetOrListCQLLiteral(buffer, version, target, size, elements);
               target.append('}');
               break;
            case 3:
               target.append('{');
               this.generateMapCQLLiteral(buffer, version, target, size);
               target.append('}');
            }

            return target.toString();
         }
      }

      private void generateMapCQLLiteral(ByteBuffer buffer, ProtocolVersion version, StringBuilder target, int size) {
         CQL3Type keys = ((MapType)this.type).getKeysType().asCQL3Type();
         CQL3Type values = ((MapType)this.type).getValuesType().asCQL3Type();

         for(int i = 0; i < size; ++i) {
            if(i > 0) {
               target.append(", ");
            }

            ByteBuffer element = CollectionSerializer.readValue(buffer, version);
            target.append(keys.toCQLLiteral(element, version));
            target.append(": ");
            element = CollectionSerializer.readValue(buffer, version);
            target.append(values.toCQLLiteral(element, version));
         }

      }

      private static void generateSetOrListCQLLiteral(ByteBuffer buffer, ProtocolVersion version, StringBuilder target, int size, CQL3Type elements) {
         for(int i = 0; i < size; ++i) {
            if(i > 0) {
               target.append(", ");
            }

            ByteBuffer element = CollectionSerializer.readValue(buffer, version);
            target.append(elements.toCQLLiteral(element, version));
         }

      }

      public final boolean equals(Object o) {
         if(!(o instanceof CQL3Type.Collection)) {
            return false;
         } else {
            CQL3Type.Collection that = (CQL3Type.Collection)o;
            return this.type.equals(that.type);
         }
      }

      public final int hashCode() {
         return this.type.hashCode();
      }

      public String toString() {
         boolean isFrozen = !this.type.isMultiCell();
         StringBuilder sb = new StringBuilder(isFrozen?"frozen<":"");
         switch(null.$SwitchMap$org$apache$cassandra$db$marshal$CollectionType$Kind[this.type.kind.ordinal()]) {
         case 1:
            AbstractType<?> listType = ((ListType)this.type).getElementsType();
            sb.append("list<").append(listType.asCQL3Type());
            break;
         case 2:
            AbstractType<?> setType = ((SetType)this.type).getElementsType();
            sb.append("set<").append(setType.asCQL3Type());
            break;
         case 3:
            AbstractType<?> keysType = ((MapType)this.type).getKeysType();
            AbstractType<?> valuesType = ((MapType)this.type).getValuesType();
            sb.append("map<").append(keysType.asCQL3Type()).append(", ").append(valuesType.asCQL3Type());
            break;
         default:
            throw new AssertionError();
         }

         sb.append('>');
         if(isFrozen) {
            sb.append('>');
         }

         return sb.toString();
      }
   }

   public static class Custom implements CQL3Type {
      private final AbstractType<?> type;

      public Custom(AbstractType<?> type) {
         this.type = type;
      }

      public Custom(String className) throws SyntaxException, ConfigurationException {
         this(TypeParser.parse(className));
      }

      public AbstractType<?> getType() {
         return this.type;
      }

      public String toCQLLiteral(ByteBuffer buffer, ProtocolVersion version) {
         return CQL3Type.Native.BLOB.toCQLLiteral(buffer, version);
      }

      public final boolean equals(Object o) {
         if(!(o instanceof CQL3Type.Custom)) {
            return false;
         } else {
            CQL3Type.Custom that = (CQL3Type.Custom)o;
            return this.type.equals(that.type);
         }
      }

      public final int hashCode() {
         return this.type.hashCode();
      }

      public String toString() {
         return "'" + this.type + '\'';
      }
   }

   public static enum Native implements CQL3Type {
      ASCII(AsciiType.instance),
      BIGINT(LongType.instance),
      BLOB(BytesType.instance),
      BOOLEAN(BooleanType.instance),
      COUNTER(CounterColumnType.instance),
      DATE(SimpleDateType.instance),
      DECIMAL(DecimalType.instance),
      DOUBLE(DoubleType.instance),
      DURATION(DurationType.instance),
      EMPTY(EmptyType.instance),
      FLOAT(FloatType.instance),
      INET(InetAddressType.instance),
      INT(Int32Type.instance),
      SMALLINT(ShortType.instance),
      TEXT(UTF8Type.instance),
      TIME(TimeType.instance),
      TIMESTAMP(TimestampType.instance),
      TIMEUUID(TimeUUIDType.instance),
      TINYINT(ByteType.instance),
      UUID(UUIDType.instance),
      VARCHAR(UTF8Type.instance),
      VARINT(IntegerType.instance);

      private final AbstractType<?> type;

      private Native(AbstractType<?> type) {
         this.type = type;
      }

      public AbstractType<?> getType() {
         return this.type;
      }

      public String toCQLLiteral(ByteBuffer buffer, ProtocolVersion version) {
         return this.type.getSerializer().toCQLLiteral(buffer);
      }

      public String toString() {
         return super.toString().toLowerCase();
      }
   }
}
