package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.cql3.Json;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.UserTypes;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.UserTypeSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserType extends TupleType {
   private static final Logger logger = LoggerFactory.getLogger(UserType.class);
   public final String keyspace;
   public final ByteBuffer name;
   private final List<FieldIdentifier> fieldNames;
   private final List<String> stringFieldNames;
   private final UserTypeSerializer serializer;

   public UserType(String keyspace, ByteBuffer name, List<FieldIdentifier> fieldNames, List<AbstractType<?>> fieldTypes, boolean isMultiCell) {
      super(fieldTypes, isMultiCell);

      assert fieldNames.size() == fieldTypes.size();

      this.keyspace = keyspace;
      this.name = name;
      this.fieldNames = fieldNames;
      this.stringFieldNames = new ArrayList(fieldNames.size());
      LinkedHashMap<String, TypeSerializer<?>> fieldSerializers = new LinkedHashMap(fieldTypes.size());
      int i = 0;

      for(int m = fieldNames.size(); i < m; ++i) {
         String stringFieldName = ((FieldIdentifier)fieldNames.get(i)).toString();
         this.stringFieldNames.add(stringFieldName);
         fieldSerializers.put(stringFieldName, ((AbstractType)fieldTypes.get(i)).getSerializer());
      }

      this.serializer = new UserTypeSerializer(fieldSerializers);
   }

   public static UserType getInstance(TypeParser parser) throws ConfigurationException, SyntaxException {
      Pair<Pair<String, ByteBuffer>, List<Pair<ByteBuffer, AbstractType>>> params = parser.getUserTypeParameters();
      String keyspace = (String)((Pair)params.left).left;
      ByteBuffer name = (ByteBuffer)((Pair)params.left).right;
      List<FieldIdentifier> columnNames = new ArrayList(((List)params.right).size());
      List<AbstractType<?>> columnTypes = new ArrayList(((List)params.right).size());
      Iterator var6 = ((List)params.right).iterator();

      while(var6.hasNext()) {
         Pair<ByteBuffer, AbstractType> p = (Pair)var6.next();
         columnNames.add(new FieldIdentifier((ByteBuffer)p.left));
         columnTypes.add(p.right);
      }

      return new UserType(keyspace, name, columnNames, columnTypes, true);
   }

   public boolean isUDT() {
      return true;
   }

   public boolean isTuple() {
      return false;
   }

   public boolean isMultiCell() {
      return this.isMultiCell;
   }

   public String getString(CellPath path, ByteBuffer bytes) {
      assert this.isMultiCell() : "Should only be called when isMultiCell() returns true";

      int f = Integer.parseInt(this.nameComparator().getString(path.get(0)));
      return String.format("{%s:%s}", new Object[]{this.fieldName(f), this.fieldType(f).getString(bytes)});
   }

   public boolean isFreezable() {
      return true;
   }

   public AbstractType<?> fieldType(int i) {
      return this.type(i);
   }

   public ByteBuffer decomposeField(int i, Object value) {
      return this.type(i).decompose(value);
   }

   public <T> T composeField(int i, ByteBuffer value) {
      return this.type(i).compose(value);
   }

   public List<AbstractType<?>> fieldTypes() {
      return this.types;
   }

   public FieldIdentifier fieldName(int i) {
      return (FieldIdentifier)this.fieldNames.get(i);
   }

   public String fieldNameAsString(int i) {
      return (String)this.stringFieldNames.get(i);
   }

   public List<FieldIdentifier> fieldNames() {
      return this.fieldNames;
   }

   public String getNameAsString() {
      return (String)UTF8Type.instance.compose(this.name);
   }

   public int fieldPosition(FieldIdentifier fieldName) {
      return this.fieldNames.indexOf(fieldName);
   }

   public CellPath cellPathForField(FieldIdentifier fieldName) {
      return CellPath.create(ByteBufferUtil.bytes((short)this.fieldPosition(fieldName)));
   }

   public void validateCell(Cell cell) throws MarshalException {
      if(this.isMultiCell) {
         ByteBuffer path = cell.path().get(0);
         this.nameComparator().validate(path);
         Short fieldPosition = (Short)this.nameComparator().getSerializer().deserialize(path);
         this.fieldType(fieldPosition.shortValue()).validate(cell.value());
      } else {
         this.validate(cell.value());
      }

   }

   public Term fromJSONObject(Object parsed) throws MarshalException {
      if(parsed instanceof String) {
         parsed = Json.decodeJson((String)parsed);
      }

      if(!(parsed instanceof Map)) {
         throw new MarshalException(String.format("Expected a map, but got a %s: %s", new Object[]{parsed.getClass().getSimpleName(), parsed}));
      } else {
         Map<String, Object> map = (Map)parsed;
         Json.handleCaseSensitivity(map);
         List<Term> terms = new ArrayList(this.types.size());
         Set keys = map.keySet();

         assert keys.isEmpty() || keys.iterator().next() instanceof String;

         int foundValues = 0;

         Object fieldName;
         for(int i = 0; i < this.types.size(); ++i) {
            fieldName = map.get(this.stringFieldNames.get(i));
            if(fieldName == null) {
               terms.add(Constants.NULL_VALUE);
            } else {
               terms.add(((AbstractType)this.types.get(i)).fromJSONObject(fieldName));
               ++foundValues;
            }
         }

         if(foundValues != map.size()) {
            Iterator var8 = keys.iterator();

            while(var8.hasNext()) {
               fieldName = var8.next();
               if(!this.stringFieldNames.contains(fieldName)) {
                  throw new MarshalException(String.format("Unknown field '%s' in value of user defined type %s", new Object[]{fieldName, this.getNameAsString()}));
               }
            }
         }

         return new UserTypes.DelayedValue(this, terms);
      }
   }

   public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion) {
      ByteBuffer[] buffers = this.split(buffer);
      StringBuilder sb = new StringBuilder("{");

      for(int i = 0; i < this.types.size(); ++i) {
         if(i > 0) {
            sb.append(", ");
         }

         String name = (String)this.stringFieldNames.get(i);
         if(!name.equals(name.toLowerCase(Locale.US))) {
            name = "\"" + name + "\"";
         }

         sb.append('"');
         sb.append(Json.quoteAsJsonString(name));
         sb.append("\": ");
         ByteBuffer valueBuffer = i >= buffers.length?null:buffers[i];
         if(valueBuffer == null) {
            sb.append("null");
         } else {
            sb.append(((AbstractType)this.types.get(i)).toJSONString(valueBuffer, protocolVersion));
         }
      }

      return sb.append("}").toString();
   }

   public UserType freeze() {
      return this.isMultiCell?new UserType(this.keyspace, this.name, this.fieldNames, this.fieldTypes(), false):this;
   }

   public AbstractType<?> freezeNestedMulticellTypes() {
      return !this.isMultiCell()?this:new UserType(this.keyspace, this.name, this.fieldNames, freezeTypes(this.types), this.isMultiCell);
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.keyspace, this.name, this.fieldNames, this.types, Boolean.valueOf(this.isMultiCell)});
   }

   public boolean isValueCompatibleWith(AbstractType<?> previous) {
      if(this == previous) {
         return true;
      } else if(!(previous instanceof UserType)) {
         return false;
      } else {
         UserType other = (UserType)previous;
         if(this.isMultiCell != other.isMultiCell()) {
            return false;
         } else if(!this.keyspace.equals(other.keyspace)) {
            return false;
         } else {
            Iterator<AbstractType<?>> thisTypeIter = this.types.iterator();
            Iterator previousTypeIter = other.types.iterator();

            while(thisTypeIter.hasNext() && previousTypeIter.hasNext()) {
               if(!((AbstractType)thisTypeIter.next()).isCompatibleWith((AbstractType)previousTypeIter.next())) {
                  return false;
               }
            }

            return !previousTypeIter.hasNext();
         }
      }
   }

   public boolean equals(Object o) {
      return o instanceof UserType && this.equals(o, false);
   }

   public boolean equals(Object o, boolean ignoreFreezing) {
      if(!(o instanceof UserType)) {
         return false;
      } else {
         UserType that = (UserType)o;
         if(this.keyspace.equals(that.keyspace) && this.name.equals(that.name) && this.fieldNames.equals(that.fieldNames)) {
            if(!ignoreFreezing && this.isMultiCell != that.isMultiCell) {
               return false;
            } else if(this.types.size() != that.types.size()) {
               return false;
            } else {
               Iterator<AbstractType<?>> otherTypeIter = that.types.iterator();
               Iterator var5 = this.types.iterator();

               AbstractType type;
               do {
                  if(!var5.hasNext()) {
                     return true;
                  }

                  type = (AbstractType)var5.next();
               } while(type.equals(otherTypeIter.next(), ignoreFreezing));

               return false;
            }
         } else {
            return false;
         }
      }
   }

   public CQL3Type asCQL3Type() {
      return CQL3Type.UserDefined.create(this);
   }

   public boolean referencesUserType(String userTypeName) {
      return this.getNameAsString().equals(userTypeName) || this.fieldTypes().stream().anyMatch((f) -> {
         return f.referencesUserType(userTypeName);
      });
   }

   public boolean referencesDuration() {
      return this.fieldTypes().stream().anyMatch((f) -> {
         return f.referencesDuration();
      });
   }

   public String toString() {
      return this.toString(false);
   }

   protected String stringifyTypeParameters(boolean ignoreFreezing) {
      return TypeParser.stringifyUserTypeParameters(this.keyspace, this.name, this.fieldNames, this.types, ignoreFreezing || !this.isMultiCell);
   }

   public String toCQLString() {
      return String.format("%s.%s", new Object[]{ColumnIdentifier.maybeQuote(this.keyspace), ColumnIdentifier.maybeQuote(this.getNameAsString())});
   }

   public TypeSerializer<ByteBuffer> getSerializer() {
      return this.serializer;
   }
}
