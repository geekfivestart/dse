package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Json;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.Tuples;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TupleSerializer;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ByteSource;

public class TupleType extends AbstractType<ByteBuffer> implements MultiCellType {
   private static final String COLON = ":";
   private static final Pattern COLON_PAT = Pattern.compile(":");
   private static final String ESCAPED_COLON = "\\\\:";
   private static final Pattern ESCAPED_COLON_PAT = Pattern.compile("\\\\:");
   private static final String AT = "@";
   private static final Pattern AT_PAT = Pattern.compile("@");
   private static final String ESCAPED_AT = "\\\\@";
   private static final Pattern ESCAPED_AT_PAT = Pattern.compile("\\\\@");
   protected final List<AbstractType<?>> types;
   private final TupleSerializer serializer;
   protected final boolean isMultiCell;

   public TupleType(List<AbstractType<?>> types) {
      this(freezeTypes(types), false);
   }

   public TupleType(List<AbstractType<?>> types, boolean isMultiCell) {
      super(AbstractType.ComparisonType.CUSTOM, -1);
      this.types = types;
      this.serializer = new TupleSerializer(fieldSerializers(types));
      this.isMultiCell = isMultiCell;
   }

   private static List<TypeSerializer<?>> fieldSerializers(List<AbstractType<?>> types) {
      int size = types.size();
      List<TypeSerializer<?>> serializers = new ArrayList(size);

      for(int i = 0; i < size; ++i) {
         serializers.add(((AbstractType)types.get(i)).getSerializer());
      }

      return serializers;
   }

   public ShortType nameComparator() {
      return ShortType.instance;
   }

   public boolean isMultiCell() {
      return this.isMultiCell;
   }

   public boolean frozen() {
      return !this.isMultiCell;
   }

   public static TupleType getInstance(TypeParser parser) throws ConfigurationException, SyntaxException {
      List<AbstractType<?>> types = parser.getTypeParameters();

      for(int i = 0; i < types.size(); ++i) {
         types.set(i, ((AbstractType)types.get(i)).freeze());
      }

      return new TupleType(types);
   }

   public ByteBuffer serializeForNativeProtocol(Iterator<Cell> cells, ProtocolVersion protocolVersion) {
      assert this.isMultiCell;

      ByteBuffer[] components = new ByteBuffer[this.size()];

      short fieldPosition;
      Cell cell;
      for(fieldPosition = 0; cells.hasNext(); components[fieldPosition++] = cell.value()) {
         cell = (Cell)cells.next();

         for(short fieldPositionOfCell = ByteBufferUtil.toShort(cell.path().get(0)); fieldPosition < fieldPositionOfCell; components[fieldPosition++] = null) {
            ;
         }
      }

      while(fieldPosition < this.size()) {
         components[fieldPosition++] = null;
      }

      return buildValue(components);
   }

   public AbstractType<?> freeze() {
      return this.isMultiCell?new TupleType(freezeTypes(this.types), false):this;
   }

   public AbstractType<?> freezeNestedMulticellTypes() {
      return !this.isMultiCell()?this:new TupleType(freezeTypes(this.types), true);
   }

   protected static List<AbstractType<?>> freezeTypes(List<AbstractType<?>> types) {
      return (List)types.stream().map((subtype) -> {
         return subtype.isFreezable() && subtype.isMultiCell()?subtype.freeze():subtype;
      }).collect(Collectors.toList());
   }

   public boolean referencesUserType(String name) {
      return this.allTypes().stream().anyMatch((f) -> {
         return f.referencesUserType(name);
      });
   }

   public boolean referencesDuration() {
      return this.allTypes().stream().anyMatch((f) -> {
         return f.referencesDuration();
      });
   }

   public AbstractType<?> type(int i) {
      return (AbstractType)this.types.get(i);
   }

   public int size() {
      return this.types.size();
   }

   public List<AbstractType<?>> allTypes() {
      return this.types;
   }

   public boolean isTuple() {
      return true;
   }

   public int compareCustom(ByteBuffer o1, ByteBuffer o2) {
      ByteBuffer bb1 = o1.duplicate();
      ByteBuffer bb2 = o2.duplicate();

      int size;
      for(size = 0; bb1.remaining() > 0 && bb2.remaining() > 0; ++size) {
         AbstractType<?> comparator = (AbstractType)this.types.get(size);
         int size1 = bb1.getInt();
         int size2 = bb2.getInt();
         if(size1 < 0) {
            if(size2 >= 0) {
               return -1;
            }
         } else {
            if(size2 < 0) {
               return 1;
            }

            ByteBuffer value1 = ByteBufferUtil.readBytes(bb1, size1);
            ByteBuffer value2 = ByteBufferUtil.readBytes(bb2, size2);
            int cmp = comparator.compare(value1, value2);
            if(cmp != 0) {
               return cmp;
            }
         }
      }

      do {
         if(bb1.remaining() <= 0) {
            do {
               if(bb2.remaining() <= 0) {
                  return 0;
               }

               size = bb2.getInt();
            } while(size <= 0);

            return -1;
         }

         size = bb1.getInt();
      } while(size <= 0);

      return 1;
   }

   public ByteSource asByteComparableSource(ByteBuffer byteBuffer) {
      ByteBuffer[] bufs = this.split(byteBuffer);
      ByteSource[] srcs = new ByteSource[this.types.size()];

      for(int i = 0; i < bufs.length; ++i) {
         srcs[i] = ((AbstractType)this.types.get(i)).asByteComparableSource(bufs[i]);
      }

      return ByteSource.of(srcs);
   }

   public ByteBuffer[] split(ByteBuffer value) {
      ByteBuffer[] components = new ByteBuffer[this.size()];
      ByteBuffer input = value.duplicate();

      for(int i = 0; i < this.size(); ++i) {
         if(!input.hasRemaining()) {
            return (ByteBuffer[])Arrays.copyOfRange(components, 0, i);
         }

         int size = input.getInt();
         if(input.remaining() < size) {
            throw new MarshalException(String.format("Not enough bytes to read %dth component", new Object[]{Integer.valueOf(i)}));
         }

         components[i] = size < 0?null:ByteBufferUtil.readBytes(input, size);
      }

      if(input.hasRemaining()) {
         throw new InvalidRequestException(String.format("Expected %s %s for %s column, but got more", new Object[]{Integer.valueOf(this.size()), this.size() == 1?"value":"values", this.asCQL3Type()}));
      } else {
         return components;
      }
   }

   public static ByteBuffer buildValue(ByteBuffer... components) {
      int totalLength = 0;
      ByteBuffer[] var2 = components;
      int var3 = components.length;

      int var4;
      for(var4 = 0; var4 < var3; ++var4) {
         ByteBuffer component = var2[var4];
         totalLength += 4 + (component == null?0:component.remaining());
      }

      ByteBuffer result = ByteBuffer.allocate(totalLength);
      ByteBuffer[] var8 = components;
      var4 = components.length;

      for(int var9 = 0; var9 < var4; ++var9) {
         ByteBuffer component = var8[var9];
         if(component == null) {
            result.putInt(-1);
         } else {
            result.putInt(component.remaining());
            result.put(component.duplicate());
         }
      }

      result.rewind();
      return result;
   }

   public String getString(ByteBuffer value) {
      if(value == null) {
         return "null";
      } else {
         StringBuilder sb = new StringBuilder();
         ByteBuffer input = value.duplicate();

         for(int i = 0; i < this.size(); ++i) {
            if(!input.hasRemaining()) {
               return sb.toString();
            }

            if(i > 0) {
               sb.append(":");
            }

            AbstractType<?> type = this.type(i);
            int size = input.getInt();
            if(size < 0) {
               sb.append("@");
            } else {
               ByteBuffer field = ByteBufferUtil.readBytes(input, size);
               String fld = COLON_PAT.matcher(type.getString(field)).replaceAll("\\\\:");
               fld = AT_PAT.matcher(fld).replaceAll("\\\\@");
               sb.append(fld);
            }
         }

         return sb.toString();
      }
   }

   public ByteBuffer fromString(String source) {
      List<String> fieldStrings = AbstractCompositeType.split(source);
      if(fieldStrings.size() > this.size()) {
         throw new MarshalException(String.format("Invalid tuple literal: too many elements. Type %s expects %d but got %d", new Object[]{this.asCQL3Type(), Integer.valueOf(this.size()), Integer.valueOf(fieldStrings.size())}));
      } else {
         ByteBuffer[] fields = new ByteBuffer[fieldStrings.size()];

         for(int i = 0; i < fieldStrings.size(); ++i) {
            String fieldString = (String)fieldStrings.get(i);
            if(!fieldString.equals("@")) {
               AbstractType<?> type = this.type(i);
               fieldString = ESCAPED_COLON_PAT.matcher(fieldString).replaceAll(":");
               fieldString = ESCAPED_AT_PAT.matcher(fieldString).replaceAll("@");
               fields[i] = type.fromString(fieldString);
            }
         }

         return buildValue(fields);
      }
   }

   public Term fromJSONObject(Object parsed) throws MarshalException {
      if(parsed instanceof String) {
         parsed = Json.decodeJson((String)parsed);
      }

      if(!(parsed instanceof List)) {
         throw new MarshalException(String.format("Expected a list representation of a tuple, but got a %s: %s", new Object[]{parsed.getClass().getSimpleName(), parsed}));
      } else {
         List list = (List)parsed;
         if(list.size() > this.types.size()) {
            throw new MarshalException(String.format("Tuple contains extra items (expected %s): %s", new Object[]{Integer.valueOf(this.types.size()), parsed}));
         } else if(this.types.size() > list.size()) {
            throw new MarshalException(String.format("Tuple is missing items (expected %s): %s", new Object[]{Integer.valueOf(this.types.size()), parsed}));
         } else {
            List<Term> terms = new ArrayList(list.size());
            Iterator<AbstractType<?>> typeIterator = this.types.iterator();
            Iterator var5 = list.iterator();

            while(var5.hasNext()) {
               Object element = var5.next();
               if(element == null) {
                  typeIterator.next();
                  terms.add(Constants.NULL_VALUE);
               } else {
                  terms.add(((AbstractType)typeIterator.next()).fromJSONObject(element));
               }
            }

            return new Tuples.DelayedValue(this, terms);
         }
      }
   }

   public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion) {
      ByteBuffer duplicated = buffer.duplicate();
      StringBuilder sb = new StringBuilder("[");

      for(int i = 0; i < this.types.size(); ++i) {
         if(i > 0) {
            sb.append(", ");
         }

         ByteBuffer value = CollectionSerializer.readValue(duplicated, protocolVersion);
         if(value == null) {
            sb.append("null");
         } else {
            sb.append(((AbstractType)this.types.get(i)).toJSONString(value, protocolVersion));
         }
      }

      return sb.append("]").toString();
   }

   public TypeSerializer<ByteBuffer> getSerializer() {
      return this.serializer;
   }

   public boolean isCompatibleWith(AbstractType<?> previous) {
      if(!(previous instanceof TupleType)) {
         return false;
      } else {
         TupleType tt = (TupleType)previous;
         if(this.size() < tt.size()) {
            return false;
         } else if(this.isMultiCell() != tt.isMultiCell()) {
            return false;
         } else {
            for(int i = 0; i < tt.size(); ++i) {
               AbstractType<?> tprev = tt.type(i);
               AbstractType<?> tnew = this.type(i);
               if(!tnew.isCompatibleWith(tprev)) {
                  return false;
               }
            }

            return true;
         }
      }
   }

   public boolean isValueCompatibleWithInternal(AbstractType<?> otherType) {
      if(!(otherType instanceof TupleType)) {
         return false;
      } else {
         TupleType tt = (TupleType)otherType;
         if(this.size() < tt.size()) {
            return false;
         } else {
            for(int i = 0; i < tt.size(); ++i) {
               AbstractType<?> tprev = tt.type(i);
               AbstractType<?> tnew = this.type(i);
               if(!tnew.isValueCompatibleWith(tprev)) {
                  return false;
               }
            }

            return true;
         }
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{Boolean.valueOf(this.isMultiCell), this.types});
   }

   public boolean equals(Object o) {
      if(!(o instanceof TupleType)) {
         return false;
      } else {
         TupleType that = (TupleType)o;
         return this.isMultiCell == that.isMultiCell && this.types.equals(that.types);
      }
   }

   public CQL3Type asCQL3Type() {
      return CQL3Type.Tuple.create(this);
   }

   public String toString() {
      return this.toString(true);
   }

   public String toString(boolean ignoreFreezing) {
      boolean includeFrozenType = !ignoreFreezing && this.frozen();
      StringBuilder sb = new StringBuilder();
      if(includeFrozenType) {
         sb.append(FrozenType.class.getName()).append("(");
      }

      sb.append(this.getClass().getName());
      sb.append(this.stringifyTypeParameters(true));
      if(includeFrozenType) {
         sb.append(")");
      }

      return sb.toString();
   }

   protected String stringifyTypeParameters(boolean ignoreFreezing) {
      return TypeParser.stringifyTypeParameters(this.types, ignoreFreezing);
   }
}
