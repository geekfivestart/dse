package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import org.apache.cassandra.cql3.Json;
import org.apache.cassandra.cql3.Maps;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.serializers.MapSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteSource;
import org.apache.cassandra.utils.Pair;

public class MapType<K, V> extends CollectionType<Map<K, V>> {
   private static final ConcurrentMap<Pair<AbstractType<?>, AbstractType<?>>, MapType> instances = new ConcurrentHashMap();
   private static final ConcurrentMap<Pair<AbstractType<?>, AbstractType<?>>, MapType> frozenInstances = new ConcurrentHashMap();
   private final AbstractType<K> keys;
   private final AbstractType<V> values;
   private final MapSerializer<K, V> serializer;
   private final boolean isMultiCell;

   public static MapType<?, ?> getInstance(TypeParser parser) throws ConfigurationException, SyntaxException {
      List<AbstractType<?>> l = parser.getTypeParameters();
      if(l.size() != 2) {
         throw new ConfigurationException("MapType takes exactly 2 type parameters");
      } else {
         return getInstance((AbstractType)l.get(0), (AbstractType)l.get(1), true);
      }
   }

   public static <K, V> MapType<K, V> getInstance(AbstractType<K> keys, AbstractType<V> values, boolean isMultiCell) {
      ConcurrentMap<Pair<AbstractType<?>, AbstractType<?>>, MapType> internMap = isMultiCell?instances:frozenInstances;
      Pair<AbstractType<?>, AbstractType<?>> p = Pair.create(keys, values);
      MapType<K, V> t = (MapType)internMap.get(p);
      if(t == null) {
         t = (MapType)internMap.computeIfAbsent(p, (k) -> {
            return new MapType((AbstractType)k.left, (AbstractType)k.right, isMultiCell);
         });
      }

      return t;
   }

   private MapType(AbstractType<K> keys, AbstractType<V> values, boolean isMultiCell) {
      super(AbstractType.ComparisonType.CUSTOM, CollectionType.Kind.MAP);
      this.keys = keys;
      this.values = values;
      this.serializer = MapSerializer.getInstance(keys.getSerializer(), values.getSerializer(), keys);
      this.isMultiCell = isMultiCell;
   }

   public boolean referencesUserType(String userTypeName) {
      return this.getKeysType().referencesUserType(userTypeName) || this.getValuesType().referencesUserType(userTypeName);
   }

   public boolean referencesDuration() {
      return this.getValuesType().referencesDuration();
   }

   public AbstractType<K> getKeysType() {
      return this.keys;
   }

   public AbstractType<V> getValuesType() {
      return this.values;
   }

   public AbstractType<K> nameComparator() {
      return this.keys;
   }

   public AbstractType<V> valueComparator() {
      return this.values;
   }

   public boolean isMultiCell() {
      return this.isMultiCell;
   }

   public AbstractType<?> freeze() {
      return this.isMultiCell?getInstance(this.keys, this.values, false):this;
   }

   public AbstractType<?> freezeNestedMulticellTypes() {
      if(!this.isMultiCell()) {
         return this;
      } else {
         AbstractType<?> keyType = this.keys.isFreezable() && this.keys.isMultiCell()?this.keys.freeze():this.keys.freezeNestedMulticellTypes();
         AbstractType<?> valueType = this.values.isFreezable() && this.values.isMultiCell()?this.values.freeze():this.values.freezeNestedMulticellTypes();
         return getInstance(keyType, valueType, this.isMultiCell);
      }
   }

   public boolean isCompatibleWithFrozen(CollectionType<?> previous) {
      assert !this.isMultiCell;

      MapType tprev = (MapType)previous;
      return this.keys.isCompatibleWith(tprev.keys) && this.values.isCompatibleWith(tprev.values);
   }

   public boolean isValueCompatibleWithFrozen(CollectionType<?> previous) {
      assert !this.isMultiCell;

      MapType tprev = (MapType)previous;
      return this.keys.isCompatibleWith(tprev.keys) && this.values.isValueCompatibleWith(tprev.values);
   }

   public int compareCustom(ByteBuffer o1, ByteBuffer o2) {
      return compareMaps(this.keys, this.values, o1, o2);
   }

   public static int compareMaps(AbstractType<?> keysComparator, AbstractType<?> valuesComparator, ByteBuffer o1, ByteBuffer o2) {
      ByteBuffer bb1 = o1.duplicate();
      ByteBuffer bb2 = o2.duplicate();
      ProtocolVersion protocolVersion = ProtocolVersion.V3;
      int size1 = CollectionSerializer.readCollectionSize(bb1, protocolVersion);
      int size2 = CollectionSerializer.readCollectionSize(bb2, protocolVersion);

      for(int i = 0; i < Math.min(size1, size2); ++i) {
         ByteBuffer k1 = CollectionSerializer.readValue(bb1, protocolVersion);
         ByteBuffer k2 = CollectionSerializer.readValue(bb2, protocolVersion);
         int cmp = keysComparator.compare(k1, k2);
         if(cmp != 0) {
            return cmp;
         }

         ByteBuffer v1 = CollectionSerializer.readValue(bb1, protocolVersion);
         ByteBuffer v2 = CollectionSerializer.readValue(bb2, protocolVersion);
         cmp = valuesComparator.compare(v1, v2);
         if(cmp != 0) {
            return cmp;
         }
      }

      return size1 == size2?0:(size1 < size2?-1:1);
   }

   public ByteSource asByteComparableSource(ByteBuffer b) {
      return asByteSourceMap(this.keys, this.values, b);
   }

   static ByteSource asByteSourceMap(AbstractType<?> keysComparator, AbstractType<?> valuesComparator, ByteBuffer b) {
      if(!b.hasRemaining()) {
         return null;
      } else {
         b = b.duplicate();
         ProtocolVersion protocolVersion = ProtocolVersion.V3;
         int size = CollectionSerializer.readCollectionSize(b, protocolVersion);
         ByteSource[] srcs = new ByteSource[size * 2];

         for(int i = 0; i < size; ++i) {
            ByteBuffer k = CollectionSerializer.readValue(b, protocolVersion);
            srcs[i * 2 + 0] = keysComparator.asByteComparableSource(k);
            ByteBuffer v = CollectionSerializer.readValue(b, protocolVersion);
            srcs[i * 2 + 1] = valuesComparator.asByteComparableSource(v);
         }

         return ByteSource.withTerminator(0, srcs);
      }
   }

   public MapSerializer<K, V> getSerializer() {
      return this.serializer;
   }

   protected int collectionSize(List<ByteBuffer> values) {
      return values.size() / 2;
   }

   public String toString(boolean ignoreFreezing) {
      boolean includeFrozenType = !ignoreFreezing && !this.isMultiCell();
      StringBuilder sb = new StringBuilder();
      if(includeFrozenType) {
         sb.append(FrozenType.class.getName()).append("(");
      }

      sb.append(this.getClass().getName()).append(TypeParser.stringifyTypeParameters(Arrays.asList(new AbstractType[]{this.keys, this.values}), ignoreFreezing || !this.isMultiCell));
      if(includeFrozenType) {
         sb.append(")");
      }

      return sb.toString();
   }

   public List<ByteBuffer> serializedValues(Iterator<Cell> cells) {
      assert this.isMultiCell;

      ArrayList bbs = new ArrayList();

      while(cells.hasNext()) {
         Cell c = (Cell)cells.next();
         bbs.add(c.path().get(0));
         bbs.add(c.value());
      }

      return bbs;
   }

   public Term fromJSONObject(Object parsed) throws MarshalException {
      if(parsed instanceof String) {
         parsed = Json.decodeJson((String)parsed);
      }

      if(!(parsed instanceof Map)) {
         throw new MarshalException(String.format("Expected a map, but got a %s: %s", new Object[]{parsed.getClass().getSimpleName(), parsed}));
      } else {
         Map<Object, Object> map = (Map)parsed;
         Map<Term, Term> terms = new HashMap(map.size());
         Iterator var4 = map.entrySet().iterator();

         while(var4.hasNext()) {
            Entry<Object, Object> entry = (Entry)var4.next();
            if(entry.getKey() == null) {
               throw new MarshalException("Invalid null key in map");
            }

            if(entry.getValue() == null) {
               throw new MarshalException("Invalid null value in map");
            }

            terms.put(this.keys.fromJSONObject(entry.getKey()), this.values.fromJSONObject(entry.getValue()));
         }

         return new Maps.DelayedValue(this.keys, terms);
      }
   }

   public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion) {
      ByteBuffer value = buffer.duplicate();
      StringBuilder sb = new StringBuilder("{");
      int size = CollectionSerializer.readCollectionSize(value, protocolVersion);

      for(int i = 0; i < size; ++i) {
         if(i > 0) {
            sb.append(", ");
         }

         String key = this.keys.toJSONString(CollectionSerializer.readValue(value, protocolVersion), protocolVersion);
         if(key.startsWith("\"")) {
            sb.append(key);
         } else {
            sb.append('"').append(Json.quoteAsJsonString(key)).append('"');
         }

         sb.append(": ");
         sb.append(this.values.toJSONString(CollectionSerializer.readValue(value, protocolVersion), protocolVersion));
      }

      return sb.append("}").toString();
   }
}
