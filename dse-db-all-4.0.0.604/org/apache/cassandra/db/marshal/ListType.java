package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import org.apache.cassandra.cql3.Json;
import org.apache.cassandra.cql3.Lists;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.serializers.ListSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ListType<T> extends CollectionType<List<T>> {
   private static final Logger logger = LoggerFactory.getLogger(ListType.class);
   private static final ConcurrentMap<AbstractType<?>, ListType> instances = new ConcurrentHashMap();
   private static final ConcurrentMap<AbstractType<?>, ListType> frozenInstances = new ConcurrentHashMap();
   private final AbstractType<T> elements;
   public final ListSerializer<T> serializer;
   private final boolean isMultiCell;

   public static ListType<?> getInstance(TypeParser parser) throws ConfigurationException, SyntaxException {
      List<AbstractType<?>> l = parser.getTypeParameters();
      if(l.size() != 1) {
         throw new ConfigurationException("ListType takes exactly 1 type parameter");
      } else {
         return getInstance((AbstractType)l.get(0), true);
      }
   }

   public static <T> ListType<T> getInstance(AbstractType<T> elements, boolean isMultiCell) {
      ConcurrentMap<AbstractType<?>, ListType> internMap = isMultiCell?instances:frozenInstances;
      ListType<T> t = (ListType)internMap.get(elements);
      if(t == null) {
         t = (ListType)internMap.computeIfAbsent(elements, (k) -> {
            return new ListType(k, isMultiCell);
         });
      }

      return t;
   }

   private ListType(AbstractType<T> elements, boolean isMultiCell) {
      super(AbstractType.ComparisonType.CUSTOM, CollectionType.Kind.LIST);
      this.elements = elements;
      this.serializer = ListSerializer.getInstance(elements.getSerializer());
      this.isMultiCell = isMultiCell;
   }

   public boolean referencesUserType(String userTypeName) {
      return this.getElementsType().referencesUserType(userTypeName);
   }

   public boolean referencesDuration() {
      return this.getElementsType().referencesDuration();
   }

   public AbstractType<T> getElementsType() {
      return this.elements;
   }

   public AbstractType<UUID> nameComparator() {
      return TimeUUIDType.instance;
   }

   public AbstractType<T> valueComparator() {
      return this.elements;
   }

   public ListSerializer<T> getSerializer() {
      return this.serializer;
   }

   public AbstractType<?> freeze() {
      return this.isMultiCell?getInstance(this.elements, false):this;
   }

   public AbstractType<?> freezeNestedMulticellTypes() {
      return !this.isMultiCell()?this:(this.elements.isFreezable() && this.elements.isMultiCell()?getInstance(this.elements.freeze(), this.isMultiCell):getInstance(this.elements.freezeNestedMulticellTypes(), this.isMultiCell));
   }

   public boolean isMultiCell() {
      return this.isMultiCell;
   }

   public boolean isCompatibleWithFrozen(CollectionType<?> previous) {
      assert !this.isMultiCell;

      return this.elements.isCompatibleWith(((ListType)previous).elements);
   }

   public boolean isValueCompatibleWithFrozen(CollectionType<?> previous) {
      assert !this.isMultiCell;

      return this.elements.isValueCompatibleWithInternal(((ListType)previous).elements);
   }

   public int compareCustom(ByteBuffer o1, ByteBuffer o2) {
      return compareListOrSet(this.elements, o1, o2);
   }

   static int compareListOrSet(AbstractType<?> elementsComparator, ByteBuffer o1, ByteBuffer o2) {
      ByteBuffer bb1 = o1.duplicate();
      ByteBuffer bb2 = o2.duplicate();
      int size1 = CollectionSerializer.readCollectionSize(bb1, ProtocolVersion.V3);
      int size2 = CollectionSerializer.readCollectionSize(bb2, ProtocolVersion.V3);

      for(int i = 0; i < Math.min(size1, size2); ++i) {
         ByteBuffer v1 = CollectionSerializer.readValue(bb1, ProtocolVersion.V3);
         ByteBuffer v2 = CollectionSerializer.readValue(bb2, ProtocolVersion.V3);
         int cmp = elementsComparator.compare(v1, v2);
         if(cmp != 0) {
            return cmp;
         }
      }

      return size1 == size2?0:(size1 < size2?-1:1);
   }

   public ByteSource asByteComparableSource(ByteBuffer b) {
      return asByteSourceListOrSet(this.elements, b);
   }

   static ByteSource asByteSourceListOrSet(AbstractType<?> elementsComparator, ByteBuffer b) {
      if(!b.hasRemaining()) {
         return null;
      } else {
         b = b.duplicate();
         int size = CollectionSerializer.readCollectionSize(b, ProtocolVersion.V3);
         ByteSource[] srcs = new ByteSource[size];

         for(int i = 0; i < size; ++i) {
            ByteBuffer v = CollectionSerializer.readValue(b, ProtocolVersion.V3);
            srcs[i] = elementsComparator.asByteComparableSource(v);
         }

         return ByteSource.withTerminator(0, srcs);
      }
   }

   public String toString(boolean ignoreFreezing) {
      boolean includeFrozenType = !ignoreFreezing && !this.isMultiCell();
      StringBuilder sb = new StringBuilder();
      if(includeFrozenType) {
         sb.append(FrozenType.class.getName()).append("(");
      }

      sb.append(this.getClass().getName());
      sb.append(TypeParser.stringifyTypeParameters(Collections.singletonList(this.elements), ignoreFreezing || !this.isMultiCell));
      if(includeFrozenType) {
         sb.append(")");
      }

      return sb.toString();
   }

   public List<ByteBuffer> serializedValues(Iterator<Cell> cells) {
      assert this.isMultiCell;

      ArrayList bbs = new ArrayList();

      while(cells.hasNext()) {
         bbs.add(((Cell)cells.next()).value());
      }

      return bbs;
   }

   public Term fromJSONObject(Object parsed) throws MarshalException {
      if(parsed instanceof String) {
         parsed = Json.decodeJson((String)parsed);
      }

      if(!(parsed instanceof List)) {
         throw new MarshalException(String.format("Expected a list, but got a %s: %s", new Object[]{parsed.getClass().getSimpleName(), parsed}));
      } else {
         List list = (List)parsed;
         List<Term> terms = new ArrayList(list.size());
         Iterator var4 = list.iterator();

         while(var4.hasNext()) {
            Object element = var4.next();
            if(element == null) {
               throw new MarshalException("Invalid null element in list");
            }

            terms.add(this.elements.fromJSONObject(element));
         }

         return new Lists.DelayedValue(terms);
      }
   }

   public static String setOrListToJsonString(ByteBuffer buffer, AbstractType elementsType, ProtocolVersion protocolVersion) {
      ByteBuffer value = buffer.duplicate();
      StringBuilder sb = new StringBuilder("[");
      int size = CollectionSerializer.readCollectionSize(value, protocolVersion);

      for(int i = 0; i < size; ++i) {
         if(i > 0) {
            sb.append(", ");
         }

         sb.append(elementsType.toJSONString(CollectionSerializer.readValue(value, protocolVersion), protocolVersion));
      }

      return sb.append("]").toString();
   }

   public ByteBuffer getSliceFromSerialized(ByteBuffer collection, ByteBuffer from, ByteBuffer to) {
      throw new UnsupportedOperationException();
   }

   public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion) {
      return setOrListToJsonString(buffer, this.elements, protocolVersion);
   }
}
