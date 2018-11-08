package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import org.apache.cassandra.cql3.Json;
import org.apache.cassandra.cql3.Sets;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.SetSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteSource;

public class SetType<T> extends CollectionType<Set<T>> {
   private static final ConcurrentMap<AbstractType<?>, SetType> instances = new ConcurrentHashMap();
   private static final ConcurrentMap<AbstractType<?>, SetType> frozenInstances = new ConcurrentHashMap();
   private final AbstractType<T> elements;
   private final SetSerializer<T> serializer;
   private final boolean isMultiCell;

   public static SetType<?> getInstance(TypeParser parser) throws ConfigurationException, SyntaxException {
      List<AbstractType<?>> l = parser.getTypeParameters();
      if(l.size() != 1) {
         throw new ConfigurationException("SetType takes exactly 1 type parameter");
      } else {
         return getInstance((AbstractType)l.get(0), true);
      }
   }

   public static <T> SetType<T> getInstance(AbstractType<T> elements, boolean isMultiCell) {
      ConcurrentMap<AbstractType<?>, SetType> internMap = isMultiCell?instances:frozenInstances;
      SetType<T> t = (SetType)internMap.get(elements);
      if(t == null) {
         t = (SetType)internMap.computeIfAbsent(elements, (k) -> {
            return new SetType(k, isMultiCell);
         });
      }

      return t;
   }

   public SetType(AbstractType<T> elements, boolean isMultiCell) {
      super(AbstractType.ComparisonType.CUSTOM, CollectionType.Kind.SET);
      this.elements = elements;
      this.serializer = SetSerializer.getInstance(elements.getSerializer(), elements);
      this.isMultiCell = isMultiCell;
   }

   public boolean referencesUserType(String userTypeName) {
      return this.getElementsType().referencesUserType(userTypeName);
   }

   public AbstractType<T> getElementsType() {
      return this.elements;
   }

   public AbstractType<T> nameComparator() {
      return this.elements;
   }

   public AbstractType<?> valueComparator() {
      return EmptyType.instance;
   }

   public boolean isMultiCell() {
      return this.isMultiCell;
   }

   public AbstractType<?> freeze() {
      return this.isMultiCell?getInstance(this.elements, false):this;
   }

   public AbstractType<?> freezeNestedMulticellTypes() {
      return !this.isMultiCell()?this:(this.elements.isFreezable() && this.elements.isMultiCell()?getInstance(this.elements.freeze(), this.isMultiCell):getInstance(this.elements.freezeNestedMulticellTypes(), this.isMultiCell));
   }

   public boolean isCompatibleWithFrozen(CollectionType<?> previous) {
      assert !this.isMultiCell;

      return this.elements.isCompatibleWith(((SetType)previous).elements);
   }

   public boolean isValueCompatibleWithFrozen(CollectionType<?> previous) {
      return this.isCompatibleWithFrozen(previous);
   }

   public int compareCustom(ByteBuffer o1, ByteBuffer o2) {
      return ListType.compareListOrSet(this.elements, o1, o2);
   }

   public ByteSource asByteComparableSource(ByteBuffer b) {
      return ListType.asByteSourceListOrSet(this.elements, b);
   }

   public SetSerializer<T> getSerializer() {
      return this.serializer;
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
      ArrayList bbs = new ArrayList();

      while(cells.hasNext()) {
         bbs.add(((Cell)cells.next()).path().get(0));
      }

      return bbs;
   }

   public Term fromJSONObject(Object parsed) throws MarshalException {
      if(parsed instanceof String) {
         parsed = Json.decodeJson((String)parsed);
      }

      if(!(parsed instanceof List)) {
         throw new MarshalException(String.format("Expected a list (representing a set), but got a %s: %s", new Object[]{parsed.getClass().getSimpleName(), parsed}));
      } else {
         List list = (List)parsed;
         Set<Term> terms = new HashSet(list.size());
         Iterator var4 = list.iterator();

         while(var4.hasNext()) {
            Object element = var4.next();
            if(element == null) {
               throw new MarshalException("Invalid null element in set");
            }

            terms.add(this.elements.fromJSONObject(element));
         }

         return new Sets.DelayedValue(this.elements, terms);
      }
   }

   public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion) {
      return ListType.setOrListToJsonString(buffer, this.elements, protocolVersion);
   }
}
