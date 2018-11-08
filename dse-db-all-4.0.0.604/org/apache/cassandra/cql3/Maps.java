package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

public abstract class Maps {
   private Maps() {
   }

   public static ColumnSpecification keySpecOf(ColumnSpecification column) {
      return new ColumnSpecification(column.ksName, column.cfName, new ColumnIdentifier("key(" + column.name + ")", true), ((MapType)column.type).getKeysType());
   }

   public static ColumnSpecification valueSpecOf(ColumnSpecification column) {
      return new ColumnSpecification(column.ksName, column.cfName, new ColumnIdentifier("value(" + column.name + ")", true), ((MapType)column.type).getValuesType());
   }

   public static <T extends AssignmentTestable> AssignmentTestable.TestResult testMapAssignment(ColumnSpecification receiver, List<Pair<T, T>> entries) {
      ColumnSpecification keySpec = keySpecOf(receiver);
      ColumnSpecification valueSpec = valueSpecOf(receiver);
      AssignmentTestable.TestResult res = AssignmentTestable.TestResult.EXACT_MATCH;
      Iterator var5 = entries.iterator();

      while(var5.hasNext()) {
         Pair<T, T> entry = (Pair)var5.next();
         AssignmentTestable.TestResult t1 = ((AssignmentTestable)entry.left).testAssignment(receiver.ksName, keySpec);
         AssignmentTestable.TestResult t2 = ((AssignmentTestable)entry.right).testAssignment(receiver.ksName, valueSpec);
         if(t1 == AssignmentTestable.TestResult.NOT_ASSIGNABLE || t2 == AssignmentTestable.TestResult.NOT_ASSIGNABLE) {
            return AssignmentTestable.TestResult.NOT_ASSIGNABLE;
         }

         if(t1 != AssignmentTestable.TestResult.EXACT_MATCH || t2 != AssignmentTestable.TestResult.EXACT_MATCH) {
            res = AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
         }
      }

      return res;
   }

   public static <T> String mapToString(List<Pair<T, T>> entries) {
      return mapToString(entries, Object::toString);
   }

   public static <T> String mapToString(List<Pair<T, T>> items, Function<T, String> mapper) {
      return (String)items.stream().map((p) -> {
         return String.format("%s: %s", new Object[]{mapper.apply(p.left), mapper.apply(p.right)});
      }).collect(Collectors.joining(", ", "{", "}"));
   }

   public static <T> AbstractType<?> getExactMapTypeIfKnown(List<Pair<T, T>> entries, Function<T, AbstractType<?>> mapper) {
      AbstractType<?> keyType = null;
      AbstractType<?> valueType = null;

      Pair entry;
      for(Iterator var4 = entries.iterator(); var4.hasNext(); valueType = selectType(valueType, (AbstractType)mapper.apply(entry.right))) {
         entry = (Pair)var4.next();
         keyType = selectType(keyType, (AbstractType)mapper.apply(entry.left));
      }

      return keyType != null && valueType != null?MapType.getInstance(keyType, valueType, false):null;
   }

   private static <T> AbstractType<?> selectType(AbstractType<?> type, AbstractType<?> otherType) {
      if(otherType == null) {
         return type;
      } else if(type != null && !otherType.isCompatibleWith(type)) {
         if(type.isCompatibleWith(otherType)) {
            return type;
         } else {
            throw new InvalidRequestException("Invalid collection literal: all selectors must have the same CQL type inside collection literals");
         }
      } else {
         return otherType;
      }
   }

   public static class DiscarderByKey extends Operation {
      public DiscarderByKey(ColumnMetadata column, Term k) {
         super(column, k);
      }

      public void execute(DecoratedKey partitionKey, UpdateParameters params) throws InvalidRequestException {
         assert this.column.type.isMultiCell() : "Attempted to delete a single key in a frozen map";

         Term.Terminal key = this.t.bind(params.options);
         if(key == null) {
            throw new InvalidRequestException("Invalid null map key");
         } else if(key == Constants.UNSET_VALUE) {
            throw new InvalidRequestException("Invalid unset map key");
         } else {
            params.addTombstone(this.column, CellPath.create(key.get(params.options.getProtocolVersion())));
         }
      }
   }

   public static class Putter extends Operation {
      public Putter(ColumnMetadata column, Term t) {
         super(column, t);
      }

      public void execute(DecoratedKey partitionKey, UpdateParameters params) throws InvalidRequestException {
         assert this.column.type.isMultiCell() : "Attempted to add items to a frozen map";

         Term.Terminal value = this.t.bind(params.options);
         if(value != Constants.UNSET_VALUE) {
            doPut(value, this.column, params);
         }

      }

      static void doPut(Term.Terminal value, ColumnMetadata column, UpdateParameters params) throws InvalidRequestException {
         if(column.type.isMultiCell()) {
            if(value == null) {
               return;
            }

            Map<ByteBuffer, ByteBuffer> elements = ((Maps.Value)value).map;
            Iterator var4 = elements.entrySet().iterator();

            while(var4.hasNext()) {
               Entry<ByteBuffer, ByteBuffer> entry = (Entry)var4.next();
               params.addCell(column, CellPath.create((ByteBuffer)entry.getKey()), (ByteBuffer)entry.getValue());
            }
         } else if(value == null) {
            params.addTombstone(column);
         } else {
            params.addCell(column, value.get(ProtocolVersion.CURRENT));
         }

      }
   }

   public static class SetterByKey extends Operation {
      private final Term k;

      public SetterByKey(ColumnMetadata column, Term k, Term t) {
         super(column, t);
         this.k = k;
      }

      public void collectMarkerSpecification(VariableSpecifications boundNames) {
         super.collectMarkerSpecification(boundNames);
         this.k.collectMarkerSpecification(boundNames);
      }

      public void execute(DecoratedKey partitionKey, UpdateParameters params) throws InvalidRequestException {
         assert this.column.type.isMultiCell() : "Attempted to set a value for a single key on a frozen map";

         ByteBuffer key = this.k.bindAndGet(params.options);
         ByteBuffer value = this.t.bindAndGet(params.options);
         if(key == null) {
            throw new InvalidRequestException("Invalid null map key");
         } else if(key == ByteBufferUtil.UNSET_BYTE_BUFFER) {
            throw new InvalidRequestException("Invalid unset map key");
         } else {
            CellPath path = CellPath.create(key);
            if(value == null) {
               params.addTombstone(this.column, path);
            } else if(value != ByteBufferUtil.UNSET_BYTE_BUFFER) {
               params.addCell(this.column, path, value);
            }

         }
      }
   }

   public static class Setter extends Operation {
      public Setter(ColumnMetadata column, Term t) {
         super(column, t);
      }

      public void execute(DecoratedKey partitionKey, UpdateParameters params) throws InvalidRequestException {
         Term.Terminal value = this.t.bind(params.options);
         if(value != Constants.UNSET_VALUE) {
            if(this.column.type.isMultiCell()) {
               params.setComplexDeletionTimeForOverwrite(this.column);
            }

            Maps.Putter.doPut(value, this.column, params);
         }
      }
   }

   public static class Marker extends AbstractMarker {
      protected Marker(int bindIndex, ColumnSpecification receiver) {
         super(bindIndex, receiver);

         assert receiver.type instanceof MapType;

      }

      public Term.Terminal bind(QueryOptions options) throws InvalidRequestException {
         ByteBuffer value = (ByteBuffer)options.getValues().get(this.bindIndex);
         return (Term.Terminal)(value == null?null:(value == ByteBufferUtil.UNSET_BYTE_BUFFER?Constants.UNSET_VALUE:Maps.Value.fromSerialized(value, (MapType)this.receiver.type, options.getProtocolVersion())));
      }
   }

   public static class DelayedValue extends Term.NonTerminal {
      private final Comparator<ByteBuffer> comparator;
      private final Map<Term, Term> elements;

      public DelayedValue(Comparator<ByteBuffer> comparator, Map<Term, Term> elements) {
         this.comparator = comparator;
         this.elements = elements;
      }

      public boolean containsBindMarker() {
         return false;
      }

      public void collectMarkerSpecification(VariableSpecifications boundNames) {
      }

      public Term.Terminal bind(QueryOptions options) throws InvalidRequestException {
         Map<ByteBuffer, ByteBuffer> buffers = new TreeMap(this.comparator);
         Iterator var3 = this.elements.entrySet().iterator();

         while(var3.hasNext()) {
            Entry<Term, Term> entry = (Entry)var3.next();
            ByteBuffer keyBytes = ((Term)entry.getKey()).bindAndGet(options);
            if(keyBytes == null) {
               throw new InvalidRequestException("null is not supported inside collections");
            }

            if(keyBytes == ByteBufferUtil.UNSET_BYTE_BUFFER) {
               throw new InvalidRequestException("unset value is not supported for map keys");
            }

            ByteBuffer valueBytes = ((Term)entry.getValue()).bindAndGet(options);
            if(valueBytes == null) {
               throw new InvalidRequestException("null is not supported inside collections");
            }

            if(valueBytes == ByteBufferUtil.UNSET_BYTE_BUFFER) {
               return Constants.UNSET_VALUE;
            }

            buffers.put(keyBytes, valueBytes);
         }

         return new Maps.Value(buffers);
      }

      public void addFunctionsTo(List<org.apache.cassandra.cql3.functions.Function> functions) {
         Terms.addFunctions(this.elements.keySet(), functions);
         Terms.addFunctions(this.elements.values(), functions);
      }
   }

   public static class Value extends Term.Terminal {
      public final Map<ByteBuffer, ByteBuffer> map;

      public Value(Map<ByteBuffer, ByteBuffer> map) {
         this.map = map;
      }

      public static Maps.Value fromSerialized(ByteBuffer value, MapType type, ProtocolVersion version) throws InvalidRequestException {
         try {
            Map<?, ?> m = type.getSerializer().deserializeForNativeProtocol(value, version);
            Map<ByteBuffer, ByteBuffer> map = new LinkedHashMap(m.size());
            Iterator var5 = m.entrySet().iterator();

            while(var5.hasNext()) {
               Entry<?, ?> entry = (Entry)var5.next();
               map.put(type.getKeysType().decompose(entry.getKey()), type.getValuesType().decompose(entry.getValue()));
            }

            return new Maps.Value(map);
         } catch (MarshalException var7) {
            throw new InvalidRequestException(var7.getMessage());
         }
      }

      public ByteBuffer get(ProtocolVersion protocolVersion) {
         List<ByteBuffer> buffers = new ArrayList(2 * this.map.size());
         Iterator var3 = this.map.entrySet().iterator();

         while(var3.hasNext()) {
            Entry<ByteBuffer, ByteBuffer> entry = (Entry)var3.next();
            buffers.add(entry.getKey());
            buffers.add(entry.getValue());
         }

         return CollectionSerializer.pack(buffers, this.map.size(), protocolVersion);
      }

      public boolean equals(MapType mt, Maps.Value v) {
         if(this.map.size() != v.map.size()) {
            return false;
         } else {
            Iterator<Entry<ByteBuffer, ByteBuffer>> thisIter = this.map.entrySet().iterator();
            Iterator thatIter = v.map.entrySet().iterator();

            Entry thisEntry;
            Entry thatEntry;
            do {
               if(!thisIter.hasNext()) {
                  return true;
               }

               thisEntry = (Entry)thisIter.next();
               thatEntry = (Entry)thatIter.next();
            } while(mt.getKeysType().compare((ByteBuffer)thisEntry.getKey(), (ByteBuffer)thatEntry.getKey()) == 0 && mt.getValuesType().compare((ByteBuffer)thisEntry.getValue(), (ByteBuffer)thatEntry.getValue()) == 0);

            return false;
         }
      }
   }

   public static class Literal extends Term.Raw {
      public final List<Pair<Term.Raw, Term.Raw>> entries;

      public Literal(List<Pair<Term.Raw, Term.Raw>> entries) {
         this.entries = entries;
      }

      public Term prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException {
         this.validateAssignableTo(keyspace, receiver);
         ColumnSpecification keySpec = Maps.keySpecOf(receiver);
         ColumnSpecification valueSpec = Maps.valueSpecOf(receiver);
         Map<Term, Term> values = new HashMap(this.entries.size());
         boolean allTerminal = true;

         Term k;
         Term v;
         for(Iterator var7 = this.entries.iterator(); var7.hasNext(); values.put(k, v)) {
            Pair<Term.Raw, Term.Raw> entry = (Pair)var7.next();
            k = ((Term.Raw)entry.left).prepare(keyspace, keySpec);
            v = ((Term.Raw)entry.right).prepare(keyspace, valueSpec);
            if(k.containsBindMarker() || v.containsBindMarker()) {
               throw new InvalidRequestException(String.format("Invalid map literal for %s: bind variables are not supported inside collection literals", new Object[]{receiver.name}));
            }

            if(k instanceof Term.NonTerminal || v instanceof Term.NonTerminal) {
               allTerminal = false;
            }
         }

         Maps.DelayedValue value = new Maps.DelayedValue(((MapType)receiver.type).getKeysType(), values);
         return (Term)(allTerminal?value.bind(QueryOptions.DEFAULT):value);
      }

      private void validateAssignableTo(String keyspace, ColumnSpecification receiver) throws InvalidRequestException {
         if(!(receiver.type instanceof MapType)) {
            throw new InvalidRequestException(String.format("Invalid map literal for %s of type %s", new Object[]{receiver.name, receiver.type.asCQL3Type()}));
         } else {
            ColumnSpecification keySpec = Maps.keySpecOf(receiver);
            ColumnSpecification valueSpec = Maps.valueSpecOf(receiver);
            Iterator var5 = this.entries.iterator();

            Pair entry;
            do {
               if(!var5.hasNext()) {
                  return;
               }

               entry = (Pair)var5.next();
               if(!((Term.Raw)entry.left).testAssignment(keyspace, keySpec).isAssignable()) {
                  throw new InvalidRequestException(String.format("Invalid map literal for %s: key %s is not of type %s", new Object[]{receiver.name, entry.left, keySpec.type.asCQL3Type()}));
               }
            } while(((Term.Raw)entry.right).testAssignment(keyspace, valueSpec).isAssignable());

            throw new InvalidRequestException(String.format("Invalid map literal for %s: value %s is not of type %s", new Object[]{receiver.name, entry.right, valueSpec.type.asCQL3Type()}));
         }
      }

      public AssignmentTestable.TestResult testAssignment(String keyspace, ColumnSpecification receiver) {
         return Maps.testMapAssignment(receiver, this.entries);
      }

      public AbstractType<?> getExactTypeIfKnown(String keyspace) {
         return Maps.getExactMapTypeIfKnown(this.entries, (p) -> {
            return p.getExactTypeIfKnown(keyspace);
         });
      }

      public String getText() {
         return Maps.mapToString(this.entries, Term.Raw::getText);
      }
   }
}
