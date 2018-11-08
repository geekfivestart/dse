package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

public abstract class Sets {
   private Sets() {
   }

   public static ColumnSpecification valueSpecOf(ColumnSpecification column) {
      return new ColumnSpecification(column.ksName, column.cfName, new ColumnIdentifier("value(" + column.name + ")", true), ((SetType)column.type).getElementsType());
   }

   public static AssignmentTestable.TestResult testSetAssignment(ColumnSpecification receiver, List<? extends AssignmentTestable> elements) {
      if(!(receiver.type instanceof SetType)) {
         return receiver.type instanceof MapType && elements.isEmpty()?AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE:AssignmentTestable.TestResult.NOT_ASSIGNABLE;
      } else if(elements.isEmpty()) {
         return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
      } else {
         ColumnSpecification valueSpec = valueSpecOf(receiver);
         return AssignmentTestable.TestResult.testAll(receiver.ksName, valueSpec, elements);
      }
   }

   public static String setToString(List<?> elements) {
      return setToString(elements, Object::toString);
   }

   public static <T> String setToString(Iterable<T> items, Function<T, String> mapper) {
      return (String)StreamSupport.stream(items.spliterator(), false).map((e) -> {
         return (String)mapper.apply(e);
      }).collect(Collectors.joining(", ", "{", "}"));
   }

   public static <T> AbstractType<?> getExactSetTypeIfKnown(List<T> items, Function<T, AbstractType<?>> mapper) {
      AbstractType<?> type = Lists.getElementType(items, mapper);
      return type != null?SetType.getInstance(type, false):null;
   }

   public static class ElementDiscarder extends Operation {
      public ElementDiscarder(ColumnMetadata column, Term k) {
         super(column, k);
      }

      public void execute(DecoratedKey partitionKey, UpdateParameters params) throws InvalidRequestException {
         assert this.column.type.isMultiCell() : "Attempted to delete a single element in a frozen set";

         Term.Terminal elt = this.t.bind(params.options);
         if(elt == null) {
            throw new InvalidRequestException("Invalid null set element");
         } else {
            params.addTombstone(this.column, CellPath.create(elt.get(params.options.getProtocolVersion())));
         }
      }
   }

   public static class Discarder extends Operation {
      public Discarder(ColumnMetadata column, Term t) {
         super(column, t);
      }

      public void execute(DecoratedKey partitionKey, UpdateParameters params) throws InvalidRequestException {
         assert this.column.type.isMultiCell() : "Attempted to remove items from a frozen set";

         Term.Terminal value = this.t.bind(params.options);
         if(value != null && value != Constants.UNSET_VALUE) {
            Set<ByteBuffer> toDiscard = value instanceof Sets.Value?((Sets.Value)value).elements:Collections.singleton(value.get(params.options.getProtocolVersion()));
            Iterator var5 = ((Set)toDiscard).iterator();

            while(var5.hasNext()) {
               ByteBuffer bb = (ByteBuffer)var5.next();
               params.addTombstone(this.column, CellPath.create(bb));
            }

         }
      }
   }

   public static class Adder extends Operation {
      public Adder(ColumnMetadata column, Term t) {
         super(column, t);
      }

      public void execute(DecoratedKey partitionKey, UpdateParameters params) throws InvalidRequestException {
         assert this.column.type.isMultiCell() : "Attempted to add items to a frozen set";

         Term.Terminal value = this.t.bind(params.options);
         if(value != Constants.UNSET_VALUE) {
            doAdd(value, this.column, params);
         }

      }

      static void doAdd(Term.Terminal value, ColumnMetadata column, UpdateParameters params) throws InvalidRequestException {
         if(column.type.isMultiCell()) {
            if(value == null) {
               return;
            }

            Iterator var3 = ((Sets.Value)value).elements.iterator();

            while(var3.hasNext()) {
               ByteBuffer bb = (ByteBuffer)var3.next();
               if(bb != ByteBufferUtil.UNSET_BYTE_BUFFER) {
                  params.addCell(column, CellPath.create(bb), ByteBufferUtil.EMPTY_BYTE_BUFFER);
               }
            }
         } else if(value == null) {
            params.addTombstone(column);
         } else {
            params.addCell(column, value.get(ProtocolVersion.CURRENT));
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

            Sets.Adder.doAdd(value, this.column, params);
         }
      }
   }

   public static class Marker extends AbstractMarker {
      protected Marker(int bindIndex, ColumnSpecification receiver) {
         super(bindIndex, receiver);

         assert receiver.type instanceof SetType;

      }

      public Term.Terminal bind(QueryOptions options) throws InvalidRequestException {
         ByteBuffer value = (ByteBuffer)options.getValues().get(this.bindIndex);
         return (Term.Terminal)(value == null?null:(value == ByteBufferUtil.UNSET_BYTE_BUFFER?Constants.UNSET_VALUE:Sets.Value.fromSerialized(value, (SetType)this.receiver.type, options.getProtocolVersion())));
      }
   }

   public static class DelayedValue extends Term.NonTerminal {
      private final Comparator<ByteBuffer> comparator;
      private final Set<Term> elements;

      public DelayedValue(Comparator<ByteBuffer> comparator, Set<Term> elements) {
         this.comparator = comparator;
         this.elements = elements;
      }

      public boolean containsBindMarker() {
         return false;
      }

      public void collectMarkerSpecification(VariableSpecifications boundNames) {
      }

      public Term.Terminal bind(QueryOptions options) throws InvalidRequestException {
         SortedSet<ByteBuffer> buffers = new TreeSet(this.comparator);
         Iterator var3 = this.elements.iterator();

         while(var3.hasNext()) {
            Term t = (Term)var3.next();
            ByteBuffer bytes = t.bindAndGet(options);
            if(bytes == null) {
               throw new InvalidRequestException("null is not supported inside collections");
            }

            if(bytes == ByteBufferUtil.UNSET_BYTE_BUFFER) {
               return Constants.UNSET_VALUE;
            }

            buffers.add(bytes);
         }

         return new Sets.Value(buffers);
      }

      public void addFunctionsTo(List<org.apache.cassandra.cql3.functions.Function> functions) {
         Terms.addFunctions(this.elements, functions);
      }
   }

   public static class Value extends Term.Terminal {
      public final SortedSet<ByteBuffer> elements;

      public Value(SortedSet<ByteBuffer> elements) {
         this.elements = elements;
      }

      public static Sets.Value fromSerialized(ByteBuffer value, SetType type, ProtocolVersion version) throws InvalidRequestException {
         try {
            Set<?> s = type.getSerializer().deserializeForNativeProtocol(value, version);
            SortedSet<ByteBuffer> elements = new TreeSet(type.getElementsType());
            Iterator var5 = s.iterator();

            while(var5.hasNext()) {
               Object element = var5.next();
               elements.add(type.getElementsType().decompose(element));
            }

            return new Sets.Value(elements);
         } catch (MarshalException var7) {
            throw new InvalidRequestException(var7.getMessage());
         }
      }

      public ByteBuffer get(ProtocolVersion protocolVersion) {
         return CollectionSerializer.pack(this.elements, this.elements.size(), protocolVersion);
      }

      public boolean equals(SetType st, Sets.Value v) {
         if(this.elements.size() != v.elements.size()) {
            return false;
         } else {
            Iterator<ByteBuffer> thisIter = this.elements.iterator();
            Iterator<ByteBuffer> thatIter = v.elements.iterator();
            AbstractType elementsType = st.getElementsType();

            do {
               if(!thisIter.hasNext()) {
                  return true;
               }
            } while(elementsType.compare((ByteBuffer)thisIter.next(), (ByteBuffer)thatIter.next()) == 0);

            return false;
         }
      }
   }

   public static class Literal extends Term.Raw {
      private final List<Term.Raw> elements;

      public Literal(List<Term.Raw> elements) {
         this.elements = elements;
      }

      public Term prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException {
         this.validateAssignableTo(keyspace, receiver);
         if(receiver.type instanceof MapType && this.elements.isEmpty()) {
            return new Maps.Value(Collections.emptyMap());
         } else {
            ColumnSpecification valueSpec = Sets.valueSpecOf(receiver);
            Set<Term> values = new HashSet(this.elements.size());
            boolean allTerminal = true;

            Term t;
            for(Iterator var6 = this.elements.iterator(); var6.hasNext(); values.add(t)) {
               Term.Raw rt = (Term.Raw)var6.next();
               t = rt.prepare(keyspace, valueSpec);
               if(t.containsBindMarker()) {
                  throw new InvalidRequestException(String.format("Invalid set literal for %s: bind variables are not supported inside collection literals", new Object[]{receiver.name}));
               }

               if(t instanceof Term.NonTerminal) {
                  allTerminal = false;
               }
            }

            Sets.DelayedValue value = new Sets.DelayedValue(((SetType)receiver.type).getElementsType(), values);
            return (Term)(allTerminal?value.bind(QueryOptions.DEFAULT):value);
         }
      }

      private void validateAssignableTo(String keyspace, ColumnSpecification receiver) throws InvalidRequestException {
         if(!(receiver.type instanceof SetType)) {
            if(!(receiver.type instanceof MapType) || !this.elements.isEmpty()) {
               throw new InvalidRequestException(String.format("Invalid set literal for %s of type %s", new Object[]{receiver.name, receiver.type.asCQL3Type()}));
            }
         } else {
            ColumnSpecification valueSpec = Sets.valueSpecOf(receiver);
            Iterator var4 = this.elements.iterator();

            Term.Raw rt;
            do {
               if(!var4.hasNext()) {
                  return;
               }

               rt = (Term.Raw)var4.next();
            } while(rt.testAssignment(keyspace, valueSpec).isAssignable());

            throw new InvalidRequestException(String.format("Invalid set literal for %s: value %s is not of type %s", new Object[]{receiver.name, rt, valueSpec.type.asCQL3Type()}));
         }
      }

      public AssignmentTestable.TestResult testAssignment(String keyspace, ColumnSpecification receiver) {
         return Sets.testSetAssignment(receiver, this.elements);
      }

      public AbstractType<?> getExactTypeIfKnown(String keyspace) {
         return Sets.getExactSetTypeIfKnown(this.elements, (p) -> {
            return p.getExactTypeIfKnown(keyspace);
         });
      }

      public String getText() {
         return Sets.setToString(this.elements, Term.Raw::getText);
      }
   }
}
