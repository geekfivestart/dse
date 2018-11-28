package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.cassandra.cql3.statements.RequestValidations;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Tuples {
   private static final Logger logger = LoggerFactory.getLogger(Tuples.class);

   private Tuples() {
   }

   public static ColumnSpecification componentSpecOf(ColumnSpecification column, int component) {
      return new ColumnSpecification(column.ksName, column.cfName, new ColumnIdentifier(String.format("%s[%d]", new Object[]{column.name, Integer.valueOf(component)}), true), getTupleType(column.type).type(component));
   }

   public static String tupleToString(List<?> elements) {
      return tupleToString(elements, Object::toString);
   }

   public static <T> String tupleToString(Iterable<T> items, Function<T, String> mapper) {
      return (String)StreamSupport.stream(items.spliterator(), false).map((e) -> {
         return (String)mapper.apply(e);
      }).collect(Collectors.joining(", ", "(", ")"));
   }


   public static <T> AbstractType<?> getExactTupleTypeIfKnown(List<T> items, java.util.function.Function<T, AbstractType<?>> mapper) {
      ArrayList types = new ArrayList(items.size());
      for (T item : items) {
         AbstractType<?> type = mapper.apply(item);
         if (type == null) {
            return null;
         }
         types.add(type);
      }
      return new TupleType(types);
   }

   public static void validateTupleAssignableTo(ColumnSpecification receiver, List<? extends AssignmentTestable> elements) {
      if(!checkIfTupleType(receiver.type)) {
         throw RequestValidations.invalidRequest("Invalid tuple type literal for %s of type %s", new Object[]{receiver.name, receiver.type.asCQL3Type()});
      } else {
         TupleType tt = getTupleType(receiver.type);

         for(int i = 0; i < elements.size(); ++i) {
            if(i >= tt.size()) {
               throw RequestValidations.invalidRequest("Invalid tuple literal for %s: too many elements. Type %s expects %d but got %d", new Object[]{receiver.name, tt.asCQL3Type(), Integer.valueOf(tt.size()), Integer.valueOf(elements.size())});
            }

            AssignmentTestable value = (AssignmentTestable)elements.get(i);
            ColumnSpecification spec = componentSpecOf(receiver, i);
            if(!value.testAssignment(receiver.ksName, spec).isAssignable()) {
               throw RequestValidations.invalidRequest("Invalid tuple literal for %s: component %d is not of type %s", new Object[]{receiver.name, Integer.valueOf(i), spec.type.asCQL3Type()});
            }
         }

      }
   }

   public static AssignmentTestable.TestResult testTupleAssignment(ColumnSpecification receiver, List<? extends AssignmentTestable> elements) {
      try {
         validateTupleAssignableTo(receiver, elements);
         return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
      } catch (InvalidRequestException var3) {
         return AssignmentTestable.TestResult.NOT_ASSIGNABLE;
      }
   }

   public static boolean checkIfTupleType(AbstractType<?> tuple) {
      return tuple instanceof TupleType || tuple instanceof ReversedType && ((ReversedType)tuple).baseType instanceof TupleType;
   }

   public static TupleType getTupleType(AbstractType<?> tuple) {
      return tuple instanceof ReversedType?(TupleType)((ReversedType)tuple).baseType:(TupleType)tuple;
   }

   public static class InMarker extends AbstractMarker {
      protected InMarker(int bindIndex, ColumnSpecification receiver) {
         super(bindIndex, receiver);

         assert receiver.type instanceof ListType;

      }

      public Tuples.InValue bind(QueryOptions options) throws InvalidRequestException {
         ByteBuffer value = (ByteBuffer)options.getValues().get(this.bindIndex);
         if(value == ByteBufferUtil.UNSET_BYTE_BUFFER) {
            throw new InvalidRequestException(String.format("Invalid unset value for %s", new Object[]{this.receiver.name}));
         } else {
            return value == null?null:Tuples.InValue.fromSerialized(value, (ListType)this.receiver.type, options);
         }
      }
   }

   public static class Marker extends AbstractMarker {
      public Marker(int bindIndex, ColumnSpecification receiver) {
         super(bindIndex, receiver);
      }

      public Tuples.Value bind(QueryOptions options) throws InvalidRequestException {
         ByteBuffer value = (ByteBuffer)options.getValues().get(this.bindIndex);
         if(value == ByteBufferUtil.UNSET_BYTE_BUFFER) {
            throw new InvalidRequestException(String.format("Invalid unset value for tuple %s", new Object[]{this.receiver.name}));
         } else {
            return value == null?null:Tuples.Value.fromSerialized(value, Tuples.getTupleType(this.receiver.type));
         }
      }
   }

   public static class INRaw extends AbstractMarker.MultiColumnRaw {
      public INRaw(int bindIndex) {
         super(bindIndex);
      }

      private static ColumnSpecification makeInReceiver(List<? extends ColumnSpecification> receivers) throws InvalidRequestException {
         List<AbstractType<?>> types = new ArrayList(receivers.size());
         StringBuilder inName = new StringBuilder("in(");

         for(int i = 0; i < receivers.size(); ++i) {
            ColumnSpecification receiver = (ColumnSpecification)receivers.get(i);
            inName.append(receiver.name);
            if(i < receivers.size() - 1) {
               inName.append(",");
            }

            if(receiver.type.isCollection() && receiver.type.isMultiCell()) {
               throw new InvalidRequestException("Non-frozen collection columns do not support IN relations");
            }

            types.add(receiver.type);
         }

         inName.append(')');
         ColumnIdentifier identifier = new ColumnIdentifier(inName.toString(), true);
         TupleType type = new TupleType(types);
         return new ColumnSpecification(((ColumnSpecification)receivers.get(0)).ksName, ((ColumnSpecification)receivers.get(0)).cfName, identifier, ListType.getInstance(type, false));
      }

      public AbstractType<?> getExactTypeIfKnown(String keyspace) {
         return null;
      }

      public AbstractMarker prepare(String keyspace, List<? extends ColumnSpecification> receivers) throws InvalidRequestException {
         return new Tuples.InMarker(this.bindIndex, makeInReceiver(receivers));
      }
   }

   public static class Raw extends AbstractMarker.MultiColumnRaw {
      public Raw(int bindIndex) {
         super(bindIndex);
      }

      private static ColumnSpecification makeReceiver(List<? extends ColumnSpecification> receivers) {
         List<AbstractType<?>> types = new ArrayList(receivers.size());
         StringBuilder inName = new StringBuilder("(");

         for(int i = 0; i < receivers.size(); ++i) {
            ColumnSpecification receiver = (ColumnSpecification)receivers.get(i);
            inName.append(receiver.name);
            if(i < receivers.size() - 1) {
               inName.append(",");
            }

            types.add(receiver.type);
         }

         inName.append(')');
         ColumnIdentifier identifier = new ColumnIdentifier(inName.toString(), true);
         TupleType type = new TupleType(types);
         return new ColumnSpecification(((ColumnSpecification)receivers.get(0)).ksName, ((ColumnSpecification)receivers.get(0)).cfName, identifier, type);
      }

      public AbstractType<?> getExactTypeIfKnown(String keyspace) {
         return null;
      }

      public AbstractMarker prepare(String keyspace, List<? extends ColumnSpecification> receivers) throws InvalidRequestException {
         return new Tuples.Marker(this.bindIndex, makeReceiver(receivers));
      }
   }

   public static class InValue extends Term.Terminal {
      List<List<ByteBuffer>> elements;

      public InValue(List<List<ByteBuffer>> items) {
         this.elements = items;
      }

      public static Tuples.InValue fromSerialized(ByteBuffer value, ListType type, QueryOptions options) throws InvalidRequestException {
         try {
            List<?> l = type.getSerializer().deserializeForNativeProtocol(value, options.getProtocolVersion());

            assert type.getElementsType() instanceof TupleType;

            TupleType tupleType = Tuples.getTupleType(type.getElementsType());
            List<List<ByteBuffer>> elements = new ArrayList(l.size());
            Iterator var6 = l.iterator();

            while(var6.hasNext()) {
               Object element = var6.next();
               elements.add(Arrays.asList(tupleType.split(type.getElementsType().decompose(element))));
            }

            return new Tuples.InValue(elements);
         } catch (MarshalException var8) {
            throw new InvalidRequestException(var8.getMessage());
         }
      }

      public ByteBuffer get(ProtocolVersion protocolVersion) {
         throw new UnsupportedOperationException();
      }

      public List<List<ByteBuffer>> getSplitValues() {
         return this.elements;
      }
   }

   public static class DelayedValue extends Term.NonTerminal {
      public final TupleType type;
      public final List<Term> elements;

      public DelayedValue(TupleType type, List<Term> elements) {
         this.type = type;
         this.elements = elements;
      }

      public boolean containsBindMarker() {
         Iterator var1 = this.elements.iterator();

         Term term;
         do {
            if(!var1.hasNext()) {
               return false;
            }

            term = (Term)var1.next();
         } while(!term.containsBindMarker());

         return true;
      }

      public void collectMarkerSpecification(VariableSpecifications boundNames) {
         Iterator var2 = this.elements.iterator();

         while(var2.hasNext()) {
            Term term = (Term)var2.next();
            term.collectMarkerSpecification(boundNames);
         }

      }

      private ByteBuffer[] bindInternal(QueryOptions options) throws InvalidRequestException {
         if(this.elements.size() > this.type.size()) {
            throw new InvalidRequestException(String.format("Tuple value contained too many fields (expected %s, got %s)", new Object[]{Integer.valueOf(this.type.size()), Integer.valueOf(this.elements.size())}));
         } else {
            ByteBuffer[] buffers = new ByteBuffer[this.elements.size()];

            for(int i = 0; i < this.elements.size(); ++i) {
               buffers[i] = ((Term)this.elements.get(i)).bindAndGet(options);
               if(buffers[i] == ByteBufferUtil.UNSET_BYTE_BUFFER) {
                  throw new InvalidRequestException(String.format("Invalid unset value for tuple field number %d", new Object[]{Integer.valueOf(i)}));
               }
            }

            return buffers;
         }
      }

      public Tuples.Value bind(QueryOptions options) throws InvalidRequestException {
         return new Tuples.Value(this.bindInternal(options));
      }

      public ByteBuffer bindAndGet(QueryOptions options) throws InvalidRequestException {
         return TupleType.buildValue(this.bindInternal(options));
      }

      public String toString() {
         return Tuples.tupleToString(this.elements);
      }

      public void addFunctionsTo(List<org.apache.cassandra.cql3.functions.Function> functions) {
         Terms.addFunctions(this.elements, functions);
      }
   }

   public static class Value extends Term.MultiItemTerminal {
      public final ByteBuffer[] elements;

      public Value(ByteBuffer[] elements) {
         this.elements = elements;
      }

      public static Tuples.Value fromSerialized(ByteBuffer bytes, TupleType type) {
         ByteBuffer[] values = type.split(bytes);
         if(values.length > type.size()) {
            throw new InvalidRequestException(String.format("Tuple value contained too many fields (expected %s, got %s)", new Object[]{Integer.valueOf(type.size()), Integer.valueOf(values.length)}));
         } else {
            return new Tuples.Value(type.split(bytes));
         }
      }

      public ByteBuffer get(ProtocolVersion protocolVersion) {
         return TupleType.buildValue(this.elements);
      }

      public List<ByteBuffer> getElements() {
         return Arrays.asList(this.elements);
      }
   }

   public static class Literal extends Term.MultiColumnRaw {
      private final List<Term.Raw> elements;

      public Literal(List<Term.Raw> elements) {
         this.elements = elements;
      }

      public Term prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException {
         if(this.elements.size() == 1 && !Tuples.checkIfTupleType(receiver.type)) {
            return ((Term.Raw)this.elements.get(0)).prepare(keyspace, receiver);
         } else {
            Tuples.validateTupleAssignableTo(receiver, this.elements);
            List<Term> values = new ArrayList(this.elements.size());
            boolean allTerminal = true;

            for(int i = 0; i < this.elements.size(); ++i) {
               Term value = ((Term.Raw)this.elements.get(i)).prepare(keyspace, Tuples.componentSpecOf(receiver, i));
               if(value instanceof Term.NonTerminal) {
                  allTerminal = false;
               }

               values.add(value);
            }

            Tuples.DelayedValue value = new Tuples.DelayedValue(Tuples.getTupleType(receiver.type), values);
            return (Term)(allTerminal?value.bind(QueryOptions.DEFAULT):value);
         }
      }

      public Term prepare(String keyspace, List<? extends ColumnSpecification> receivers) throws InvalidRequestException {
         if(this.elements.size() != receivers.size()) {
            throw new InvalidRequestException(String.format("Expected %d elements in value tuple, but got %d: %s", new Object[]{Integer.valueOf(receivers.size()), Integer.valueOf(this.elements.size()), this}));
         } else {
            List<Term> values = new ArrayList(this.elements.size());
            List<AbstractType<?>> types = new ArrayList(this.elements.size());
            boolean allTerminal = true;

            for(int i = 0; i < this.elements.size(); ++i) {
               Term t = ((Term.Raw)this.elements.get(i)).prepare(keyspace, (ColumnSpecification)receivers.get(i));
               if(t instanceof Term.NonTerminal) {
                  allTerminal = false;
               }

               values.add(t);
               types.add(((ColumnSpecification)receivers.get(i)).type);
            }

            Tuples.DelayedValue value = new Tuples.DelayedValue(new TupleType(types), values);
            return (Term)(allTerminal?value.bind(QueryOptions.DEFAULT):value);
         }
      }

      public AssignmentTestable.TestResult testAssignment(String keyspace, ColumnSpecification receiver) {
         return this.elements.size() == 1 && !Tuples.checkIfTupleType(receiver.type)?((Term.Raw)this.elements.get(0)).testAssignment(keyspace, receiver):Tuples.testTupleAssignment(receiver, this.elements);
      }

      public AbstractType<?> getExactTypeIfKnown(String keyspace) {
         List<AbstractType<?>> types = new ArrayList(this.elements.size());
         Iterator var3 = this.elements.iterator();

         while(var3.hasNext()) {
            Term.Raw term = (Term.Raw)var3.next();
            AbstractType<?> type = term.getExactTypeIfKnown(keyspace);
            if(type == null) {
               return null;
            }

            types.add(type);
         }

         return new TupleType(types);
      }

      public String getText() {
         return Tuples.tupleToString(this.elements, Term.Raw::getText);
      }
   }
}
