package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

public abstract class UserTypes {
   private UserTypes() {
   }

   public static ColumnSpecification fieldSpecOf(ColumnSpecification column, int field) {
      UserType ut = (UserType)column.type;
      return new ColumnSpecification(column.ksName, column.cfName, new ColumnIdentifier(column.name + "." + ut.fieldName(field), true), ut.fieldType(field));
   }

   public static <T extends AssignmentTestable> void validateUserTypeAssignableTo(ColumnSpecification receiver, Map<FieldIdentifier, T> entries) {
      if(!receiver.type.isUDT()) {
         throw new InvalidRequestException(String.format("Invalid user type literal for %s of type %s", new Object[]{receiver.name, receiver.type.asCQL3Type()}));
      } else {
         UserType ut = (UserType)receiver.type;

         for(int i = 0; i < ut.size(); ++i) {
            FieldIdentifier field = ut.fieldName(i);
            AssignmentTestable value = (AssignmentTestable)entries.get(field);
            if(value != null) {
               ColumnSpecification fieldSpec = fieldSpecOf(receiver, i);
               if(!value.testAssignment(receiver.ksName, fieldSpec).isAssignable()) {
                  throw new InvalidRequestException(String.format("Invalid user type literal for %s: field %s is not of type %s", new Object[]{receiver.name, field, fieldSpec.type.asCQL3Type()}));
               }
            }
         }

      }
   }

   public static <T extends AssignmentTestable> AssignmentTestable.TestResult testUserTypeAssignment(ColumnSpecification receiver, Map<FieldIdentifier, T> entries) {
      try {
         validateUserTypeAssignableTo(receiver, entries);
         return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
      } catch (InvalidRequestException var3) {
         return AssignmentTestable.TestResult.NOT_ASSIGNABLE;
      }
   }

   public static <T> String userTypeToString(Map<FieldIdentifier, T> items) {
      return userTypeToString(items, Object::toString);
   }

   public static <T> String userTypeToString(Map<FieldIdentifier, T> items, Function<T, String> mapper) {
      return (String)items.entrySet().stream().map((p) -> {
         return String.format("%s: %s", new Object[]{p.getKey(), mapper.apply(p.getValue())});
      }).collect(Collectors.joining(", ", "{", "}"));
   }

   public static class DeleterByField extends Operation {
      private final FieldIdentifier field;

      public DeleterByField(ColumnMetadata column, FieldIdentifier field) {
         super(column, (Term)null);
         this.field = field;
      }

      public void execute(DecoratedKey partitionKey, UpdateParameters params) throws InvalidRequestException {
         assert this.column.type.isMultiCell() : "Attempted to delete a single field from a frozen UDT";

         CellPath fieldPath = ((UserType)this.column.type).cellPathForField(this.field);
         params.addTombstone(this.column, fieldPath);
      }
   }

   public static class SetterByField extends Operation {
      private final FieldIdentifier field;

      public SetterByField(ColumnMetadata column, FieldIdentifier field, Term t) {
         super(column, t);
         this.field = field;
      }

      public void execute(DecoratedKey partitionKey, UpdateParameters params) throws InvalidRequestException {
         assert this.column.type.isMultiCell() : "Attempted to set an individual field on a frozen UDT";

         Term.Terminal value = this.t.bind(params.options);
         if(value != Constants.UNSET_VALUE) {
            CellPath fieldPath = ((UserType)this.column.type).cellPathForField(this.field);
            if(value == null) {
               params.addTombstone(this.column, fieldPath);
            } else {
               params.addCell(this.column, fieldPath, value.get(params.options.getProtocolVersion()));
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
            UserTypes.Value userTypeValue = (UserTypes.Value)value;
            if(this.column.type.isMultiCell()) {
               params.setComplexDeletionTimeForOverwrite(this.column);
               if(value == null) {
                  return;
               }

               Iterator<FieldIdentifier> fieldNameIter = userTypeValue.type.fieldNames().iterator();
               ByteBuffer[] var6 = userTypeValue.elements;
               int var7 = var6.length;

               for(int var8 = 0; var8 < var7; ++var8) {
                  ByteBuffer buffer = var6[var8];

                  assert fieldNameIter.hasNext();

                  FieldIdentifier fieldName = (FieldIdentifier)fieldNameIter.next();
                  if(buffer != null) {
                     CellPath fieldPath = userTypeValue.type.cellPathForField(fieldName);
                     params.addCell(this.column, fieldPath, buffer);
                  }
               }
            } else if(value == null) {
               params.addTombstone(this.column);
            } else {
               params.addCell(this.column, value.get(params.options.getProtocolVersion()));
            }

         }
      }
   }

   public static class Marker extends AbstractMarker {
      protected Marker(int bindIndex, ColumnSpecification receiver) {
         super(bindIndex, receiver);

         assert receiver.type.isUDT();

      }

      public Term.Terminal bind(QueryOptions options) throws InvalidRequestException {
         ByteBuffer value = (ByteBuffer)options.getValues().get(this.bindIndex);
         return (Term.Terminal)(value == null?null:(value == ByteBufferUtil.UNSET_BYTE_BUFFER?Constants.UNSET_VALUE:UserTypes.Value.fromSerialized(value, (UserType)this.receiver.type)));
      }
   }

   public static class DelayedValue extends Term.NonTerminal {
      private final UserType type;
      private final List<Term> values;

      public DelayedValue(UserType type, List<Term> values) {
         this.type = type;
         this.values = values;
      }

      public void addFunctionsTo(List<org.apache.cassandra.cql3.functions.Function> functions) {
         Terms.addFunctions(this.values, functions);
      }

      public boolean containsBindMarker() {
         Iterator var1 = this.values.iterator();

         Term t;
         do {
            if(!var1.hasNext()) {
               return false;
            }

            t = (Term)var1.next();
         } while(!t.containsBindMarker());

         return true;
      }

      public void collectMarkerSpecification(VariableSpecifications boundNames) {
         for(int i = 0; i < this.type.size(); ++i) {
            ((Term)this.values.get(i)).collectMarkerSpecification(boundNames);
         }

      }

      private ByteBuffer[] bindInternal(QueryOptions options) throws InvalidRequestException {
         if(this.values.size() > this.type.size()) {
            throw new InvalidRequestException(String.format("UDT value contained too many fields (expected %s, got %s)", new Object[]{Integer.valueOf(this.type.size()), Integer.valueOf(this.values.size())}));
         } else {
            ByteBuffer[] buffers = new ByteBuffer[this.values.size()];

            for(int i = 0; i < this.type.size(); ++i) {
               buffers[i] = ((Term)this.values.get(i)).bindAndGet(options);
               if(!this.type.isMultiCell() && buffers[i] == ByteBufferUtil.UNSET_BYTE_BUFFER) {
                  throw new InvalidRequestException(String.format("Invalid unset value for field '%s' of user defined type %s", new Object[]{this.type.fieldNameAsString(i), this.type.getNameAsString()}));
               }
            }

            return buffers;
         }
      }

      public UserTypes.Value bind(QueryOptions options) throws InvalidRequestException {
         return new UserTypes.Value(this.type, this.bindInternal(options));
      }

      public ByteBuffer bindAndGet(QueryOptions options) throws InvalidRequestException {
         return UserType.buildValue(this.bindInternal(options));
      }
   }

   public static class Value extends Term.MultiItemTerminal {
      private final UserType type;
      public final ByteBuffer[] elements;

      public Value(UserType type, ByteBuffer[] elements) {
         this.type = type;
         this.elements = elements;
      }

      public static UserTypes.Value fromSerialized(ByteBuffer bytes, UserType type) {
         type.validate(bytes);
         return new UserTypes.Value(type, type.split(bytes));
      }

      public ByteBuffer get(ProtocolVersion protocolVersion) {
         return TupleType.buildValue(this.elements);
      }

      public boolean equals(UserType userType, UserTypes.Value v) {
         if(this.elements.length != v.elements.length) {
            return false;
         } else {
            for(int i = 0; i < this.elements.length; ++i) {
               if(userType.fieldType(i).compare(this.elements[i], v.elements[i]) != 0) {
                  return false;
               }
            }

            return true;
         }
      }

      public List<ByteBuffer> getElements() {
         return Arrays.asList(this.elements);
      }
   }

   public static class Literal extends Term.Raw {
      public final Map<FieldIdentifier, Term.Raw> entries;

      public Literal(Map<FieldIdentifier, Term.Raw> entries) {
         this.entries = entries;
      }

      public Term prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException {
         this.validateAssignableTo(keyspace, receiver);
         UserType ut = (UserType)receiver.type;
         boolean allTerminal = true;
         List<Term> values = new ArrayList(this.entries.size());
         int foundValues = 0;

         FieldIdentifier id;
         for(int i = 0; i < ut.size(); ++i) {
            id = ut.fieldName(i);
            Term.Raw raw = (Term.Raw)this.entries.get(id);
            if(raw == null) {
               raw = Constants.NULL_LITERAL;
            } else {
               ++foundValues;
            }

            Term value = ((Term.Raw)raw).prepare(keyspace, UserTypes.fieldSpecOf(receiver, i));
            if(value instanceof Term.NonTerminal) {
               allTerminal = false;
            }

            values.add(value);
         }

         if(foundValues != this.entries.size()) {
            Iterator var11 = this.entries.keySet().iterator();

            while(var11.hasNext()) {
               id = (FieldIdentifier)var11.next();
               if(!ut.fieldNames().contains(id)) {
                  throw new InvalidRequestException(String.format("Unknown field '%s' in value of user defined type %s", new Object[]{id, ut.getNameAsString()}));
               }
            }
         }

         UserTypes.DelayedValue value = new UserTypes.DelayedValue((UserType)receiver.type, values);
         return (Term)(allTerminal?value.bind(QueryOptions.DEFAULT):value);
      }

      private void validateAssignableTo(String keyspace, ColumnSpecification receiver) throws InvalidRequestException {
         if(!receiver.type.isUDT()) {
            throw new InvalidRequestException(String.format("Invalid user type literal for %s of type %s", new Object[]{receiver.name, receiver.type.asCQL3Type()}));
         } else {
            UserType ut = (UserType)receiver.type;

            for(int i = 0; i < ut.size(); ++i) {
               FieldIdentifier field = ut.fieldName(i);
               Term.Raw value = (Term.Raw)this.entries.get(field);
               if(value != null) {
                  ColumnSpecification fieldSpec = UserTypes.fieldSpecOf(receiver, i);
                  if(!value.testAssignment(keyspace, fieldSpec).isAssignable()) {
                     throw new InvalidRequestException(String.format("Invalid user type literal for %s: field %s is not of type %s", new Object[]{receiver.name, field, fieldSpec.type.asCQL3Type()}));
                  }
               }
            }

         }
      }

      public AssignmentTestable.TestResult testAssignment(String keyspace, ColumnSpecification receiver) {
         return UserTypes.testUserTypeAssignment(receiver, this.entries);
      }

      public AbstractType<?> getExactTypeIfKnown(String keyspace) {
         return null;
      }

      public String getText() {
         return UserTypes.userTypeToString(this.entries, Term.Raw::getText);
      }
   }
}
