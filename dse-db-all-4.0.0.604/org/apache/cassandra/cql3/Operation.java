package org.apache.cassandra.cql3;

import java.util.List;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

public abstract class Operation {
   public final ColumnMetadata column;
   protected final Term t;

   protected Operation(ColumnMetadata column, Term t) {
      assert column != null;

      this.column = column;
      this.t = t;
   }

   public void addFunctionsTo(List<Function> functions) {
      if(this.t != null) {
         this.t.addFunctionsTo(functions);
      }

   }

   public boolean requiresRead() {
      return false;
   }

   public void collectMarkerSpecification(VariableSpecifications boundNames) {
      if(this.t != null) {
         this.t.collectMarkerSpecification(boundNames);
      }

   }

   public abstract void execute(DecoratedKey var1, UpdateParameters var2) throws InvalidRequestException;

   public static class FieldDeletion implements Operation.RawDeletion {
      private final ColumnMetadata.Raw id;
      private final FieldIdentifier field;

      public FieldDeletion(ColumnMetadata.Raw id, FieldIdentifier field) {
         this.id = id;
         this.field = field;
      }

      public ColumnMetadata.Raw affectedColumn() {
         return this.id;
      }

      public Operation prepare(String keyspace, ColumnMetadata receiver, TableMetadata metadata) throws InvalidRequestException {
         if(!receiver.type.isUDT()) {
            throw new InvalidRequestException(String.format("Invalid field deletion operation for non-UDT column %s", new Object[]{receiver.name}));
         } else if(!receiver.type.isMultiCell()) {
            throw new InvalidRequestException(String.format("Frozen UDT column %s does not support field deletions", new Object[]{receiver.name}));
         } else if(((UserType)receiver.type).fieldPosition(this.field) == -1) {
            throw new InvalidRequestException(String.format("UDT column %s does not have a field named %s", new Object[]{receiver.name, this.field}));
         } else {
            return new UserTypes.DeleterByField(receiver, this.field);
         }
      }
   }

   public static class ElementDeletion implements Operation.RawDeletion {
      private final ColumnMetadata.Raw id;
      private final Term.Raw element;

      public ElementDeletion(ColumnMetadata.Raw id, Term.Raw element) {
         this.id = id;
         this.element = element;
      }

      public ColumnMetadata.Raw affectedColumn() {
         return this.id;
      }

      public Operation prepare(String keyspace, ColumnMetadata receiver, TableMetadata metadata) throws InvalidRequestException {
         if(!receiver.type.isCollection()) {
            throw new InvalidRequestException(String.format("Invalid deletion operation for non collection column %s", new Object[]{receiver.name}));
         } else if(!receiver.type.isMultiCell()) {
            throw new InvalidRequestException(String.format("Invalid deletion operation for frozen collection column %s", new Object[]{receiver.name}));
         } else {
            switch(null.$SwitchMap$org$apache$cassandra$db$marshal$CollectionType$Kind[((CollectionType)receiver.type).kind.ordinal()]) {
            case 1:
               Term idx = this.element.prepare(keyspace, Lists.indexSpecOf(receiver));
               return new Lists.DiscarderByIndex(receiver, idx);
            case 2:
               Term elt = this.element.prepare(keyspace, Sets.valueSpecOf(receiver));
               return new Sets.ElementDiscarder(receiver, elt);
            case 3:
               Term key = this.element.prepare(keyspace, Maps.keySpecOf(receiver));
               return new Maps.DiscarderByKey(receiver, key);
            default:
               throw new AssertionError();
            }
         }
      }
   }

   public static class ColumnDeletion implements Operation.RawDeletion {
      private final ColumnMetadata.Raw id;

      public ColumnDeletion(ColumnMetadata.Raw id) {
         this.id = id;
      }

      public ColumnMetadata.Raw affectedColumn() {
         return this.id;
      }

      public Operation prepare(String keyspace, ColumnMetadata receiver, TableMetadata metadata) throws InvalidRequestException {
         return new Constants.Deleter(receiver);
      }
   }

   public static class Prepend implements Operation.RawUpdate {
      private final Term.Raw value;

      public Prepend(Term.Raw value) {
         this.value = value;
      }

      public Operation prepare(TableMetadata metadata, ColumnMetadata receiver) throws InvalidRequestException {
         Term v = this.value.prepare(metadata.keyspace, receiver);
         if(!(receiver.type instanceof ListType)) {
            throw new InvalidRequestException(String.format("Invalid operation (%s) for non list column %s", new Object[]{this.toString(receiver), receiver.name}));
         } else if(!receiver.type.isMultiCell()) {
            throw new InvalidRequestException(String.format("Invalid operation (%s) for frozen list column %s", new Object[]{this.toString(receiver), receiver.name}));
         } else {
            return new Lists.Prepender(receiver, v);
         }
      }

      protected String toString(ColumnSpecification column) {
         return String.format("%s = %s - %s", new Object[]{column.name, this.value, column.name});
      }

      public boolean isCompatibleWith(Operation.RawUpdate other) {
         return !(other instanceof Operation.SetValue);
      }
   }

   public static class Substraction implements Operation.RawUpdate {
      private final Term.Raw value;

      public Substraction(Term.Raw value) {
         this.value = value;
      }

      public Operation prepare(TableMetadata metadata, ColumnMetadata receiver) throws InvalidRequestException {
         if(!(receiver.type instanceof CollectionType)) {
            if(!(receiver.type instanceof CounterColumnType)) {
               throw new InvalidRequestException(String.format("Invalid operation (%s) for non counter column %s", new Object[]{this.toString(receiver), receiver.name}));
            } else {
               return new Constants.Substracter(receiver, this.value.prepare(metadata.keyspace, receiver));
            }
         } else if(!receiver.type.isMultiCell()) {
            throw new InvalidRequestException(String.format("Invalid operation (%s) for frozen collection column %s", new Object[]{this.toString(receiver), receiver.name}));
         } else {
            switch(null.$SwitchMap$org$apache$cassandra$db$marshal$CollectionType$Kind[((CollectionType)receiver.type).kind.ordinal()]) {
            case 1:
               return new Lists.Discarder(receiver, this.value.prepare(metadata.keyspace, receiver));
            case 2:
               return new Sets.Discarder(receiver, this.value.prepare(metadata.keyspace, receiver));
            case 3:
               ColumnSpecification vr = new ColumnSpecification(receiver.ksName, receiver.cfName, receiver.name, SetType.getInstance(((MapType)receiver.type).getKeysType(), false));

               Term term;
               try {
                  term = this.value.prepare(metadata.keyspace, vr);
               } catch (InvalidRequestException var6) {
                  throw new InvalidRequestException(String.format("Value for a map substraction has to be a set, but was: '%s'", new Object[]{this.value}));
               }

               return new Sets.Discarder(receiver, term);
            default:
               throw new AssertionError();
            }
         }
      }

      protected String toString(ColumnSpecification column) {
         return String.format("%s = %s - %s", new Object[]{column.name, column.name, this.value});
      }

      public boolean isCompatibleWith(Operation.RawUpdate other) {
         return !(other instanceof Operation.SetValue);
      }
   }

   public static class Addition implements Operation.RawUpdate {
      private final Term.Raw value;

      public Addition(Term.Raw value) {
         this.value = value;
      }

      public Operation prepare(TableMetadata metadata, ColumnMetadata receiver) throws InvalidRequestException {
         if(!(receiver.type instanceof CollectionType)) {
            if(receiver.type instanceof TupleType) {
               throw new InvalidRequestException(String.format("Invalid operation (%s) for tuple column %s", new Object[]{this.toString(receiver), receiver.name}));
            } else if(!(receiver.type instanceof CounterColumnType)) {
               throw new InvalidRequestException(String.format("Invalid operation (%s) for non counter column %s", new Object[]{this.toString(receiver), receiver.name}));
            } else {
               return new Constants.Adder(receiver, this.value.prepare(metadata.keyspace, receiver));
            }
         } else if(!receiver.type.isMultiCell()) {
            throw new InvalidRequestException(String.format("Invalid operation (%s) for frozen collection column %s", new Object[]{this.toString(receiver), receiver.name}));
         } else {
            switch(null.$SwitchMap$org$apache$cassandra$db$marshal$CollectionType$Kind[((CollectionType)receiver.type).kind.ordinal()]) {
            case 1:
               return new Lists.Appender(receiver, this.value.prepare(metadata.keyspace, receiver));
            case 2:
               return new Sets.Adder(receiver, this.value.prepare(metadata.keyspace, receiver));
            case 3:
               Term term;
               try {
                  term = this.value.prepare(metadata.keyspace, receiver);
               } catch (InvalidRequestException var5) {
                  throw new InvalidRequestException(String.format("Value for a map addition has to be a map, but was: '%s'", new Object[]{this.value}));
               }

               return new Maps.Putter(receiver, term);
            default:
               throw new AssertionError();
            }
         }
      }

      protected String toString(ColumnSpecification column) {
         return String.format("%s = %s + %s", new Object[]{column.name, column.name, this.value});
      }

      public boolean isCompatibleWith(Operation.RawUpdate other) {
         return !(other instanceof Operation.SetValue);
      }
   }

   public static class SetField implements Operation.RawUpdate {
      private final FieldIdentifier field;
      private final Term.Raw value;

      public SetField(FieldIdentifier field, Term.Raw value) {
         this.field = field;
         this.value = value;
      }

      public Operation prepare(TableMetadata metadata, ColumnMetadata receiver) throws InvalidRequestException {
         if(!receiver.type.isUDT()) {
            throw new InvalidRequestException(String.format("Invalid operation (%s) for non-UDT column %s", new Object[]{this.toString(receiver), receiver.name}));
         } else if(!receiver.type.isMultiCell()) {
            throw new InvalidRequestException(String.format("Invalid operation (%s) for frozen UDT column %s", new Object[]{this.toString(receiver), receiver.name}));
         } else {
            int fieldPosition = ((UserType)receiver.type).fieldPosition(this.field);
            if(fieldPosition == -1) {
               throw new InvalidRequestException(String.format("UDT column %s does not have a field named %s", new Object[]{receiver.name, this.field}));
            } else {
               Term val = this.value.prepare(metadata.keyspace, UserTypes.fieldSpecOf(receiver, fieldPosition));
               return new UserTypes.SetterByField(receiver, this.field, val);
            }
         }
      }

      protected String toString(ColumnSpecification column) {
         return String.format("%s.%s = %s", new Object[]{column.name, this.field, this.value});
      }

      public boolean isCompatibleWith(Operation.RawUpdate other) {
         return other instanceof Operation.SetField?!((Operation.SetField)other).field.equals(this.field):!(other instanceof Operation.SetValue);
      }
   }

   public static class SetElement implements Operation.RawUpdate {
      private final Term.Raw selector;
      private final Term.Raw value;

      public SetElement(Term.Raw selector, Term.Raw value) {
         this.selector = selector;
         this.value = value;
      }

      public Operation prepare(TableMetadata metadata, ColumnMetadata receiver) throws InvalidRequestException {
         if(!(receiver.type instanceof CollectionType)) {
            throw new InvalidRequestException(String.format("Invalid operation (%s) for non collection column %s", new Object[]{this.toString(receiver), receiver.name}));
         } else if(!receiver.type.isMultiCell()) {
            throw new InvalidRequestException(String.format("Invalid operation (%s) for frozen collection column %s", new Object[]{this.toString(receiver), receiver.name}));
         } else {
            switch(null.$SwitchMap$org$apache$cassandra$db$marshal$CollectionType$Kind[((CollectionType)receiver.type).kind.ordinal()]) {
            case 1:
               Term idx = this.selector.prepare(metadata.keyspace, Lists.indexSpecOf(receiver));
               Term lval = this.value.prepare(metadata.keyspace, Lists.valueSpecOf(receiver));
               return new Lists.SetterByIndex(receiver, idx, lval);
            case 2:
               throw new InvalidRequestException(String.format("Invalid operation (%s) for set column %s", new Object[]{this.toString(receiver), receiver.name}));
            case 3:
               Term key = this.selector.prepare(metadata.keyspace, Maps.keySpecOf(receiver));
               Term mval = this.value.prepare(metadata.keyspace, Maps.valueSpecOf(receiver));
               return new Maps.SetterByKey(receiver, key, mval);
            default:
               throw new AssertionError();
            }
         }
      }

      protected String toString(ColumnSpecification column) {
         return String.format("%s[%s] = %s", new Object[]{column.name, this.selector, this.value});
      }

      public boolean isCompatibleWith(Operation.RawUpdate other) {
         return !(other instanceof Operation.SetValue);
      }
   }

   public static class SetValue implements Operation.RawUpdate {
      private final Term.Raw value;

      public SetValue(Term.Raw value) {
         this.value = value;
      }

      public Operation prepare(TableMetadata metadata, ColumnMetadata receiver) throws InvalidRequestException {
         Term v = this.value.prepare(metadata.keyspace, receiver);
         if(receiver.type instanceof CounterColumnType) {
            throw new InvalidRequestException(String.format("Cannot set the value of counter column %s (counters can only be incremented/decremented, not set)", new Object[]{receiver.name}));
         } else if(receiver.type.isCollection()) {
            switch(null.$SwitchMap$org$apache$cassandra$db$marshal$CollectionType$Kind[((CollectionType)receiver.type).kind.ordinal()]) {
            case 1:
               return new Lists.Setter(receiver, v);
            case 2:
               return new Sets.Setter(receiver, v);
            case 3:
               return new Maps.Setter(receiver, v);
            default:
               throw new AssertionError();
            }
         } else {
            return (Operation)(receiver.type.isUDT()?new UserTypes.Setter(receiver, v):new Constants.Setter(receiver, v));
         }
      }

      protected String toString(ColumnSpecification column) {
         return String.format("%s = %s", new Object[]{column, this.value});
      }

      public boolean isCompatibleWith(Operation.RawUpdate other) {
         return false;
      }
   }

   public interface RawDeletion {
      ColumnMetadata.Raw affectedColumn();

      Operation prepare(String var1, ColumnMetadata var2, TableMetadata var3) throws InvalidRequestException;
   }

   public interface RawUpdate {
      Operation prepare(TableMetadata var1, ColumnMetadata var2) throws InvalidRequestException;

      boolean isCompatibleWith(Operation.RawUpdate var1);
   }
}
