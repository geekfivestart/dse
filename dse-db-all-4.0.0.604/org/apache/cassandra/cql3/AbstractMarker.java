package org.apache.cassandra.cql3;

import java.util.List;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.exceptions.InvalidRequestException;

public abstract class AbstractMarker extends Term.NonTerminal {
   protected final int bindIndex;
   protected final ColumnSpecification receiver;

   protected AbstractMarker(int bindIndex, ColumnSpecification receiver) {
      this.bindIndex = bindIndex;
      this.receiver = receiver;
   }

   public void collectMarkerSpecification(VariableSpecifications boundNames) {
      boundNames.add(this.bindIndex, this.receiver);
   }

   public boolean containsBindMarker() {
      return true;
   }

   public void addFunctionsTo(List<Function> functions) {
   }

   public static final class INRaw extends AbstractMarker.Raw {
      public INRaw(int bindIndex) {
         super(bindIndex);
      }

      private static ColumnSpecification makeInReceiver(ColumnSpecification receiver) {
         ColumnIdentifier inName = new ColumnIdentifier("in(" + receiver.name + ")", true);
         return new ColumnSpecification(receiver.ksName, receiver.cfName, inName, ListType.getInstance(receiver.type, false));
      }

      public Lists.Marker prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException {
         return new Lists.Marker(this.bindIndex, makeInReceiver(receiver));
      }
   }

   public abstract static class MultiColumnRaw extends Term.MultiColumnRaw {
      protected final int bindIndex;

      public MultiColumnRaw(int bindIndex) {
         this.bindIndex = bindIndex;
      }

      public Term.NonTerminal prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException {
         throw new AssertionError("MultiColumnRaw..prepare() requires a list of receivers");
      }

      public AssignmentTestable.TestResult testAssignment(String keyspace, ColumnSpecification receiver) {
         return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
      }

      public String getText() {
         return "?";
      }
   }

   public static class Raw extends Term.Raw {
      protected final int bindIndex;

      public Raw(int bindIndex) {
         this.bindIndex = bindIndex;
      }

      public Term.NonTerminal prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException {
         if (receiver.type.isCollection()) {
            switch (((CollectionType)receiver.type).kind) {
               case LIST: {
                  return new Lists.Marker(this.bindIndex, receiver);
               }
               case SET: {
                  return new Sets.Marker(this.bindIndex, receiver);
               }
               case MAP: {
                  return new Maps.Marker(this.bindIndex, receiver);
               }
            }
            throw new AssertionError();
         }
         if (receiver.type.isUDT()) {
            return new UserTypes.Marker(this.bindIndex, receiver);
         }
         return new Constants.Marker(this.bindIndex, receiver);
      }


      public AssignmentTestable.TestResult testAssignment(String keyspace, ColumnSpecification receiver) {
         return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
      }

      public AbstractType<?> getExactTypeIfKnown(String keyspace) {
         return null;
      }

      public String getText() {
         return "?";
      }
   }
}
