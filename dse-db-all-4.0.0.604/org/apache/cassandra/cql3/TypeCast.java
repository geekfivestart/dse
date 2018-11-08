package org.apache.cassandra.cql3;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;

public class TypeCast extends Term.Raw {
   private final CQL3Type.Raw type;
   private final Term.Raw term;

   public TypeCast(CQL3Type.Raw type, Term.Raw term) {
      this.type = type;
      this.term = term;
   }

   public Term prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException {
      if(!this.term.testAssignment(keyspace, this.castedSpecOf(keyspace, receiver)).isAssignable()) {
         throw new InvalidRequestException(String.format("Cannot cast value %s to type %s", new Object[]{this.term, this.type}));
      } else if(!this.testAssignment(keyspace, receiver).isAssignable()) {
         throw new InvalidRequestException(String.format("Cannot assign value %s to %s of type %s", new Object[]{this, receiver.name, receiver.type.asCQL3Type()}));
      } else {
         return this.term.prepare(keyspace, receiver);
      }
   }

   private ColumnSpecification castedSpecOf(String keyspace, ColumnSpecification receiver) throws InvalidRequestException {
      return new ColumnSpecification(receiver.ksName, receiver.cfName, new ColumnIdentifier(this.toString(), true), this.type.prepare(keyspace).getType());
   }

   public AssignmentTestable.TestResult testAssignment(String keyspace, ColumnSpecification receiver) {
      AbstractType<?> castedType = this.type.prepare(keyspace).getType();
      return receiver.type.equals(castedType)?AssignmentTestable.TestResult.EXACT_MATCH:(receiver.type.isValueCompatibleWith(castedType)?AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE:AssignmentTestable.TestResult.NOT_ASSIGNABLE);
   }

   public AbstractType<?> getExactTypeIfKnown(String keyspace) {
      return this.type.prepare(keyspace).getType();
   }

   public String getText() {
      return "(" + this.type + ")" + this.term;
   }
}
