package org.apache.cassandra.cql3.functions;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.cassandra.cql3.AssignmentTestable;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.commons.lang3.text.StrBuilder;

public abstract class AbstractFunction implements Function {
   protected final FunctionName name;
   protected final List<AbstractType<?>> argTypes;
   protected final AbstractType<?> returnType;

   protected AbstractFunction(FunctionName name, List<AbstractType<?>> argTypes, AbstractType<?> returnType) {
      this.name = name;
      this.argTypes = argTypes;
      this.returnType = returnType;
   }

   public FunctionName name() {
      return this.name;
   }

   public List<AbstractType<?>> argTypes() {
      return this.argTypes;
   }

   public AbstractType<?> returnType() {
      return this.returnType;
   }

   public List<String> argumentsList() {
      return (List)this.argTypes().stream().map(AbstractType::asCQL3Type).map(Object::toString).collect(Collectors.toList());
   }

   public boolean equals(Object o) {
      if(!(o instanceof AbstractFunction)) {
         return false;
      } else {
         AbstractFunction that = (AbstractFunction)o;
         return Objects.equals(this.name, that.name) && Objects.equals(this.argTypes, that.argTypes) && Objects.equals(this.returnType, that.returnType);
      }
   }

   public void addFunctionsTo(List<Function> functions) {
      functions.add(this);
   }

   public boolean hasReferenceTo(Function function) {
      return false;
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.name, this.argTypes, this.returnType});
   }

   public final AssignmentTestable.TestResult testAssignment(String keyspace, ColumnSpecification receiver) {
      AbstractType<?> returnType = this.returnType();
      if(receiver.type.isFreezable() && !receiver.type.isMultiCell()) {
         returnType = returnType.freeze();
      }

      return receiver.type.equals(returnType)?AssignmentTestable.TestResult.EXACT_MATCH:(receiver.type.isValueCompatibleWith(returnType)?AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE:AssignmentTestable.TestResult.NOT_ASSIGNABLE);
   }

   public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(this.name).append(" : (");

      for(int i = 0; i < this.argTypes.size(); ++i) {
         if(i > 0) {
            sb.append(", ");
         }

         sb.append(((AbstractType)this.argTypes.get(i)).asCQL3Type());
      }

      sb.append(") -> ").append(this.returnType.asCQL3Type());
      return sb.toString();
   }

   public String columnName(List<String> columnNames) {
      return (new StrBuilder(this.name().toString())).append('(').appendWithSeparators(columnNames, ", ").append(')').toString();
   }
}
