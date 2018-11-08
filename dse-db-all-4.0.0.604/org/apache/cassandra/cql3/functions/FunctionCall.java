package org.apache.cassandra.cql3.functions;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.cassandra.cql3.AssignmentTestable;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Lists;
import org.apache.cassandra.cql3.Maps;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.Sets;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.Terms;
import org.apache.cassandra.cql3.UserTypes;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.statements.RequestValidations;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

public class FunctionCall extends Term.NonTerminal {
   private final ScalarFunction fun;
   private final List<Term> terms;

   private FunctionCall(ScalarFunction fun, List<Term> terms) {
      this.fun = fun;
      this.terms = terms;
   }

   public void addFunctionsTo(List<Function> functions) {
      Terms.addFunctions(this.terms, functions);
      this.fun.addFunctionsTo(functions);
   }

   public void collectMarkerSpecification(VariableSpecifications boundNames) {
      Iterator var2 = this.terms.iterator();

      while(var2.hasNext()) {
         Term t = (Term)var2.next();
         t.collectMarkerSpecification(boundNames);
      }

   }

   public Term.Terminal bind(QueryOptions options) throws InvalidRequestException {
      return makeTerminal(this.fun, this.bindAndGet(options), options.getProtocolVersion());
   }

   public ByteBuffer bindAndGet(QueryOptions options) throws InvalidRequestException {
      Arguments arguments = this.fun.newArguments(options.getProtocolVersion());
      int i = 0;

      for(int m = this.terms.size(); i < m; ++i) {
         Term t = (Term)this.terms.get(i);
         ByteBuffer argument = t.bindAndGet(options);
         RequestValidations.checkBindValueSet(argument, "Invalid unset value for argument in call to function %s", this.fun.name().name);
         arguments.set(i, argument);
      }

      return executeInternal(this.fun, arguments);
   }

   private static ByteBuffer executeInternal(ScalarFunction fun, Arguments arguments) throws InvalidRequestException {
      ByteBuffer result = fun.execute(arguments);

      try {
         if(result != null) {
            fun.returnType().validate(result);
         }

         return result;
      } catch (MarshalException var4) {
         throw new RuntimeException(String.format("Return of function %s (%s) is not a valid value for its declared return type %s", new Object[]{fun, ByteBufferUtil.bytesToHex(result), fun.returnType().asCQL3Type()}), var4);
      }
   }

   public boolean containsBindMarker() {
      Iterator var1 = this.terms.iterator();

      Term t;
      do {
         if(!var1.hasNext()) {
            return false;
         }

         t = (Term)var1.next();
      } while(!t.containsBindMarker());

      return true;
   }

   private static Term.Terminal makeTerminal(Function fun, ByteBuffer result, ProtocolVersion version) throws InvalidRequestException {
      if(result == null) {
         return null;
      } else {
         if(fun.returnType().isCollection()) {
            switch(null.$SwitchMap$org$apache$cassandra$db$marshal$CollectionType$Kind[((CollectionType)fun.returnType()).kind.ordinal()]) {
            case 1:
               return Lists.Value.fromSerialized(result, (ListType)fun.returnType(), version);
            case 2:
               return Sets.Value.fromSerialized(result, (SetType)fun.returnType(), version);
            case 3:
               return Maps.Value.fromSerialized(result, (MapType)fun.returnType(), version);
            }
         } else if(fun.returnType().isUDT()) {
            return UserTypes.Value.fromSerialized(result, (UserType)fun.returnType());
         }

         return new Constants.Value(result);
      }
   }

   public static class Raw extends Term.Raw {
      private FunctionName name;
      private final List<Term.Raw> terms;

      public Raw(FunctionName name, List<Term.Raw> terms) {
         this.name = name;
         this.terms = terms;
      }

      public static FunctionCall.Raw newOperation(char operator, Term.Raw left, Term.Raw right) {
         FunctionName name = OperationFcts.getFunctionNameFromOperator(operator);
         return new FunctionCall.Raw(name, Arrays.asList(new Term.Raw[]{left, right}));
      }

      public static FunctionCall.Raw newNegation(Term.Raw raw) {
         FunctionName name = FunctionName.nativeFunction("_negate");
         return new FunctionCall.Raw(name, Collections.singletonList(raw));
      }

      public Term prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException {
         Function fun = FunctionResolver.get(keyspace, this.name, this.terms, receiver.ksName, receiver.cfName, receiver.type);
         if(fun == null) {
            throw RequestValidations.invalidRequest("Unknown function %s called", new Object[]{this.name});
         } else if(fun.isAggregate()) {
            throw RequestValidations.invalidRequest("Aggregation function are not supported in the where clause");
         } else {
            ScalarFunction scalarFun = (ScalarFunction)fun;
            if(!scalarFun.testAssignment(keyspace, receiver).isAssignable()) {
               if(OperationFcts.isOperation(this.name)) {
                  throw RequestValidations.invalidRequest("Type error: cannot assign result of operation %s (type %s) to %s (type %s)", new Object[]{Character.valueOf(OperationFcts.getOperator(scalarFun.name())), scalarFun.returnType().asCQL3Type(), receiver.name, receiver.type.asCQL3Type()});
               } else {
                  throw RequestValidations.invalidRequest("Type error: cannot assign result of function %s (type %s) to %s (type %s)", new Object[]{scalarFun.name(), scalarFun.returnType().asCQL3Type(), receiver.name, receiver.type.asCQL3Type()});
               }
            } else if(fun.argTypes().size() != this.terms.size()) {
               throw RequestValidations.invalidRequest("Incorrect number of arguments specified for function %s (expected %d, found %d)", new Object[]{fun, Integer.valueOf(fun.argTypes().size()), Integer.valueOf(this.terms.size())});
            } else {
               List<Term> parameters = new ArrayList(this.terms.size());

               for(int i = 0; i < this.terms.size(); ++i) {
                  Term t = ((Term.Raw)this.terms.get(i)).prepare(keyspace, FunctionResolver.makeArgSpec(receiver.ksName, receiver.cfName, scalarFun, i));
                  parameters.add(t);
               }

               return new FunctionCall(scalarFun, parameters, null);
            }
         }
      }

      public AssignmentTestable.TestResult testAssignment(String keyspace, ColumnSpecification receiver) {
         try {
            Function fun = FunctionResolver.get(keyspace, this.name, this.terms, receiver.ksName, receiver.cfName, receiver.type);
            return fun != null && fun.name().equals(FromJsonFct.NAME)?AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE:(fun != null && receiver.type.equals(fun.returnType())?AssignmentTestable.TestResult.EXACT_MATCH:(fun != null && !receiver.type.isValueCompatibleWith(fun.returnType())?AssignmentTestable.TestResult.NOT_ASSIGNABLE:AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE));
         } catch (InvalidRequestException var4) {
            return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
         }
      }

      public AbstractType<?> getExactTypeIfKnown(String keyspace) {
         throw new UnsupportedOperationException();
      }

      public String getText() {
         return this.name.toCQLString() + (String)this.terms.stream().map(Term.Raw::getText).collect(Collectors.joining(", ", "(", ")"));
      }
   }
}
