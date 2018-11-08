package org.apache.cassandra.cql3.functions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.cassandra.cql3.AbstractMarker;
import org.apache.cassandra.cql3.AssignmentTestable;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.statements.RequestValidations;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.Schema;

public final class FunctionResolver {
   private static final FunctionName TOKEN_FUNCTION_NAME = FunctionName.nativeFunction("token");

   private FunctionResolver() {
   }

   public static ColumnSpecification makeArgSpec(String receiverKs, String receiverCf, Function fun, int i) {
      return new ColumnSpecification(receiverKs, receiverCf, new ColumnIdentifier("arg" + i + '(' + fun.name().toString().toLowerCase() + ')', true), (AbstractType)fun.argTypes().get(i));
   }

   public static Function get(String keyspace, FunctionName name, List<? extends AssignmentTestable> providedArgs, String receiverKs, String receiverCf, AbstractType<?> receiverType) throws InvalidRequestException {
      if(name.equalsNativeFunction(TOKEN_FUNCTION_NAME)) {
         Function tokenFct = new TokenFct(Schema.instance.getTableMetadata(receiverKs, receiverCf));
         int requiredNumberOfArguments = tokenFct.argTypes().size();
         if(requiredNumberOfArguments != providedArgs.size()) {
            throw new InvalidRequestException(String.format("Invalid number of arguments for %s() function: %d required but %d provided", new Object[]{TOKEN_FUNCTION_NAME, Integer.valueOf(requiredNumberOfArguments), Integer.valueOf(providedArgs.size())}));
         } else {
            return tokenFct;
         }
      } else if(name.equalsNativeFunction(ToJsonFct.NAME)) {
         throw new InvalidRequestException("toJson() may only be used within the selection clause of SELECT statements");
      } else if(name.equalsNativeFunction(FromJsonFct.NAME)) {
         if(receiverType == null) {
            throw new InvalidRequestException("fromJson() cannot be used in the selection clause of a SELECT statement");
         } else {
            return FromJsonFct.getInstance(receiverType);
         }
      } else {
         Object candidates;
         if(!name.hasKeyspace()) {
            candidates = new ArrayList();
            ((Collection)candidates).addAll(Schema.instance.getFunctions(name.asNativeFunction()));
            ((Collection)candidates).addAll(Schema.instance.getFunctions(new FunctionName(keyspace, name.name)));
         } else {
            candidates = Schema.instance.getFunctions(name);
         }

         if(((Collection)candidates).isEmpty()) {
            return null;
         } else if(((Collection)candidates).size() == 1) {
            Function fun = (Function)((Collection)candidates).iterator().next();
            validateTypes(keyspace, fun, providedArgs, receiverKs, receiverCf);
            return fun;
         } else {
            List<Function> compatibles = null;
            Iterator var8 = ((Collection)candidates).iterator();

            Function toTest;
            while(var8.hasNext()) {
               toTest = (Function)var8.next();
               if(matchReturnType(toTest, receiverType)) {
                  AssignmentTestable.TestResult r = matchAguments(keyspace, toTest, providedArgs, receiverKs, receiverCf);
                  switch(null.$SwitchMap$org$apache$cassandra$cql3$AssignmentTestable$TestResult[r.ordinal()]) {
                  case 1:
                     return toTest;
                  case 2:
                     if(compatibles == null) {
                        compatibles = new ArrayList();
                     }

                     compatibles.add(toTest);
                  }
               }
            }

            if(compatibles == null) {
               if(OperationFcts.isOperation(name)) {
                  throw RequestValidations.invalidRequest("the '%s' operation is not supported between %s and %s", new Object[]{Character.valueOf(OperationFcts.getOperator(name)), providedArgs.get(0), providedArgs.get(1)});
               } else {
                  throw RequestValidations.invalidRequest("Invalid call to function %s, none of its type signatures match (known type signatures: %s)", new Object[]{name, format((Collection)candidates)});
               }
            } else if(compatibles.size() > 1) {
               if(!OperationFcts.isOperation(name)) {
                  if(OperationFcts.isNegation(name)) {
                     throw RequestValidations.invalidRequest("Ambiguous negation: use type casts to disambiguate");
                  } else {
                     throw RequestValidations.invalidRequest("Ambiguous call to function %s (can be matched by following signatures: %s): use type casts to disambiguate", new Object[]{name, format(compatibles)});
                  }
               } else {
                  if(receiverType != null && !containsMarkers(providedArgs)) {
                     var8 = compatibles.iterator();

                     while(var8.hasNext()) {
                        toTest = (Function)var8.next();
                        List<AbstractType<?>> argTypes = toTest.argTypes();
                        if(receiverType.equals(argTypes.get(0)) && receiverType.equals(argTypes.get(1))) {
                           return toTest;
                        }
                     }
                  }

                  throw RequestValidations.invalidRequest("Ambiguous '%s' operation with args %s and %s: use type casts to disambiguate", new Object[]{Character.valueOf(OperationFcts.getOperator(name)), providedArgs.get(0), providedArgs.get(1)});
               }
            } else {
               return (Function)compatibles.get(0);
            }
         }
      }
   }

   private static boolean containsMarkers(List<? extends AssignmentTestable> args) {
      Stream var10000 = args.stream();
      AbstractMarker.Raw.class.getClass();
      return var10000.anyMatch(AbstractMarker.Raw.class::isInstance);
   }

   private static boolean matchReturnType(Function fun, AbstractType<?> receiverType) {
      return receiverType == null || fun.returnType().testAssignment(receiverType).isAssignable();
   }

   private static void validateTypes(String keyspace, Function fun, List<? extends AssignmentTestable> providedArgs, String receiverKs, String receiverCf) {
      if(providedArgs.size() != fun.argTypes().size()) {
         throw RequestValidations.invalidRequest("Invalid number of arguments in call to function %s: %d required but %d provided", new Object[]{fun.name(), Integer.valueOf(fun.argTypes().size()), Integer.valueOf(providedArgs.size())});
      } else {
         for(int i = 0; i < providedArgs.size(); ++i) {
            AssignmentTestable provided = (AssignmentTestable)providedArgs.get(i);
            if(provided != null) {
               ColumnSpecification expected = makeArgSpec(receiverKs, receiverCf, fun, i);
               if(!provided.testAssignment(keyspace, expected).isAssignable()) {
                  throw RequestValidations.invalidRequest("Type error: %s cannot be passed as argument %d of function %s of type %s", new Object[]{provided, Integer.valueOf(i), fun.name(), expected.type.asCQL3Type()});
               }
            }
         }

      }
   }

   private static AssignmentTestable.TestResult matchAguments(String keyspace, Function fun, List<? extends AssignmentTestable> providedArgs, String receiverKs, String receiverCf) {
      if(providedArgs.size() != fun.argTypes().size()) {
         return AssignmentTestable.TestResult.NOT_ASSIGNABLE;
      } else {
         AssignmentTestable.TestResult res = AssignmentTestable.TestResult.EXACT_MATCH;

         for(int i = 0; i < providedArgs.size(); ++i) {
            AssignmentTestable provided = (AssignmentTestable)providedArgs.get(i);
            if(provided == null) {
               res = AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
            } else {
               ColumnSpecification expected = makeArgSpec(receiverKs, receiverCf, fun, i);
               AssignmentTestable.TestResult argRes = provided.testAssignment(keyspace, expected);
               if(argRes == AssignmentTestable.TestResult.NOT_ASSIGNABLE) {
                  return AssignmentTestable.TestResult.NOT_ASSIGNABLE;
               }

               if(argRes == AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE) {
                  res = AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
               }
            }
         }

         return res;
      }
   }

   private static String format(Collection<Function> funs) {
      return (String)funs.stream().map(Object::toString).collect(Collectors.joining(", "));
   }
}
