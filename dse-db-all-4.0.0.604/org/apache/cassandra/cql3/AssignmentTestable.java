package org.apache.cassandra.cql3;

import java.util.Collection;
import java.util.Iterator;

public interface AssignmentTestable {
   AssignmentTestable.TestResult testAssignment(String var1, ColumnSpecification var2);

   public static enum TestResult {
      EXACT_MATCH,
      WEAKLY_ASSIGNABLE,
      NOT_ASSIGNABLE;

      private TestResult() {
      }

      public boolean isAssignable() {
         return this != NOT_ASSIGNABLE;
      }

      public boolean isExactMatch() {
         return this == EXACT_MATCH;
      }

      public static AssignmentTestable.TestResult testAll(String keyspace, ColumnSpecification receiver, Collection<? extends AssignmentTestable> toTest) {
         AssignmentTestable.TestResult res = EXACT_MATCH;
         Iterator var4 = toTest.iterator();

         while(var4.hasNext()) {
            AssignmentTestable rt = (AssignmentTestable)var4.next();
            if(rt == null) {
               res = WEAKLY_ASSIGNABLE;
            } else {
               AssignmentTestable.TestResult t = rt.testAssignment(keyspace, receiver);
               if(t == NOT_ASSIGNABLE) {
                  return NOT_ASSIGNABLE;
               }

               if(t == WEAKLY_ASSIGNABLE) {
                  res = WEAKLY_ASSIGNABLE;
               }
            }
         }

         return res;
      }
   }
}
