package org.apache.cassandra.exceptions;

import java.util.List;
import org.apache.cassandra.db.marshal.AbstractType;

public final class OperationExecutionException extends RequestExecutionException {
   public static OperationExecutionException create(char operator, List<AbstractType<?>> argTypes, Exception e) {
      List<String> cqlTypes = AbstractType.asCQLTypeStringList(argTypes);
      return new OperationExecutionException(String.format("the operation '%s %s %s' failed: %s", new Object[]{cqlTypes.get(0), Character.valueOf(operator), cqlTypes.get(1), e.getMessage()}));
   }

   public OperationExecutionException(String msg) {
      super(ExceptionCode.FUNCTION_FAILURE, msg);
   }
}
