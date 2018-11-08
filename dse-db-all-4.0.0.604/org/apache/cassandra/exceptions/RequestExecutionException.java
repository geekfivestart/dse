package org.apache.cassandra.exceptions;

import org.apache.cassandra.utils.flow.Flow;

public abstract class RequestExecutionException extends CassandraException implements Flow.NonWrappableException {
   protected RequestExecutionException(ExceptionCode code, String msg) {
      super(code, msg);
   }

   protected RequestExecutionException(ExceptionCode code, String msg, Throwable e) {
      super(code, msg, e);
   }
}
