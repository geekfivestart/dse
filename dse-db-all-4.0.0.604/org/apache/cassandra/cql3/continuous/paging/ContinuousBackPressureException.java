package org.apache.cassandra.cql3.continuous.paging;

import org.apache.cassandra.utils.flow.Flow;

public class ContinuousBackPressureException extends RuntimeException implements Flow.NonWrappableException {
   public ContinuousBackPressureException(String message) {
      super(message);
   }
}
