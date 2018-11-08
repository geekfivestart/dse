package com.datastax.bdp.node.transport.internal;

public class FailedProcessorException extends Exception {
   public final Class<? extends Throwable> errorClass;

   public FailedProcessorException(Class<? extends Throwable> errorClass, String errorMessage) {
      super(errorMessage);
      this.errorClass = errorClass;
   }
}
