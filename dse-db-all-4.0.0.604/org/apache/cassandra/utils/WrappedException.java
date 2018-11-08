package org.apache.cassandra.utils;

public class WrappedException extends RuntimeException {
   public WrappedException(Exception cause) {
      super(cause);
   }
}
