package org.apache.cassandra.exceptions;

public interface TransportException {
   ExceptionCode code();

   String getMessage();
}
