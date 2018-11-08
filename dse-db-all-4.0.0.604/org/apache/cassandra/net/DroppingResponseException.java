package org.apache.cassandra.net;

public class DroppingResponseException extends RuntimeException {
   public DroppingResponseException() {
   }

   public DroppingResponseException(String message) {
      super(message);
   }
}
