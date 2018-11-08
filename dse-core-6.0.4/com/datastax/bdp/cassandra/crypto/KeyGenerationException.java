package com.datastax.bdp.cassandra.crypto;

public class KeyGenerationException extends Exception {
   public KeyGenerationException() {
   }

   public KeyGenerationException(String message) {
      super(message);
   }

   public KeyGenerationException(String message, Throwable cause) {
      super(message, cause);
   }

   public KeyGenerationException(Throwable cause) {
      super(cause);
   }
}
