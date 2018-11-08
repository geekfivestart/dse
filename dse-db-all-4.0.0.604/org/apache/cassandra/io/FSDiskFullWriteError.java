package org.apache.cassandra.io;

public class FSDiskFullWriteError extends FSWriteError {
   public FSDiskFullWriteError(Throwable cause) {
      super(cause);
   }

   public String toString() {
      return "FSDiskFullWriteError";
   }
}
