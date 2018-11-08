package org.apache.cassandra.io;

import java.io.File;

public class FSReadError extends FSError {
   public FSReadError(Throwable cause, File path) {
      super(cause, path);
   }

   public FSReadError(Throwable cause, String path) {
      this(cause, new File(path));
   }

   public FSReadError(Throwable cause) {
      super(cause);
   }

   public String toString() {
      return this.path.isPresent()?"FSReadError in " + this.path.get():"FSReadError in unspecified location";
   }
}
