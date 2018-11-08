package org.apache.cassandra.io;

import java.io.File;

public class FSWriteError extends FSError {
   public FSWriteError(Throwable cause, File path) {
      super(cause, path);
   }

   public FSWriteError(Throwable cause, String path) {
      this(cause, new File(path));
   }

   public FSWriteError(Throwable cause) {
      super(cause);
   }

   public String toString() {
      return this.path.isPresent()?"FSWriteError in " + this.path.get():"FSWriteError in unspecified location";
   }
}
