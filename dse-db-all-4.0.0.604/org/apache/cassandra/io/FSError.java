package org.apache.cassandra.io;

import java.io.File;
import java.io.IOError;
import java.util.Optional;

public abstract class FSError extends IOError {
   public final Optional<File> path;

   public FSError(Throwable cause, File path) {
      super(cause);
      this.path = Optional.of(path);
   }

   public FSError(Throwable cause) {
      super(cause);
      this.path = Optional.empty();
   }

   public static FSError findNested(Throwable top) {
      for(Throwable t = top; t != null; t = t.getCause()) {
         if(t instanceof FSError) {
            return (FSError)t;
         }
      }

      return null;
   }
}
