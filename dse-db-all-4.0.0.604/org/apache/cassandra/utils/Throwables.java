package org.apache.cassandra.utils;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.FSWriteError;

public final class Throwables {
   public Throwables() {
   }

   public static <T extends Throwable> T merge(T existingFail, T newFail) {
      if(existingFail == null) {
         return newFail;
      } else {
         existingFail.addSuppressed(newFail);
         return existingFail;
      }
   }

   public static void maybeFail(Throwable fail) {
      if(failIfCanCast(fail, (Class)null)) {
         throw new RuntimeException(fail);
      }
   }

   public static <T extends Throwable> void maybeFail(Throwable fail, Class<T> checked) throws T {
      if(failIfCanCast(fail, checked)) {
         throw new RuntimeException(fail);
      }
   }

   public static <T extends Throwable> boolean failIfCanCast(Throwable fail, Class<T> checked) throws T {
      if(fail == null) {
         return false;
      } else if(fail instanceof Error) {
         throw (Error)fail;
      } else if(fail instanceof RuntimeException) {
         throw (RuntimeException)fail;
      } else if(checked != null && checked.isInstance(fail)) {
         throw (Throwable)checked.cast(fail);
      } else {
         return true;
      }
   }

   @SafeVarargs
   public static <E extends Exception> void perform(Throwables.DiscreteAction... actions) throws E {
      Throwable accumulate = null;
      Throwables.DiscreteAction[] var2 = actions;
      int var3 = actions.length;

      for(int var4 = 0; var4 < var3; ++var4) {
         Throwables.DiscreteAction<? extends E> action = var2[var4];
         accumulate = perform(accumulate, action);
      }

      if(failIfCanCast(accumulate, (Class)null)) {
         throw (Exception)accumulate;
      }
   }

   public static <E extends Exception> void perform(Stream<? extends Throwables.DiscreteAction<? extends E>> stream, Throwables.DiscreteAction... extra) throws E {
      perform(Stream.concat(stream, Stream.of(extra)));
   }

   public static <E extends Exception> void perform(Stream<Throwables.DiscreteAction<? extends E>> actions) throws E {
      Throwable fail = perform((Throwable)null, actions);
      if(failIfCanCast(fail, (Class)null)) {
         throw (Exception)fail;
      }
   }

   public static Throwable perform(Throwable accumulate, Throwables.DiscreteAction... actions) {
      return perform(accumulate, Arrays.stream(actions));
   }

   public static Throwable perform(Throwable accumulate, Stream<? extends Throwables.DiscreteAction<?>> actions) {
      return perform(accumulate, actions.iterator());
   }

   public static Throwable perform(Throwable accumulate, Iterator<? extends Throwables.DiscreteAction<?>> actions) {
      while(actions.hasNext()) {
         accumulate = perform(accumulate, (Throwables.DiscreteAction)actions.next());
      }

      return accumulate;
   }

   public static Throwable perform(Throwable accumulate, Throwables.DiscreteAction<?> action) {
      try {
         action.perform();
      } catch (Throwable var3) {
         accumulate = merge(accumulate, var3);
      }

      return accumulate;
   }

   @SafeVarargs
   public static void perform(File against, Throwables.FileOpType opType, Throwables.DiscreteAction... actions) {
      perform(against.getPath(), opType, actions);
   }

   @SafeVarargs
   public static void perform(String filePath, Throwables.FileOpType opType, Throwables.DiscreteAction... actions) {
      maybeFail(perform((Throwable)null, filePath, opType, (Throwables.DiscreteAction[])actions));
   }

   @SafeVarargs
   public static Throwable perform(Throwable accumulate, String filePath, Throwables.FileOpType opType, Throwables.DiscreteAction... actions) {
      return perform(accumulate, filePath, opType, Arrays.stream(actions));
   }

   public static Throwable perform(Throwable accumulate, String filePath, Throwables.FileOpType opType, Stream<Throwables.DiscreteAction<? extends IOException>> actions) {
      return perform(accumulate, actions.map((action) -> {
         return () -> {
            try {
               action.perform();
            } catch (IOException var4) {
               throw (Exception)(opType == Throwables.FileOpType.WRITE?new FSWriteError(var4, filePath):new FSReadError(var4, filePath));
            }
         };
      }));
   }

   public static Throwable close(Throwable accumulate, Iterable<? extends AutoCloseable> closeables) {
      Iterator var2 = closeables.iterator();

      while(var2.hasNext()) {
         AutoCloseable closeable = (AutoCloseable)var2.next();

         try {
            closeable.close();
         } catch (Throwable var5) {
            accumulate = merge(accumulate, var5);
         }
      }

      return accumulate;
   }

   public static Throwable closeNonNull(Throwable accumulate, AutoCloseable... closeables) {
      AutoCloseable[] var2 = closeables;
      int var3 = closeables.length;

      for(int var4 = 0; var4 < var3; ++var4) {
         AutoCloseable closeable = var2[var4];
         accumulate = closeNonNull(accumulate, closeable);
      }

      return accumulate;
   }

   public static Throwable closeNonNull(Throwable accumulate, AutoCloseable closeable) {
      if(closeable == null) {
         return accumulate;
      } else {
         try {
            closeable.close();
         } catch (Throwable var3) {
            accumulate = merge(accumulate, var3);
         }

         return accumulate;
      }
   }

   public static Optional<IOException> extractIOExceptionCause(Throwable t) {
      if(t instanceof IOException) {
         return Optional.of((IOException)t);
      } else {
         Throwable cause = t;

         do {
            if((cause = cause.getCause()) == null) {
               return Optional.empty();
            }
         } while(!(cause instanceof IOException));

         return Optional.of((IOException)cause);
      }
   }

   public static <T extends Throwable> boolean isCausedBy(Throwable t, Class<T> causeClass) {
      while(t != null) {
         if(causeClass.isInstance(t)) {
            return true;
         }

         t = t.getCause();
      }

      return false;
   }

   public static Throwable unwrapped(Throwable t) {
      Throwable unwrapped;
      for(unwrapped = t; unwrapped instanceof CompletionException || unwrapped instanceof ExecutionException || unwrapped instanceof InvocationTargetException; unwrapped = unwrapped.getCause()) {
         ;
      }

      return (Throwable)(unwrapped == null?new RuntimeException("Got wrapping exception not wrapping anything", t):unwrapped);
   }

   public static RuntimeException unchecked(Throwable t) {
      return t instanceof RuntimeException?(RuntimeException)t:new RuntimeException(t);
   }

   public static RuntimeException cleaned(Throwable t) {
      return unchecked(unwrapped(t));
   }

   public interface DiscreteAction<E extends Exception> {
      void perform() throws E;
   }

   public static enum FileOpType {
      READ,
      WRITE;

      private FileOpType() {
      }
   }
}
