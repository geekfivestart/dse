package com.datastax.bdp.util;

import java.util.function.Function;
import org.apache.cassandra.utils.NoSpamLogger;

public class LambdaMayThrow {
   public LambdaMayThrow() {
   }

   public static <I, O> Function<I, O> logAndReturnNullOnException(NoSpamLogger logger, LambdaMayThrow.FunctionMayThrow<I, O> wrapped) {
      return (x) -> {
         try {
            return wrapped.apply(x);
         } catch (Exception var4) {
            logger.warn("Caught exception inside lambda, continuing", new Object[]{var4});
            return null;
         }
      };
   }

   public interface RunnableMayThrow {
      void run() throws Exception;
   }

   public interface SupplierMayThrow<O> {
      O get() throws Exception;
   }

   public interface TriConsumerMayThrow<I1, I2, I3> {
      void accept(I1 var1, I2 var2, I3 var3) throws Exception;
   }

   public interface BiConsumerMayThrow<I1, I2> {
      void accept(I1 var1, I2 var2) throws Exception;
   }

   public interface BiFunctionMayThrow<I1, I2, O> {
      O apply(I1 var1, I2 var2) throws Exception;
   }

   public interface FunctionMayThrow<I, O> {
      O apply(I var1) throws Exception;
   }
}
