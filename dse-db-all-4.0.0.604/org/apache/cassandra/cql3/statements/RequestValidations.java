package org.apache.cassandra.cql3.statements;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;

public final class RequestValidations {
   public static void checkTrue(boolean expression, String message) throws InvalidRequestException {
      if(!expression) {
         throw invalidRequest(message);
      }
   }

   public static void checkTrue(boolean expression, String messageTemplate, Object messageArg) throws InvalidRequestException {
      if(!expression) {
         throw invalidRequest(messageTemplate, new Object[]{messageArg});
      }
   }

   public static void checkTrue(boolean expression, String messageTemplate, Object firstArg, Object secondArg) throws InvalidRequestException {
      if(!expression) {
         throw invalidRequest(messageTemplate, new Object[]{firstArg, secondArg});
      }
   }

   public static void checkTrue(boolean expression, String messageTemplate, Object firstArg, Object secondArg, Object thirdArg) throws InvalidRequestException {
      if(!expression) {
         throw invalidRequest(messageTemplate, new Object[]{firstArg, secondArg, thirdArg});
      }
   }

   public static void checkContainsNoDuplicates(List<?> list, String message) throws InvalidRequestException {
      if((new HashSet(list)).size() != list.size()) {
         throw invalidRequest(message);
      }
   }

   public static <E> void checkContainsOnly(List<E> list, List<E> expectedElements, String message) throws InvalidRequestException {
      List<E> copy = new ArrayList(list);
      copy.removeAll(expectedElements);
      if(!copy.isEmpty()) {
         throw invalidRequest(message);
      }
   }

   public static void checkFalse(boolean expression, String messageTemplate, Object messageArg) throws InvalidRequestException {
      checkTrue(!expression, messageTemplate, messageArg);
   }

   public static void checkFalse(boolean expression, String messageTemplate, Object firstArg, Object secondArg) throws InvalidRequestException {
      checkTrue(!expression, messageTemplate, firstArg, secondArg);
   }

   public static void checkFalse(boolean expression, String messageTemplate, Object firstArg, Object secondArg, Object thirdArg) throws InvalidRequestException {
      checkTrue(!expression, messageTemplate, firstArg, secondArg, thirdArg);
   }

   public static void checkFalse(boolean expression, String message) throws InvalidRequestException {
      checkTrue(!expression, message);
   }

   public static <T> T checkNotNull(T object, String message) throws InvalidRequestException {
      checkTrue(object != null, message);
      return object;
   }

   public static <T> T checkNotNull(T object, String messageTemplate, Object messageArg) throws InvalidRequestException {
      checkTrue(object != null, messageTemplate, messageArg);
      return object;
   }

   public static void checkBindValueSet(ByteBuffer b, String messageTemplate, Object messageArg) throws InvalidRequestException {
      checkTrue(b != ByteBufferUtil.UNSET_BYTE_BUFFER, messageTemplate, messageArg);
   }

   public static <T> T checkNull(T object, String messageTemplate, Object messageArg) throws InvalidRequestException {
      checkTrue(object == null, messageTemplate, messageArg);
      return object;
   }

   public static <T> T checkNull(T object, String message) throws InvalidRequestException {
      checkTrue(object == null, message);
      return object;
   }

   public static <T> T checkNull(T object, String messageTemplate, Object firstArg, Object secondArg) throws InvalidRequestException {
      checkTrue(object == null, messageTemplate, firstArg, secondArg);
      return object;
   }

   public static InvalidRequestException invalidRequest(String message) {
      return new InvalidRequestException(message);
   }

   public static InvalidRequestException invalidRequest(String messageTemplate, Object... messageArgs) {
      return new InvalidRequestException(String.format(messageTemplate, messageArgs));
   }

   private RequestValidations() {
   }
}
