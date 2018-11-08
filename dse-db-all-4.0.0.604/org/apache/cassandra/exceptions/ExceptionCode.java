package org.apache.cassandra.exceptions;

import java.util.HashMap;
import java.util.Map;
import org.apache.cassandra.transport.ProtocolException;

public enum ExceptionCode {
   SERVER_ERROR(0),
   PROTOCOL_ERROR(10),
   BAD_CREDENTIALS(256),
   UNAVAILABLE(4096),
   OVERLOADED(4097),
   IS_BOOTSTRAPPING(4098),
   TRUNCATE_ERROR(4099),
   WRITE_TIMEOUT(4352),
   READ_TIMEOUT(4608),
   READ_FAILURE(4864),
   FUNCTION_FAILURE(5120),
   WRITE_FAILURE(5376),
   SYNTAX_ERROR(8192),
   UNAUTHORIZED(8448),
   INVALID(8704),
   CONFIG_ERROR(8960),
   ALREADY_EXISTS(9216),
   UNPREPARED(9472),
   CLIENT_WRITE_FAILURE('耀');

   public final int value;
   private static final Map<Integer, ExceptionCode> valueToCode = new HashMap(values().length);

   private ExceptionCode(int value) {
      this.value = value;
   }

   public static ExceptionCode fromValue(int value) {
      ExceptionCode code = (ExceptionCode)valueToCode.get(Integer.valueOf(value));
      if(code == null) {
         throw new ProtocolException(String.format("Unknown error code %d", new Object[]{Integer.valueOf(value)}));
      } else {
         return code;
      }
   }

   static {
      ExceptionCode[] var0 = values();
      int var1 = var0.length;

      for(int var2 = 0; var2 < var1; ++var2) {
         ExceptionCode code = var0[var2];
         valueToCode.put(Integer.valueOf(code.value), code);
      }

   }
}
