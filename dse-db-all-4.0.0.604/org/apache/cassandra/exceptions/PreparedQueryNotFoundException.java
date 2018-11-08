package org.apache.cassandra.exceptions;

import org.apache.cassandra.utils.MD5Digest;

public class PreparedQueryNotFoundException extends RequestValidationException {
   public final MD5Digest id;

   public PreparedQueryNotFoundException(MD5Digest id) {
      super(ExceptionCode.UNPREPARED, makeMsg(id));
      this.id = id;
   }

   private static String makeMsg(MD5Digest id) {
      return String.format("Prepared query with ID %s not found (either the query was not prepared on this host (maybe the host has been restarted?) or you have prepared too many queries and it has been evicted from the internal cache)", new Object[]{id});
   }
}
