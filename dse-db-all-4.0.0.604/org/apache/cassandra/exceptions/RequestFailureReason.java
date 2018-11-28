package org.apache.cassandra.exceptions;

import org.apache.cassandra.net.MessagingVersion;

public enum RequestFailureReason {
   UNKNOWN(0),
   READ_TOO_MANY_TOMBSTONES(1),
   INDEX_NOT_AVAILABLE(2),
   CDC_SEGMENT_FULL(3),
   COUNTER_FORWARDING_FAILURE(4),
   UNKNOWN_TABLE(5),
   UNKNOWN_KEYSPACE(6),
   UNKNOWN_COLUMN(7),
   NODESYNC_NOT_RUNNING(256),
   UNKNOWN_NODESYNC_USER_VALIDATION(257),
   CANCELLED_NODESYNC_USER_VALIDATION(258),
   NODESYNC_TRACING_ALREADY_ENABLED(259);

   private final int code;
   public static final RequestFailureReason[] VALUES = values();

   private RequestFailureReason(int code) {
      this.code = code;
   }

   public int codeForInternodeProtocol(MessagingVersion version) {
      return version.compareTo(MessagingVersion.OSS_40) < 0?(this == READ_TOO_MANY_TOMBSTONES?this.code:UNKNOWN.code):this.code;
   }

   public int codeForNativeProtocol() {
      return this.code;
   }

   public static RequestFailureReason fromCode(int code) {
      RequestFailureReason[] var1 = VALUES;
      int var2 = var1.length;

      for(int var3 = 0; var3 < var2; ++var3) {
         RequestFailureReason reasonCode = var1[var3];
         if(reasonCode.code == code) {
            return reasonCode;
         }
      }

      return UNKNOWN;
   }

   public boolean isSchemaRelated() {
      switch (this) {
         case UNKNOWN_TABLE:
         case UNKNOWN_KEYSPACE:
         case UNKNOWN_COLUMN: {
            return true;
         }
      }
      return false;
   }
}
