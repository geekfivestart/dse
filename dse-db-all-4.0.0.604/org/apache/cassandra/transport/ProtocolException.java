package org.apache.cassandra.transport;

import org.apache.cassandra.exceptions.ExceptionCode;
import org.apache.cassandra.exceptions.TransportException;

public class ProtocolException extends RuntimeException implements TransportException {
   private final ProtocolVersion forcedProtocolVersion;

   public ProtocolException(String msg) {
      this(msg, (ProtocolVersion)null);
   }

   public ProtocolException(String msg, ProtocolVersion forcedProtocolVersion) {
      super(msg);
      this.forcedProtocolVersion = forcedProtocolVersion;
   }

   public ExceptionCode code() {
      return ExceptionCode.PROTOCOL_ERROR;
   }

   public ProtocolVersion getForcedProtocolVersion() {
      return this.forcedProtocolVersion;
   }
}
