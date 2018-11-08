package com.datastax.bdp.node.transport.internal;

import com.datastax.bdp.node.transport.MessageType;

public class SystemMessageTypes {
   public static final MessageType HANDSHAKE;
   public static final MessageType UNSUPPORTED_MESSAGE;
   public static final MessageType FAILED_PROCESSOR;

   private SystemMessageTypes() {
   }

   static {
      HANDSHAKE = MessageType.of(MessageType.Domain.SYSTEM, 1);
      UNSUPPORTED_MESSAGE = MessageType.of(MessageType.Domain.SYSTEM, 2);
      FAILED_PROCESSOR = MessageType.of(MessageType.Domain.SYSTEM, 3);
   }
}
