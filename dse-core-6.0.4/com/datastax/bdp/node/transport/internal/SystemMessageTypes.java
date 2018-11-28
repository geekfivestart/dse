package com.datastax.bdp.node.transport.internal;

import com.datastax.bdp.node.transport.MessageType;

public class SystemMessageTypes {

   private SystemMessageTypes() {
   }

   public static final MessageType  HANDSHAKE = MessageType.of(MessageType.Domain.SYSTEM, (byte)1);
   public static final MessageType  UNSUPPORTED_MESSAGE = MessageType.of(MessageType.Domain.SYSTEM, (byte)2);
   public static final MessageType  FAILED_PROCESSOR = MessageType.of(MessageType.Domain.SYSTEM, (byte)3);
}
