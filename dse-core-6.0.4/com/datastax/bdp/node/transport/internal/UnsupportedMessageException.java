package com.datastax.bdp.node.transport.internal;

import com.datastax.bdp.node.transport.MessageType;

public class UnsupportedMessageException extends Exception {
   public final Long messageId;

   public UnsupportedMessageException(String message) {
      super(message);
      this.messageId = null;
   }

   public UnsupportedMessageException(Long id, MessageType type, Byte version) {
      super(String.format("Unsupported message type: %s with version: %s", new Object[]{type, version}));
      this.messageId = id;
   }
}
