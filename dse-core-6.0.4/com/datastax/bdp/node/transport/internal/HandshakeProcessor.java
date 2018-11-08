package com.datastax.bdp.node.transport.internal;

import com.datastax.bdp.node.transport.Message;
import com.datastax.bdp.node.transport.RequestContext;
import com.datastax.bdp.node.transport.ServerProcessor;

public class HandshakeProcessor implements ServerProcessor<Handshake, Handshake> {
   private final byte currentVersion;

   public HandshakeProcessor(byte currentVersion) {
      this.currentVersion = currentVersion;
   }

   public Message<Handshake> process(RequestContext ctx, Handshake h) {
      return new Message(ctx.getId(), SystemMessageTypes.HANDSHAKE, new Handshake((byte)Math.min(this.currentVersion, h.version)));
   }

   public void onComplete(Message<Handshake> response) {
   }
}
