package com.datastax.bdp.node.transport.internal;

public class Handshake {
   public final byte version;

   public Handshake(byte version) {
      this.version = version;
   }
}
