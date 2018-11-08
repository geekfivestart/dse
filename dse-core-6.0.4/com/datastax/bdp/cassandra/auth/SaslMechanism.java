package com.datastax.bdp.cassandra.auth;

import java.nio.charset.StandardCharsets;

public enum SaslMechanism {
   PLAIN("PLAIN", "PLAIN-START".getBytes(StandardCharsets.UTF_8)),
   DIGEST("DIGEST-MD5", "DIGEST-MD5-START".getBytes(StandardCharsets.UTF_8)),
   GSSAPI("GSSAPI", "GSSAPI-START".getBytes(StandardCharsets.UTF_8)),
   INPROCESS("INPROCESS", "INPROCESS-START".getBytes(StandardCharsets.UTF_8)),
   INCLUSTER("INCLUSTER", "INCLUSTER-START".getBytes(StandardCharsets.UTF_8));

   public final String mechanism;
   public final byte[] mechanism_bytes;
   public final byte[] response;

   private SaslMechanism(String mechanism, byte[] response) {
      this.mechanism = mechanism;
      this.mechanism_bytes = mechanism.getBytes(StandardCharsets.UTF_8);
      this.response = response;
   }
}
