package com.datastax.bdp.cassandra.auth;

import com.datastax.driver.core.Authenticator;

abstract class BaseDseAuthenticator implements Authenticator {
   final byte[] EMPTY_BYTE_ARRAY = new byte[0];

   BaseDseAuthenticator() {
   }

   abstract byte[] getMechanism();

   public byte[] initialResponse() {
      return this.getMechanism();
   }

   public void onAuthenticationSuccess(byte[] token) {
   }
}
