package com.datastax.bdp.cassandra.auth;

import com.datastax.bdp.transport.server.DigestAuthUtils;
import java.util.Objects;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;

public class CassandraDelegationTokenIdentifier extends AbstractDelegationTokenIdentifier {
   public static final Text CASSANDRA_DELEGATION_KIND = new Text("CASSANDRA_DELEGATION_TOKEN");

   public CassandraDelegationTokenIdentifier() {
   }

   public CassandraDelegationTokenIdentifier(Text owner, Text renewer, Text realUser) {
      super(owner, renewer, realUser);
   }

   public Text getKind() {
      return CASSANDRA_DELEGATION_KIND;
   }

   private static void ensureInitialized() {
      UserGroupInformation.isSecurityEnabled();
   }

   public boolean hasRenewer() {
      return this.getRenewer() != null && !Objects.equals(this.getRenewer().toString(), "");
   }

   public static String toStringFromId(TokenIdentifier id) {
      try {
         return id.toString();
      } catch (Throwable var2) {
         return null;
      }
   }

   public static String toStringFromId(byte[] serializedId) {
      try {
         return toStringFromId((TokenIdentifier)DigestAuthUtils.getCassandraDTIdentifier(serializedId));
      } catch (Throwable var2) {
         return null;
      }
   }

   public static String toStringFromId(String serializedId) {
      try {
         return toStringFromId(Base64.decodeBase64(serializedId));
      } catch (Throwable var2) {
         return null;
      }
   }

   public static String toStringFromToken(Token<? extends TokenIdentifier> token) {
      try {
         return toStringFromId(token.getIdentifier());
      } catch (Throwable var2) {
         return null;
      }
   }

   public static String toStringFromToken(byte[] serializedToken) {
      try {
         return toStringFromToken(Base64.encodeBase64URLSafeString(serializedToken));
      } catch (Throwable var2) {
         return null;
      }
   }

   public static String toStringFromToken(String serializedToken) {
      try {
         return toStringFromToken(DigestAuthUtils.getTokenFromTokenString(serializedToken));
      } catch (Throwable var2) {
         return null;
      }
   }

   static {
      ensureInitialized();
   }
}
