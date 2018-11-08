package com.datastax.bdp.cassandra.auth;

import com.google.common.base.MoreObjects;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.output.ByteArrayOutputStream;

public class InClusterAuthenticator {
   private static final SecureRandom random = new SecureRandom();

   public InClusterAuthenticator() {
   }

   public static class Credentials implements Serializable {
      public final InClusterAuthenticator.TokenId id;
      public final byte[] password;

      public Credentials(InClusterAuthenticator.TokenId id, byte[] password) {
         this.id = id;
         this.password = password;
      }

      public char[] getPasswordChars() {
         return getPasswordChars(this.password);
      }

      public String getIdString() {
         return Base64.encodeBase64String(this.id.decompose());
      }

      public static char[] getPasswordChars(byte[] password) {
         return Base64.encodeBase64String(password).toCharArray();
      }

      public static InClusterAuthenticator.Credentials create(String username) {
         InClusterAuthenticator.TokenId id = InClusterAuthenticator.TokenId.create(username);
         byte[] password = new byte[16];
         InClusterAuthenticator.random.nextBytes(password);
         return new InClusterAuthenticator.Credentials(id, password);
      }

      public boolean equals(Object o) {
         if(this == o) {
            return true;
         } else if(o != null && this.getClass() == o.getClass()) {
            InClusterAuthenticator.Credentials that = (InClusterAuthenticator.Credentials)o;
            return Objects.equals(this.id, that.id) && Arrays.equals(this.password, that.password);
         } else {
            return false;
         }
      }

      public int hashCode() {
         return Objects.hash(new Object[]{this.id, Integer.valueOf(Arrays.hashCode(this.password))});
      }

      public String toString() {
         return MoreObjects.toStringHelper(this).add("id", this.id).add("password", this.password).toString();
      }
   }

   public static class TokenId implements Serializable {
      public final String username;
      public final UUID code;

      public TokenId(String username, UUID code) {
         this.username = username;
         this.code = code;
      }

      public static InClusterAuthenticator.TokenId create(String username) {
         return new InClusterAuthenticator.TokenId(username, UUIDGen.getTimeUUID());
      }

      public static InClusterAuthenticator.TokenId compose(byte[] bytes) {
         try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
            String username = in.readUTF();
            long msb = in.readLong();
            long lsb = in.readLong();
            UUID code = new UUID(msb, lsb);
            return new InClusterAuthenticator.TokenId(username, code);
         } catch (Exception var8) {
            throw new IllegalArgumentException("Could not decode token");
         }
      }

      public static InClusterAuthenticator.TokenId compose(String base64EncodedBytes) {
         byte[] decomposedId = Base64.decodeBase64(base64EncodedBytes);
         return compose(decomposedId);
      }

      public byte[] decompose() {
         try {
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(buf);
            out.writeUTF(this.username);
            out.writeLong(this.code.getMostSignificantBits());
            out.writeLong(this.code.getLeastSignificantBits());
            out.flush();
            out.close();
            return buf.toByteArray();
         } catch (IOException var3) {
            throw new RuntimeException("Failed to encode the token");
         }
      }

      public boolean equals(Object o) {
         if(this == o) {
            return true;
         } else if(o != null && this.getClass() == o.getClass()) {
            InClusterAuthenticator.TokenId tokenId = (InClusterAuthenticator.TokenId)o;
            return Objects.equals(this.username, tokenId.username) && Objects.equals(this.code, tokenId.code);
         } else {
            return false;
         }
      }

      public int hashCode() {
         return Objects.hash(new Object[]{this.username, this.code});
      }

      public String toString() {
         return MoreObjects.toStringHelper(this).add("username", this.username).add("code", this.code).toString();
      }
   }
}
