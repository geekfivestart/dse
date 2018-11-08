package com.datastax.bdp.transport.server;

import com.datastax.bdp.cassandra.auth.CassandraDelegationTokenIdentifier;
import com.datastax.bdp.util.rpc.RpcUtil;
import com.datastax.driver.core.Session;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DigestAuthUtils {
   private static final Logger logger = LoggerFactory.getLogger(DigestAuthUtils.class);
   public static final String DSE_RENEWER = "";

   public DigestAuthUtils() {
   }

   public static CassandraDelegationTokenIdentifier getCassandraDTIdentifier(byte[] tokenBytes) throws IOException {
      CassandraDelegationTokenIdentifier dt = new CassandraDelegationTokenIdentifier();
      dt.readFields(ByteStreams.newDataInput(tokenBytes));
      return dt;
   }

   public static String getUserNameFromDelegationToken(String authId) throws IOException {
      CassandraDelegationTokenIdentifier dtIdentifier = getCassandraDTIdentifier(Base64.decodeBase64(authId.getBytes()));
      UserGroupInformation user = dtIdentifier.getUser();
      if(user != null) {
         return dtIdentifier.getUser().getRealUser() != null?dtIdentifier.getUser().getRealUser().getUserName():dtIdentifier.getUser().getUserName();
      } else {
         throw new IOException("The delegation token is invalid");
      }
   }

   public static boolean moveIfStartsWith(ByteBuffer buf, byte[] prefix) {
      if(buf.remaining() >= prefix.length) {
         ByteBuffer preview = buf.duplicate();
         byte[] tmp = new byte[prefix.length];
         preview.get(tmp);
         if(Arrays.equals(tmp, prefix)) {
            buf.position(buf.position() + prefix.length);
            return true;
         }
      }

      return false;
   }

   public static void saveTokenToFile(Token<TokenIdentifier> token, Path tokenFile, String alias) throws IOException {
      Credentials credentials = new Credentials();
      credentials.addToken(new Text(alias), token);
      saveCredentialToFile(credentials, tokenFile);
   }

   public static void saveCredentialToFile(Credentials credentials, Path tokenFile) throws IOException {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      credentials.writeTokenStorageToStream(new DataOutputStream(buf));
      saveFile(ByteBuffer.wrap(buf.toByteArray()), tokenFile, Sets.newHashSet(new StandardOpenOption[]{StandardOpenOption.CREATE_NEW, StandardOpenOption.APPEND}), Sets.newHashSet(new PosixFilePermission[]{PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE}), true);
   }

   public static void saveFile(ByteBuffer data, Path path, Set<? extends OpenOption> openOptions, Set<PosixFilePermission> permissions, boolean deleteBefore) throws IOException {
      if(!Files.isDirectory(path.getParent(), new LinkOption[0])) {
         throw new IOException("Directory does not exists: " + path.getParent().toString());
      } else {
         if(deleteBefore) {
            Files.deleteIfExists(path);
         }

         FileAttribute attr = PosixFilePermissions.asFileAttribute(permissions);

         try {
            SeekableByteChannel sbc = Files.newByteChannel(path, openOptions, new FileAttribute[]{attr});
            Throwable var7 = null;

            try {
               sbc.write(data);
            } catch (Throwable var17) {
               var7 = var17;
               throw var17;
            } finally {
               if(sbc != null) {
                  if(var7 != null) {
                     try {
                        sbc.close();
                     } catch (Throwable var16) {
                        var7.addSuppressed(var16);
                     }
                  } else {
                     sbc.close();
                  }
               }

            }

         } catch (Exception var19) {
            throw new IOException(String.format("Cannot write file %s.", new Object[]{path.toAbsolutePath()}), var19);
         }
      }
   }

   public static String getEncodedToken(Session session, String renewer) throws IOException {
      Map<String, ByteBuffer> idAndPassword = (Map)RpcUtil.call(session, "DseClientTool", "generateDelegationToken", new Object[]{null, renewer});
      Token<TokenIdentifier> token = new Token(ByteBufferUtil.getArray((ByteBuffer)idAndPassword.get("id")), ByteBufferUtil.getArray((ByteBuffer)idAndPassword.get("password")), CassandraDelegationTokenIdentifier.CASSANDRA_DELEGATION_KIND, new Text());
      return token.encodeToUrlString();
   }

   public static void cancelToken(Session session, String tokenString) throws IOException {
      byte[] id = getTokenId(tokenString);
      RpcUtil.call(session, "DseClientTool", "cancelDelegationToken", new Object[]{ByteBuffer.wrap(id)});
   }

   public static Long renewToken(Session session, String tokenString) throws IOException {
      byte[] id = getTokenId(tokenString);
      return (Long)RpcUtil.call(session, "DseClientTool", "renewDelegationToken", new Object[]{ByteBuffer.wrap(id)});
   }

   public static Token<? extends TokenIdentifier> getTokenFromTokenString(String tokenString) throws IOException {
      Token<TokenIdentifier> token = new Token();
      token.decodeFromUrlString(tokenString);
      return token;
   }

   public static Optional<Token<? extends TokenIdentifier>> getCassandraTokenFromUGI() {
      try {
         UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
         Collection<Token<? extends TokenIdentifier>> tokens = ugi.getTokens();
         Iterator var2 = tokens.iterator();

         Token token;
         do {
            if(!var2.hasNext()) {
               logger.debug("No delegate token found for " + ugi.getUserName());
               return Optional.empty();
            }

            token = (Token)var2.next();
         } while(!token.getKind().equals(CassandraDelegationTokenIdentifier.CASSANDRA_DELEGATION_KIND));

         return Optional.of(token);
      } catch (NoClassDefFoundError | IOException var4) {
         return Optional.empty();
      }
   }

   public static Optional<String> getEncodedTokenId(Token<? extends TokenIdentifier> token) {
      try {
         return Optional.ofNullable(Base64.encodeBase64URLSafeString(token.getIdentifier()));
      } catch (Exception var2) {
         return Optional.empty();
      }
   }

   public static Optional<String> getEncodedTokenId(String token) {
      try {
         return getEncodedTokenId(getTokenFromTokenString(token));
      } catch (IOException var2) {
         return Optional.empty();
      }
   }

   public static byte[] getTokenId(String tokenString) {
      byte[] id = null;

      try {
         Token<TokenIdentifier> token = new Token();
         token.decodeFromUrlString(tokenString);
         id = token.getIdentifier();
      } catch (Exception var3) {
         ;
      }

      if(id == null || id.length == 0) {
         id = Base64.decodeBase64(tokenString);
      }

      return id;
   }
}
