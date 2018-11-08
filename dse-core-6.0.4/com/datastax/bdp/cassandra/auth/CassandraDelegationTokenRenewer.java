package com.datastax.bdp.cassandra.auth;

import com.google.inject.Inject;
import com.google.inject.Provider;
import java.io.IOException;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenRenewer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraDelegationTokenRenewer extends TokenRenewer {
   private static final Logger logger = LoggerFactory.getLogger(TokenRenewer.class);
   @Inject
   private static Provider<CassandraDelegationTokenSecretManager> tokenSecretManager;

   public CassandraDelegationTokenRenewer() {
      logger.info("CassandraDelegationTokenRenewer initialized successfully.");
   }

   public boolean handleKind(Text kind) {
      return CassandraDelegationTokenIdentifier.CASSANDRA_DELEGATION_KIND.equals(kind);
   }

   public boolean isManaged(Token<?> token) throws IOException {
      return Objects.equals(token.getKind(), CassandraDelegationTokenIdentifier.CASSANDRA_DELEGATION_KIND);
   }

   public long renew(Token<?> token, Configuration conf) throws IOException, InterruptedException {
      CassandraDelegationTokenIdentifier tokenId = this.getTokenId(token);
      return ((CassandraDelegationTokenSecretManager)tokenSecretManager.get()).renewToken(tokenId);
   }

   public void cancel(Token<?> token, Configuration conf) throws IOException, InterruptedException {
      CassandraDelegationTokenIdentifier tokenId = this.getTokenId(token);
      ((CassandraDelegationTokenSecretManager)tokenSecretManager.get()).cancelToken(tokenId);
   }

   private CassandraDelegationTokenIdentifier getTokenId(Token<?> token) throws IOException {
      return ((CassandraDelegationTokenSecretManager)tokenSecretManager.get()).getIdentifier(token);
   }
}
