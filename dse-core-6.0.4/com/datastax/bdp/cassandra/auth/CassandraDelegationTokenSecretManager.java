package com.datastax.bdp.cassandra.auth;

import com.datastax.bdp.gms.VersionBarrier;
import com.datastax.bdp.system.TimeSource;
import com.datastax.bdp.transport.server.KerberosServerUtils;
import com.google.common.base.Preconditions;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOError;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.function.Function;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraDelegationTokenSecretManager extends AbstractDelegationTokenSecretManager<CassandraDelegationTokenIdentifier> {
   private static final Logger logger = LoggerFactory.getLogger(CassandraDelegationTokenSecretManager.class);
   private static final Random randomGen = new Random(UUID.randomUUID().getLeastSignificantBits());
   final long tokenMaxLifetime;
   final long tokenRenewInterval;
   private ConsistencyLevel consistencyLevelRead;
   private ConsistencyLevel consistencyLevelWrite;
   private DigestTokensManager digestTokensManager;
   private TimeSource clock;

   public CassandraDelegationTokenSecretManager(long tokenMaxLifetime, long tokenRenewInterval, DigestTokensManager digestTokensManager, TimeSource clock) {
      super(0L, tokenMaxLifetime, tokenRenewInterval, Duration.ofHours(1L).toMillis());
      this.consistencyLevelRead = ConsistencyLevel.LOCAL_QUORUM;
      this.consistencyLevelWrite = ConsistencyLevel.LOCAL_QUORUM;
      Preconditions.checkArgument(toSeconds(tokenMaxLifetime) >= 1, "Delegation token max life time must be at least 1000 ms (1 second)");
      Preconditions.checkArgument(toSeconds(tokenRenewInterval) >= 1, "Delegation token renew interval must be at least 1000 ms (1 second)");
      this.digestTokensManager = digestTokensManager;
      this.tokenMaxLifetime = tokenMaxLifetime;
      this.tokenRenewInterval = tokenRenewInterval;
      this.clock = clock;
      logger.info("Created CassandraDelegationTokenSecretManager with token max-life: {} and renewal interval: {}", ch.qos.logback.core.util.Duration.buildByMilliseconds((double)tokenMaxLifetime), ch.qos.logback.core.util.Duration.buildByMilliseconds((double)tokenRenewInterval));
   }

   public CassandraDelegationTokenIdentifier createIdentifier() {
      return new CassandraDelegationTokenIdentifier();
   }

   public void addKey(DelegationKey key) throws IOException {
   }

   public CassandraDelegationTokenIdentifier cancelToken(Token<CassandraDelegationTokenIdentifier> token, String canceller) throws IOException {
      return this.cancelToken(this.getIdentifier(token), canceller);
   }

   public CassandraDelegationTokenIdentifier cancelToken(CassandraDelegationTokenIdentifier identifier, String canceller) throws IOException {
      logger.info("User {} requested removal of token: {}", canceller, CassandraDelegationTokenIdentifier.toStringFromId((TokenIdentifier)identifier));
      String cancellerName = (String)Optional.ofNullable(canceller).map(KerberosServerUtils::getOrExtractServiceName).orElse((Object)null);
      if(Objects.equals(canceller, identifier.getUser().getUserName()) || identifier.hasRenewer() && Objects.equals(cancellerName, identifier.getRenewer().toString())) {
         return this.cancelToken(identifier);
      } else {
         throw new AccessControlException(String.format("%s is not authorized to cancel the token %s", new Object[]{canceller, CassandraDelegationTokenIdentifier.toStringFromId((TokenIdentifier)identifier)}));
      }
   }

   public synchronized CassandraDelegationTokenIdentifier cancelToken(CassandraDelegationTokenIdentifier identifier) throws IOException {
      try {
         this.digestTokensManager.deleteTokenById(identifier.getBytes(), this.consistencyLevelWrite);
         logger.info("Removed token: {}", CassandraDelegationTokenIdentifier.toStringFromId((TokenIdentifier)identifier));
         return identifier;
      } catch (RequestValidationException | VersionBarrier.UpgradingException | RequestExecutionException var3) {
         throw new IOException(String.format("Failed to remove token: %s", new Object[]{CassandraDelegationTokenIdentifier.toStringFromId((TokenIdentifier)identifier)}), var3);
      }
   }

   public synchronized byte[] createPassword(CassandraDelegationTokenIdentifier identifier) {
      try {
         long now = this.clock.currentTimeMillis();
         identifier.setIssueDate(now);
         identifier.setMaxDate(now + this.tokenMaxLifetime);
         identifier.setMasterKeyId(randomGen.nextInt());
         identifier.setSequenceNumber(randomGen.nextInt());
         byte[] id = identifier.getBytes();
         byte[] password = UUIDGen.decompose(UUID.randomUUID());
         this.digestTokensManager.createToken(id, password, Duration.ofMillis(this.tokenMaxLifetime), this.consistencyLevelWrite, now);
         logger.info("Created token: {}", CassandraDelegationTokenIdentifier.toStringFromId((TokenIdentifier)identifier));
         return password;
      } catch (RequestValidationException | VersionBarrier.UpgradingException | RequestExecutionException var6) {
         throw new IOError(var6);
      }
   }

   public boolean isRunning() {
      return true;
   }

   public long renewToken(Token<CassandraDelegationTokenIdentifier> token, String renewer) throws IOException {
      return this.renewToken(this.getIdentifier(token), renewer);
   }

   public long renewToken(CassandraDelegationTokenIdentifier identifier, String renewer) throws IOException {
      logger.info("User {} requested renewal of token: {}", renewer, CassandraDelegationTokenIdentifier.toStringFromId((TokenIdentifier)identifier));
      String renewerName = (String)Optional.ofNullable(renewer).map(KerberosServerUtils::getOrExtractServiceName).orElse((Object)null);
      if(identifier.hasRenewer() && Objects.equals(identifier.getRenewer().toString(), renewerName)) {
         return this.renewToken(identifier);
      } else {
         throw new AccessControlException(String.format("User %s is not authorized to renew a token: %s", new Object[]{renewerName, CassandraDelegationTokenIdentifier.toStringFromId((TokenIdentifier)identifier)}));
      }
   }

   public synchronized long renewToken(CassandraDelegationTokenIdentifier identifier) throws IOException {
      try {
         long now = this.clock.currentTimeMillis();
         Optional<Pair<byte[], Long>> current = this.digestTokensManager.getPasswordById(identifier.getBytes(), this.consistencyLevelRead);
         int newTtl = toSeconds(identifier.getMaxDate() - now);
         if(current.isPresent() && newTtl >= 1) {
            byte[] password = (byte[])((Pair)current.get()).left;
            this.digestTokensManager.updateToken(identifier.getBytes(), password, Duration.ofSeconds((long)newTtl), this.consistencyLevelWrite, now);
            logger.info("Renewed token: {}", CassandraDelegationTokenIdentifier.toStringFromId((TokenIdentifier)identifier));
            return now + this.tokenRenewInterval;
         } else {
            throw new InvalidToken(String.format("Failed to renew token: Token not found: %s", new Object[]{CassandraDelegationTokenIdentifier.toStringFromId((TokenIdentifier)identifier)}));
         }
      } catch (RequestValidationException | VersionBarrier.UpgradingException | RequestExecutionException var7) {
         throw new IOException(String.format("Failed to renew token: %s", new Object[]{CassandraDelegationTokenIdentifier.toStringFromId((TokenIdentifier)identifier)}), var7);
      }
   }

   public synchronized byte[] retrievePassword(CassandraDelegationTokenIdentifier identifier) throws InvalidToken {
      logger.debug("Requested password of token: {}", CassandraDelegationTokenIdentifier.toStringFromId((TokenIdentifier)identifier));
      long now = this.clock.currentTimeMillis();
      Optional<Pair<byte[], Long>> current = this.digestTokensManager.getPasswordById(identifier.getBytes(), this.consistencyLevelRead);
      if(current.isPresent()) {
         long timestamp = ((Long)((Pair)current.get()).right).longValue();
         byte[] password = (byte[])((Pair)current.get()).left;
         if(timestamp + this.tokenRenewInterval > now) {
            return password;
         }

         logger.info("Attempted to retrieve an expired token: {}", CassandraDelegationTokenIdentifier.toStringFromId((TokenIdentifier)identifier));
      }

      logger.info("Attempted to retrieve a token with invalid identifier: {}", CassandraDelegationTokenIdentifier.toStringFromId((TokenIdentifier)identifier));
      throw new InvalidToken(CassandraDelegationTokenIdentifier.toStringFromId((TokenIdentifier)identifier));
   }

   public synchronized Optional<Long> getLastRenewalTime(CassandraDelegationTokenIdentifier identifier) {
      return this.digestTokensManager.getPasswordById(identifier.getBytes(), this.consistencyLevelRead).map((p) -> {
         return (Long)p.right;
      });
   }

   public CassandraDelegationTokenIdentifier getIdentifier(Token<?> token) throws IOException {
      CassandraDelegationTokenIdentifier id = new CassandraDelegationTokenIdentifier();
      id.readFields(new DataInputStream(new ByteArrayInputStream(token.getIdentifier())));
      return id;
   }

   public void startThreads() throws IOException {
   }

   public void stopThreads() {
   }

   public synchronized void verifyToken(CassandraDelegationTokenIdentifier identifier, byte[] password) throws InvalidToken {
      byte[] internalPass = this.retrievePassword(identifier);

      assert internalPass != null && internalPass.length > 0;

      if(!Arrays.equals(password, internalPass)) {
         logger.info("Attempted to verify a token: {} with an invalid password", CassandraDelegationTokenIdentifier.toStringFromId((TokenIdentifier)identifier));
         throw new InvalidToken(CassandraDelegationTokenIdentifier.toStringFromId((TokenIdentifier)identifier));
      } else {
         logger.debug("Successfully verified a token: {}", CassandraDelegationTokenIdentifier.toStringFromId((TokenIdentifier)identifier));
      }
   }

   private static int toSeconds(long ms) {
      return (int)Duration.ofMillis(ms).getSeconds();
   }
}
