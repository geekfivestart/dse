package com.datastax.bdp.util;

import com.datastax.bdp.cassandra.auth.DseJavaDriverAuthProvider;
import com.datastax.bdp.config.ClientConfiguration;
import com.datastax.bdp.transport.server.DigestAuthUtils;
import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.RemoteEndpointAwareJdkSSLOptions;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.auth.DseGSSAPIAuthProvider;
import com.datastax.driver.dse.auth.DsePlainTextAuthProvider;
import com.diffplug.common.base.Errors;
import com.diffplug.common.base.Suppliers;
import com.diffplug.common.base.Throwing.Function;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DseConnectionUtil {
   private static final Logger logger = LoggerFactory.getLogger(DseConnectionUtil.class);

   public DseConnectionUtil() {
   }

   public static Cluster createCluster(ClientConfiguration config, String username, String password, String token) throws IOException, GeneralSecurityException {
      Builder clusterBuilder = createClusterBuilder(config, username, password, token);
      return clusterBuilder.build();
   }

   public static Builder createClusterBuilder(ClientConfiguration config, String username, String password, String token) throws IOException, GeneralSecurityException {
      return createClusterBuilder(config, username, password, token, false);
   }

   public static Builder createClusterBuilder(ClientConfiguration config, String username, String password, String token, boolean metricsEnabled) throws IOException, GeneralSecurityException {
      return createClusterBuilder(config, username, password, token, metricsEnabled, (String)null);
   }

   public static Builder createClusterBuilder(ClientConfiguration config, String username, String password, String token, boolean metricsEnabled, String authorizationId) throws IOException, GeneralSecurityException {
      AuthProvider authProvider = getAuthProvider(StringUtil.stringToOptional(username), StringUtil.stringToOptional(password), StringUtil.stringToOptional(token).map(Errors.rethrow().wrap(DigestAuthUtils::getTokenFromTokenString)), StringUtil.stringToOptional(authorizationId), config);
      return createClusterBuilder(config, authProvider, metricsEnabled);
   }

   public static Cluster createCluster(ClientConfiguration config, AuthProvider authProvider) throws IOException, GeneralSecurityException {
      Builder clusterBuilder = createClusterBuilder(config, authProvider);
      return clusterBuilder.build();
   }

   public static Builder createClusterBuilder(ClientConfiguration config, AuthProvider authProvider) throws IOException, GeneralSecurityException {
      return createClusterBuilder(config, authProvider, false);
   }

   public static Builder createClusterBuilder(ClientConfiguration config, AuthProvider authProvider, boolean metricsEnabled) throws IOException, GeneralSecurityException {
      com.datastax.driver.dse.DseCluster.Builder clusterBuilder = DseCluster.builder().addContactPoints(getContactPoints(config)).withPort(config.getNativePort()).withAuthProvider(authProvider);
      if(!metricsEnabled) {
         clusterBuilder.withoutJMXReporting().withoutMetrics();
      }

      SSLUtil.createSSLContext(config).ifPresent((ctx) -> {
         com.datastax.driver.core.RemoteEndpointAwareJdkSSLOptions.Builder sslOptionsBuilder = RemoteEndpointAwareJdkSSLOptions.builder();
         sslOptionsBuilder.withSSLContext(ctx);
         sslOptionsBuilder.withCipherSuites(config.getCipherSuites());
         clusterBuilder.withSSL(sslOptionsBuilder.build());
      });
      return clusterBuilder;
   }

   public static Cluster createWhitelistingCluster(ClientConfiguration config, String username, String password) throws IOException, GeneralSecurityException {
      Builder builder = createClusterBuilder(config, username, password, (String)null);
      List<InetSocketAddress> whitelist = (List)Arrays.asList(getContactPoints(config)).stream().map((address) -> {
         return new InetSocketAddress(address, config.getNativePort());
      }).collect(Collectors.toList());
      WhiteListPolicy policy = new WhiteListPolicy(DCAwareRoundRobinPolicy.builder().build(), whitelist);
      return builder.withLoadBalancingPolicy(policy).build();
   }

   private static InetAddress[] getContactPoints(ClientConfiguration config) throws UnknownHostException {
      InetAddress[] cassandraHosts = config.getCassandraHosts();
      return cassandraHosts != null && cassandraHosts.length > 0?cassandraHosts:new InetAddress[]{InetAddress.getLocalHost()};
   }

   public static AuthProvider getAuthProvider(Optional<String> username, Optional<String> password, Optional<Token<? extends TokenIdentifier>> token, ClientConfiguration config) {
      return getAuthProvider(username, password, token, Optional.empty(), config);
   }

   public static AuthProvider getAuthProvider(Optional<String> username, Optional<String> password, Optional<Token<? extends TokenIdentifier>> token, Optional<String> authorizationId, ClientConfiguration config) {
      Supplier<Optional<Token<? extends TokenIdentifier>>> tokenFromUGI = Suppliers.memoize(DigestAuthUtils::getCassandraTokenFromUGI);
      if(username.isPresent() && password.isPresent()) {
         logger.info("Using plain text credentials to authenticate");
         return (AuthProvider)authorizationId.map((proxyUser) -> {
            return new DsePlainTextAuthProvider((String)username.get(), (String)password.get(), proxyUser);
         }).orElse(new DsePlainTextAuthProvider((String)username.get(), (String)password.get()));
      } else if(token.isPresent()) {
         Preconditions.checkArgument(!authorizationId.isPresent(), "Authorization user cannot be specified with token authentication");
         logger.info("Using delegation token to authenticate");
         return new DseJavaDriverAuthProvider(config, (Token)token.get());
      } else if(((Optional)tokenFromUGI.get()).isPresent()) {
         Preconditions.checkArgument(!authorizationId.isPresent(), "Authorization user cannot be specified with token authentication");
         logger.info("Using delegation token to authenticate");
         return new DseJavaDriverAuthProvider(config, (Token)((Optional)tokenFromUGI.get()).get());
      } else if(config.isKerberosEnabled()) {
         logger.info("Using Kerberos credentials to authenticate");
         com.datastax.driver.dse.auth.DseGSSAPIAuthProvider.Builder builder = DseGSSAPIAuthProvider.builder().withSaslProtocol(config.getSaslProtocolName());
         authorizationId.ifPresent(builder::withAuthorizationId);
         return builder.build();
      } else {
         logger.info("Using no credentials");
         return AuthProvider.NONE;
      }
   }
}
