package org.apache.cassandra.hadoop.cql3;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.JdkSSLOptions;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.google.common.base.Optional;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.Iterator;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

public class CqlConfigHelper {
   private static final String INPUT_CQL_COLUMNS_CONFIG = "cassandra.input.columnfamily.columns";
   private static final String INPUT_CQL_PAGE_ROW_SIZE_CONFIG = "cassandra.input.page.row.size";
   private static final String INPUT_CQL_WHERE_CLAUSE_CONFIG = "cassandra.input.where.clause";
   private static final String INPUT_CQL = "cassandra.input.cql";
   private static final String USERNAME = "cassandra.username";
   private static final String PASSWORD = "cassandra.password";
   private static final String INPUT_NATIVE_PORT = "cassandra.input.native.port";
   private static final String INPUT_NATIVE_CORE_CONNECTIONS_PER_HOST = "cassandra.input.native.core.connections.per.host";
   private static final String INPUT_NATIVE_MAX_CONNECTIONS_PER_HOST = "cassandra.input.native.max.connections.per.host";
   private static final String INPUT_NATIVE_MAX_SIMULT_REQ_PER_CONNECTION = "cassandra.input.native.max.simult.reqs.per.connection";
   private static final String INPUT_NATIVE_CONNECTION_TIMEOUT = "cassandra.input.native.connection.timeout";
   private static final String INPUT_NATIVE_READ_CONNECTION_TIMEOUT = "cassandra.input.native.read.connection.timeout";
   private static final String INPUT_NATIVE_RECEIVE_BUFFER_SIZE = "cassandra.input.native.receive.buffer.size";
   private static final String INPUT_NATIVE_SEND_BUFFER_SIZE = "cassandra.input.native.send.buffer.size";
   private static final String INPUT_NATIVE_SOLINGER = "cassandra.input.native.solinger";
   private static final String INPUT_NATIVE_TCP_NODELAY = "cassandra.input.native.tcp.nodelay";
   private static final String INPUT_NATIVE_REUSE_ADDRESS = "cassandra.input.native.reuse.address";
   private static final String INPUT_NATIVE_KEEP_ALIVE = "cassandra.input.native.keep.alive";
   private static final String INPUT_NATIVE_AUTH_PROVIDER = "cassandra.input.native.auth.provider";
   private static final String INPUT_NATIVE_SSL_TRUST_STORE_PATH = "cassandra.input.native.ssl.trust.store.path";
   private static final String INPUT_NATIVE_SSL_KEY_STORE_PATH = "cassandra.input.native.ssl.key.store.path";
   private static final String INPUT_NATIVE_SSL_TRUST_STORE_PASSWARD = "cassandra.input.native.ssl.trust.store.password";
   private static final String INPUT_NATIVE_SSL_KEY_STORE_PASSWARD = "cassandra.input.native.ssl.key.store.password";
   private static final String INPUT_NATIVE_SSL_CIPHER_SUITES = "cassandra.input.native.ssl.cipher.suites";
   private static final String INPUT_NATIVE_PROTOCOL_VERSION = "cassandra.input.native.protocol.version";
   private static final String OUTPUT_CQL = "cassandra.output.cql";
   private static final String OUTPUT_NATIVE_PORT = "cassandra.output.native.port";

   public CqlConfigHelper() {
   }

   public static void setInputColumns(Configuration conf, String columns) {
      if(columns != null && !columns.isEmpty()) {
         conf.set("cassandra.input.columnfamily.columns", columns);
      }
   }

   public static void setInputCQLPageRowSize(Configuration conf, String cqlPageRowSize) {
      if(cqlPageRowSize == null) {
         throw new UnsupportedOperationException("cql page row size may not be null");
      } else {
         conf.set("cassandra.input.page.row.size", cqlPageRowSize);
      }
   }

   public static void setInputWhereClauses(Configuration conf, String clauses) {
      if(clauses != null && !clauses.isEmpty()) {
         conf.set("cassandra.input.where.clause", clauses);
      }
   }

   public static void setOutputCql(Configuration conf, String cql) {
      if(cql != null && !cql.isEmpty()) {
         conf.set("cassandra.output.cql", cql);
      }
   }

   public static void setInputCql(Configuration conf, String cql) {
      if(cql != null && !cql.isEmpty()) {
         conf.set("cassandra.input.cql", cql);
      }
   }

   public static void setUserNameAndPassword(Configuration conf, String username, String password) {
      if(StringUtils.isNotBlank(username)) {
         conf.set("cassandra.input.native.auth.provider", PlainTextAuthProvider.class.getName());
         conf.set("cassandra.username", username);
         conf.set("cassandra.password", password);
      }

   }

   public static Optional<Integer> getInputCoreConnections(Configuration conf) {
      return getIntSetting("cassandra.input.native.core.connections.per.host", conf);
   }

   public static Optional<Integer> getInputMaxConnections(Configuration conf) {
      return getIntSetting("cassandra.input.native.max.connections.per.host", conf);
   }

   public static int getInputNativePort(Configuration conf) {
      return Integer.parseInt(conf.get("cassandra.input.native.port", "9042"));
   }

   public static int getOutputNativePort(Configuration conf) {
      return Integer.parseInt(conf.get("cassandra.output.native.port", "9042"));
   }

   public static Optional<Integer> getInputMaxSimultReqPerConnections(Configuration conf) {
      return getIntSetting("cassandra.input.native.max.simult.reqs.per.connection", conf);
   }

   public static Optional<Integer> getInputNativeConnectionTimeout(Configuration conf) {
      return getIntSetting("cassandra.input.native.connection.timeout", conf);
   }

   public static Optional<Integer> getInputNativeReadConnectionTimeout(Configuration conf) {
      return getIntSetting("cassandra.input.native.read.connection.timeout", conf);
   }

   public static Optional<Integer> getInputNativeReceiveBufferSize(Configuration conf) {
      return getIntSetting("cassandra.input.native.receive.buffer.size", conf);
   }

   public static Optional<Integer> getInputNativeSendBufferSize(Configuration conf) {
      return getIntSetting("cassandra.input.native.send.buffer.size", conf);
   }

   public static Optional<Integer> getInputNativeSolinger(Configuration conf) {
      return getIntSetting("cassandra.input.native.solinger", conf);
   }

   public static Optional<Boolean> getInputNativeTcpNodelay(Configuration conf) {
      return getBooleanSetting("cassandra.input.native.tcp.nodelay", conf);
   }

   public static Optional<Boolean> getInputNativeReuseAddress(Configuration conf) {
      return getBooleanSetting("cassandra.input.native.reuse.address", conf);
   }

   public static Optional<String> getInputNativeAuthProvider(Configuration conf) {
      return getStringSetting("cassandra.input.native.auth.provider", conf);
   }

   public static Optional<String> getInputNativeSSLTruststorePath(Configuration conf) {
      return getStringSetting("cassandra.input.native.ssl.trust.store.path", conf);
   }

   public static Optional<String> getInputNativeSSLKeystorePath(Configuration conf) {
      return getStringSetting("cassandra.input.native.ssl.key.store.path", conf);
   }

   public static Optional<String> getInputNativeSSLKeystorePassword(Configuration conf) {
      return getStringSetting("cassandra.input.native.ssl.key.store.password", conf);
   }

   public static Optional<String> getInputNativeSSLTruststorePassword(Configuration conf) {
      return getStringSetting("cassandra.input.native.ssl.trust.store.password", conf);
   }

   public static Optional<String> getInputNativeSSLCipherSuites(Configuration conf) {
      return getStringSetting("cassandra.input.native.ssl.cipher.suites", conf);
   }

   public static Optional<Boolean> getInputNativeKeepAlive(Configuration conf) {
      return getBooleanSetting("cassandra.input.native.keep.alive", conf);
   }

   public static String getInputcolumns(Configuration conf) {
      return conf.get("cassandra.input.columnfamily.columns");
   }

   public static Optional<Integer> getInputPageRowSize(Configuration conf) {
      return getIntSetting("cassandra.input.page.row.size", conf);
   }

   public static String getInputWhereClauses(Configuration conf) {
      return conf.get("cassandra.input.where.clause");
   }

   public static String getInputCql(Configuration conf) {
      return conf.get("cassandra.input.cql");
   }

   public static String getOutputCql(Configuration conf) {
      return conf.get("cassandra.output.cql");
   }

   private static Optional<Integer> getProtocolVersion(Configuration conf) {
      return getIntSetting("cassandra.input.native.protocol.version", conf);
   }

   public static Cluster getInputCluster(String host, Configuration conf) {
      return getInputCluster(new String[]{host}, conf);
   }

   public static Cluster getInputCluster(String[] hosts, Configuration conf) {
      int port = getInputNativePort(conf);
      return getCluster(hosts, conf, port);
   }

   public static Cluster getOutputCluster(String host, Configuration conf) {
      return getOutputCluster(new String[]{host}, conf);
   }

   public static Cluster getOutputCluster(String[] hosts, Configuration conf) {
      int port = getOutputNativePort(conf);
      return getCluster(hosts, conf, port);
   }

   public static Cluster getCluster(String[] hosts, Configuration conf, int port) {
      Optional<AuthProvider> authProvider = getAuthProvider(conf);
      Optional<SSLOptions> sslOptions = getSSLOptions(conf);
      Optional<Integer> protocolVersion = getProtocolVersion(conf);
      LoadBalancingPolicy loadBalancingPolicy = getReadLoadBalancingPolicy(hosts);
      SocketOptions socketOptions = getReadSocketOptions(conf);
      QueryOptions queryOptions = getReadQueryOptions(conf);
      PoolingOptions poolingOptions = getReadPoolingOptions(conf);
      Builder builder = Cluster.builder().addContactPoints(hosts).withPort(port).withCompression(Compression.NONE);
      if(authProvider.isPresent()) {
         builder.withAuthProvider((AuthProvider)authProvider.get());
      }

      if(sslOptions.isPresent()) {
         builder.withSSL((SSLOptions)sslOptions.get());
      }

      if(protocolVersion.isPresent()) {
         builder.withProtocolVersion(ProtocolVersion.fromInt(((Integer)protocolVersion.get()).intValue()));
      }

      builder.withLoadBalancingPolicy(loadBalancingPolicy).withSocketOptions(socketOptions).withQueryOptions(queryOptions).withPoolingOptions(poolingOptions);
      return builder.build();
   }

   public static void setInputCoreConnections(Configuration conf, String connections) {
      conf.set("cassandra.input.native.core.connections.per.host", connections);
   }

   public static void setInputMaxConnections(Configuration conf, String connections) {
      conf.set("cassandra.input.native.max.connections.per.host", connections);
   }

   public static void setInputMaxSimultReqPerConnections(Configuration conf, String reqs) {
      conf.set("cassandra.input.native.max.simult.reqs.per.connection", reqs);
   }

   public static void setInputNativeConnectionTimeout(Configuration conf, String timeout) {
      conf.set("cassandra.input.native.connection.timeout", timeout);
   }

   public static void setInputNativeReadConnectionTimeout(Configuration conf, String timeout) {
      conf.set("cassandra.input.native.read.connection.timeout", timeout);
   }

   public static void setInputNativeReceiveBufferSize(Configuration conf, String size) {
      conf.set("cassandra.input.native.receive.buffer.size", size);
   }

   public static void setInputNativeSendBufferSize(Configuration conf, String size) {
      conf.set("cassandra.input.native.send.buffer.size", size);
   }

   public static void setInputNativeSolinger(Configuration conf, String solinger) {
      conf.set("cassandra.input.native.solinger", solinger);
   }

   public static void setInputNativeTcpNodelay(Configuration conf, String tcpNodelay) {
      conf.set("cassandra.input.native.tcp.nodelay", tcpNodelay);
   }

   public static void setInputNativeAuthProvider(Configuration conf, String authProvider) {
      conf.set("cassandra.input.native.auth.provider", authProvider);
   }

   public static void setInputNativeSSLTruststorePath(Configuration conf, String path) {
      conf.set("cassandra.input.native.ssl.trust.store.path", path);
   }

   public static void setInputNativeSSLKeystorePath(Configuration conf, String path) {
      conf.set("cassandra.input.native.ssl.key.store.path", path);
   }

   public static void setInputNativeSSLKeystorePassword(Configuration conf, String pass) {
      conf.set("cassandra.input.native.ssl.key.store.password", pass);
   }

   public static void setInputNativeSSLTruststorePassword(Configuration conf, String pass) {
      conf.set("cassandra.input.native.ssl.trust.store.password", pass);
   }

   public static void setInputNativeSSLCipherSuites(Configuration conf, String suites) {
      conf.set("cassandra.input.native.ssl.cipher.suites", suites);
   }

   public static void setInputNativeReuseAddress(Configuration conf, String reuseAddress) {
      conf.set("cassandra.input.native.reuse.address", reuseAddress);
   }

   public static void setInputNativeKeepAlive(Configuration conf, String keepAlive) {
      conf.set("cassandra.input.native.keep.alive", keepAlive);
   }

   public static void setInputNativePort(Configuration conf, String port) {
      conf.set("cassandra.input.native.port", port);
   }

   private static PoolingOptions getReadPoolingOptions(Configuration conf) {
      Optional<Integer> coreConnections = getInputCoreConnections(conf);
      Optional<Integer> maxConnections = getInputMaxConnections(conf);
      Optional<Integer> maxSimultaneousRequests = getInputMaxSimultReqPerConnections(conf);
      PoolingOptions poolingOptions = new PoolingOptions();
      Iterator var5 = Arrays.asList(new HostDistance[]{HostDistance.LOCAL, HostDistance.REMOTE}).iterator();

      while(var5.hasNext()) {
         HostDistance hostDistance = (HostDistance)var5.next();
         if(coreConnections.isPresent()) {
            poolingOptions.setCoreConnectionsPerHost(hostDistance, ((Integer)coreConnections.get()).intValue());
         }

         if(maxConnections.isPresent()) {
            poolingOptions.setMaxConnectionsPerHost(hostDistance, ((Integer)maxConnections.get()).intValue());
         }

         if(maxSimultaneousRequests.isPresent()) {
            poolingOptions.setNewConnectionThreshold(hostDistance, ((Integer)maxSimultaneousRequests.get()).intValue());
         }
      }

      return poolingOptions;
   }

   private static QueryOptions getReadQueryOptions(Configuration conf) {
      String CL = ConfigHelper.getReadConsistencyLevel(conf);
      Optional<Integer> fetchSize = getInputPageRowSize(conf);
      QueryOptions queryOptions = new QueryOptions();
      if(CL != null && !CL.isEmpty()) {
         queryOptions.setConsistencyLevel(ConsistencyLevel.valueOf(CL));
      }

      if(fetchSize.isPresent()) {
         queryOptions.setFetchSize(((Integer)fetchSize.get()).intValue());
      }

      return queryOptions;
   }

   private static SocketOptions getReadSocketOptions(Configuration conf) {
      SocketOptions socketOptions = new SocketOptions();
      Optional<Integer> connectTimeoutMillis = getInputNativeConnectionTimeout(conf);
      Optional<Integer> readTimeoutMillis = getInputNativeReadConnectionTimeout(conf);
      Optional<Integer> receiveBufferSize = getInputNativeReceiveBufferSize(conf);
      Optional<Integer> sendBufferSize = getInputNativeSendBufferSize(conf);
      Optional<Integer> soLinger = getInputNativeSolinger(conf);
      Optional<Boolean> tcpNoDelay = getInputNativeTcpNodelay(conf);
      Optional<Boolean> reuseAddress = getInputNativeReuseAddress(conf);
      Optional<Boolean> keepAlive = getInputNativeKeepAlive(conf);
      if(connectTimeoutMillis.isPresent()) {
         socketOptions.setConnectTimeoutMillis(((Integer)connectTimeoutMillis.get()).intValue());
      }

      if(readTimeoutMillis.isPresent()) {
         socketOptions.setReadTimeoutMillis(((Integer)readTimeoutMillis.get()).intValue());
      }

      if(receiveBufferSize.isPresent()) {
         socketOptions.setReceiveBufferSize(((Integer)receiveBufferSize.get()).intValue());
      }

      if(sendBufferSize.isPresent()) {
         socketOptions.setSendBufferSize(((Integer)sendBufferSize.get()).intValue());
      }

      if(soLinger.isPresent()) {
         socketOptions.setSoLinger(((Integer)soLinger.get()).intValue());
      }

      if(tcpNoDelay.isPresent()) {
         socketOptions.setTcpNoDelay(((Boolean)tcpNoDelay.get()).booleanValue());
      }

      if(reuseAddress.isPresent()) {
         socketOptions.setReuseAddress(((Boolean)reuseAddress.get()).booleanValue());
      }

      if(keepAlive.isPresent()) {
         socketOptions.setKeepAlive(((Boolean)keepAlive.get()).booleanValue());
      }

      return socketOptions;
   }

   private static LoadBalancingPolicy getReadLoadBalancingPolicy(String[] stickHosts) {
      return new LimitedLocalNodeFirstLocalBalancingPolicy(stickHosts);
   }

   private static Optional<AuthProvider> getDefaultAuthProvider(Configuration conf) {
      Optional<String> username = getStringSetting("cassandra.username", conf);
      Optional<String> password = getStringSetting("cassandra.password", conf);
      return username.isPresent() && password.isPresent()?Optional.of(new PlainTextAuthProvider((String)username.get(), (String)password.get())):Optional.absent();
   }

   private static Optional<AuthProvider> getAuthProvider(Configuration conf) {
      Optional<String> authProvider = getInputNativeAuthProvider(conf);
      return !authProvider.isPresent()?getDefaultAuthProvider(conf):Optional.of(getClientAuthProvider((String)authProvider.get(), conf));
   }

   public static Optional<SSLOptions> getSSLOptions(Configuration conf) {
      Optional<String> truststorePath = getInputNativeSSLTruststorePath(conf);
      if(truststorePath.isPresent()) {
         Optional<String> keystorePath = getInputNativeSSLKeystorePath(conf);
         Optional<String> truststorePassword = getInputNativeSSLTruststorePassword(conf);
         Optional<String> keystorePassword = getInputNativeSSLKeystorePassword(conf);
         Optional cipherSuites = getInputNativeSSLCipherSuites(conf);

         SSLContext context;
         try {
            context = getSSLContext(truststorePath, truststorePassword, keystorePath, keystorePassword);
         } catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException | CertificateException | IOException | UnrecoverableKeyException var8) {
            throw new RuntimeException(var8);
         }

         String[] css = null;
         if(cipherSuites.isPresent()) {
            css = ((String)cipherSuites.get()).split(",");
         }

         return Optional.of(JdkSSLOptions.builder().withSSLContext(context).withCipherSuites(css).build());
      } else {
         return Optional.absent();
      }
   }

   private static Optional<Integer> getIntSetting(String parameter, Configuration conf) {
      String setting = conf.get(parameter);
      return setting == null?Optional.absent():Optional.of(Integer.valueOf(setting));
   }

   private static Optional<Boolean> getBooleanSetting(String parameter, Configuration conf) {
      String setting = conf.get(parameter);
      return setting == null?Optional.absent():Optional.of(Boolean.valueOf(setting));
   }

   private static Optional<String> getStringSetting(String parameter, Configuration conf) {
      String setting = conf.get(parameter);
      return setting == null?Optional.absent():Optional.of(setting);
   }

   private static AuthProvider getClientAuthProvider(String factoryClassName, Configuration conf) {
      try {
         Class<?> c = Class.forName(factoryClassName);
         if(PlainTextAuthProvider.class.equals(c)) {
            String username = (String)getStringSetting("cassandra.username", conf).or("");
            String password = (String)getStringSetting("cassandra.password", conf).or("");
            return (AuthProvider)c.getConstructor(new Class[]{String.class, String.class}).newInstance(new Object[]{username, password});
         } else {
            return (AuthProvider)c.newInstance();
         }
      } catch (Exception var5) {
         throw new RuntimeException("Failed to instantiate auth provider:" + factoryClassName, var5);
      }
   }

   private static SSLContext getSSLContext(Optional<String> truststorePath, Optional<String> truststorePassword, Optional<String> keystorePath, Optional<String> keystorePassword) throws NoSuchAlgorithmException, KeyStoreException, CertificateException, IOException, UnrecoverableKeyException, KeyManagementException {
      SSLContext ctx = SSLContext.getInstance("SSL");
      TrustManagerFactory tmf = null;
      if(truststorePath.isPresent()) {
         InputStream tsf = Files.newInputStream(Paths.get((String)truststorePath.get(), new String[0]), new OpenOption[0]);
         Throwable var7 = null;

         try {
            KeyStore ts = KeyStore.getInstance("JKS");
            ts.load(tsf, truststorePassword.isPresent()?((String)truststorePassword.get()).toCharArray():null);
            tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(ts);
         } catch (Throwable var31) {
            var7 = var31;
            throw var31;
         } finally {
            if(tsf != null) {
               if(var7 != null) {
                  try {
                     tsf.close();
                  } catch (Throwable var28) {
                     var7.addSuppressed(var28);
                  }
               } else {
                  tsf.close();
               }
            }

         }
      }

      KeyManagerFactory kmf = null;
      if(keystorePath.isPresent()) {
         InputStream ksf = Files.newInputStream(Paths.get((String)keystorePath.get(), new String[0]), new OpenOption[0]);
         Throwable var36 = null;

         try {
            KeyStore ks = KeyStore.getInstance("JKS");
            ks.load(ksf, keystorePassword.isPresent()?((String)keystorePassword.get()).toCharArray():null);
            kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(ks, keystorePassword.isPresent()?((String)keystorePassword.get()).toCharArray():null);
         } catch (Throwable var30) {
            var36 = var30;
            throw var30;
         } finally {
            if(ksf != null) {
               if(var36 != null) {
                  try {
                     ksf.close();
                  } catch (Throwable var29) {
                     var36.addSuppressed(var29);
                  }
               } else {
                  ksf.close();
               }
            }

         }
      }

      ctx.init(kmf != null?kmf.getKeyManagers():null, tmf != null?tmf.getTrustManagers():null, new SecureRandom());
      return ctx;
   }
}
