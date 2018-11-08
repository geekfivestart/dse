package com.datastax.bdp.config;

import com.datastax.bdp.transport.common.ServicePrincipal;
import com.datastax.bdp.util.MapBuilder;
import com.datastax.bdp.util.SSLUtil;
import com.google.common.collect.Sets;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.hadoop.util.StringUtils;

public abstract class AbstractPropertyBasedClientConfiguration implements ClientConfiguration {
   public static final String CASSANDRA_HOST = "cassandra.host";
   public static final String CASSANDRA_CONNECTION_RPC_PORT = "cassandra.connection.rpc.port";
   public static final String CASSANDRA_CONNECTION_NATIVE_PORT = "cassandra.connection.native.port";
   public static final String CASSANDRA_PARTITIONER = "cassandra.partitioner";
   public static final String CASSANDRA_CLIENT_TRANSPORT_FACTORY = "cassandra.client.transport.factory";
   public static final String CASSANDRA_DC_PROPERTY = "cassandra.dc";
   public static final String ANALYTICS_DATACENTERS = "cassandra.analyticsDataCenters";
   public static final String DSE_FS_PORT = "cassandra.dsefs.port";
   public static final String SSL_ENABLED = "cassandra.ssl.enabled";
   public static final String SSL_OPTIONAL = "cassandra.ssl.optional";
   public static final String SSL_PROTOCOL = "cassandra.ssl.protocol";
   public static final String SSL_ALGORITHM = "cassandra.ssl.algorithm";
   public static final String SSL_CIPHER_SUITES = "cassandra.ssl.cipherSuites";
   public static final String SSL_TRUST_STORE_PATH = "cassandra.ssl.trustStore.path";
   public static final String SSL_TRUST_STORE_TYPE = "cassandra.ssl.trustStore.type";
   public static final String SSL_TRUST_STORE_PASSWORD = "cassandra.ssl.trustStore.password";
   public static final String SSL_KEY_STORE_PATH = "cassandra.ssl.keyStore.path";
   public static final String SSL_KEY_STORE_TYPE = "cassandra.ssl.keyStore.type";
   public static final String SSL_KEY_STORE_PASSWORD = "cassandra.ssl.keyStore.password";
   public static final String KERBEROS_DEFAULT_SCHEME = "cassandra.auth.kerberos.defaultScheme";
   public static final String KERBEROS_ENABLED = "cassandra.auth.kerberos.enabled";
   public static final String DSE_SERVICE_PRINCIPAL = "dse.kerberos.principal";
   public static final String HTTP_SERVICE_PRINCIPAL = "cassandra.kerberos.httpServicePrincipal";
   public static final String SASL_QOP = "cassandra.sasl.qop";
   public static final String SASL_PROTOCOL_NAME = "dse.sasl.protocol";
   public static final String CDC_RAW_DIRECTORY = "cassandra.cdc_raw.directory";
   public static final String ADVANCED_REPLICATION_DIRECTORY = "dse.advanced_replication.directory";
   private static final Set<String> notExportableProps = Sets.newHashSet(new String[]{"cassandra.ssl.trustStore.path", "cassandra.ssl.trustStore.type", "cassandra.ssl.trustStore.password", "cassandra.ssl.keyStore.path", "cassandra.ssl.keyStore.type", "cassandra.ssl.keyStore.password"});

   public AbstractPropertyBasedClientConfiguration() {
   }

   public static Map<String, String> configAsMap(ClientConfiguration config) {
      Map<String, String> props = new HashMap();
      MapBuilder.putIfNotNull(props, "cassandra.host", stringify(config.getCassandraHost()));
      MapBuilder.putIfNotNull(props, "cassandra.connection.native.port", String.valueOf(config.getNativePort()));
      MapBuilder.putIfNotNull(props, "cassandra.partitioner", config.getPartitionerClassName());
      MapBuilder.putIfNotNull(props, "cassandra.ssl.enabled", String.valueOf(config.isSslEnabled()));
      MapBuilder.putIfNotNull(props, "cassandra.ssl.optional", String.valueOf(config.isSslOptional()));
      if(config.isSslEnabled()) {
         MapBuilder.putIfNotNull(props, "cassandra.ssl.protocol", config.getSslProtocol());
         MapBuilder.putIfNotNull(props, "cassandra.ssl.algorithm", config.getSslAlgorithm());
         MapBuilder.putIfNotNull(props, "cassandra.ssl.cipherSuites", stringify(config.getCipherSuites()));
         MapBuilder.putIfNotNull(props, "cassandra.ssl.trustStore.path", config.getSslTruststorePath());
         MapBuilder.putIfNotNull(props, "cassandra.ssl.trustStore.type", config.getSslTruststoreType());
         MapBuilder.putIfNotNull(props, "cassandra.ssl.trustStore.password", config.getSslTruststorePassword());
         MapBuilder.putIfNotNull(props, "cassandra.ssl.keyStore.path", config.getSslKeystorePath());
         MapBuilder.putIfNotNull(props, "cassandra.ssl.keyStore.type", config.getSslKeystoreType());
         MapBuilder.putIfNotNull(props, "cassandra.ssl.keyStore.password", config.getSslKeystorePassword());
      }

      MapBuilder.putIfNotNull(props, "cassandra.auth.kerberos.enabled", String.valueOf(config.isKerberosEnabled()));
      MapBuilder.putIfNotNull(props, "cassandra.auth.kerberos.defaultScheme", String.valueOf(config.isKerberosDefaultScheme()));
      if(config.isKerberosEnabled()) {
         MapBuilder.putIfNotNull(props, "dse.kerberos.principal", stringify(config.getDseServicePrincipal()));
         MapBuilder.putIfNotNull(props, "cassandra.kerberos.httpServicePrincipal", stringify(config.getHttpServicePrincipal()));
         MapBuilder.putIfNotNull(props, "cassandra.sasl.qop", config.getSaslQop());
         MapBuilder.putIfNotNull(props, "dse.sasl.protocol", config.getSaslProtocolName());
      }

      MapBuilder.putIfNotNull(props, "cassandra.dsefs.port", String.valueOf(config.getDseFsPort()));
      MapBuilder.putIfNotNull(props, "dse.advanced_replication.directory", config.getAdvancedReplicationDirectory());
      return props;
   }

   public static boolean isSafeToExport(String key) {
      return !notExportableProps.contains(key);
   }

   private static String stringify(String[] strings) {
      return strings != null?StringUtils.join(",", strings):null;
   }

   private static String stringify(ServicePrincipal servicePrincipal) {
      return servicePrincipal != null?servicePrincipal.asPattern():null;
   }

   private static String stringify(InetAddress address) {
      return address != null?address.getHostAddress():null;
   }

   public abstract String get(String var1);

   public String get(String key, String defaultValue) {
      return (String)ObjectUtils.defaultIfNull(this.get(key), defaultValue);
   }

   public boolean getBoolean(String key, boolean defaultValue) {
      return Boolean.valueOf(this.get(key, String.valueOf(defaultValue))).booleanValue();
   }

   public int getInt(String key, int defaultValue) {
      return Integer.parseInt(this.get(key, String.valueOf(defaultValue)));
   }

   public String[] getStrings(String key, String[] defaultValue) {
      String v = this.get(key);
      return v != null?v.trim().split("\\s*,\\s*"):defaultValue;
   }

   public InetAddress getInetAddress(String key) {
      String address = this.get(key);
      if(address != null) {
         try {
            return InetAddress.getByName(address);
         } catch (UnknownHostException var4) {
            throw new RuntimeException("Could not resolve " + address, var4);
         }
      } else {
         try {
            return InetAddress.getLocalHost();
         } catch (UnknownHostException var5) {
            throw new RuntimeException("Could not resolve local host address", var5);
         }
      }
   }

   public ServicePrincipal getServicePrincipal(String key) {
      String servicePrincipalString = this.get(key);
      return servicePrincipalString != null?new ServicePrincipal(servicePrincipalString):null;
   }

   public ServicePrincipal getDseServicePrincipal() {
      return this.getServicePrincipal("dse.kerberos.principal");
   }

   public String getSaslProtocolName() {
      return this.get("dse.sasl.protocol");
   }

   public boolean isSslEnabled() {
      return this.getBoolean("cassandra.ssl.enabled", false);
   }

   public boolean isSslOptional() {
      return this.getBoolean("cassandra.ssl.optional", false);
   }

   public String getSslKeystorePath() {
      return this.get("cassandra.ssl.keyStore.path");
   }

   public String getSslKeystorePassword() {
      return this.get("cassandra.ssl.keyStore.password");
   }

   public String getSslKeystoreType() {
      return this.get("cassandra.ssl.keyStore.type", "JKS");
   }

   public String getSslTruststorePath() {
      return this.get("cassandra.ssl.trustStore.path");
   }

   public String getSslTruststorePassword() {
      return this.get("cassandra.ssl.trustStore.password");
   }

   public String getSslTruststoreType() {
      return this.get("cassandra.ssl.trustStore.type", "JKS");
   }

   public String getSslProtocol() {
      return this.get("cassandra.ssl.protocol", "TLS");
   }

   public String getSslAlgorithm() {
      return this.get("cassandra.ssl.algorithm", "SunX509");
   }

   public String[] getCipherSuites() {
      return this.getStrings("cassandra.ssl.cipherSuites", SSLUtil.DEFAULT_CIPHER_SUITES);
   }

   public boolean isKerberosEnabled() {
      return this.getBoolean("cassandra.auth.kerberos.enabled", false);
   }

   public boolean isKerberosDefaultScheme() {
      return this.getBoolean("cassandra.auth.kerberos.defaultScheme", false);
   }

   public String getSaslQop() {
      return this.get("cassandra.sasl.qop", "auth");
   }

   public String getPartitionerClassName() {
      return this.get("cassandra.partitioner", Murmur3Partitioner.class.getCanonicalName());
   }

   public ServicePrincipal getHttpServicePrincipal() {
      return this.getServicePrincipal("cassandra.kerberos.httpServicePrincipal");
   }

   public InetAddress getCassandraHost() {
      return this.getInetAddress("cassandra.host");
   }

   public int getNativePort() {
      return this.getInt("cassandra.connection.native.port", 9042);
   }

   public int getDseFsPort() {
      return this.getInt("cassandra.dsefs.port", 5598);
   }

   public String getCdcRawDirectory() {
      return this.get("cassandra.cdc_raw.directory", "/var/lib/cassandra/cdc_raw");
   }

   public String getAdvancedReplicationDirectory() {
      return DseConfigUtil.getAdvancedReplicationDirectory(this.get("dse.advanced_replication.directory", (String)null), this::getCdcRawDirectory);
   }
}
