package com.datastax.bdp.config;

import com.datastax.bdp.cassandra.auth.AuthenticationScheme;
import com.datastax.bdp.cassandra.auth.DseAuthenticator;
import com.datastax.bdp.cassandra.auth.KerberosAuthenticator;
import com.datastax.bdp.transport.common.ServicePrincipal;
import com.datastax.bdp.util.DseUtil;
import com.google.common.annotations.VisibleForTesting;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class YamlClientConfiguration implements ClientConfiguration {
   private static final Logger logger = LoggerFactory.getLogger(YamlClientConfiguration.class);
   private final org.apache.cassandra.config.Config cassandraConfig;
   private final Config dseConfig;

   @VisibleForTesting
   public YamlClientConfiguration(org.apache.cassandra.config.Config cassandraConfig, Config dseConfig) {
      this.cassandraConfig = cassandraConfig;
      this.dseConfig = dseConfig;
   }

   public YamlClientConfiguration() throws ConfigurationException {
      this(DseConfigurationLoader.getConfig(), loadDseConfig());
   }

   @VisibleForTesting
   public static Config loadDseConfig() throws ConfigurationException {
      return DseConfigYamlLoader.config;
   }

   private InetAddress getNativeTransportAddress() throws UnknownHostException, SocketException {
      return this.nativeTransportAddress() != null?InetAddress.getByName(this.nativeTransportAddress()):(this.nativeTransportInterface() != null?getListenInterface(this.nativeTransportInterface(), this.nativeTransportInterfacePreferIPv6()):(this.cassandraConfig.listen_address != null?InetAddress.getByName(this.cassandraConfig.listen_address):(this.cassandraConfig.listen_interface != null?getListenInterface(this.cassandraConfig.listen_interface, Boolean.valueOf(this.cassandraConfig.listen_interface_prefer_ipv6)):InetAddress.getLocalHost())));
   }

   @VisibleForTesting
   String nativeTransportAddress() {
      return null != this.cassandraConfig.rpc_address?this.cassandraConfig.rpc_address:this.cassandraConfig.native_transport_address;
   }

   @VisibleForTesting
   String nativeTransportInterface() {
      return null != this.cassandraConfig.rpc_interface?this.cassandraConfig.rpc_interface:this.cassandraConfig.native_transport_interface;
   }

   @VisibleForTesting
   Boolean nativeTransportInterfacePreferIPv6() {
      return null != this.cassandraConfig.rpc_interface_prefer_ipv6?this.cassandraConfig.rpc_interface_prefer_ipv6:this.cassandraConfig.native_transport_interface_prefer_ipv6;
   }

   @VisibleForTesting
   String nativeTransportBroadcastAddress() {
      return null != this.cassandraConfig.broadcast_rpc_address?this.cassandraConfig.broadcast_rpc_address:this.cassandraConfig.native_transport_broadcast_address;
   }

   private static InetAddress getListenInterface(String listenInterface, Boolean preferIpv6) throws SocketException {
      InetAddress retval = null;
      Enumeration addrs = NetworkInterface.getByName(listenInterface).getInetAddresses();

      while(addrs.hasMoreElements()) {
         InetAddress temp = (InetAddress)addrs.nextElement();
         if(preferIpv6.booleanValue() && temp instanceof Inet6Address || !preferIpv6.booleanValue() && temp instanceof Inet4Address) {
            return temp;
         }

         if(retval == null) {
            retval = temp;
         }
      }

      return retval;
   }

   public ServicePrincipal getDseServicePrincipal() {
      ServicePrincipal principal = null;
      if(this.dseConfig.kerberos_options != null && this.dseConfig.kerberos_options.service_principal != null) {
         principal = new ServicePrincipal(this.dseConfig.kerberos_options.service_principal.trim());
      }

      logger.debug("Resolved service principal to {}", principal);
      return principal;
   }

   public ServicePrincipal getHttpServicePrincipal() {
      ServicePrincipal principal = null;
      if(this.dseConfig.kerberos_options != null && this.dseConfig.kerberos_options.http_principal != null) {
         principal = new ServicePrincipal(this.dseConfig.kerberos_options.http_principal.trim());
      }

      logger.debug("Resolved http service principal to {}", principal);
      return principal;
   }

   public boolean isSslEnabled() {
      return this.cassandraConfig.client_encryption_options.enabled;
   }

   public boolean isSslOptional() {
      return this.cassandraConfig.client_encryption_options.optional;
   }

   public String getSslKeystorePath() {
      String keystore = this.cassandraConfig.client_encryption_options.keystore;
      return keystore == null?null:DseUtil.makeAbsolute(keystore);
   }

   public String getSslKeystorePassword() {
      return this.cassandraConfig.client_encryption_options.keystore_password;
   }

   public String getSslKeystoreType() {
      return this.cassandraConfig.client_encryption_options.store_type;
   }

   public String getSslTruststorePath() {
      String truststore = this.cassandraConfig.client_encryption_options.truststore;
      return truststore == null?null:DseUtil.makeAbsolute(truststore);
   }

   public String getSslTruststorePassword() {
      return this.cassandraConfig.client_encryption_options.truststore_password;
   }

   public String getSslTruststoreType() {
      return this.cassandraConfig.client_encryption_options.store_type;
   }

   public String getSslProtocol() {
      return this.cassandraConfig.client_encryption_options.protocol;
   }

   public String getSslAlgorithm() {
      return DseConfigurationLoader.getClientEncryptionAlgorithm();
   }

   public String[] getCipherSuites() {
      return this.cassandraConfig.client_encryption_options.enabled?this.cassandraConfig.client_encryption_options.cipher_suites:null;
   }

   public boolean isKerberosDefaultScheme() {
      if(KerberosAuthenticator.class.getName().equals(this.cassandraConfig.authenticator)) {
         return true;
      } else {
         if(Boolean.TRUE.equals(this.dseConfig.authentication_options.enabled) && DseAuthenticator.class.getName().equals(this.cassandraConfig.authenticator)) {
            String name = AuthenticationScheme.KERBEROS.name();
            if(name.equalsIgnoreCase(StringUtils.stripToNull(this.dseConfig.authentication_options.default_scheme))) {
               return true;
            }
         }

         return false;
      }
   }

   public boolean isKerberosEnabled() {
      if(this.isKerberosDefaultScheme()) {
         return true;
      } else {
         if(Boolean.TRUE.equals(this.dseConfig.authentication_options.enabled) && DseAuthenticator.class.getName().equals(this.cassandraConfig.authenticator)) {
            String name = AuthenticationScheme.KERBEROS.name();
            Set<String> rawParam = this.dseConfig.authentication_options.other_schemes;
            if(rawParam != null) {
               Iterator var3 = rawParam.iterator();

               while(var3.hasNext()) {
                  String scheme = (String)var3.next();
                  if(name.equalsIgnoreCase(StringUtils.stripToNull(scheme))) {
                     return true;
                  }
               }
            }
         }

         return false;
      }
   }

   public String getSaslQop() {
      String param = StringUtils.stripToNull(this.dseConfig.kerberos_options.qop);
      String result;
      if(param == null) {
         result = "auth";
         logger.debug("kerberos_options.qop is missing; defaulting to {}", result);
      } else {
         result = param;
         logger.debug("Resolved kerberos_options.qop = {}", param);
      }

      return result;
   }

   public String getSaslProtocolName() {
      String fromProp = System.getProperty("dse.sasl.protocol");
      return StringUtils.isNotBlank(fromProp)?fromProp:(this.dseConfig.kerberos_options != null && this.dseConfig.kerberos_options.service_principal != null?(new ServicePrincipal(this.dseConfig.kerberos_options.service_principal)).service:"dse");
   }

   public int getNativePort() {
      return this.isSslEnabled() && this.cassandraConfig.native_transport_port_ssl != null?this.cassandraConfig.native_transport_port_ssl.intValue():Integer.parseInt(System.getProperty("cassandra.native_transport_port", String.valueOf(this.cassandraConfig.native_transport_port)));
   }

   public InetAddress getCassandraHost() {
      try {
         return this.nativeTransportBroadcastAddress() == null?this.getNativeTransportAddress():InetAddress.getByName(this.nativeTransportBroadcastAddress());
      } catch (SocketException | UnknownHostException var2) {
         throw new RuntimeException("Unable to locate Cassandra host", var2);
      }
   }

   public String getPartitionerClassName() {
      String partitionerClassName = System.getProperty("cassandra.partitioner", this.cassandraConfig.partitioner);
      return partitionerClassName.contains(".")?partitionerClassName:"org.apache.cassandra.dht." + partitionerClassName;
   }

   public int getDseFsPort() {
      String port = StringUtils.stripToNull(this.dseConfig.dsefs_options.public_port);
      int result;
      if(port == null) {
         result = 5598;
         logger.debug("dsefs_options.public_port is missing; defaulting to {}", Integer.valueOf(result));
      } else {
         result = Integer.valueOf(port).intValue();
         logger.debug("dsefs_options.public_port = {}", Integer.valueOf(result));
      }

      return result;
   }

   public String getCdcRawDirectory() {
      return this.cassandraConfig.cdc_raw_directory;
   }

   public String getAdvancedReplicationDirectory() {
      return DseConfigUtil.getAdvancedReplicationDirectory(this.dseConfig.advanced_replication_options.advanced_replication_directory, this::getCdcRawDirectory);
   }
}
