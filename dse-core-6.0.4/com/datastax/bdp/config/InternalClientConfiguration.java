package com.datastax.bdp.config;

import com.datastax.bdp.server.DseDaemon;
import com.datastax.bdp.transport.common.ServicePrincipal;
import com.datastax.bdp.util.Addresses;
import java.net.InetAddress;
import org.apache.cassandra.config.DatabaseDescriptor;

public class InternalClientConfiguration implements ClientConfiguration {
   private static final ClientConfiguration instance = new InternalClientConfiguration();

   private InternalClientConfiguration() {
   }

   private static boolean isDseDaemon() {
      try {
         return DseDaemon.isSetup();
      } catch (NoClassDefFoundError var1) {
         return false;
      }
   }

   public static ClientConfiguration getInstance() {
      if(!isDseDaemon()) {
         DatabaseDescriptor.toolInitialization(false);
         DatabaseDescriptor.applyAddressConfig();
         DseConfig.init();
      }

      return instance;
   }

   public ServicePrincipal getDseServicePrincipal() {
      return DseConfig.getDseServicePrincipal();
   }

   public ServicePrincipal getHttpServicePrincipal() {
      return DseConfig.getHttpKrbprincipal();
   }

   public String getSaslProtocolName() {
      ServicePrincipal principal = this.getDseServicePrincipal();
      return principal != null?principal.service:"dse";
   }

   public boolean isSslEnabled() {
      return DseConfig.isSslEnabled();
   }

   public boolean isSslOptional() {
      return DseConfig.isSslOptional();
   }

   public String getSslKeystorePath() {
      return DseConfig.getSslKeystorePath();
   }

   public String getSslKeystorePassword() {
      return DseConfig.getSslKeystorePassword();
   }

   public String getSslKeystoreType() {
      return DseConfig.getSslKeystoreType();
   }

   public String getSslTruststorePath() {
      return DseConfig.getSslTruststorePath();
   }

   public String getSslTruststorePassword() {
      return DseConfig.getSslTruststorePassword();
   }

   public String getSslTruststoreType() {
      return DseConfig.getSslTruststoreType();
   }

   public String getSslProtocol() {
      return DseConfig.getSslProtocol();
   }

   public String getSslAlgorithm() {
      return DseConfigurationLoader.getClientEncryptionAlgorithm();
   }

   public String[] getCipherSuites() {
      return DseConfig.getCipherSuites();
   }

   public boolean isKerberosEnabled() {
      return DseConfig.isKerberosEnabled();
   }

   public boolean isKerberosDefaultScheme() {
      return DseConfig.isKerberosDefaultScheme();
   }

   public String getSaslQop() {
      return DseConfig.getSaslQop();
   }

   public int getNativePort() {
      return this.isSslEnabled()?DatabaseDescriptor.getNativeTransportPortSSL():DatabaseDescriptor.getNativeTransportPort();
   }

   public InetAddress getCassandraHost() {
      return Addresses.Client.getBroadcastAddress();
   }

   public int getDseFsPort() {
      return DseFsConfig.getPublicPort();
   }

   public String getPartitionerClassName() {
      return DatabaseDescriptor.getPartitionerName();
   }

   public String getCdcRawDirectory() {
      return DatabaseDescriptor.getCDCLogLocation();
   }

   public String getAdvancedReplicationDirectory() {
      return DseConfig.getAdvRepBaseDirectory();
   }
}
