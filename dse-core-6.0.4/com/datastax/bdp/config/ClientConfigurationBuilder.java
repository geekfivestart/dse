package com.datastax.bdp.config;

import com.datastax.bdp.transport.common.ServicePrincipal;
import com.diffplug.common.base.Errors;
import com.diffplug.common.base.Throwing.Function;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import org.apache.commons.beanutils.PropertyUtils;

public class ClientConfigurationBuilder {
   private final ClientConfigurationBuilder.CustomClientConfiguration configuration;

   public ClientConfigurationBuilder() {
      this.configuration = new ClientConfigurationBuilder.CustomClientConfiguration();
   }

   public ClientConfigurationBuilder(ClientConfiguration source) {
      this.configuration = ClientConfigurationBuilder.CustomClientConfiguration.createFrom(source);
   }

   public ClientConfiguration build() {
      return this.configuration.clone();
   }

   public ClientConfigurationBuilder withDseServicePrincipal(ServicePrincipal dseServicePrincipal) {
      this.configuration.dseServicePrincipal = dseServicePrincipal;
      return this;
   }

   public ClientConfigurationBuilder withHttpServicePrincipal(ServicePrincipal httpServicePrincipal) {
      this.configuration.httpServicePrincipal = httpServicePrincipal;
      return this;
   }

   public ClientConfigurationBuilder withSaslProtocolName(String saslProtocolName) {
      this.configuration.saslProtocolName = saslProtocolName;
      return this;
   }

   public ClientConfigurationBuilder withSslEnabled(boolean sslEnabled) {
      this.configuration.sslEnabled = sslEnabled;
      return this;
   }

   public ClientConfigurationBuilder withSslOptional(boolean sslOptional) {
      this.configuration.sslOptional = sslOptional;
      return this;
   }

   public ClientConfigurationBuilder withSslKeystorePath(String sslKeystorePath) {
      this.configuration.sslKeystorePath = sslKeystorePath;
      return this;
   }

   public ClientConfigurationBuilder withSslKeystorePassword(String sslKeystorePassword) {
      this.configuration.sslKeystorePassword = sslKeystorePassword;
      return this;
   }

   public ClientConfigurationBuilder withSslKeystoreType(String sslKeystoreType) {
      this.configuration.sslKeystoreType = sslKeystoreType;
      return this;
   }

   public ClientConfigurationBuilder withSslTruststorePath(String sslTruststorePath) {
      this.configuration.sslTruststorePath = sslTruststorePath;
      return this;
   }

   public ClientConfigurationBuilder withSslTruststorePassword(String sslTruststorePassword) {
      this.configuration.sslTruststorePassword = sslTruststorePassword;
      return this;
   }

   public ClientConfigurationBuilder withSslTruststoreType(String sslTruststoreType) {
      this.configuration.sslTruststoreType = sslTruststoreType;
      return this;
   }

   public ClientConfigurationBuilder withSslProtocol(String sslProtocol) {
      this.configuration.sslProtocol = sslProtocol;
      return this;
   }

   public ClientConfigurationBuilder withSslAlgorithm(String sslAlgorithm) {
      this.configuration.sslAlgorithm = sslAlgorithm;
      return this;
   }

   public ClientConfigurationBuilder withCipherSuites(String[] cipherSuites) {
      this.configuration.cipherSuites = cipherSuites;
      return this;
   }

   public ClientConfigurationBuilder withKerberosEnabled(boolean kerberosEnabled) {
      this.configuration.kerberosEnabled = kerberosEnabled;
      return this;
   }

   public ClientConfigurationBuilder withSaslQop(String saslQop) {
      this.configuration.saslQop = saslQop;
      return this;
   }

   public ClientConfigurationBuilder withRpcPort(int rpcPort) {
      this.configuration.rpcPort = rpcPort;
      return this;
   }

   public ClientConfigurationBuilder withNativePort(int nativePort) {
      this.configuration.nativePort = nativePort;
      return this;
   }

   public ClientConfigurationBuilder withClientTransportFactory(String clientTransportFactory) {
      this.configuration.clientTransportFactory = clientTransportFactory;
      return this;
   }

   public ClientConfigurationBuilder withCassandraHost(InetAddress cassandraHost) {
      this.configuration.cassandraHost = cassandraHost;
      this.configuration.cassandraHosts = new InetAddress[]{cassandraHost};
      return this;
   }

   public ClientConfigurationBuilder withCassandraHosts(InetAddress... cassandraHosts) {
      this.configuration.cassandraHost = cassandraHosts.length > 0?cassandraHosts[0]:null;
      this.configuration.cassandraHosts = cassandraHosts;
      return this;
   }

   public ClientConfigurationBuilder withCassandraHosts(String... cassandraHosts) {
      InetAddress[] inetAddresses = (InetAddress[])Arrays.stream(cassandraHosts).map(Errors.rethrow().wrap(InetAddress::getByName)).toArray((x$0) -> {
         return new InetAddress[x$0];
      });
      return this.withCassandraHosts(inetAddresses);
   }

   public ClientConfigurationBuilder withPartitionerClassName(String partitionerClassName) {
      this.configuration.partitionerClassName = partitionerClassName;
      return this;
   }

   public ClientConfigurationBuilder withDseFsPort(int dseFsPort) {
      this.configuration.dseFsPort = dseFsPort;
      return this;
   }

   public ClientConfigurationBuilder withKerberosDefaultScheme(boolean kerberosDefaultScheme) {
      this.configuration.kerberosDefaultScheme = kerberosDefaultScheme;
      return this;
   }

   public ClientConfigurationBuilder withCdcRawDirectory(String directory) {
      this.configuration.cdcRawDirectory = directory;
      return this;
   }

   public ClientConfigurationBuilder withAdvancedReplicationDirectory(String directory) {
      this.configuration.advancedReplicationDirectory = directory;
      return this;
   }

   private static class CustomClientConfiguration implements ClientConfiguration {
      private ServicePrincipal dseServicePrincipal;
      private ServicePrincipal httpServicePrincipal;
      private String saslProtocolName;
      private boolean sslEnabled;
      private boolean sslOptional;
      private String sslKeystorePath;
      private String sslKeystorePassword;
      private String sslKeystoreType;
      private String sslTruststorePath;
      private String sslTruststorePassword;
      private String sslTruststoreType;
      private String sslProtocol;
      private String sslAlgorithm;
      private String[] cipherSuites;
      private boolean kerberosEnabled;
      private boolean kerberosDefaultScheme;
      private String saslQop;
      private int rpcPort;
      private int nativePort;
      private String clientTransportFactory;
      private InetAddress cassandraHost;
      private InetAddress[] cassandraHosts;
      private String partitionerClassName;
      private int dseFsPort;
      private String cdcRawDirectory;
      private String advancedReplicationDirectory;

      private CustomClientConfiguration() {
         this.cassandraHosts = new InetAddress[0];
      }

      private static ClientConfigurationBuilder.CustomClientConfiguration createFrom(ClientConfiguration source) {
         ClientConfigurationBuilder.CustomClientConfiguration newConfig = new ClientConfigurationBuilder.CustomClientConfiguration();
         Arrays.stream(PropertyUtils.getPropertyDescriptors(ClientConfiguration.class)).forEach((pd) -> {
            try {
               Object v = pd.getReadMethod().invoke(source, new Object[0]);
               Field f = ClientConfigurationBuilder.CustomClientConfiguration.class.getDeclaredField(pd.getName());
               f.setAccessible(true);
               f.set(newConfig, v);
            } catch (InvocationTargetException | NoSuchFieldException | IllegalAccessException var5) {
               throw new AssertionError(var5.getMessage(), var5);
            }
         });
         return newConfig;
      }

      public ServicePrincipal getDseServicePrincipal() {
         return this.dseServicePrincipal;
      }

      public ServicePrincipal getHttpServicePrincipal() {
         return this.httpServicePrincipal;
      }

      public String getSaslProtocolName() {
         return this.saslProtocolName;
      }

      public boolean isSslEnabled() {
         return this.sslEnabled;
      }

      public boolean isSslOptional() {
         return this.sslOptional;
      }

      public String getSslKeystorePath() {
         return this.sslKeystorePath;
      }

      public String getSslKeystorePassword() {
         return this.sslKeystorePassword;
      }

      public String getSslKeystoreType() {
         return this.sslKeystoreType;
      }

      public String getSslTruststorePath() {
         return this.sslTruststorePath;
      }

      public String getSslTruststorePassword() {
         return this.sslTruststorePassword;
      }

      public String getSslTruststoreType() {
         return this.sslTruststoreType;
      }

      public String getSslProtocol() {
         return this.sslProtocol;
      }

      public String getSslAlgorithm() {
         return this.sslAlgorithm;
      }

      public String[] getCipherSuites() {
         return this.cipherSuites;
      }

      public boolean isKerberosEnabled() {
         return this.kerberosEnabled;
      }

      public boolean isKerberosDefaultScheme() {
         return this.kerberosDefaultScheme;
      }

      public String getSaslQop() {
         return this.saslQop;
      }

      public int getNativePort() {
         return this.nativePort;
      }

      public InetAddress getCassandraHost() {
         return this.cassandraHost;
      }

      public InetAddress[] getCassandraHosts() {
         return this.cassandraHosts;
      }

      public String getPartitionerClassName() {
         return this.partitionerClassName;
      }

      public int getDseFsPort() {
         return this.dseFsPort;
      }

      public String getCdcRawDirectory() {
         return this.cdcRawDirectory;
      }

      public String getAdvancedReplicationDirectory() {
         return DseConfigUtil.getAdvancedReplicationDirectory(this.advancedReplicationDirectory, this::getCdcRawDirectory);
      }

      protected ClientConfigurationBuilder.CustomClientConfiguration clone() {
         return createFrom(this);
      }
   }
}
