package com.datastax.bdp.config;

import com.datastax.bdp.cassandra.crypto.ConfigDecryptor;
import com.datastax.bdp.db.audit.AuditLoggingOptions;
import com.datastax.bdp.db.audit.SLF4JAuditWriter;
import com.datastax.bdp.server.DseDaemon;
import com.datastax.bdp.snitch.DseDelegateSnitch;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import org.apache.cassandra.config.YamlConfigurationLoader;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions.InternodeEncryption;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DseConfigurationLoader extends YamlConfigurationLoader {
   private static final Logger logger = LoggerFactory.getLogger(DseConfigurationLoader.class);
   private static String initialEndpointSnitch;
   private static String cassandraYamlLocation;
   private static String server_encryption_algorithm;
   private static String client_encryption_algorithm;
   private static final AtomicReference<org.apache.cassandra.config.Config> configRef = new AtomicReference();

   public DseConfigurationLoader() {
   }

   public static org.apache.cassandra.config.Config getConfig() throws ConfigurationException {
      return (org.apache.cassandra.config.Config)configRef.updateAndGet((previous) -> {
         return previous == null?(new DseConfigurationLoader()).loadConfig():previous;
      });
   }

   public org.apache.cassandra.config.Config loadConfig() throws ConfigurationException {
      org.apache.cassandra.config.Config cassandraYamlConfig = super.loadConfig();
      String configuredSnitch = cassandraYamlConfig.endpoint_snitch;
      if(null == configuredSnitch) {
         throw new ConfigurationException("Missing endpoint_snitch directive");
      } else if(configuredSnitch.equals(DseDelegateSnitch.class.getCanonicalName())) {
         throw new ConfigurationException("DseDelegateSnitch is now handled automatically; please set endpoint_snitch in cassandra.yaml to any valid IEndpointSnitch (probably whatever is configured for delegated_snitch in dse.yaml).  Also remove delegated_snitch from dse.yaml if it is still set there.");
      } else {
         initialEndpointSnitch = configuredSnitch;
         logger.debug("Cassandra configuration specifies {}, forcing to DseDelegateSnitch", initialEndpointSnitch);
         if(!initialEndpointSnitch.contains(".")) {
            initialEndpointSnitch = "org.apache.cassandra.locator." + initialEndpointSnitch;
         }

         cassandraYamlConfig.endpoint_snitch = DseDelegateSnitch.class.getCanonicalName();

         try {
            if(cassandraYamlConfig.server_encryption_options != null) {
               server_encryption_algorithm = cassandraYamlConfig.server_encryption_options.algorithm;
               cassandraYamlConfig.server_encryption_options.algorithm = "DseServerReloadableTrustManager";
            }

            if(cassandraYamlConfig.client_encryption_options != null) {
               client_encryption_algorithm = cassandraYamlConfig.client_encryption_options.algorithm;
               cassandraYamlConfig.client_encryption_options.algorithm = "DseClientReloadableTrustManager";
            }

            if(cassandraYamlConfig.server_encryption_options.internode_encryption != InternodeEncryption.none) {
               cassandraYamlConfig.server_encryption_options.keystore_password = ConfigDecryptor.getInstance().decryptIfActive(cassandraYamlConfig.server_encryption_options.keystore_password);
               cassandraYamlConfig.server_encryption_options.truststore_password = ConfigDecryptor.getInstance().decryptIfActive(cassandraYamlConfig.server_encryption_options.truststore_password);
            }

            if(cassandraYamlConfig.client_encryption_options.enabled) {
               cassandraYamlConfig.client_encryption_options.keystore_password = ConfigDecryptor.getInstance().decryptIfActive(cassandraYamlConfig.client_encryption_options.keystore_password);
               cassandraYamlConfig.client_encryption_options.truststore_password = ConfigDecryptor.getInstance().decryptIfActive(cassandraYamlConfig.client_encryption_options.truststore_password);
            }
         } catch (IOException var5) {
            throw new ConfigurationException("Error decrypting password", var5);
         }

         try {
            Method method = YamlConfigurationLoader.class.getDeclaredMethod("getStorageConfigURL", new Class[0]);
            method.setAccessible(true);
            cassandraYamlLocation = ((URL)method.invoke(this, new Object[0])).getFile();
         } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException var4) {
            logger.debug("Couldn't get location of cassandra.yaml: " + var4.getLocalizedMessage());
         }

         if(!DseDaemon.isDaemonMode()) {
            if(cassandraYamlConfig.data_file_directories == null || cassandraYamlConfig.data_file_directories.length == 0) {
               String defaultDataDir = System.getProperty("cassandra.storagedir", (String)null);
               if(defaultDataDir == null) {
                  throw new ConfigurationException("data_file_directories is not missing and -Dcassandra.storagedir is not set", false);
               }

               cassandraYamlConfig.data_file_directories = new String[]{defaultDataDir + File.separator + "data"};
            }

            cassandraYamlConfig.data_file_directories = (String[])Stream.concat(Arrays.stream(cassandraYamlConfig.data_file_directories), DseConfig.getTieredStorageOptions().values().stream().flatMap((tsc) -> {
               return tsc.tiers.stream();
            }).flatMap((tier) -> {
               return tier.paths.stream();
            })).sorted().distinct().toArray((n) -> {
               return new String[n];
            });
         }

         maybeApplyMemoryOnlySettingsFromDseConfig(cassandraYamlConfig);
         maybeApplySystemTableEncryptionSettingsFromDseConfig(cassandraYamlConfig);
         maybeApplyAuditLoggerSettingsFromDseConfig(cassandraYamlConfig);
         return cassandraYamlConfig;
      }
   }

   private static void maybeApplyAuditLoggerSettingsFromDseConfig(org.apache.cassandra.config.Config config) {
      if(DseConfigYamlLoader.originalConfig.audit_logging_options != null) {
         AuditLoggingOptions options = config.audit_logging_options;
         if(!options.enabled && options.logger.equalsIgnoreCase(SLF4JAuditWriter.class.getName()) && options.retention_time == 0) {
            config.audit_logging_options = DseConfig.getauditLoggingOptions();
         }
      }

   }

   private static void maybeApplyMemoryOnlySettingsFromDseConfig(org.apache.cassandra.config.Config cassandraYamlConfig) {
      if(DseConfigYamlLoader.originalConfig.max_memory_to_lock_mb != null || DseConfigYamlLoader.originalConfig.max_memory_to_lock_fraction != null) {
         if(Double.compare(cassandraYamlConfig.max_memory_to_lock_fraction, 0.2D) != 0 || cassandraYamlConfig.max_memory_to_lock_mb != 0) {
            throw new ConfigurationException("max_memory_to_lock_fraction and/or max_memory_to_lock_mb cannot be specified in both dse.yaml and cassandra.yaml");
         }

         cassandraYamlConfig.max_memory_to_lock_fraction = DseConfig.getMaxMemoryToLockFraction();
         cassandraYamlConfig.max_memory_to_lock_mb = DseConfig.getMaxMemoryToLockMBAsRaw();
      }

   }

   private static void maybeApplySystemTableEncryptionSettingsFromDseConfig(org.apache.cassandra.config.Config cassandraYamlConfig) {
      SystemTableEncryptionOptions.Wrapper dseOptions = DseConfig.getSystemInfoEncryptionOptions();
      if(cassandraYamlConfig.system_key_directory != null) {
         throw new ConfigurationException("system_key_directory cannot be specified in the cassandra.yaml it should be specified in dse.yaml");
      } else {
         cassandraYamlConfig.system_key_directory = DseConfig.config.system_key_directory;
         if(cassandraYamlConfig.system_info_encryption.enabled) {
            if(DseConfigYamlLoader.originalConfig.system_info_encryption.enabled) {
               throw new ConfigurationException("system_info_encryption cannot be specified in both dse.yaml and cassandra.yaml");
            }

            DseConfig.config.system_info_encryption.enabled = cassandraYamlConfig.system_info_encryption.enabled;
            DseConfig.config.system_info_encryption.cipher_algorithm = cassandraYamlConfig.system_info_encryption.cipher_algorithm;
            DseConfig.config.system_info_encryption.secret_key_strength = Integer.valueOf(cassandraYamlConfig.system_info_encryption.secret_key_strength);
            DseConfig.config.system_info_encryption.chunk_length_kb = Integer.valueOf(cassandraYamlConfig.system_info_encryption.chunk_length_kb);
            DseConfig.config.system_info_encryption.key_name = cassandraYamlConfig.system_info_encryption.key_name;
            DseConfig.config.system_info_encryption.key_provider = cassandraYamlConfig.system_info_encryption.key_provider;
            DseConfig.config.system_info_encryption.kmip_host = cassandraYamlConfig.system_info_encryption.kmip_host;
         } else {
            cassandraYamlConfig.system_info_encryption.enabled = dseOptions.isEnabled();
            cassandraYamlConfig.system_info_encryption.cipher_algorithm = dseOptions.getCipherAlgorithm();
            cassandraYamlConfig.system_info_encryption.secret_key_strength = dseOptions.getSecretKeyStrength().intValue();
            cassandraYamlConfig.system_info_encryption.chunk_length_kb = dseOptions.getChunkLengthInKB().intValue();
            cassandraYamlConfig.system_info_encryption.key_name = dseOptions.getKeyName();
            cassandraYamlConfig.system_info_encryption.key_provider = dseOptions.getKeyProvider();
            cassandraYamlConfig.system_info_encryption.kmip_host = dseOptions.getKmipHost();
         }

      }
   }

   public static String getInitialEndpointSnitch() throws ConfigurationException {
      getConfig();
      return initialEndpointSnitch;
   }

   public static String getCassandraYamlLocation() throws ConfigurationException {
      getConfig();
      return cassandraYamlLocation;
   }

   public static String getServerEncryptionAlgorithm() {
      try {
         getConfig();
         return server_encryption_algorithm;
      } catch (ConfigurationException var1) {
         logger.warn("Unable to get server encryption algorithm. Using default.");
         return null;
      }
   }

   public static String getClientEncryptionAlgorithm() {
      try {
         getConfig();
         return client_encryption_algorithm;
      } catch (Exception var1) {
         logger.warn("Unable to get client encryption algorithm. Using default.");
         return null;
      }
   }
}
