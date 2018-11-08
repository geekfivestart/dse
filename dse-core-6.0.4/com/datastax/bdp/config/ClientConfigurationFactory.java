package com.datastax.bdp.config;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ClientConfigurationFactory {
   private static final Logger logger = LoggerFactory.getLogger(ClientConfigurationFactory.class);
   public static final String CLIENT_CONFIGURATION_IMPL = "dse.client.configuration.impl";

   public ClientConfigurationFactory() {
   }

   public static ClientConfiguration getClientConfiguration() {
      String implClassName = System.getProperty("dse.client.configuration.impl");
      if(StringUtils.isNotBlank(implClassName)) {
         try {
            Class<?> implClass = Class.forName(implClassName);
            ClientConfiguration impl = (ClientConfiguration)implClass.newInstance();
            logger.debug("Using {} implementation of ClientConfiguration", implClassName);
            return impl;
         } catch (Exception var3) {
            logger.error("Failed to create ClientConfiguration - could not create instance of " + implClassName, var3);
         }
      }

      logger.debug("Using InternalClientConfiguration implementation");
      return getInternalClientConfiguration();
   }

   public static ClientConfiguration getInternalClientConfiguration() {
      return InternalClientConfiguration.getInstance();
   }

   public static ClientConfiguration getYamlClientConfiguration() {
      return new YamlClientConfiguration();
   }
}
