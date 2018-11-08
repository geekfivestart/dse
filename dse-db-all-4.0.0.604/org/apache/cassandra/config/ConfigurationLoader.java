package org.apache.cassandra.config;

import java.net.URL;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;

public interface ConfigurationLoader {
   static default ConfigurationLoader create() throws ConfigurationException {
      String loaderClass = System.getProperty("cassandra.config.loader");
      return (ConfigurationLoader)(loaderClass == null?new YamlConfigurationLoader():(ConfigurationLoader)FBUtilities.construct(loaderClass, "configuration loading"));
   }

   Config loadConfig() throws ConfigurationException;

   Config loadConfig(URL var1) throws ConfigurationException;
}
