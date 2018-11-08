package com.datastax.bdp.config;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class YamlLocation implements YamlLocationMXBean {
   private static final Logger logger = LoggerFactory.getLogger(YamlLocation.class);
   private final String dse;
   private final String cassandra;

   public YamlLocation() {
      String dse = "";

      try {
         dse = DseConfigYamlLoader.getConfigURL().getFile();
      } catch (ConfigurationException var3) {
         logger.debug("Couldn't get location of dse.yaml: " + var3.getLocalizedMessage());
      }

      this.dse = dse;
      this.cassandra = DseConfigurationLoader.getCassandraYamlLocation();
   }

   public String getDse() {
      return this.dse;
   }

   public String getCassandra() {
      return this.cassandra;
   }
}
