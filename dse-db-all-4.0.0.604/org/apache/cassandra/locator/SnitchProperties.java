package org.apache.cassandra.locator;

import java.io.Closeable;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;
import org.apache.cassandra.io.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnitchProperties {
   private static final Logger logger = LoggerFactory.getLogger(SnitchProperties.class);
   public static final String RACKDC_PROPERTY_FILENAME = "cassandra-rackdc.properties";
   private Properties properties = new Properties();

   public SnitchProperties() {
      InputStream stream = null;
      String configURL = System.getProperty("cassandra-rackdc.properties");

      try {
         URL url;
         if(configURL == null) {
            url = SnitchProperties.class.getClassLoader().getResource("cassandra-rackdc.properties");
         } else {
            url = new URL(configURL);
         }

         stream = url.openStream();
         this.properties.load(stream);
      } catch (Exception var7) {
         logger.warn("Unable to read {}", configURL != null?configURL:"cassandra-rackdc.properties");
      } finally {
         FileUtils.closeQuietly((Closeable)stream);
      }

   }

   public String get(String propertyName, String defaultValue) {
      return this.properties.getProperty(propertyName, defaultValue);
   }

   public boolean contains(String propertyName) {
      return this.properties.containsKey(propertyName);
   }
}
