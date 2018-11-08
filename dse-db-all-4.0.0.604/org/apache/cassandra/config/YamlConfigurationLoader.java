package org.apache.cassandra.config;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import java.beans.IntrospectionException;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.error.YAMLException;
import org.yaml.snakeyaml.introspector.MissingProperty;
import org.yaml.snakeyaml.introspector.Property;
import org.yaml.snakeyaml.introspector.PropertyUtils;

public class YamlConfigurationLoader implements ConfigurationLoader {
   private static final Logger logger = LoggerFactory.getLogger(YamlConfigurationLoader.class);
   private static final String DEFAULT_CONFIGURATION = "cassandra.yaml";
   private static URL storageConfigURL;

   public YamlConfigurationLoader() {
   }

   private static URL getStorageConfigURL() throws ConfigurationException {
      String configUrl = System.getProperty("cassandra.config");
      if(configUrl == null) {
         configUrl = "cassandra.yaml";
      }

      URL url;
      try {
         url = new URL(configUrl);
         url.openStream().close();
      } catch (Exception var5) {
         ClassLoader loader = DatabaseDescriptor.class.getClassLoader();
         url = loader.getResource(configUrl);
         if(url == null) {
            String required = "file:" + File.separator + File.separator;
            if(!configUrl.startsWith(required)) {
               throw new ConfigurationException(String.format("Expecting URI in variable: [cassandra.config]. Found[%s]. Please prefix the file with [%s%s] for local files and [%s<server>%s] for remote files. If you are executing this from an external tool, it needs to set Config.setClientMode(true) to avoid loading configuration.", new Object[]{configUrl, required, File.separator, required, File.separator}));
            }

            throw new ConfigurationException("Cannot locate " + configUrl + ".  If this is a local file, please confirm you've provided " + required + File.separator + " as a URI prefix.");
         }
      }

      logger.info("Configuration location: {}", url);
      return url;
   }

   public Config loadConfig() throws ConfigurationException {
      if(storageConfigURL == null) {
         storageConfigURL = getStorageConfigURL();
      }

      return this.loadConfig(storageConfigURL);
   }

   public Config loadConfig(URL url) throws ConfigurationException {
      try {
         logger.debug("Loading settings from {}", url);

         byte[] configBytes;
         try {
            InputStream is = url.openStream();
            Throwable var4 = null;

            try {
               configBytes = ByteStreams.toByteArray(is);
            } catch (Throwable var15) {
               var4 = var15;
               throw var15;
            } finally {
               if(is != null) {
                  if(var4 != null) {
                     try {
                        is.close();
                     } catch (Throwable var14) {
                        var4.addSuppressed(var14);
                     }
                  } else {
                     is.close();
                  }
               }

            }
         } catch (IOException var17) {
            throw new AssertionError(var17);
         }

         Constructor constructor = new YamlConfigurationLoader.CustomConstructor(Config.class);
         YamlConfigurationLoader.PropertiesChecker propertiesChecker = new YamlConfigurationLoader.PropertiesChecker();
         constructor.setPropertyUtils(propertiesChecker);
         Yaml yaml = new Yaml(constructor);
         Config result = this.loadConfig(yaml, configBytes);
         propertiesChecker.check();
         return result;
      } catch (YAMLException var18) {
         throw new ConfigurationException("Invalid yaml: " + url + SystemUtils.LINE_SEPARATOR + " Error: " + var18.getMessage(), false);
      }
   }

   private Config loadConfig(Yaml yaml, byte[] configBytes) {
      Config config = (Config)yaml.loadAs(new ByteArrayInputStream(configBytes), Config.class);
      return config == null?new Config():config;
   }

   private static class PropertiesChecker extends PropertyUtils {
      private final Set<String> missingProperties = new HashSet();
      private final Set<String> nullProperties = new HashSet();

      public PropertiesChecker() {
         this.setSkipMissingProperties(true);
      }

      public Property getProperty(Class<? extends Object> type, String name) throws IntrospectionException {
         final Property result = super.getProperty(type, name);
         if(result instanceof MissingProperty) {
            this.missingProperties.add(result.getName());
         }

         return new Property(result.getName(), result.getType()) {
            public void set(Object object, Object value) throws Exception {
               if(value == null && this.get(object) != null) {
                  PropertiesChecker.this.nullProperties.add(this.getName());
               }

               result.set(object, value);
            }

            public Class<?>[] getActualTypeArguments() {
               return result.getActualTypeArguments();
            }

            public Object get(Object object) {
               return result.get(object);
            }
         };
      }

      public void check() throws ConfigurationException {
         if(!this.nullProperties.isEmpty()) {
            throw new ConfigurationException("Invalid yaml. Those properties " + this.nullProperties + " are not valid", false);
         } else if(!this.missingProperties.isEmpty()) {
            throw new ConfigurationException("Invalid yaml. Please remove properties " + this.missingProperties + " from your cassandra.yaml", false);
         }
      }
   }

   static class CustomConstructor extends Constructor {
      CustomConstructor(Class<?> theRoot) {
         super(theRoot);
         TypeDescription seedDesc = new TypeDescription(ParameterizedClass.class);
         seedDesc.putMapPropertyType("parameters", String.class, String.class);
         this.addTypeDescription(seedDesc);
      }

      protected List<Object> createDefaultList(int initSize) {
         return Lists.newCopyOnWriteArrayList();
      }

      protected Map<Object, Object> createDefaultMap() {
         return Maps.newConcurrentMap();
      }

      protected Set<Object> createDefaultSet(int initSize) {
         return Sets.newConcurrentHashSet();
      }

      protected Set<Object> createDefaultSet() {
         return Sets.newConcurrentHashSet();
      }
   }
}
