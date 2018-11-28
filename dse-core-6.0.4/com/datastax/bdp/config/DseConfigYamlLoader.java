package com.datastax.bdp.config;

import com.datastax.bdp.DseModule;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URL;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.error.MarkedYAMLException;

public class DseConfigYamlLoader {
   public static final String DEFAULT_CONFIGURATION = "dse.yaml";
   public static final String CONFIG_FILE_PROPERTY = "dse.config";
   private static final Logger logger = LoggerFactory.getLogger(DseConfigYamlLoader.class);
   protected static final Config config;
   protected static final Config originalConfig;

   public DseConfigYamlLoader() {
   }

   public static URL getConfigURL() throws ConfigurationException {
      String configUrl = System.getProperty("dse.config");
      if(configUrl == null) {
         configUrl = "dse.yaml";
      }

      URL url;
      try {
         url = new URL(configUrl);
         url.openStream().close();
      } catch (Exception var4) {
         ClassLoader loader = DseConfig.class.getClassLoader();
         url = loader.getResource(configUrl);
         if(url == null) {
            throw new ConfigurationException("Cannot locate " + configUrl);
         }
      }

      return url;
   }

   protected static void enableResolvers(Class<?> c) {
      try {
         Field[] var1 = c.getDeclaredFields();
         int var2 = var1.length;

         for(int var3 = 0; var3 < var2; ++var3) {
            Field f = var1[var3];
            if(Modifier.isStatic(f.getModifiers())) {
               Class superClass = f.getType().getSuperclass();
               if(superClass != null && superClass.equals(ConfigUtil.ParamResolver.class)) {
                  f.setAccessible(true);
                  ConfigUtil.ParamResolver<?> pr = (ConfigUtil.ParamResolver)f.get((Object)null);
                  pr.enable();
               }
            }
         }

      } catch (IllegalAccessException var7) {
         throw new RuntimeException("this really should not happen after f.setAccessible()");
      }
   }

   static {
      InputStream input = null;
      InputStream input2 = null;

      try {
         URL url = getConfigURL();
         logger.info("Loading settings from " + url);
         if(!System.getProperty("cassandra.config.loader", "default").equals(DseConfigurationLoader.class.getCanonicalName())) {
            logger.warn("Incorrect configuration loader detected ({}), DSE functionality may be impaired", System.getProperty("cassandra.config.loader", "default"));
         }

         try {
            input = url.openStream();
            input2 = url.openStream();
         } catch (IOException var9) {
            throw new AssertionError(var9);
         }

         Yaml parser = DseModule.createParser();
         config = (Config)parser.loadAs(input, Config.class);
         originalConfig = (Config)parser.loadAs(input2, Config.class);
      } catch (ConfigurationException var10) {
         throw new ExceptionInInitializerError(var10);
      } catch (MarkedYAMLException e3) {
         RuntimeException t;
         for (t = (RuntimeException)e3; t.getCause() != null && t.getCause() instanceof RuntimeException; t = (RuntimeException)t.getCause()) {}
         throw t;
      } finally {
         IOUtils.closeQuietly(input);
         IOUtils.closeQuietly(input2);
      }

   }
}
