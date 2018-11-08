package com.datastax.bdp.config;

import com.google.common.collect.ImmutableSet;
import java.beans.IntrospectionException;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.error.YAMLException;
import org.yaml.snakeyaml.introspector.BeanAccess;
import org.yaml.snakeyaml.introspector.MissingProperty;
import org.yaml.snakeyaml.introspector.Property;
import org.yaml.snakeyaml.introspector.PropertyUtils;

public class DseYamlPropertyUtils extends PropertyUtils {
   private static final Logger logger = LoggerFactory.getLogger(DseYamlPropertyUtils.class);
   public static final String GRAPH_PARENT_CONFIG_KEY = "graph";
   private static final Set<String> IGNORED_PROPERTY_NAMES = ImmutableSet.of("graph");
   private static final Set<String> REMOVED_PROPERTY_NAMES = ImmutableSet.of("hive_meta_store_enabled", "hive_options", "hadoop_options", "jps_directory");

   public DseYamlPropertyUtils() {
   }

   public Property getProperty(Class<? extends Object> type, String name, BeanAccess bAccess) throws IntrospectionException {
      Map<String, Property> properties = this.getPropertiesMap(type, bAccess);
      boolean removed = REMOVED_PROPERTY_NAMES.contains(name);
      Property property = (Property)properties.get(name);
      if(property == null && null != name && (IGNORED_PROPERTY_NAMES.contains(name) || removed)) {
         property = new MissingProperty(name);
         if(removed) {
            logger.warn("setting " + name + " in dse.yaml is obsolete and is being ignored");
         }
      }

      if(property != null && ((Property)property).isWritable()) {
         return (Property)property;
      } else {
         throw new YAMLException("Unable to find property '" + name + "' on class: " + type.getName());
      }
   }

   public void setSkipMissingProperties(boolean skipMissingProperties) {
      throw new UnsupportedOperationException("Use IGNORED_PROPERTY_NAMES instead of skipping missing properties");
   }
}
