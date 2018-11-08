package org.apache.cassandra.cql3.statements;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.cassandra.exceptions.SyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropertyDefinitions {
   private static final Pattern PATTERN_POSITIVE = Pattern.compile("(1|true|yes)");
   protected static final Logger logger = LoggerFactory.getLogger(PropertyDefinitions.class);
   protected final Map<String, Object> properties = new HashMap();

   public PropertyDefinitions() {
   }

   public void addProperty(String name, String value) throws SyntaxException {
      if(this.properties.put(name, value) != null) {
         throw new SyntaxException(String.format("Multiple definition for property '%s'", new Object[]{name}));
      }
   }

   public void addProperty(String name, Map<String, String> value) throws SyntaxException {
      if(this.properties.put(name, value) != null) {
         throw new SyntaxException(String.format("Multiple definition for property '%s'", new Object[]{name}));
      }
   }

   public void validate(Set<String> keywords, Set<String> obsolete) throws SyntaxException {
      Iterator var3 = this.properties.keySet().iterator();

      while(var3.hasNext()) {
         String name = (String)var3.next();
         if(!keywords.contains(name)) {
            if(!obsolete.contains(name)) {
               throw new SyntaxException(String.format("Unknown property '%s'", new Object[]{name}));
            }

            logger.warn("Ignoring obsolete property {}", name);
         }
      }

   }

   public int size() {
      return this.properties.size();
   }

   protected String getSimple(String name) throws SyntaxException {
      Object val = this.properties.get(name);
      if(val == null) {
         return null;
      } else if(!(val instanceof String)) {
         throw new SyntaxException(String.format("Invalid value for property '%s'. It should be a string", new Object[]{name}));
      } else {
         return (String)val;
      }
   }

   protected Map<String, String> getMap(String name) throws SyntaxException {
      Object val = this.properties.get(name);
      if(val == null) {
         return null;
      } else if(!(val instanceof Map)) {
         throw new SyntaxException(String.format("Invalid value for property '%s'. It should be a map.", new Object[]{name}));
      } else {
         return (Map)val;
      }
   }

   public Boolean hasProperty(String name) {
      return Boolean.valueOf(this.properties.containsKey(name));
   }

   public String getString(String key, String defaultValue) throws SyntaxException {
      String value = this.getSimple(key);
      return value != null?value:defaultValue;
   }

   public Boolean getBoolean(String key, Boolean defaultValue) throws SyntaxException {
      String value = this.getSimple(key);
      return Boolean.valueOf(value == null?defaultValue.booleanValue():PATTERN_POSITIVE.matcher(value.toLowerCase()).matches());
   }

   public double getDouble(String key, double defaultValue) throws SyntaxException {
      String value = this.getSimple(key);
      if(value == null) {
         return defaultValue;
      } else {
         try {
            return Double.parseDouble(value);
         } catch (NumberFormatException var6) {
            throw new SyntaxException(String.format("Invalid double value %s for '%s'", new Object[]{value, key}));
         }
      }
   }

   public Integer getInt(String key, Integer defaultValue) throws SyntaxException {
      String value = this.getSimple(key);
      return toInt(key, value, defaultValue);
   }

   public static Integer toInt(String key, String value, Integer defaultValue) throws SyntaxException {
      if(value == null) {
         return defaultValue;
      } else {
         try {
            return Integer.valueOf(value);
         } catch (NumberFormatException var4) {
            throw new SyntaxException(String.format("Invalid integer value %s for '%s'", new Object[]{value, key}));
         }
      }
   }
}
