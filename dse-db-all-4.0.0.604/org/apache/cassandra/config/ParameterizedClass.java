package org.apache.cassandra.config;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ParameterizedClass {
   public static final String CLASS_NAME = "class_name";
   public static final String PARAMETERS = "parameters";
   public String class_name;
   public Map<String, String> parameters;

   public ParameterizedClass(String class_name, Map<String, String> parameters) {
      this.class_name = class_name;
      this.parameters = parameters;
   }

   public ParameterizedClass(Map<String, ?> p) {
      this((String)p.get("class_name"), p.containsKey("parameters")?(Map)((List)p.get("parameters")).get(0):null);
   }

   public boolean equals(Object that) {
      return that instanceof ParameterizedClass && this.equals((ParameterizedClass)that);
   }

   public boolean equals(ParameterizedClass that) {
      return Objects.equals(this.class_name, that.class_name) && Objects.equals(this.parameters, that.parameters);
   }

   public String toString() {
      return this.class_name + (this.parameters == null?"":this.parameters.toString());
   }
}
