package com.datastax.bdp.util;

import com.google.common.base.Optional;
import java.util.HashMap;
import java.util.Map;

public class OptionMap {
   private Map<String, String> options;

   public OptionMap() {
      this(new HashMap());
   }

   public OptionMap(Map<String, String> options) {
      this.options = options;
   }

   public String get(String key, String defaultValue) {
      return this.options.containsKey(key)?(String)this.options.get(key):defaultValue;
   }

   public int get(String key, int defaultValue) {
      return this.options.containsKey(key)?Integer.parseInt((String)this.options.get(key)):defaultValue;
   }

   public Class<?> get(String key, Class<?> defaultValue) throws ClassNotFoundException {
      return this.options.containsKey(key)?Class.forName((String)this.options.get(key)):defaultValue;
   }

   public Optional<Integer> getOptionalInteger(String key) {
      String value = (String)this.options.get(key);
      return value == null?Optional.absent():Optional.of(Integer.valueOf(value));
   }

   public Optional<Double> getOptionalDouble(String key) {
      String value = (String)this.options.get(key);
      return value == null?Optional.absent():Optional.of(Double.valueOf(value));
   }

   public Map<String, String> getOptions() {
      return this.options;
   }

   public void putIfPresent(String key, Optional<?> value) {
      if(value.isPresent()) {
         this.options.put(key, String.valueOf(value.get()));
      }

   }
}
