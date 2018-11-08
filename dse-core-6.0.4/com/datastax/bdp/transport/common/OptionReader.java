package com.datastax.bdp.transport.common;

import java.util.Map;

public class OptionReader {
   private final Map<String, String> options;

   public OptionReader(Map<String, String> options) {
      this.options = options;
   }

   public String getString(String optionKey, String defaultValue) {
      return this.options.containsKey(optionKey)?(String)this.options.get(optionKey):defaultValue;
   }

   public boolean getBoolean(String optionKey, boolean defaultValue) {
      return this.options.containsKey(optionKey)?Boolean.valueOf((String)this.options.get(optionKey)).booleanValue():defaultValue;
   }

   public int getInteger(String optionKey, int defaultValue) {
      return this.options.containsKey(optionKey)?Integer.valueOf((String)this.options.get(optionKey)).intValue():defaultValue;
   }

   public String[] getStrings(String optionKey, String[] defaultValue) {
      return this.options.containsKey(optionKey)?((String)this.options.get(optionKey)).replaceFirst("\\[(.*)\\]", "$1").split(",[ \t]*"):defaultValue;
   }
}
