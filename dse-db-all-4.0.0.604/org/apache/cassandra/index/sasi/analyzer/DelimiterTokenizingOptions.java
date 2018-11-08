package org.apache.cassandra.index.sasi.analyzer;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class DelimiterTokenizingOptions {
   public static final String DELIMITER = "delimiter";
   private final char delimiter;

   private DelimiterTokenizingOptions(char delimiter) {
      this.delimiter = delimiter;
   }

   char getDelimiter() {
      return this.delimiter;
   }

   static DelimiterTokenizingOptions buildFromMap(Map<String, String> optionsMap) {
      DelimiterTokenizingOptions.OptionsBuilder optionsBuilder = new DelimiterTokenizingOptions.OptionsBuilder();
      Iterator var2 = optionsMap.entrySet().iterator();

      while(var2.hasNext()) {
         Entry<String, String> entry = (Entry)var2.next();
         String var4 = (String)entry.getKey();
         byte var5 = -1;
         switch(var4.hashCode()) {
         case -250518009:
            if(var4.equals("delimiter")) {
               var5 = 0;
            }
         }

         switch(var5) {
         case 0:
            String value = (String)entry.getValue();
            if(1 != value.length()) {
               throw new IllegalArgumentException(String.format("Only single character delimiters supported, was %s", new Object[]{value}));
            }

            optionsBuilder.delimiter = ((String)entry.getValue()).charAt(0);
         }
      }

      return optionsBuilder.build();
   }

   private static class OptionsBuilder {
      private char delimiter;

      private OptionsBuilder() {
         this.delimiter = 44;
      }

      public DelimiterTokenizingOptions build() {
         return new DelimiterTokenizingOptions(this.delimiter);
      }
   }
}
