package org.apache.cassandra.index.sasi.analyzer;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class NonTokenizingOptions {
   public static final String NORMALIZE_LOWERCASE = "normalize_lowercase";
   public static final String NORMALIZE_UPPERCASE = "normalize_uppercase";
   public static final String CASE_SENSITIVE = "case_sensitive";
   private boolean caseSensitive;
   private boolean upperCaseOutput;
   private boolean lowerCaseOutput;

   public NonTokenizingOptions() {
   }

   public boolean isCaseSensitive() {
      return this.caseSensitive;
   }

   public void setCaseSensitive(boolean caseSensitive) {
      this.caseSensitive = caseSensitive;
   }

   public boolean shouldUpperCaseOutput() {
      return this.upperCaseOutput;
   }

   public void setUpperCaseOutput(boolean upperCaseOutput) {
      this.upperCaseOutput = upperCaseOutput;
   }

   public boolean shouldLowerCaseOutput() {
      return this.lowerCaseOutput;
   }

   public void setLowerCaseOutput(boolean lowerCaseOutput) {
      this.lowerCaseOutput = lowerCaseOutput;
   }

   public static NonTokenizingOptions buildFromMap(Map<String, String> optionsMap) {
      NonTokenizingOptions.OptionsBuilder optionsBuilder = new NonTokenizingOptions.OptionsBuilder();
      if(!optionsMap.containsKey("case_sensitive") || !optionsMap.containsKey("normalize_lowercase") && !optionsMap.containsKey("normalize_uppercase")) {
         Iterator var2 = optionsMap.entrySet().iterator();

         while(var2.hasNext()) {
            Entry<String, String> entry = (Entry)var2.next();
            String var4 = (String)entry.getKey();
            byte var5 = -1;
            switch(var4.hashCode()) {
            case -1406997761:
               if(var4.equals("normalize_lowercase")) {
                  var5 = 0;
               }
               break;
            case -1368356793:
               if(var4.equals("case_sensitive")) {
                  var5 = 2;
               }
               break;
            case -668966880:
               if(var4.equals("normalize_uppercase")) {
                  var5 = 1;
               }
            }

            boolean bool;
            switch(var5) {
            case 0:
               bool = Boolean.parseBoolean((String)entry.getValue());
               optionsBuilder = optionsBuilder.lowerCaseOutput(bool);
               break;
            case 1:
               bool = Boolean.parseBoolean((String)entry.getValue());
               optionsBuilder = optionsBuilder.upperCaseOutput(bool);
               break;
            case 2:
               bool = Boolean.parseBoolean((String)entry.getValue());
               optionsBuilder = optionsBuilder.caseSensitive(bool);
            }
         }

         return optionsBuilder.build();
      } else {
         throw new IllegalArgumentException("case_sensitive option cannot be specified together with either normalize_lowercase or normalize_uppercase");
      }
   }

   public static NonTokenizingOptions getDefaultOptions() {
      return (new NonTokenizingOptions.OptionsBuilder()).caseSensitive(true).lowerCaseOutput(false).upperCaseOutput(false).build();
   }

   public static class OptionsBuilder {
      private boolean caseSensitive = true;
      private boolean upperCaseOutput = false;
      private boolean lowerCaseOutput = false;

      public OptionsBuilder() {
      }

      public NonTokenizingOptions.OptionsBuilder caseSensitive(boolean caseSensitive) {
         this.caseSensitive = caseSensitive;
         return this;
      }

      public NonTokenizingOptions.OptionsBuilder upperCaseOutput(boolean upperCaseOutput) {
         this.upperCaseOutput = upperCaseOutput;
         return this;
      }

      public NonTokenizingOptions.OptionsBuilder lowerCaseOutput(boolean lowerCaseOutput) {
         this.lowerCaseOutput = lowerCaseOutput;
         return this;
      }

      public NonTokenizingOptions build() {
         if(this.lowerCaseOutput && this.upperCaseOutput) {
            throw new IllegalArgumentException("Options to normalize terms cannot be both uppercase and lowercase at the same time");
         } else {
            NonTokenizingOptions options = new NonTokenizingOptions();
            options.setCaseSensitive(this.caseSensitive);
            options.setUpperCaseOutput(this.upperCaseOutput);
            options.setLowerCaseOutput(this.lowerCaseOutput);
            return options;
         }
      }
   }
}
