package org.apache.cassandra.index.sasi.analyzer;

import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

public class StandardTokenizerOptions {
   public static final String TOKENIZATION_ENABLE_STEMMING = "tokenization_enable_stemming";
   public static final String TOKENIZATION_SKIP_STOP_WORDS = "tokenization_skip_stop_words";
   public static final String TOKENIZATION_LOCALE = "tokenization_locale";
   public static final String TOKENIZATION_NORMALIZE_LOWERCASE = "tokenization_normalize_lowercase";
   public static final String TOKENIZATION_NORMALIZE_UPPERCASE = "tokenization_normalize_uppercase";
   public static final int DEFAULT_MAX_TOKEN_LENGTH = 255;
   public static final int DEFAULT_MIN_TOKEN_LENGTH = 0;
   private boolean stemTerms;
   private boolean ignoreStopTerms;
   private Locale locale;
   private boolean caseSensitive;
   private boolean allTermsToUpperCase;
   private boolean allTermsToLowerCase;
   private int minTokenLength;
   private int maxTokenLength;

   public StandardTokenizerOptions() {
   }

   public boolean shouldStemTerms() {
      return this.stemTerms;
   }

   public void setStemTerms(boolean stemTerms) {
      this.stemTerms = stemTerms;
   }

   public boolean shouldIgnoreStopTerms() {
      return this.ignoreStopTerms;
   }

   public void setIgnoreStopTerms(boolean ignoreStopTerms) {
      this.ignoreStopTerms = ignoreStopTerms;
   }

   public Locale getLocale() {
      return this.locale;
   }

   public void setLocale(Locale locale) {
      this.locale = locale;
   }

   public boolean isCaseSensitive() {
      return this.caseSensitive;
   }

   public void setCaseSensitive(boolean caseSensitive) {
      this.caseSensitive = caseSensitive;
   }

   public boolean shouldUpperCaseTerms() {
      return this.allTermsToUpperCase;
   }

   public void setAllTermsToUpperCase(boolean allTermsToUpperCase) {
      this.allTermsToUpperCase = allTermsToUpperCase;
   }

   public boolean shouldLowerCaseTerms() {
      return this.allTermsToLowerCase;
   }

   public void setAllTermsToLowerCase(boolean allTermsToLowerCase) {
      this.allTermsToLowerCase = allTermsToLowerCase;
   }

   public int getMinTokenLength() {
      return this.minTokenLength;
   }

   public void setMinTokenLength(int minTokenLength) {
      this.minTokenLength = minTokenLength;
   }

   public int getMaxTokenLength() {
      return this.maxTokenLength;
   }

   public void setMaxTokenLength(int maxTokenLength) {
      this.maxTokenLength = maxTokenLength;
   }

   public static StandardTokenizerOptions buildFromMap(Map<String, String> optionsMap) {
      StandardTokenizerOptions.OptionsBuilder optionsBuilder = new StandardTokenizerOptions.OptionsBuilder();
      Iterator var2 = optionsMap.entrySet().iterator();

      while(var2.hasNext()) {
         Entry<String, String> entry = (Entry)var2.next();
         String var4 = (String)entry.getKey();
         byte var5 = -1;
         switch(var4.hashCode()) {
         case -965665306:
            if(var4.equals("tokenization_enable_stemming")) {
               var5 = 0;
            }
            break;
         case -315252530:
            if(var4.equals("tokenization_locale")) {
               var5 = 2;
            }
            break;
         case 1298134283:
            if(var4.equals("tokenization_normalize_lowercase")) {
               var5 = 4;
            }
            break;
         case 1675511544:
            if(var4.equals("tokenization_skip_stop_words")) {
               var5 = 1;
            }
            break;
         case 2036165164:
            if(var4.equals("tokenization_normalize_uppercase")) {
               var5 = 3;
            }
         }

         boolean bool;
         switch(var5) {
         case 0:
            bool = Boolean.parseBoolean((String)entry.getValue());
            optionsBuilder = optionsBuilder.stemTerms(bool);
            break;
         case 1:
            bool = Boolean.parseBoolean((String)entry.getValue());
            optionsBuilder = optionsBuilder.ignoreStopTerms(bool);
            break;
         case 2:
            Locale locale = new Locale((String)entry.getValue());
            optionsBuilder = optionsBuilder.useLocale(locale);
            break;
         case 3:
            bool = Boolean.parseBoolean((String)entry.getValue());
            optionsBuilder = optionsBuilder.alwaysUpperCaseTerms(bool);
            break;
         case 4:
            bool = Boolean.parseBoolean((String)entry.getValue());
            optionsBuilder = optionsBuilder.alwaysLowerCaseTerms(bool);
         }
      }

      return optionsBuilder.build();
   }

   public static StandardTokenizerOptions getDefaultOptions() {
      return (new StandardTokenizerOptions.OptionsBuilder()).ignoreStopTerms(true).alwaysLowerCaseTerms(true).stemTerms(false).useLocale(Locale.ENGLISH).build();
   }

   public static class OptionsBuilder {
      private boolean stemTerms;
      private boolean ignoreStopTerms;
      private Locale locale;
      private boolean caseSensitive;
      private boolean allTermsToUpperCase;
      private boolean allTermsToLowerCase;
      private int minTokenLength = 0;
      private int maxTokenLength = 255;

      public OptionsBuilder() {
      }

      public StandardTokenizerOptions.OptionsBuilder stemTerms(boolean stemTerms) {
         this.stemTerms = stemTerms;
         return this;
      }

      public StandardTokenizerOptions.OptionsBuilder ignoreStopTerms(boolean ignoreStopTerms) {
         this.ignoreStopTerms = ignoreStopTerms;
         return this;
      }

      public StandardTokenizerOptions.OptionsBuilder useLocale(Locale locale) {
         this.locale = locale;
         return this;
      }

      public StandardTokenizerOptions.OptionsBuilder caseSensitive(boolean caseSensitive) {
         this.caseSensitive = caseSensitive;
         return this;
      }

      public StandardTokenizerOptions.OptionsBuilder alwaysUpperCaseTerms(boolean allTermsToUpperCase) {
         this.allTermsToUpperCase = allTermsToUpperCase;
         return this;
      }

      public StandardTokenizerOptions.OptionsBuilder alwaysLowerCaseTerms(boolean allTermsToLowerCase) {
         this.allTermsToLowerCase = allTermsToLowerCase;
         return this;
      }

      public StandardTokenizerOptions.OptionsBuilder minTokenLength(int minTokenLength) {
         if(minTokenLength < 1) {
            throw new IllegalArgumentException("minTokenLength must be greater than zero");
         } else {
            this.minTokenLength = minTokenLength;
            return this;
         }
      }

      public StandardTokenizerOptions.OptionsBuilder maxTokenLength(int maxTokenLength) {
         if(maxTokenLength < 1) {
            throw new IllegalArgumentException("maxTokenLength must be greater than zero");
         } else {
            this.maxTokenLength = maxTokenLength;
            return this;
         }
      }

      public StandardTokenizerOptions build() {
         if(this.allTermsToLowerCase && this.allTermsToUpperCase) {
            throw new IllegalArgumentException("Options to normalize terms cannot be both uppercase and lowercase at the same time");
         } else {
            StandardTokenizerOptions options = new StandardTokenizerOptions();
            options.setIgnoreStopTerms(this.ignoreStopTerms);
            options.setStemTerms(this.stemTerms);
            options.setLocale(this.locale);
            options.setCaseSensitive(this.caseSensitive);
            options.setAllTermsToLowerCase(this.allTermsToLowerCase);
            options.setAllTermsToUpperCase(this.allTermsToUpperCase);
            options.setMinTokenLength(this.minTokenLength);
            options.setMaxTokenLength(this.maxTokenLength);
            return options;
         }
      }
   }
}
