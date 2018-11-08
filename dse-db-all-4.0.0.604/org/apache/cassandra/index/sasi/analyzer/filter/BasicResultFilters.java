package org.apache.cassandra.index.sasi.analyzer.filter;

import java.util.Locale;

public class BasicResultFilters {
   private static final Locale DEFAULT_LOCALE = Locale.getDefault();

   public BasicResultFilters() {
   }

   public static class NoOperation extends FilterPipelineTask<Object, Object> {
      public NoOperation() {
      }

      public Object process(Object input) throws Exception {
         return input;
      }
   }

   public static class UpperCase extends FilterPipelineTask<String, String> {
      private Locale locale;

      public UpperCase(Locale locale) {
         this.locale = locale;
      }

      public UpperCase() {
         this.locale = BasicResultFilters.DEFAULT_LOCALE;
      }

      public String process(String input) throws Exception {
         return input.toUpperCase(this.locale);
      }
   }

   public static class LowerCase extends FilterPipelineTask<String, String> {
      private Locale locale;

      public LowerCase(Locale locale) {
         this.locale = locale;
      }

      public LowerCase() {
         this.locale = BasicResultFilters.DEFAULT_LOCALE;
      }

      public String process(String input) throws Exception {
         return input.toLowerCase(this.locale);
      }
   }
}
