package org.apache.cassandra.index.sasi.analyzer.filter;

import java.util.Locale;
import java.util.Set;

public class StopWordFilters {
   public StopWordFilters() {
   }

   public static class DefaultStopWordFilter extends FilterPipelineTask<String, String> {
      private Set<String> stopWords = null;

      public DefaultStopWordFilter(Locale locale) {
         this.stopWords = StopWordFactory.getStopWordsForLanguage(locale);
      }

      public String process(String input) throws Exception {
         return this.stopWords != null && this.stopWords.contains(input)?null:input;
      }
   }
}
