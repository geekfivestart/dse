package org.apache.cassandra.index.sasi.analyzer.filter;

import java.util.Locale;
import org.tartarus.snowball.SnowballStemmer;

public class StemmingFilters {
   public StemmingFilters() {
   }

   public static class DefaultStemmingFilter extends FilterPipelineTask<String, String> {
      private SnowballStemmer stemmer;

      public DefaultStemmingFilter(Locale locale) {
         this.stemmer = StemmerFactory.getStemmer(locale);
      }

      public String process(String input) throws Exception {
         if(input != null && this.stemmer != null) {
            this.stemmer.setCurrent(input);
            return this.stemmer.stem()?this.stemmer.getCurrent():input;
         } else {
            return input;
         }
      }
   }
}
