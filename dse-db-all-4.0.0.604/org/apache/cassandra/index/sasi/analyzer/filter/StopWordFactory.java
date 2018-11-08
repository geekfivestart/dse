package org.apache.cassandra.index.sasi.analyzer.filter;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CompletionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StopWordFactory {
   private static final Logger logger = LoggerFactory.getLogger(StopWordFactory.class);
   private static final String DEFAULT_RESOURCE_EXT = "_ST.txt";
   private static final String DEFAULT_RESOURCE_PREFIX;
   private static final Set<String> SUPPORTED_LANGUAGES;
   private static final LoadingCache<String, Set<String>> STOP_WORDS_CACHE;

   public StopWordFactory() {
   }

   public static Set<String> getStopWordsForLanguage(Locale locale) {
      if(locale == null) {
         return null;
      } else {
         String rootLang = locale.getLanguage().substring(0, 2);

         try {
            return !SUPPORTED_LANGUAGES.contains(rootLang)?null:(Set)STOP_WORDS_CACHE.get(rootLang);
         } catch (CompletionException var3) {
            logger.error("Failed to populate Stop Words Cache for language [{}]", locale.getLanguage(), var3);
            return null;
         }
      }
   }

   private static Set<String> getStopWordsFromResource(String language) {
      Set<String> stopWords = new HashSet();
      String resourceName = DEFAULT_RESOURCE_PREFIX + File.separator + language + "_ST.txt";

      try {
         InputStream is = StopWordFactory.class.getClassLoader().getResourceAsStream(resourceName);
         Throwable var4 = null;

         try {
            BufferedReader r = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
            Throwable var6 = null;

            try {
               String line;
               try {
                  while((line = r.readLine()) != null) {
                     if(line.charAt(0) != 35) {
                        stopWords.add(line.trim());
                     }
                  }
               } catch (Throwable var31) {
                  var6 = var31;
                  throw var31;
               }
            } finally {
               if(r != null) {
                  if(var6 != null) {
                     try {
                        r.close();
                     } catch (Throwable var30) {
                        var6.addSuppressed(var30);
                     }
                  } else {
                     r.close();
                  }
               }

            }
         } catch (Throwable var33) {
            var4 = var33;
            throw var33;
         } finally {
            if(is != null) {
               if(var4 != null) {
                  try {
                     is.close();
                  } catch (Throwable var29) {
                     var4.addSuppressed(var29);
                  }
               } else {
                  is.close();
               }
            }

         }
      } catch (Exception var35) {
         logger.error("Failed to retrieve Stop Terms resource for language [{}]", language, var35);
      }

      return stopWords;
   }

   static {
      DEFAULT_RESOURCE_PREFIX = StopWordFactory.class.getPackage().getName().replace(".", File.separator);
      SUPPORTED_LANGUAGES = new HashSet(Arrays.asList(new String[]{"ar", "bg", "cs", "de", "en", "es", "fi", "fr", "hi", "hu", "it", "pl", "pt", "ro", "ru", "sv"}));
      STOP_WORDS_CACHE = Caffeine.newBuilder().executor(MoreExecutors.directExecutor()).build(StopWordFactory::getStopWordsFromResource);
   }
}
