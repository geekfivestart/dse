package org.apache.cassandra.index.sasi.analyzer.filter;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.util.concurrent.MoreExecutors;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tartarus.snowball.SnowballStemmer;
import org.tartarus.snowball.ext.danishStemmer;
import org.tartarus.snowball.ext.dutchStemmer;
import org.tartarus.snowball.ext.englishStemmer;
import org.tartarus.snowball.ext.finnishStemmer;
import org.tartarus.snowball.ext.frenchStemmer;
import org.tartarus.snowball.ext.germanStemmer;
import org.tartarus.snowball.ext.hungarianStemmer;
import org.tartarus.snowball.ext.italianStemmer;
import org.tartarus.snowball.ext.norwegianStemmer;
import org.tartarus.snowball.ext.portugueseStemmer;
import org.tartarus.snowball.ext.romanianStemmer;
import org.tartarus.snowball.ext.russianStemmer;
import org.tartarus.snowball.ext.spanishStemmer;
import org.tartarus.snowball.ext.swedishStemmer;
import org.tartarus.snowball.ext.turkishStemmer;

public class StemmerFactory {
   private static final Logger logger = LoggerFactory.getLogger(StemmerFactory.class);
   private static final LoadingCache<Class, Constructor<?>> STEMMER_CONSTRUCTOR_CACHE = Caffeine.newBuilder().executor(MoreExecutors.directExecutor()).build(new CacheLoader<Class, Constructor<?>>() {
      public Constructor<?> load(Class aClass) throws Exception {
         try {
            return aClass.getConstructor(new Class[0]);
         } catch (Exception var3) {
            StemmerFactory.logger.error("Failed to get stemmer constructor", var3);
            return null;
         }
      }
   });
   private static final Map<String, Class> SUPPORTED_LANGUAGES = new HashMap();

   public StemmerFactory() {
   }

   public static SnowballStemmer getStemmer(Locale locale) {
      if(locale == null) {
         return null;
      } else {
         String rootLang = locale.getLanguage().substring(0, 2);

         try {
            Class clazz = (Class)SUPPORTED_LANGUAGES.get(rootLang);
            if(clazz == null) {
               return null;
            } else {
               Constructor<?> ctor = (Constructor)STEMMER_CONSTRUCTOR_CACHE.get(clazz);
               return (SnowballStemmer)ctor.newInstance(new Object[0]);
            }
         } catch (Exception var4) {
            logger.debug("Failed to create new SnowballStemmer instance for language [{}]", locale.getLanguage(), var4);
            return null;
         }
      }
   }

   static {
      SUPPORTED_LANGUAGES.put("de", germanStemmer.class);
      SUPPORTED_LANGUAGES.put("da", danishStemmer.class);
      SUPPORTED_LANGUAGES.put("es", spanishStemmer.class);
      SUPPORTED_LANGUAGES.put("en", englishStemmer.class);
      SUPPORTED_LANGUAGES.put("fl", finnishStemmer.class);
      SUPPORTED_LANGUAGES.put("fr", frenchStemmer.class);
      SUPPORTED_LANGUAGES.put("hu", hungarianStemmer.class);
      SUPPORTED_LANGUAGES.put("it", italianStemmer.class);
      SUPPORTED_LANGUAGES.put("nl", dutchStemmer.class);
      SUPPORTED_LANGUAGES.put("no", norwegianStemmer.class);
      SUPPORTED_LANGUAGES.put("pt", portugueseStemmer.class);
      SUPPORTED_LANGUAGES.put("ro", romanianStemmer.class);
      SUPPORTED_LANGUAGES.put("ru", russianStemmer.class);
      SUPPORTED_LANGUAGES.put("sv", swedishStemmer.class);
      SUPPORTED_LANGUAGES.put("tr", turkishStemmer.class);
   }
}
