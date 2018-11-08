package org.apache.cassandra.index.sasi.analyzer;

import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sasi.analyzer.filter.BasicResultFilters;
import org.apache.cassandra.index.sasi.analyzer.filter.FilterPipelineBuilder;
import org.apache.cassandra.index.sasi.analyzer.filter.FilterPipelineExecutor;
import org.apache.cassandra.index.sasi.analyzer.filter.FilterPipelineTask;
import org.apache.cassandra.index.sasi.analyzer.filter.StemmingFilters;
import org.apache.cassandra.index.sasi.analyzer.filter.StopWordFilters;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.utils.ByteBufferUtil;

public class StandardAnalyzer extends AbstractAnalyzer {
   private AbstractType validator;
   private StandardTokenizerInterface scanner;
   private StandardTokenizerOptions options;
   private FilterPipelineTask filterPipeline;
   protected Reader inputReader = null;

   public StandardAnalyzer() {
   }

   public String getToken() {
      return this.scanner.getText();
   }

   public final boolean incrementToken() throws IOException {
      do {
         StandardAnalyzer.TokenType currentTokenType = StandardAnalyzer.TokenType.fromValue(this.scanner.getNextToken());
         if(currentTokenType == StandardAnalyzer.TokenType.EOF) {
            return false;
         }
      } while(this.scanner.yylength() > this.options.getMaxTokenLength() || this.scanner.yylength() < this.options.getMinTokenLength());

      return true;
   }

   protected String getFilteredCurrentToken() throws IOException {
      String token = this.getToken();

      Object pipelineRes;
      while(true) {
         pipelineRes = FilterPipelineExecutor.execute(this.filterPipeline, token);
         if(pipelineRes != null) {
            break;
         }

         boolean reachedEOF = this.incrementToken();
         if(!reachedEOF) {
            break;
         }

         token = this.getToken();
      }

      return (String)pipelineRes;
   }

   private FilterPipelineTask getFilterPipeline() {
      FilterPipelineBuilder builder = new FilterPipelineBuilder(new BasicResultFilters.NoOperation());
      if(!this.options.isCaseSensitive() && this.options.shouldLowerCaseTerms()) {
         builder = builder.add("to_lower", new BasicResultFilters.LowerCase());
      }

      if(!this.options.isCaseSensitive() && this.options.shouldUpperCaseTerms()) {
         builder = builder.add("to_upper", new BasicResultFilters.UpperCase());
      }

      if(this.options.shouldIgnoreStopTerms()) {
         builder = builder.add("skip_stop_words", new StopWordFilters.DefaultStopWordFilter(this.options.getLocale()));
      }

      if(this.options.shouldStemTerms()) {
         builder = builder.add("term_stemming", new StemmingFilters.DefaultStemmingFilter(this.options.getLocale()));
      }

      return builder.build();
   }

   public void init(Map<String, String> options, AbstractType validator) {
      this.init(StandardTokenizerOptions.buildFromMap(options), validator);
   }

   @VisibleForTesting
   protected void init(StandardTokenizerOptions options) {
      this.init((StandardTokenizerOptions)options, UTF8Type.instance);
   }

   public void init(StandardTokenizerOptions tokenizerOptions, AbstractType validator) {
      this.validator = validator;
      this.options = tokenizerOptions;
      this.filterPipeline = this.getFilterPipeline();
      Reader reader = new InputStreamReader(new DataInputBuffer(ByteBufferUtil.EMPTY_BYTE_BUFFER, false), StandardCharsets.UTF_8);
      this.scanner = new StandardTokenizerImpl(reader);
      this.inputReader = reader;
   }

   public boolean hasNext() {
      try {
         if(this.incrementToken() && this.getFilteredCurrentToken() != null) {
            this.next = this.validator.fromString(normalize(this.getFilteredCurrentToken()));
            return true;
         }
      } catch (IOException var2) {
         ;
      }

      return false;
   }

   public void reset(ByteBuffer input) {
      this.next = null;
      Reader reader = new InputStreamReader(new DataInputBuffer(input, false), StandardCharsets.UTF_8);
      this.scanner.yyreset(reader);
      this.inputReader = reader;
   }

   @VisibleForTesting
   public void reset(InputStream input) {
      this.next = null;
      Reader reader = new InputStreamReader(input, StandardCharsets.UTF_8);
      this.scanner.yyreset(reader);
      this.inputReader = reader;
   }

   public boolean isTokenizing() {
      return true;
   }

   public static enum TokenType {
      EOF(-1),
      ALPHANUM(0),
      NUM(6),
      SOUTHEAST_ASIAN(9),
      IDEOGRAPHIC(10),
      HIRAGANA(11),
      KATAKANA(12),
      HANGUL(13);

      private static final IntObjectMap<StandardAnalyzer.TokenType> TOKENS = new IntObjectHashMap();
      public final int value;

      private TokenType(int value) {
         this.value = value;
      }

      public int getValue() {
         return this.value;
      }

      public static StandardAnalyzer.TokenType fromValue(int val) {
         return (StandardAnalyzer.TokenType)TOKENS.get(val);
      }

      static {
         StandardAnalyzer.TokenType[] var0 = values();
         int var1 = var0.length;

         for(int var2 = 0; var2 < var1; ++var2) {
            StandardAnalyzer.TokenType type = var0[var2];
            TOKENS.put(type.value, type);
         }

      }
   }
}
