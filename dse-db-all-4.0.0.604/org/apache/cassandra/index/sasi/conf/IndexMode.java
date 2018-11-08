package org.apache.cassandra.index.sasi.conf;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.index.sasi.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sasi.analyzer.NoOpAnalyzer;
import org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer;
import org.apache.cassandra.index.sasi.analyzer.StandardAnalyzer;
import org.apache.cassandra.index.sasi.disk.OnDiskIndexBuilder;
import org.apache.cassandra.index.sasi.plan.Expression;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexMode {
   private static final Logger logger = LoggerFactory.getLogger(IndexMode.class);
   public static final IndexMode NOT_INDEXED;
   private static final Set<AbstractType<?>> TOKENIZABLE_TYPES;
   private static final String INDEX_MODE_OPTION = "mode";
   private static final String INDEX_ANALYZED_OPTION = "analyzed";
   private static final String INDEX_ANALYZER_CLASS_OPTION = "analyzer_class";
   private static final String INDEX_IS_LITERAL_OPTION = "is_literal";
   private static final String INDEX_MAX_FLUSH_MEMORY_OPTION = "max_compaction_flush_memory_in_mb";
   private static final double INDEX_MAX_FLUSH_DEFAULT_MULTIPLIER = 0.15D;
   public final OnDiskIndexBuilder.Mode mode;
   public final boolean isAnalyzed;
   public final boolean isLiteral;
   public final Class analyzerClass;
   public final long maxCompactionFlushMemoryInMb;

   private IndexMode(OnDiskIndexBuilder.Mode mode, boolean isLiteral, boolean isAnalyzed, Class analyzerClass, long maxFlushMemMb) {
      this.mode = mode;
      this.isLiteral = isLiteral;
      this.isAnalyzed = isAnalyzed;
      this.analyzerClass = analyzerClass;
      this.maxCompactionFlushMemoryInMb = maxFlushMemMb;
   }

   public AbstractAnalyzer getAnalyzer(AbstractType<?> validator) {
      Object analyzer = new NoOpAnalyzer();

      try {
         if(this.isAnalyzed) {
            if(this.analyzerClass != null) {
               analyzer = (AbstractAnalyzer)this.analyzerClass.newInstance();
            } else if(TOKENIZABLE_TYPES.contains(validator)) {
               analyzer = new StandardAnalyzer();
            }
         }
      } catch (IllegalAccessException | InstantiationException var4) {
         logger.error("Failed to create new instance of analyzer with class [{}]", this.analyzerClass.getName(), var4);
      }

      return (AbstractAnalyzer)analyzer;
   }

   public static void validateAnalyzer(Map<String, String> indexOptions) throws ConfigurationException {
      if(indexOptions.containsKey("analyzer_class")) {
         try {
            Class.forName((String)indexOptions.get("analyzer_class"));
         } catch (ClassNotFoundException var2) {
            throw new ConfigurationException(String.format("Invalid analyzer class option specified [%s]", new Object[]{indexOptions.get("analyzer_class")}));
         }
      }

   }

   public static IndexMode getMode(ColumnMetadata column, Optional<IndexMetadata> config) throws ConfigurationException {
      return getMode(column, config.isPresent()?((IndexMetadata)config.get()).options:null);
   }

   public static IndexMode getMode(ColumnMetadata column, Map<String, String> indexOptions) throws ConfigurationException {
      if(indexOptions != null && !indexOptions.isEmpty()) {
         OnDiskIndexBuilder.Mode mode;
         try {
            mode = indexOptions.get("mode") == null?OnDiskIndexBuilder.Mode.PREFIX:OnDiskIndexBuilder.Mode.mode((String)indexOptions.get("mode"));
         } catch (IllegalArgumentException var9) {
            throw new ConfigurationException("Incorrect index mode: " + (String)indexOptions.get("mode"));
         }

         boolean isAnalyzed = false;
         Class analyzerClass = null;

         try {
            if(indexOptions.get("analyzer_class") != null) {
               analyzerClass = Class.forName((String)indexOptions.get("analyzer_class"));
               isAnalyzed = indexOptions.get("analyzed") == null?true:Boolean.parseBoolean((String)indexOptions.get("analyzed"));
            } else if(indexOptions.get("analyzed") != null) {
               isAnalyzed = Boolean.parseBoolean((String)indexOptions.get("analyzed"));
            }
         } catch (ClassNotFoundException var8) {
            logger.error("Failed to find specified analyzer class [{}]. Falling back to default analyzer", indexOptions.get("analyzer_class"));
         }

         boolean isLiteral = false;

         try {
            String literalOption = (String)indexOptions.get("is_literal");
            AbstractType<?> validator = column.cellValueType();
            isLiteral = literalOption == null?validator instanceof UTF8Type || validator instanceof AsciiType:Boolean.parseBoolean(literalOption);
         } catch (Exception var10) {
            logger.error("failed to parse {} option, defaulting to 'false'.", "is_literal");
         }

         Long maxMemMb = Long.valueOf(indexOptions.get("max_compaction_flush_memory_in_mb") == null?161061273L:Long.parseLong((String)indexOptions.get("max_compaction_flush_memory_in_mb")));
         return new IndexMode(mode, isLiteral, isAnalyzed, analyzerClass, maxMemMb.longValue());
      } else {
         return NOT_INDEXED;
      }
   }

   public boolean supports(Expression.Op operator) {
      return this.mode.supports(operator);
   }

   static {
      NOT_INDEXED = new IndexMode(OnDiskIndexBuilder.Mode.PREFIX, true, false, NonTokenizingAnalyzer.class, 0L);
      TOKENIZABLE_TYPES = new HashSet<AbstractType<?>>() {
         {
            this.add(UTF8Type.instance);
            this.add(AsciiType.instance);
         }
      };
   }
}
