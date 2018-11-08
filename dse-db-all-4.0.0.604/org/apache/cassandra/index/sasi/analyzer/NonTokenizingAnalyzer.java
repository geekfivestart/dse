package org.apache.cassandra.index.sasi.analyzer;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sasi.analyzer.filter.BasicResultFilters;
import org.apache.cassandra.index.sasi.analyzer.filter.FilterPipelineBuilder;
import org.apache.cassandra.index.sasi.analyzer.filter.FilterPipelineExecutor;
import org.apache.cassandra.index.sasi.analyzer.filter.FilterPipelineTask;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NonTokenizingAnalyzer extends AbstractAnalyzer {
   private static final Logger logger = LoggerFactory.getLogger(NonTokenizingAnalyzer.class);
   private static final Set<AbstractType<?>> VALID_ANALYZABLE_TYPES = new HashSet<AbstractType<?>>() {
      {
         this.add(UTF8Type.instance);
         this.add(AsciiType.instance);
      }
   };
   private AbstractType validator;
   private NonTokenizingOptions options;
   private FilterPipelineTask filterPipeline;
   private ByteBuffer input;
   private boolean hasNext = false;

   public NonTokenizingAnalyzer() {
   }

   public void init(Map<String, String> options, AbstractType validator) {
      this.init(NonTokenizingOptions.buildFromMap(options), validator);
   }

   public void init(NonTokenizingOptions tokenizerOptions, AbstractType validator) {
      this.validator = validator;
      this.options = tokenizerOptions;
      this.filterPipeline = this.getFilterPipeline();
   }

   public boolean hasNext() {
      if(!VALID_ANALYZABLE_TYPES.contains(this.validator)) {
         return false;
      } else if(this.hasNext) {
         boolean var3;
         try {
            String inputStr = this.validator.getString(this.input);
            if(inputStr == null) {
               throw new MarshalException(String.format("'null' deserialized value for %s with %s", new Object[]{ByteBufferUtil.bytesToHex(this.input), this.validator}));
            }

            Object pipelineRes = FilterPipelineExecutor.execute(this.filterPipeline, inputStr);
            if(pipelineRes != null) {
               this.next = this.validator.fromString(normalize((String)pipelineRes));
               var3 = true;
               return var3;
            }

            var3 = false;
         } catch (MarshalException var7) {
            logger.error("Failed to deserialize value with " + this.validator, var7);
            var3 = false;
            return var3;
         } finally {
            this.hasNext = false;
         }

         return var3;
      } else {
         return false;
      }
   }

   public void reset(ByteBuffer input) {
      this.next = null;
      this.input = input;
      this.hasNext = true;
   }

   private FilterPipelineTask getFilterPipeline() {
      FilterPipelineBuilder builder = new FilterPipelineBuilder(new BasicResultFilters.NoOperation());
      if(this.options.isCaseSensitive() && this.options.shouldLowerCaseOutput()) {
         builder = builder.add("to_lower", new BasicResultFilters.LowerCase());
      }

      if(this.options.isCaseSensitive() && this.options.shouldUpperCaseOutput()) {
         builder = builder.add("to_upper", new BasicResultFilters.UpperCase());
      }

      if(!this.options.isCaseSensitive()) {
         builder = builder.add("to_lower", new BasicResultFilters.LowerCase());
      }

      return builder.build();
   }
}
