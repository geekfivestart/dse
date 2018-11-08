package org.apache.cassandra.hints;

import com.google.common.collect.ImmutableMap;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.schema.CompressionParams;

public class HintsAccessor {
   public HintsAccessor() {
   }

   public static Class<? extends ICompressor> getCompressorClass() {
      ImmutableMap<String, Object> params = HintsService.instance.getCatalog().getWriterParams();
      ParameterizedClass cl = HintsDescriptor.createCompressionConfig(params);
      return CompressionParams.createCompressor(cl).getClass();
   }
}
