package org.apache.cassandra.io.compress;

import com.google.common.collect.Lists;
import java.util.Map;

public class EncryptingLZ4Compressor extends ChainedCompressor {
   public static EncryptingLZ4Compressor create(Map<String, String> options) {
      return new EncryptingLZ4Compressor(options);
   }

   public EncryptingLZ4Compressor(Map<String, String> options) {
      super(Lists.newArrayList(new ICompressor[]{LZ4Compressor.create(options), Encryptor.create(options)}));
   }
}
