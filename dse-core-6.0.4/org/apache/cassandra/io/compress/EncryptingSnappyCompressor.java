package org.apache.cassandra.io.compress;

import com.google.common.collect.Lists;
import java.util.Map;

public class EncryptingSnappyCompressor extends ChainedCompressor {
   public static EncryptingSnappyCompressor create(Map<String, String> options) {
      return new EncryptingSnappyCompressor(options);
   }

   public EncryptingSnappyCompressor(Map<String, String> options) {
      super(Lists.newArrayList(new ICompressor[]{SnappyCompressor.create(options), Encryptor.create(options)}));
   }
}
