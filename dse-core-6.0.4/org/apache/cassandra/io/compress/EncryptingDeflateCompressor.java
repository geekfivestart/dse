package org.apache.cassandra.io.compress;

import com.google.common.collect.Lists;
import java.util.Map;

public class EncryptingDeflateCompressor extends ChainedCompressor {
   public static EncryptingDeflateCompressor create(Map<String, String> options) {
      return new EncryptingDeflateCompressor(options);
   }

   public EncryptingDeflateCompressor(Map<String, String> options) {
      super(Lists.newArrayList(new ICompressor[]{DeflateCompressor.create(options), Encryptor.create(options)}));
   }
}
