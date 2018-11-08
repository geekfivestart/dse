package com.datastax.bdp.cassandra.crypto.kmip;

import com.cryptsoft.kmip.Kmip;
import java.io.IOException;
import java.util.Map;

public class CloseableKmip extends Kmip implements AutoCloseable {
   public CloseableKmip(Map<String, String> properties) throws IOException {
      super(properties);
   }
}
