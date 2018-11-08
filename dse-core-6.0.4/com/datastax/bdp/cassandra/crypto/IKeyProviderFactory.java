package com.datastax.bdp.cassandra.crypto;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public interface IKeyProviderFactory {
   IKeyProvider getKeyProvider(Map<String, String> var1) throws IOException;

   Set<String> supportedOptions();
}
