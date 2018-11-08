package com.datastax.bdp.cassandra.crypto;

import com.datastax.bdp.util.OptionMap;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

public class LocalFileSystemKeyProviderFactory implements IKeyProviderFactory {
   private static final ConcurrentMap<File, LocalFileSystemKeyProvider> keyProviders = Maps.newConcurrentMap();
   public static final String SECRET_KEY_FILE = "secret_key_file";

   public LocalFileSystemKeyProviderFactory() {
   }

   public IKeyProvider getKeyProvider(Map<String, String> options) throws IOException {
      OptionMap optionMap = new OptionMap(options);
      File secretKeyFile = new File(optionMap.get("secret_key_file", "/etc/dse/conf/data_encryption_keys"));
      LocalFileSystemKeyProvider kp = (LocalFileSystemKeyProvider)keyProviders.get(secretKeyFile);
      if(kp == null) {
         kp = new LocalFileSystemKeyProvider(secretKeyFile);
         LocalFileSystemKeyProvider previous = (LocalFileSystemKeyProvider)keyProviders.putIfAbsent(secretKeyFile, kp);
         if(previous != null) {
            kp = previous;
         }
      }

      return kp;
   }

   public Set<String> supportedOptions() {
      return Collections.singleton("secret_key_file");
   }
}
