package com.datastax.bdp.cassandra.crypto;

import com.datastax.bdp.server.DseDaemon;
import com.datastax.bdp.system.DseSystemKeyspace;
import com.datastax.bdp.util.OptionMap;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

public class ReplicatedKeyProviderFactory implements IKeyProviderFactory {
   private static ConcurrentMap<String, ReplicatedKeyProvider> providers = Maps.newConcurrentMap();
   public static final String SYSTEM_KEY_FILE = "system_key_file";
   public static final String DEFAULT_SYSTEM_KEY_FILE = "system_key";
   public static final String LOCAL_KEY_FILE = "secret_key_file";
   public static final String DEFAULT_LOCAL_KEY_FILE = "/etc/dse/conf/data_encryption_keys";
   private static volatile boolean schemaSetup = false;

   public ReplicatedKeyProviderFactory() {
   }

   private static void setupSchema() {
      if(DseDaemon.isDaemonMode() && !schemaSetup && !DseSystemKeyspace.isCreated() && DseDaemon.isSetup()) {
         Class var0 = ReplicatedKeyProvider.class;
         synchronized(ReplicatedKeyProvider.class) {
            if(!schemaSetup) {
               DseSystemKeyspace.maybeConfigure();
            }

            schemaSetup = true;
         }
      }

   }

   public IKeyProvider getKeyProvider(Map<String, String> options) throws IOException {
      setupSchema();
      OptionMap optionMap = new OptionMap(options);
      String systemKeyName = optionMap.get("system_key_file", "system_key");
      if(systemKeyName.contains(File.pathSeparator)) {
         throw new IOException("system_key cannot contain " + File.pathSeparator);
      } else {
         SystemKey systemKey = null;
         systemKey = SystemKey.getSystemKey(systemKeyName);
         File localKeyFile = new File(optionMap.get("secret_key_file", "/etc/dse/conf/data_encryption_keys"));
         String mapKey;
         if(systemKey instanceof LocalSystemKey) {
            mapKey = (new File(systemKey.getName())).getAbsolutePath();
            if(mapKey.equals(localKeyFile.getAbsolutePath())) {
               throw new IOException("system key and local key cannot be the same");
            }
         }

         mapKey = systemKey.getName() + ":" + localKeyFile.getAbsolutePath();
         ReplicatedKeyProvider provider = (ReplicatedKeyProvider)providers.get(mapKey);
         if(provider == null) {
            provider = new ReplicatedKeyProvider(systemKey, localKeyFile);
            ReplicatedKeyProvider previous = (ReplicatedKeyProvider)providers.putIfAbsent(mapKey, provider);
            if(previous != null) {
               provider = previous;
            }
         }

         return provider;
      }
   }

   public Set<String> supportedOptions() {
      return new HashSet(Arrays.asList(new String[]{"system_key_file", "secret_key_file"}));
   }
}
