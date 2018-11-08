package com.datastax.bdp.cassandra.crypto;

import com.datastax.bdp.cassandra.crypto.kmip.KmipHost;
import com.datastax.bdp.cassandra.crypto.kmip.KmipHosts;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import org.apache.cassandra.utils.Pair;

public class KmipKeyProviderFactory implements IKeyProviderFactory {
   private static ConcurrentMap<Pair<String, KmipHost.Options>, KmipKeyProvider> providers = Maps.newConcurrentMap();
   public static final String HOST_NAME = "kmip_host";
   public static final String TEMPLATE_NAME = "template_name";
   static final String KEY_NAMESPACE = "key_namespace";

   public KmipKeyProviderFactory() {
   }

   public IKeyProvider getKeyProvider(Map<String, String> options) throws IOException {
      String host = (String)options.get("kmip_host");
      if(host == null) {
         throw new IOException("kmip_host must be provided");
      } else {
         KmipHost.Options hostOpts = new KmipHost.Options((String)options.get("template_name"), (String)options.get("key_namespace"));
         Pair<String, KmipHost.Options> cacheKey = Pair.create(host, hostOpts);
         KmipKeyProvider provider = (KmipKeyProvider)providers.get(cacheKey);
         if(provider == null) {
            KmipHost kmipHost = KmipHosts.getHost(host);
            if(kmipHost == null) {
               throw new IOException("Invalid kmip host name: " + host);
            }

            provider = new KmipKeyProvider(kmipHost, hostOpts);
            KmipKeyProvider previous = (KmipKeyProvider)providers.putIfAbsent(cacheKey, provider);
            provider = previous == null?provider:previous;
         }

         return provider;
      }
   }

   public Set<String> supportedOptions() {
      return Sets.newHashSet(new String[]{"kmip_host", "template_name", "key_namespace"});
   }
}
