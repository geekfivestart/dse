package org.apache.cassandra.locator;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleSeedProvider implements SeedProvider {
   private static final Logger logger = LoggerFactory.getLogger(SimpleSeedProvider.class);

   public SimpleSeedProvider(Map<String, String> args) {
   }

   public List<InetAddress> getSeeds() {
      Config conf;
      try {
         conf = DatabaseDescriptor.loadConfig();
      } catch (Exception var10) {
         throw new AssertionError(var10);
      }

      String[] hosts = ((String)conf.seed_provider.parameters.get("seeds")).split(",", -1);
      List<InetAddress> seeds = new ArrayList(hosts.length);
      String[] var4 = hosts;
      int var5 = hosts.length;

      for(int var6 = 0; var6 < var5; ++var6) {
         String host = var4[var6];

         try {
            seeds.add(InetAddress.getByName(host.trim()));
         } catch (UnknownHostException var9) {
            logger.warn("Seed provider couldn't lookup host {}", host);
         }
      }

      return Collections.unmodifiableList(seeds);
   }
}
