package com.datastax.bdp.config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KmipHostOptions {
   public String hosts = null;
   public String keystore_path = null;
   public String keystore_type = null;
   public String keystore_password = null;
   public String truststore_path = null;
   public String truststore_type = null;
   public String truststore_password = null;
   public Integer timeout = null;
   public long key_cache_millis = 300000L;

   public KmipHostOptions() {
   }

   public Map<String, String> getConnectionOptions() {
      Map<String, String> opts = new HashMap();
      if(this.keystore_path != null) {
         opts.put("com.cryptsoft.kmip.keyStore", this.keystore_path);
      }

      if(this.keystore_type != null) {
         opts.put("com.cryptsoft.kmip.keyStoreType", this.keystore_type);
      }

      if(this.keystore_password != null) {
         opts.put("com.cryptsoft.kmip.keyStorePassword", this.keystore_password);
      }

      if(this.truststore_path != null) {
         opts.put("com.cryptsoft.kmip.trustStore", this.truststore_path);
      }

      if(this.truststore_type != null) {
         opts.put("com.cryptsoft.kmip.trustStoreType", this.truststore_type);
      }

      if(this.truststore_password != null) {
         opts.put("com.cryptsoft.kmip.trustStorePassword", this.truststore_password);
      }

      if(this.timeout != null) {
         opts.put("com.cryptsoft.kmip.timeout", this.timeout.toString());
      }

      return opts;
   }

   public List<String> getHosts() {
      if(this.hosts == null) {
         return Collections.emptyList();
      } else {
         String[] s = this.hosts.split(",");
         List<String> l = new ArrayList(s.length);

         for(int i = 0; i < s.length; ++i) {
            s[i] = s[i].trim();
            if(!s[i].isEmpty()) {
               l.add(s[i]);
            }
         }

         return l;
      }
   }
}
