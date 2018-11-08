package com.datastax.bdp.transport.common;

import com.datastax.bdp.config.ClientConfiguration;
import java.util.HashMap;
import java.util.Map;

public class SaslProperties {
   public static final String SASL_DEFAULT_REALM = "default";
   public static final String SASL_DEFAULT_SERVICE_NAME = "dse";
   public static final String SASL_GSSAPI_MECHANISM = "GSSAPI";
   public static final String SASL_DIGEST_MECHANISM = "DIGEST-MD5";

   public SaslProperties() {
   }

   public static Map<String, String> defaultProperties(ClientConfiguration clientConf) {
      Map<String, String> saslProperties = new HashMap();
      saslProperties.put("javax.security.sasl.server.authentication", "true");
      saslProperties.put("javax.security.sasl.qop", defaultQop(clientConf));
      return saslProperties;
   }

   private static String defaultQop(ClientConfiguration clientConf) {
      try {
         return clientConf.getSaslQop();
      } catch (Throwable var2) {
         return "auth";
      }
   }

   public static void copyProperties(Map<String, String> options, Map<String, String> saslProperties) {
      OptionReader reader = new OptionReader(options);
      if(options.containsKey("javax.security.sasl.server.authentication")) {
         saslProperties.put("javax.security.sasl.server.authentication", reader.getString("javax.security.sasl.server.authentication", (String)null));
      }

      if(options.containsKey("javax.security.sasl.qop")) {
         saslProperties.put("javax.security.sasl.qop", reader.getString("javax.security.sasl.qop", (String)null));
      }

   }
}
