package com.datastax.bdp.transport.server;

import com.datastax.bdp.config.DseConfig;
import com.sun.security.auth.module.Krb5LoginModule;
import java.util.HashMap;
import java.util.Map;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;

public class KrbConfiguration extends Configuration {
   private static final Map<String, String> KEYTAB_KERBEROS_OPTIONS = new HashMap();
   private static final AppConfigurationEntry KEYTAB_KERBEROS_LOGIN;

   public KrbConfiguration() {
   }

   public AppConfigurationEntry[] getAppConfigurationEntry(String arg0) {
      return new AppConfigurationEntry[]{KEYTAB_KERBEROS_LOGIN};
   }

   static {
      KEYTAB_KERBEROS_OPTIONS.put("doNotPrompt", "true");
      KEYTAB_KERBEROS_OPTIONS.put("useKeyTab", "true");
      KEYTAB_KERBEROS_OPTIONS.put("storeKey", "true");
      KEYTAB_KERBEROS_OPTIONS.put("keyTab", DseConfig.getDseServiceKeytab());
      KEYTAB_KERBEROS_OPTIONS.put("principal", DseConfig.getDseServicePrincipal().asLocal());
      KEYTAB_KERBEROS_LOGIN = new AppConfigurationEntry(Krb5LoginModule.class.getName(), LoginModuleControlFlag.REQUIRED, KEYTAB_KERBEROS_OPTIONS);
   }
}
