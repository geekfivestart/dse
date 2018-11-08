package com.datastax.bdp.transport.common;

import java.security.Provider;
import java.security.Security;

public class DseReloadableTrustManagerProvider extends Provider {
   private static final DseReloadableTrustManagerProvider INSTANCE = new DseReloadableTrustManagerProvider();

   private DseReloadableTrustManagerProvider() {
      super(DseReloadableTrustManager.name, 0.1D, "DSE Reloadable TrustManager");
      this.put("TrustManagerFactory.DseServerReloadableTrustManager", "com.datastax.bdp.transport.common.DseServerReloadableTrustManagerFactorySpi");
      this.put("KeyManagerFactory.DseServerReloadableTrustManager", "com.datastax.bdp.transport.common.DseServerReloadableKeymanagerFactorySpi");
      this.put("TrustManagerFactory.DseClientReloadableTrustManager", "com.datastax.bdp.transport.common.DseClientReloadableTrustManagerFactorySpi");
      this.put("KeyManagerFactory.DseClientReloadableTrustManager", "com.datastax.bdp.transport.common.DseClientReloadableKeymanagerFactorySpi");
   }

   public static synchronized void maybeInstall() {
      if(Security.getProvider(INSTANCE.getName()) == null) {
         Security.addProvider(INSTANCE);
      }

   }

   public static synchronized void maybeUninstall() {
      if(Security.getProvider(INSTANCE.getName()) == INSTANCE) {
         Security.removeProvider(INSTANCE.getName());
      }

   }
}
