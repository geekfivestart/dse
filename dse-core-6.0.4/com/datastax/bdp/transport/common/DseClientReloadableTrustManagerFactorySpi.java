package com.datastax.bdp.transport.common;

import java.security.InvalidAlgorithmParameterException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import javax.net.ssl.ManagerFactoryParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactorySpi;

public class DseClientReloadableTrustManagerFactorySpi extends TrustManagerFactorySpi {
   private DseReloadableTrustManager manager;

   public DseClientReloadableTrustManagerFactorySpi() {
   }

   protected void engineInit(KeyStore keyStore) throws KeyStoreException {
   }

   protected void engineInit(ManagerFactoryParameters managerFactoryParameters) throws InvalidAlgorithmParameterException {
   }

   protected TrustManager[] engineGetTrustManagers() {
      if(null == this.manager) {
         this.manager = DseReloadableTrustManager.clientEncryptionInstance();
      }

      return new TrustManager[]{this.manager};
   }
}
