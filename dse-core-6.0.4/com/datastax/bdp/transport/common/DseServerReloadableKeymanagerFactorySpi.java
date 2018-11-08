package com.datastax.bdp.transport.common;

import com.datastax.bdp.config.DseConfigurationLoader;
import java.security.InvalidAlgorithmParameterException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.KeyManagerFactorySpi;
import javax.net.ssl.ManagerFactoryParameters;

public class DseServerReloadableKeymanagerFactorySpi extends KeyManagerFactorySpi {
   private KeyManagerFactory wrapped = KeyManagerFactory.getInstance(DseConfigurationLoader.getServerEncryptionAlgorithm());

   public DseServerReloadableKeymanagerFactorySpi() throws NoSuchAlgorithmException {
   }

   protected void engineInit(KeyStore keyStore, char[] chars) throws KeyStoreException, NoSuchAlgorithmException, UnrecoverableKeyException {
      this.wrapped.init(keyStore, chars);
   }

   protected void engineInit(ManagerFactoryParameters managerFactoryParameters) throws InvalidAlgorithmParameterException {
      this.wrapped.init(managerFactoryParameters);
   }

   protected KeyManager[] engineGetKeyManagers() {
      return this.wrapped.getKeyManagers();
   }
}
