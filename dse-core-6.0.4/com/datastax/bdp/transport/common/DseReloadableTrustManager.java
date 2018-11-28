package com.datastax.bdp.transport.common;

import com.datastax.bdp.config.DseConfigurationLoader;
import com.datastax.bdp.jmx.JMX;
import com.datastax.bdp.util.DseUtil;
import com.datastax.bdp.util.MapBuilder;
import java.io.FileInputStream;
import java.net.Socket;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DseReloadableTrustManager extends X509ExtendedTrustManager implements DseReloadableTrustManagerMXBean {
   private static final transient Logger logger = LoggerFactory.getLogger(DseReloadableTrustManager.class);
   public static final String name = DseReloadableTrustManager.class.getName();
   private final String algorithm;
   private final String storeType;
   private X509ExtendedTrustManager manager;
   private final String tsPath;
   public static final String serverName = "DseServerReloadableTrustManager";
   public static final String clientName = "DseClientReloadableTrustManager";

   public DseReloadableTrustManager(String name, String tsPath, String algorithm, String storeType) {
      logger.info("Using DSE reloadable " + name + " TM with: " + tsPath);
      this.tsPath = tsPath;
      this.algorithm = algorithm == null?TrustManagerFactory.getDefaultAlgorithm():algorithm;
      this.storeType = storeType == null?KeyStore.getDefaultType():storeType;

      try {
         this.reloadTrustManager();
         JMX.registerMBean(this, JMX.Type.CORE, MapBuilder.<String,String>immutable().withKeys(new String[]{"name"}).withValues(new String[]{name}).build());
      } catch (Exception var6) {
         throw new RuntimeException(var6);
      }
   }

   public static DseReloadableTrustManager serverEncryptionInstance() {
      return DseReloadableTrustManager.DseServerReloadableTrustManagerLoader.instance;
   }

   public static DseReloadableTrustManager clientEncryptionInstance() {
      return DseReloadableTrustManager.DseClientReloadableTrustManagerLoader.instance;
   }

   public void reloadTrustManager() throws Exception {
      logger.info("Reloading TS from:" + this.tsPath);
      KeyStore keyStore = KeyStore.getInstance(this.storeType);
      FileInputStream inputStream = new FileInputStream(this.tsPath);

      try {
         keyStore.load(inputStream, (char[])null);
      } finally {
         inputStream.close();
      }

      TrustManagerFactory tmf = TrustManagerFactory.getInstance(this.algorithm);
      tmf.init(keyStore);
      TrustManager[] var4 = tmf.getTrustManagers();
      TrustManager[] var5 = var4;
      int var6 = var4.length;

      for(int var7 = 0; var7 < var6; ++var7) {
         TrustManager tm = var5[var7];
         if(tm instanceof X509ExtendedTrustManager) {
            this.manager = (X509ExtendedTrustManager)tm;
            return;
         }
      }

      throw new NoSuchAlgorithmException("No " + this.algorithm + " in TrustManagerFactory");
   }

   public void checkClientTrusted(X509Certificate[] x509Certificates, String s, Socket socket) throws CertificateException {
      this.manager.checkClientTrusted(x509Certificates, s, socket);
   }

   public void checkServerTrusted(X509Certificate[] x509Certificates, String s, Socket socket) throws CertificateException {
      this.manager.checkServerTrusted(x509Certificates, s, socket);
   }

   public void checkClientTrusted(X509Certificate[] x509Certificates, String s, SSLEngine sslEngine) throws CertificateException {
      this.manager.checkClientTrusted(x509Certificates, s, sslEngine);
   }

   public void checkServerTrusted(X509Certificate[] x509Certificates, String s, SSLEngine sslEngine) throws CertificateException {
      this.manager.checkServerTrusted(x509Certificates, s, sslEngine);
   }

   public void checkClientTrusted(X509Certificate[] certificates, String authType) throws CertificateException {
      this.manager.checkClientTrusted(certificates, authType);
   }

   public void checkServerTrusted(X509Certificate[] certificates, String authType) throws CertificateException {
      this.manager.checkServerTrusted(certificates, authType);
   }

   public X509Certificate[] getAcceptedIssuers() {
      return this.manager.getAcceptedIssuers();
   }

   private static class DseClientReloadableTrustManagerLoader {
      static DseReloadableTrustManager instance;

      private DseClientReloadableTrustManagerLoader() {
      }

      static {
         instance = new DseReloadableTrustManager("DseClientReloadableTrustManager", DseUtil.makeAbsolute(DatabaseDescriptor.getClientEncryptionOptions().truststore), DseConfigurationLoader.getClientEncryptionAlgorithm(), DatabaseDescriptor.getClientEncryptionOptions().store_type);
      }
   }

   private static class DseServerReloadableTrustManagerLoader {
      static DseReloadableTrustManager instance;

      private DseServerReloadableTrustManagerLoader() {
      }

      static {
         instance = new DseReloadableTrustManager("DseServerReloadableTrustManager", DseUtil.makeAbsolute(DatabaseDescriptor.getServerEncryptionOptions().truststore), DseConfigurationLoader.getServerEncryptionAlgorithm(), DatabaseDescriptor.getServerEncryptionOptions().store_type);
      }
   }
}
