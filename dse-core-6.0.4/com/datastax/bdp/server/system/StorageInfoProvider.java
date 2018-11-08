package com.datastax.bdp.server.system;

import com.datastax.bdp.util.Addresses;
import com.google.common.collect.ImmutableSet;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.cassandra.service.StorageServiceMBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface StorageInfoProvider {
   Set<String> getTokens();

   float getOwnership();

   String getState();

   public static class JmxStorageInfoProvider implements StorageInfoProvider {
      private static final Logger logger = LoggerFactory.getLogger(StorageInfoProvider.JmxStorageInfoProvider.class);
      private StorageServiceMBean storage;
      private InetAddress localAddress = Addresses.Internode.getBroadcastAddress();

      public JmxStorageInfoProvider() {
         MBeanServer server = ManagementFactory.getPlatformMBeanServer();

         try {
            this.storage = (StorageServiceMBean)JMX.newMBeanProxy(server, ObjectName.getInstance("org.apache.cassandra.db:type=StorageService"), StorageServiceMBean.class);
         } catch (MalformedObjectNameException var3) {
            throw new RuntimeException(var3);
         }
      }

      public Set<String> getTokens() {
         List tokens;
         try {
            tokens = this.storage.getTokens(this.localAddress.getHostAddress());
            Collections.sort(tokens);
         } catch (UnknownHostException var3) {
            logger.info("Unable to retrieve tokens for local host {}", this.localAddress, var3);
            tokens = Collections.emptyList();
         }

         return ImmutableSet.copyOf(tokens);
      }

      public float getOwnership() {
         return ((Float)this.storage.getOwnership().get(this.localAddress)).floatValue();
      }

      public String getState() {
         return this.storage.getJoiningNodes().contains(this.localAddress.getHostAddress())?"joining":(this.storage.getLeavingNodes().contains(this.localAddress.getHostAddress())?"leaving":(this.storage.getMovingNodes().contains(this.localAddress.getHostAddress())?"moving":"normal"));
      }
   }
}
