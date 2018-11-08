package com.datastax.bdp.db.tools.nodesync;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

class ClusterBuilder {
   private final InetAddress contactPoint;
   private final int port;
   private AuthProvider authProvider;
   private String username;
   private String password;
   private boolean ssl;

   ClusterBuilder(InetAddress contactPoint, int port) {
      this.contactPoint = contactPoint;
      this.port = port;
   }

   ClusterBuilder withAuthProvider(@Nullable String className) {
      if(className == null) {
         this.authProvider = null;
      } else {
         try {
            Class<?> clazz = Class.forName(className);
            if(!AuthProvider.class.isAssignableFrom(clazz)) {
               throw new NodeSyncException(clazz + " is not a valid auth provider");
            }

            if(PlainTextAuthProvider.class.equals(clazz)) {
               this.authProvider = (AuthProvider)clazz.getConstructor(new Class[]{String.class, String.class}).newInstance(new Object[]{this.username, this.password});
            } else {
               this.authProvider = (AuthProvider)clazz.newInstance();
            }
         } catch (Exception var3) {
            throw new NodeSyncException("Invalid auth provider: " + className);
         }
      }

      return this;
   }

   ClusterBuilder withUsername(@Nullable String username) {
      this.username = username;
      return this;
   }

   ClusterBuilder withPassword(@Nullable String password) {
      this.password = password;
      return this;
   }

   ClusterBuilder withSSL(boolean ssl) {
      this.ssl = ssl;
      return this;
   }

   Cluster build() {
      Builder builder = Cluster.builder().addContactPoints(new InetAddress[]{this.contactPoint}).withPort(this.port);
      if(this.ssl) {
         builder.withSSL();
      }

      List<InetSocketAddress> whitelist = Collections.singletonList(new InetSocketAddress(this.contactPoint, this.port));
      LoadBalancingPolicy policy = builder.getConfiguration().getPolicies().getLoadBalancingPolicy();
      builder.withLoadBalancingPolicy(new WhiteListPolicy(policy, whitelist));
      if(this.authProvider != null) {
         builder.withAuthProvider(this.authProvider);
      } else if(this.username != null) {
         builder.withCredentials(this.username, this.password);
      }

      return builder.build();
   }
}
