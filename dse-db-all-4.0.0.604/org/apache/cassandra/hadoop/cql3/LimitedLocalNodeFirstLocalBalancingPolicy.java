package org.apache.cassandra.hadoop.cql3;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class LimitedLocalNodeFirstLocalBalancingPolicy implements LoadBalancingPolicy {
   private static final Logger logger = LoggerFactory.getLogger(LimitedLocalNodeFirstLocalBalancingPolicy.class);
   private static final Set<InetAddress> localAddresses = Collections.unmodifiableSet(getLocalInetAddresses());
   private final CopyOnWriteArraySet<Host> liveReplicaHosts = new CopyOnWriteArraySet();
   private final Set<InetAddress> replicaAddresses = new HashSet();

   public LimitedLocalNodeFirstLocalBalancingPolicy(String[] replicas) {
      String[] var2 = replicas;
      int var3 = replicas.length;

      for(int var4 = 0; var4 < var3; ++var4) {
         String replica = var2[var4];

         try {
            InetAddress[] addresses = InetAddress.getAllByName(replica);
            Collections.addAll(this.replicaAddresses, addresses);
         } catch (UnknownHostException var7) {
            logger.warn("Invalid replica host name: {}, skipping it", replica);
         }
      }

      logger.trace("Created instance with the following replicas: {}", Arrays.asList(replicas));
   }

   public void init(Cluster cluster, Collection<Host> hosts) {
      List<Host> replicaHosts = new ArrayList();
      Iterator var4 = hosts.iterator();

      while(var4.hasNext()) {
         Host host = (Host)var4.next();
         if(this.replicaAddresses.contains(host.getAddress())) {
            replicaHosts.add(host);
         }
      }

      this.liveReplicaHosts.addAll(replicaHosts);
      logger.trace("Initialized with replica hosts: {}", replicaHosts);
   }

   public void close() {
   }

   public HostDistance distance(Host host) {
      return isLocalHost(host)?HostDistance.LOCAL:HostDistance.REMOTE;
   }

   public Iterator<Host> newQueryPlan(String keyspace, Statement statement) {
      List<Host> local = new ArrayList(1);
      List<Host> remote = new ArrayList(this.liveReplicaHosts.size());
      Iterator var5 = this.liveReplicaHosts.iterator();

      while(var5.hasNext()) {
         Host liveReplicaHost = (Host)var5.next();
         if(isLocalHost(liveReplicaHost)) {
            local.add(liveReplicaHost);
         } else {
            remote.add(liveReplicaHost);
         }
      }

      Collections.shuffle(remote);
      logger.trace("Using the following hosts order for the new query plan: {} | {}", local, remote);
      return Iterators.concat(local.iterator(), remote.iterator());
   }

   public void onAdd(Host host) {
      if(this.replicaAddresses.contains(host.getAddress())) {
         this.liveReplicaHosts.add(host);
         logger.trace("Added a new host {}", host);
      }

   }

   public void onUp(Host host) {
      if(this.replicaAddresses.contains(host.getAddress())) {
         this.liveReplicaHosts.add(host);
         logger.trace("The host {} is now up", host);
      }

   }

   public void onDown(Host host) {
      if(this.liveReplicaHosts.remove(host)) {
         logger.trace("The host {} is now down", host);
      }

   }

   public void onRemove(Host host) {
      if(this.liveReplicaHosts.remove(host)) {
         logger.trace("Removed the host {}", host);
      }

   }

   public void onSuspected(Host host) {
   }

   private static boolean isLocalHost(Host host) {
      InetAddress hostAddress = host.getAddress();
      return hostAddress.isLoopbackAddress() || localAddresses.contains(hostAddress);
   }

   private static Set<InetAddress> getLocalInetAddresses() {
      try {
         return Sets.newHashSet(Iterators.concat(Iterators.transform(Iterators.forEnumeration(NetworkInterface.getNetworkInterfaces()), new Function<NetworkInterface, Iterator<InetAddress>>() {
            public Iterator<InetAddress> apply(NetworkInterface netIface) {
               return Iterators.forEnumeration(netIface.getInetAddresses());
            }
         })));
      } catch (SocketException var1) {
         logger.warn("Could not retrieve local network interfaces.", var1);
         return Collections.emptySet();
      }
   }
}
