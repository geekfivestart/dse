package com.datastax.bdp.util;

import com.datastax.bdp.gms.EndpointStateChangeAdapter;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Addresses {
   private static final Logger logger = LoggerFactory.getLogger(Addresses.class);

   public Addresses() {
   }

   public static void init() {
      Addresses.Internode.init();
      Addresses.Client.init();
   }

   public static class Client {
      private Client() {
      }

      public static void init() {
      }

      public static InetAddress getPrimaryListenAddress() {
         return DatabaseDescriptor.getNativeTransportAddress();
      }

      public static InetAddress getBroadcastAddress() {
         return FBUtilities.getNativeTransportBroadcastAddress();
      }

      public static InetAddress getBroadcastAddressOf(String internodeBroadcastAddress) {
         try {
            return getBroadcastAddressOf(InetAddress.getByName(internodeBroadcastAddress));
         } catch (UnknownHostException var2) {
            return null;
         }
      }

      public static InetAddress getBroadcastAddressOf(InetAddress internodeBroadcastAddress) {
         Preconditions.checkNotNull(internodeBroadcastAddress);
         if(Objects.equals(FBUtilities.getBroadcastAddress(), internodeBroadcastAddress)) {
            return getBroadcastAddress();
         } else {
            try {
               return InetAddress.getByName(StorageService.instance.getNativeTransportAddress(internodeBroadcastAddress));
            } catch (UnknownHostException var2) {
               return null;
            }
         }
      }
   }

   public static class Internode {
      private static ConcurrentMap<String, InetAddress> addressCache = new ConcurrentHashMap();

      private Internode() {
      }

      public static void init() {
         Gossiper.instance.register(new Addresses.Internode.AddressCacheManager());
      }

      public static InetAddress getPrimaryListenAddress() {
         return FBUtilities.getLocalAddress();
      }

      public static InetAddress getBroadcastAddress() {
         return FBUtilities.getBroadcastAddress();
      }

      public static Set<InetAddress> getListenAddresses() {
         return DatabaseDescriptor.shouldListenOnBroadcastAddress()?ImmutableSet.of(getPrimaryListenAddress(), getBroadcastAddress()):ImmutableSet.of(getPrimaryListenAddress());
      }

      public static boolean isLocalEndpoint(InetAddress address) {
         return getBroadcastAddress().equals(address) || getPrimaryListenAddress().equals(address);
      }

      public static InetAddress getPreferredHost(InetAddress address) {
         return getPreferredHost(address.getHostAddress());
      }

      public static InetAddress getPreferredHost(String address) {
         return getCachedHost(address);
      }

      private static InetAddress getCachedHost(String candidate) {
         try {
            InetAddress address = (InetAddress)addressCache.get(candidate);
            return address != null?address:InetAddress.getByName(candidate);
         } catch (UnknownHostException var2) {
            throw new IllegalStateException("Error resolving IP address " + candidate, var2);
         }
      }

      private static class AddressCacheManager extends EndpointStateChangeAdapter {
         private AddressCacheManager() {
         }

         public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value) {
            this.doChange(endpoint, state, value);
         }

         public void onJoin(InetAddress endpoint, EndpointState state) {
            this.doJoin(endpoint, state);
         }

         public void onAlive(InetAddress endpoint, EndpointState state) {
            this.doJoin(endpoint, state);
         }

         public void onRestart(InetAddress endpoint, EndpointState state) {
            this.doJoin(endpoint, state);
         }

         public void onDead(InetAddress endpoint, EndpointState state) {
            this.doLeave(endpoint);
         }

         public void onRemove(InetAddress endpoint) {
            this.doLeave(endpoint);
         }

         private void doChange(InetAddress endpoint, ApplicationState state, VersionedValue value) {
            if(state == ApplicationState.INTERNAL_IP) {
               try {
                  InetAddress internalIP = InetAddress.getByName(value.value);
                  Addresses.Internode.addressCache.put(endpoint.getHostAddress(), internalIP);
                  Addresses.Internode.addressCache.put(internalIP.getHostAddress(), internalIP);
               } catch (UnknownHostException var5) {
                  Addresses.logger.error("Error resolving IP address {}", value.value, var5);
               }
            } else {
               Addresses.Internode.addressCache.putIfAbsent(endpoint.getHostAddress(), endpoint);
            }

         }

         private void doJoin(InetAddress endpoint, EndpointState state) {
            VersionedValue versionedValue = state.getApplicationState(ApplicationState.INTERNAL_IP);
            if(versionedValue != null) {
               try {
                  InetAddress internalIP = InetAddress.getByName(versionedValue.value);
                  Addresses.Internode.addressCache.put(endpoint.getHostAddress(), internalIP);
                  Addresses.Internode.addressCache.put(internalIP.getHostAddress(), internalIP);
               } catch (UnknownHostException var5) {
                  Addresses.logger.error("Error resolving IP address {}", versionedValue.value, var5);
               }
            } else {
               Addresses.Internode.addressCache.putIfAbsent(endpoint.getHostAddress(), endpoint);
            }

         }

         private void doLeave(InetAddress endpoint) {
            InetAddress internal = (InetAddress)Addresses.Internode.addressCache.remove(endpoint.getHostAddress());
            if(internal != null) {
               Addresses.Internode.addressCache.remove(internal.getHostAddress());
            }

         }
      }
   }
}
