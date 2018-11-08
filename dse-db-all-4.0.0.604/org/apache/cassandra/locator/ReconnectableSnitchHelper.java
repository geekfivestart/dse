package org.apache.cassandra.locator;

import com.google.common.annotations.VisibleForTesting;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.net.MessagingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReconnectableSnitchHelper implements IEndpointStateChangeSubscriber {
   private static final Logger logger = LoggerFactory.getLogger(ReconnectableSnitchHelper.class);
   private final IEndpointSnitch snitch;
   private final String localDc;
   private final boolean preferLocal;

   public ReconnectableSnitchHelper(IEndpointSnitch snitch, String localDc, boolean preferLocal) {
      this.snitch = snitch;
      this.localDc = localDc;
      this.preferLocal = preferLocal;
   }

   private void reconnect(InetAddress publicAddress, VersionedValue localAddressValue) {
      try {
         reconnect(publicAddress, InetAddress.getByName(localAddressValue.value), this.snitch, this.localDc);
      } catch (UnknownHostException var4) {
         logger.error("Error in getting the IP address resolved: ", var4);
      }

   }

   @VisibleForTesting
   static CompletableFuture<Void> reconnect(InetAddress publicAddress, InetAddress localAddress, IEndpointSnitch snitch, String localDc) {
      return MessagingService.instance().getConnectionPool(publicAddress).thenCompose((cp) -> {
         if(cp == null) {
            logger.debug("InternodeAuthenticator said don't reconnect to {} on {}", publicAddress, localAddress);
            return CompletableFuture.completedFuture((Object)null);
         } else {
            return snitch.getDatacenter(publicAddress).equals(localDc) && !cp.endPoint().equals(localAddress)?SystemKeyspace.updatePreferredIP(publicAddress, localAddress).thenAccept((r) -> {
               cp.reset(localAddress);
               logger.debug("Initiated reconnect to an Internal IP {} for the {}", localAddress, publicAddress);
            }):CompletableFuture.completedFuture((Object)null);
         }
      });
   }

   public void beforeChange(InetAddress endpoint, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue) {
   }

   public void onJoin(InetAddress endpoint, EndpointState epState) {
      if(this.preferLocal && !Gossiper.instance.isDeadState(epState) && epState.getApplicationState(ApplicationState.INTERNAL_IP) != null) {
         this.reconnect(endpoint, epState.getApplicationState(ApplicationState.INTERNAL_IP));
      }

   }

   public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value) {
      if(this.preferLocal && state == ApplicationState.INTERNAL_IP && !Gossiper.instance.isDeadState(Gossiper.instance.getEndpointStateForEndpoint(endpoint))) {
         this.reconnect(endpoint, value);
      }

   }

   public void onAlive(InetAddress endpoint, EndpointState state) {
      if(this.preferLocal && state.getApplicationState(ApplicationState.INTERNAL_IP) != null) {
         this.reconnect(endpoint, state.getApplicationState(ApplicationState.INTERNAL_IP));
      }

   }

   public void onDead(InetAddress endpoint, EndpointState state) {
   }

   public void onRemove(InetAddress endpoint) {
   }

   public void onRestart(InetAddress endpoint, EndpointState state) {
   }
}
