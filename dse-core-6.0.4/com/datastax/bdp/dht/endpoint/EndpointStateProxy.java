package com.datastax.bdp.dht.endpoint;

import com.datastax.bdp.snitch.EndpointStateTracker;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import org.apache.cassandra.locator.DynamicEndpointSnitch;
import org.apache.cassandra.locator.IEndpointSnitch;

public class EndpointStateProxy {
   protected final EndpointStateTracker endpointStateTracker;
   private final InetAddress localAddress;
   private final IEndpointSnitch endpointSnitch;
   private final Map<Endpoint, Double> nodesHealth = new HashMap();
   private final Map<Endpoint, Boolean> activeEndpoints = new HashMap();
   private final Map<Endpoint, Boolean> blacklistedEndpoints = new HashMap();

   public EndpointStateProxy(IEndpointSnitch endpointSnitch, InetAddress localAddress, EndpointStateTracker endpointStateTracker) {
      this.localAddress = localAddress;
      this.endpointStateTracker = endpointStateTracker;
      this.endpointSnitch = endpointSnitch instanceof DynamicEndpointSnitch?((DynamicEndpointSnitch)endpointSnitch).subsnitch:endpointSnitch;
   }

   public boolean isActive(Endpoint endpoint) {
      if(!this.activeEndpoints.containsKey(endpoint)) {
         this.activeEndpoints.put(endpoint, Boolean.valueOf(this.endpointStateTracker.isActive(endpoint.getAddress())));
      }

      return ((Boolean)this.activeEndpoints.get(endpoint)).booleanValue();
   }

   public boolean isBlacklisted(Endpoint endpoint) {
      if(!this.blacklistedEndpoints.containsKey(endpoint)) {
         this.blacklistedEndpoints.put(endpoint, this.endpointStateTracker.getBlacklistedStatus(endpoint.getAddress()));
      }

      return ((Boolean)this.blacklistedEndpoints.get(endpoint)).booleanValue();
   }

   public double getNodeHealth(Endpoint endpoint) {
      Double nodeHealth = (Double)this.nodesHealth.get(endpoint);
      if(nodeHealth == null) {
         nodeHealth = this.endpointStateTracker.getNodeHealth(endpoint.getAddress());
         this.nodesHealth.put(endpoint, nodeHealth);
      }

      return nodeHealth.doubleValue();
   }

   public IEndpointSnitch getEndpointSnitch() {
      return this.endpointSnitch;
   }

   public InetAddress getLocalAddress() {
      return this.localAddress;
   }
}
