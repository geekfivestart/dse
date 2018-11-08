package com.datastax.bdp.dht.endpoint;

import java.net.InetAddress;
import java.util.Comparator;
import org.apache.cassandra.locator.IEndpointSnitch;

public class EndpointSnitchComparator implements Comparator<Endpoint> {
   private final EndpointStateProxy endpointStateProxy;

   public EndpointSnitchComparator(EndpointStateProxy endpointStateProxy) {
      this.endpointStateProxy = endpointStateProxy;
   }

   public int compare(Endpoint left, Endpoint right) {
      IEndpointSnitch endpointSnitch = this.endpointStateProxy.getEndpointSnitch();
      InetAddress localAddress = this.endpointStateProxy.getLocalAddress();
      return endpointSnitch.compareEndpoints(localAddress, left.getAddress(), right.getAddress());
   }
}
