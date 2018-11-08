package com.datastax.bdp.dht.endpoint;

import java.util.Comparator;

public class EndpointHealthComparator implements Comparator<Endpoint> {
   private final EndpointStateProxy endpointStateProxy;

   public EndpointHealthComparator(EndpointStateProxy endpointStateProxy) {
      this.endpointStateProxy = endpointStateProxy;
   }

   public int compare(Endpoint left, Endpoint right) {
      double leftEndpoint = (double)Math.round(this.endpointStateProxy.getNodeHealth(left) * 10.0D);
      double rightEndpoint = (double)Math.round(this.endpointStateProxy.getNodeHealth(right) * 10.0D);
      return Double.compare(rightEndpoint, leftEndpoint);
   }
}
