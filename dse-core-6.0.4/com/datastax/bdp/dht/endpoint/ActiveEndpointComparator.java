package com.datastax.bdp.dht.endpoint;

import java.util.Comparator;

public class ActiveEndpointComparator implements Comparator<Endpoint> {
   private final EndpointStateProxy endpointStateProxy;

   public ActiveEndpointComparator(EndpointStateProxy endpointStateProxy) {
      this.endpointStateProxy = endpointStateProxy;
   }

   public int compare(Endpoint left, Endpoint right) {
      boolean isLeftActive = this.endpointStateProxy.isActive(left);
      boolean isRightActive = this.endpointStateProxy.isActive(right);
      return isLeftActive && !isRightActive?-1:(!isLeftActive && isRightActive?1:0);
   }
}
