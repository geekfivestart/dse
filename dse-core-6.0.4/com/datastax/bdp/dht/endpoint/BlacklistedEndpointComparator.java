package com.datastax.bdp.dht.endpoint;

import java.util.Comparator;

public class BlacklistedEndpointComparator implements Comparator<Endpoint> {
   private final EndpointStateProxy endpointStateProxy;

   public BlacklistedEndpointComparator(EndpointStateProxy endpointStateProxy) {
      this.endpointStateProxy = endpointStateProxy;
   }

   public int compare(Endpoint left, Endpoint right) {
      boolean isLeftBlacklisted = this.endpointStateProxy.isBlacklisted(left);
      boolean isRightBlacklisted = this.endpointStateProxy.isBlacklisted(right);
      return isLeftBlacklisted && !isRightBlacklisted?1:(!isLeftBlacklisted && isRightBlacklisted?-1:0);
   }
}
