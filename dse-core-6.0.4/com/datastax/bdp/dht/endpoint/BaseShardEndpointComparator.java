package com.datastax.bdp.dht.endpoint;

import com.datastax.bdp.util.ChainedComparator;
import java.util.Comparator;

public class BaseShardEndpointComparator implements Comparator<Endpoint> {
   protected final ChainedComparator<Endpoint> chainedComparator = new ChainedComparator();

   public BaseShardEndpointComparator(EndpointStateProxy endpointStateProxy) {
      this.chainedComparator.add(new BlacklistedEndpointComparator(endpointStateProxy)).add(new ActiveEndpointComparator(endpointStateProxy));
   }

   public int compare(Endpoint left, Endpoint right) {
      return this.chainedComparator.compare(left, right);
   }
}
