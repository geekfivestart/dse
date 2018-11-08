package com.datastax.bdp.dht.endpoint;

public class InvertedEndpointSnitchComparator extends EndpointSnitchComparator {
   public InvertedEndpointSnitchComparator(EndpointStateProxy endpointStateProxy) {
      super(endpointStateProxy);
   }

   public int compare(Endpoint left, Endpoint right) {
      return -1 * super.compare(left, right);
   }
}
