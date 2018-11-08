package com.datastax.bdp.dht;

import com.datastax.bdp.dht.endpoint.Endpoint;
import com.google.common.base.Objects;
import java.util.Set;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

public final class RoutingPlan {
   private final Set<Endpoint> endpoints;
   private final Set<Range<Token>> missingRanges;

   public RoutingPlan(Set<Endpoint> endpoints, Set<Range<Token>> missingRanges) {
      this.endpoints = endpoints;
      this.missingRanges = missingRanges;
   }

   public Set<Endpoint> getEndpoints() {
      return this.endpoints;
   }

   public Set<Range<Token>> getMissingRanges() {
      return this.missingRanges;
   }

   public boolean isComplete() {
      return !this.endpoints.isEmpty() && this.missingRanges.isEmpty();
   }

   public String toString() {
      return Objects.toStringHelper(this).add("endpoints", this.endpoints).add("missingRanges", this.missingRanges).toString();
   }
}
