package org.apache.cassandra.dht;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.utils.FBUtilities;

final class SourceFilters {
   public static RangeStreamer.ISourceFilter composite(List<RangeStreamer.ISourceFilter> filters) {
      return (endpoint) -> {
         return filters.stream().allMatch((filter) -> {
            return filter.shouldInclude(endpoint);
         });
      };
   }

   public static RangeStreamer.ISourceFilter composite(RangeStreamer.ISourceFilter... filters) {
      return composite(Arrays.asList(filters));
   }

   public static RangeStreamer.ISourceFilter failureDetectorFilter(IFailureDetector fd) {
      return fd::isAlive;
   }

   public static RangeStreamer.ISourceFilter includeSources(Collection<InetAddress> sources) {
      return sources::contains;
   }

   public static RangeStreamer.ISourceFilter excludeSources(Collection<InetAddress> sources) {
      return negate(includeSources(sources));
   }

   public static RangeStreamer.ISourceFilter excludeLocalNode() {
      return excludeSources(Collections.singleton(FBUtilities.getBroadcastAddress()));
   }

   public static RangeStreamer.ISourceFilter excludeDcs(Map<String, RangeStreamer.ISourceFilter> filterPerDc, IEndpointSnitch snitch) {
      return negate(includeDcs(filterPerDc, snitch));
   }

   public static RangeStreamer.ISourceFilter includeDcs(Map<String, RangeStreamer.ISourceFilter> rackFilterPerDc, IEndpointSnitch snitch) {
      return (endpoint) -> {
         String dc = snitch.getDatacenter(endpoint);
         RangeStreamer.ISourceFilter filter = (RangeStreamer.ISourceFilter)rackFilterPerDc.get(dc);
         return filter != null && filter.shouldInclude(endpoint);
      };
   }

   public static RangeStreamer.ISourceFilter includeRacks(Collection<String> racks, IEndpointSnitch snitch) {
      return (endpoint) -> {
         return racks.contains(snitch.getRack(endpoint));
      };
   }

   public static RangeStreamer.ISourceFilter noop() {
      return (endpoint) -> {
         return true;
      };
   }

   private static RangeStreamer.ISourceFilter negate(RangeStreamer.ISourceFilter filter) {
      return (endpoint) -> {
         return !filter.shouldInclude(endpoint);
      };
   }

   private SourceFilters() {
   }
}
