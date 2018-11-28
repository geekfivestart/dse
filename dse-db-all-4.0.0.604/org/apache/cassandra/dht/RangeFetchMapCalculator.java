package org.apache.cassandra.dht;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.FBUtilities;
import org.psjava.algo.graph.flownetwork.FordFulkersonAlgorithm;
import org.psjava.algo.graph.flownetwork.MaximumFlowAlgorithm;
import org.psjava.algo.graph.flownetwork.MaximumFlowAlgorithmResult;
import org.psjava.algo.graph.pathfinder.DFSPathFinder;
import org.psjava.ds.graph.CapacityEdge;
import org.psjava.ds.graph.MutableCapacityGraph;
import org.psjava.ds.numbersystrem.IntegerNumberSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RangeFetchMapCalculator {
   private static final Logger logger = LoggerFactory.getLogger(RangeFetchMapCalculator.class);
   private static final long TRIVIAL_RANGE_LIMIT = 1000L;
   private final Multimap<Range<Token>, InetAddress> rangesWithSources;
   private final RangeStreamer.ISourceFilter filter;
   private final String keyspace;
   private final RangeFetchMapCalculator.Vertex sourceVertex = RangeFetchMapCalculator.OuterVertex.getSourceVertex();
   private final RangeFetchMapCalculator.Vertex destinationVertex = RangeFetchMapCalculator.OuterVertex.getDestinationVertex();
   private final Set<Range<Token>> trivialRanges;

   public RangeFetchMapCalculator(Multimap<Range<Token>, InetAddress> rangesWithSources, RangeStreamer.ISourceFilter filter, String keyspace) {
      this.rangesWithSources = rangesWithSources;
      this.filter = filter;
      this.keyspace = keyspace;
      this.trivialRanges = (Set)rangesWithSources.keySet().stream().filter(RangeFetchMapCalculator::isTrivial).collect(Collectors.toSet());
   }

   static boolean isTrivial(Range<Token> range) {
      IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
      if(partitioner.hasNumericTokens()) {
         BigInteger l = partitioner.valueForToken((Token)range.left);
         BigInteger r = partitioner.valueForToken((Token)range.right);
         if(r.compareTo(l) <= 0) {
            return false;
         }

         if(r.subtract(l).compareTo(BigInteger.valueOf(1000L)) < 0) {
            return true;
         }
      }

      return false;
   }

   public Multimap<InetAddress, Range<Token>> getRangeFetchMap(boolean useStrictConsistency) {
      Multimap<InetAddress, Range<Token>> fetchMap = HashMultimap.create();
      fetchMap.putAll(this.getRangeFetchMapForNonTrivialRanges(useStrictConsistency));
      fetchMap.putAll(this.getRangeFetchMapForTrivialRanges(fetchMap));
      logger.info("Output from RangeFetchMapCalculator for keyspace {}", this.keyspace);
      validateRangeFetchMap(this.rangesWithSources, fetchMap, this.keyspace, useStrictConsistency);
      return fetchMap;
   }

   @VisibleForTesting
   Multimap<InetAddress, Range<Token>> getRangeFetchMapForNonTrivialRanges(boolean useStrictConsistency) {
      MutableCapacityGraph<RangeFetchMapCalculator.Vertex, Integer> graph = this.getGraph(useStrictConsistency);
      this.addSourceAndDestination(graph, this.getDestinationLinkCapacity(graph));
      int flow = 0;

      MaximumFlowAlgorithmResult result;
      int newFlow;
      for(result = null; flow < this.getTotalRangeVertices(graph); flow = newFlow) {
         if(flow > 0) {
            this.incrementCapacity(graph, 1);
         }

         MaximumFlowAlgorithm fordFulkerson = FordFulkersonAlgorithm.getInstance(DFSPathFinder.getInstance());
         result = fordFulkerson.calc(graph, this.sourceVertex, this.destinationVertex, IntegerNumberSystem.getInstance());
         newFlow = ((Integer)result.calcTotalFlow()).intValue();

         assert newFlow > flow;
      }

      return this.getRangeFetchMapFromGraphResult(graph, result);
   }

   private static void validateRangeFetchMap(Multimap<Range<Token>, InetAddress> rangesWithSources, Multimap<InetAddress, Range<Token>> rangeFetchMapMap, String keyspace, boolean useStrictConsistency) {
      HashSet<Range<Token>> requestedRanges = new HashSet(rangesWithSources.keySet());
      Iterator var5 = rangeFetchMapMap.entries().iterator();

      while(var5.hasNext()) {
         Entry<InetAddress, Range<Token>> entry = (Entry)var5.next();
         if(((InetAddress)entry.getKey()).equals(FBUtilities.getBroadcastAddress())) {
            throw new IllegalStateException("Trying to stream locally. Range: " + entry.getValue() + " in keyspace " + keyspace);
         }

         if(!rangesWithSources.get(entry.getValue()).contains(entry.getKey())) {
            throw new IllegalStateException("Trying to stream from wrong endpoint. Range: " + entry.getValue() + " in keyspace " + keyspace + " from endpoint: " + entry.getKey());
         }

         requestedRanges.remove(entry.getValue());
         logger.info("Streaming range {} from endpoint {} for keyspace {}", new Object[]{entry.getValue(), entry.getKey(), keyspace});
      }

      if(useStrictConsistency && !requestedRanges.isEmpty()) {
         throw new IllegalStateException("Unable to find sufficient sources for streaming range(s) " + (String)requestedRanges.stream().map(Range::toString).collect(Collectors.joining(", ")) + " in keyspace " + keyspace);
      }
   }

   @VisibleForTesting
   Multimap<InetAddress, Range<Token>> getRangeFetchMapForTrivialRanges(Multimap<InetAddress, Range<Token>> optimisedMap) {
      Multimap<InetAddress, Range<Token>> fetchMap = HashMultimap.create();
      Iterator var3 = this.trivialRanges.iterator();

      while(var3.hasNext()) {
         Range<Token> trivialRange = (Range)var3.next();
         boolean added = false;

         for(boolean localDCCheck = true; !added; localDCCheck = false) {
            List<InetAddress> srcs = new ArrayList(this.rangesWithSources.get(trivialRange));
            srcs.sort(Comparator.comparingInt((o) -> {
               return optimisedMap.get(o).size();
            }));
            Iterator var8 = srcs.iterator();

            while(var8.hasNext()) {
               InetAddress src = (InetAddress)var8.next();
               if(this.passFilters(src, localDCCheck)) {
                  fetchMap.put(src, trivialRange);
                  added = true;
                  break;
               }
            }

            if(!added && !localDCCheck) {
               throw new IllegalStateException("Unable to find sufficient sources for streaming range " + trivialRange + " in keyspace " + this.keyspace);
            }

            if(!added) {
               logger.info("Using other DC endpoints for streaming for range: {} and keyspace {}", trivialRange, this.keyspace);
            }
         }
      }

      return fetchMap;
   }

   private int getTotalRangeVertices(MutableCapacityGraph<RangeFetchMapCalculator.Vertex, Integer> graph) {
      int count = 0;
      Iterator var3 = graph.getVertices().iterator();

      while(var3.hasNext()) {
         RangeFetchMapCalculator.Vertex vertex = (RangeFetchMapCalculator.Vertex)var3.next();
         if(vertex.isRangeVertex()) {
            ++count;
         }
      }

      return count;
   }

   private Multimap<InetAddress, Range<Token>> getRangeFetchMapFromGraphResult(MutableCapacityGraph<Vertex, Integer> graph, MaximumFlowAlgorithmResult<Integer, CapacityEdge<Vertex, Integer>> result) {
      HashMultimap rangeFetchMapMap = HashMultimap.create();
      if (result == null) {
         return rangeFetchMapMap;
      }
      org.psjava.ds.math.Function flowFunction = result.calcFlowFunction();
      for (Vertex vertex : graph.getVertices()) {
         if (!vertex.isRangeVertex()) continue;
         boolean sourceFound = false;
         for (CapacityEdge e : graph.getEdges(vertex)) {
            if ((Integer)flowFunction.get((Object)e) <= 0) continue;
            assert (!sourceFound);
            sourceFound = true;
            if (((Vertex)e.to()).isEndpointVertex()) {
               rangeFetchMapMap.put((Object)((EndpointVertex)e.to()).getEndpoint(), ((RangeVertex)vertex).getRange());
               continue;
            }
            if (!((Vertex)e.from()).isEndpointVertex()) continue;
            rangeFetchMapMap.put((Object)((EndpointVertex)e.from()).getEndpoint(), ((RangeVertex)vertex).getRange());
         }
         assert (sourceFound);
      }
      return rangeFetchMapMap;
   }

   private void incrementCapacity(MutableCapacityGraph<RangeFetchMapCalculator.Vertex, Integer> graph, int incrementalCapacity) {
      Iterator var3 = graph.getVertices().iterator();

      while(var3.hasNext()) {
         RangeFetchMapCalculator.Vertex vertex = (RangeFetchMapCalculator.Vertex)var3.next();
         if(vertex.isEndpointVertex()) {
            graph.addEdge(vertex, this.destinationVertex, Integer.valueOf(incrementalCapacity));
         }
      }

   }

   private void addSourceAndDestination(MutableCapacityGraph<RangeFetchMapCalculator.Vertex, Integer> graph, int destinationCapacity) {
      graph.insertVertex(this.sourceVertex);
      graph.insertVertex(this.destinationVertex);
      Iterator var3 = graph.getVertices().iterator();

      while(var3.hasNext()) {
         RangeFetchMapCalculator.Vertex vertex = (RangeFetchMapCalculator.Vertex)var3.next();
         if(vertex.isRangeVertex()) {
            graph.addEdge(this.sourceVertex, vertex, Integer.valueOf(1));
         } else if(vertex.isEndpointVertex()) {
            graph.addEdge(vertex, this.destinationVertex, Integer.valueOf(destinationCapacity));
         }
      }

   }

   private int getDestinationLinkCapacity(MutableCapacityGraph<RangeFetchMapCalculator.Vertex, Integer> graph) {
      double endpointVertices = 0.0D;
      double rangeVertices = 0.0D;
      Iterator var6 = graph.getVertices().iterator();

      while(var6.hasNext()) {
         RangeFetchMapCalculator.Vertex vertex = (RangeFetchMapCalculator.Vertex)var6.next();
         if(vertex.isEndpointVertex()) {
            ++endpointVertices;
         } else if(vertex.isRangeVertex()) {
            ++rangeVertices;
         }
      }

      return (int)Math.ceil(rangeVertices / endpointVertices);
   }

   private MutableCapacityGraph<RangeFetchMapCalculator.Vertex, Integer> getGraph(boolean useStrictConsistency) {
      MutableCapacityGraph<RangeFetchMapCalculator.Vertex, Integer> capacityGraph = MutableCapacityGraph.create();
      Iterator var3 = this.rangesWithSources.keySet().iterator();

      while(var3.hasNext()) {
         Range<Token> range = (Range)var3.next();
         if(this.trivialRanges.contains(range)) {
            logger.debug("Not optimising trivial range {} for keyspace {}", range, this.keyspace);
         } else {
            RangeFetchMapCalculator.RangeVertex rangeVertex = new RangeFetchMapCalculator.RangeVertex(range);
            boolean sourceFound = this.addEndpoints(capacityGraph, rangeVertex, true);
            if(!sourceFound) {
               logger.info("Using other DC endpoints for streaming for range: {} and keyspace {}", range, this.keyspace);
               sourceFound = this.addEndpoints(capacityGraph, rangeVertex, false);
            }

            if(!sourceFound && !this.rangesWithSources.get(range).contains(FBUtilities.getBroadcastAddress())) {
               RangeStreamer.handleSourceNotFound(this.keyspace, useStrictConsistency, range);
            }
         }
      }

      return capacityGraph;
   }

   private boolean addEndpoints(MutableCapacityGraph<RangeFetchMapCalculator.Vertex, Integer> capacityGraph, RangeFetchMapCalculator.RangeVertex rangeVertex, boolean localDCCheck) {
      boolean sourceFound = false;
      Iterator var5 = this.rangesWithSources.get(rangeVertex.getRange()).iterator();

      while(var5.hasNext()) {
         InetAddress endpoint = (InetAddress)var5.next();
         if(this.passFilters(endpoint, localDCCheck)) {
            sourceFound = true;
            RangeFetchMapCalculator.Vertex endpointVertex = new RangeFetchMapCalculator.EndpointVertex(endpoint);
            capacityGraph.insertVertex(rangeVertex);
            capacityGraph.insertVertex(endpointVertex);
            capacityGraph.addEdge(rangeVertex, endpointVertex, Integer.valueOf(2147483647));
         }
      }

      return sourceFound;
   }

   private boolean isInLocalDC(InetAddress endpoint) {
      return DatabaseDescriptor.getEndpointSnitch().isInLocalDatacenter(endpoint);
   }

   private boolean passFilters(InetAddress endpoint, boolean localDCCheck) {
      if(!this.filter.shouldInclude(endpoint) || localDCCheck && !this.isInLocalDC(endpoint)) {
         logger.debug("Excluding {}", endpoint);
         return false;
      } else {
         logger.debug("Including {}", endpoint);
         return true;
      }
   }

   private static class OuterVertex extends RangeFetchMapCalculator.Vertex {
      private final boolean source;

      private OuterVertex(boolean source) {
         super();
         this.source = source;
      }

      public static RangeFetchMapCalculator.Vertex getSourceVertex() {
         return new RangeFetchMapCalculator.OuterVertex(true);
      }

      public static RangeFetchMapCalculator.Vertex getDestinationVertex() {
         return new RangeFetchMapCalculator.OuterVertex(false);
      }

      public RangeFetchMapCalculator.Vertex.VERTEX_TYPE getVertexType() {
         return this.source?RangeFetchMapCalculator.Vertex.VERTEX_TYPE.SOURCE:RangeFetchMapCalculator.Vertex.VERTEX_TYPE.DESTINATION;
      }

      public boolean equals(Object o) {
         if(this == o) {
            return true;
         } else if(o != null && this.getClass() == o.getClass()) {
            RangeFetchMapCalculator.OuterVertex that = (RangeFetchMapCalculator.OuterVertex)o;
            return this.source == that.source;
         } else {
            return false;
         }
      }

      public int hashCode() {
         return this.source?1:0;
      }
   }

   private static class RangeVertex extends RangeFetchMapCalculator.Vertex {
      private final Range<Token> range;

      public RangeVertex(Range<Token> range) {
         super();

         assert range != null;

         this.range = range;
      }

      public Range<Token> getRange() {
         return this.range;
      }

      public RangeFetchMapCalculator.Vertex.VERTEX_TYPE getVertexType() {
         return RangeFetchMapCalculator.Vertex.VERTEX_TYPE.RANGE;
      }

      public boolean equals(Object o) {
         if(this == o) {
            return true;
         } else if(o != null && this.getClass() == o.getClass()) {
            RangeFetchMapCalculator.RangeVertex that = (RangeFetchMapCalculator.RangeVertex)o;
            return this.range.equals(that.range);
         } else {
            return false;
         }
      }

      public int hashCode() {
         return this.range.hashCode();
      }
   }

   private static class EndpointVertex extends RangeFetchMapCalculator.Vertex {
      private final InetAddress endpoint;

      public EndpointVertex(InetAddress endpoint) {
         super();

         assert endpoint != null;

         this.endpoint = endpoint;
      }

      public InetAddress getEndpoint() {
         return this.endpoint;
      }

      public RangeFetchMapCalculator.Vertex.VERTEX_TYPE getVertexType() {
         return RangeFetchMapCalculator.Vertex.VERTEX_TYPE.ENDPOINT;
      }

      public boolean equals(Object o) {
         if(this == o) {
            return true;
         } else if(o != null && this.getClass() == o.getClass()) {
            RangeFetchMapCalculator.EndpointVertex that = (RangeFetchMapCalculator.EndpointVertex)o;
            return this.endpoint.equals(that.endpoint);
         } else {
            return false;
         }
      }

      public int hashCode() {
         return this.endpoint.hashCode();
      }
   }

   private abstract static class Vertex {
      private Vertex() {
      }

      public abstract RangeFetchMapCalculator.Vertex.VERTEX_TYPE getVertexType();

      public boolean isEndpointVertex() {
         return this.getVertexType() == RangeFetchMapCalculator.Vertex.VERTEX_TYPE.ENDPOINT;
      }

      public boolean isRangeVertex() {
         return this.getVertexType() == RangeFetchMapCalculator.Vertex.VERTEX_TYPE.RANGE;
      }

      public static enum VERTEX_TYPE {
         ENDPOINT,
         RANGE,
         SOURCE,
         DESTINATION;

         private VERTEX_TYPE() {
         }
      }
   }
}
