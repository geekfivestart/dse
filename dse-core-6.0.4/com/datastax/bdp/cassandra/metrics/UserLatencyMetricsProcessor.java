package com.datastax.bdp.cassandra.metrics;

import com.datastax.bdp.cassandra.tracing.ClientConnectionMetadata;
import com.google.common.collect.MinMaxPriorityQueue;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserLatencyMetricsProcessor {
   private static final Logger logger = LoggerFactory.getLogger(UserLatencyMetricsProcessor.class);
   private final int topStatsLimit;
   private static final MinMaxPriorityQueue<AggregateUserLatency> EMPTY_AGGREGATE_METRICS;
   private static final MinMaxPriorityQueue<RawUserObjectLatency> EMPTY_RAW_USER_METRICS;
   private final MinMaxPriorityQueue<AggregateUserLatency> topAggregatedReads;
   private final MinMaxPriorityQueue<AggregateUserLatency> topAggregatedWrites;
   private final MinMaxPriorityQueue<RawUserObjectLatency> topIndividualReads;
   private final MinMaxPriorityQueue<RawUserObjectLatency> topIndividualWrites;
   Iterable<AggregateUserLatency> allByUser;

   public UserLatencyMetricsProcessor(Iterable<RawUserObjectLatency> allMetrics, int topStatsLimit) {
      this.topStatsLimit = topStatsLimit;
      if(topStatsLimit > 0) {
         this.topIndividualReads = MinMaxPriorityQueue.orderedBy(RawUserObjectLatency.READ_LATENCY_COMPARATOR).maximumSize(topStatsLimit).create();
         this.topIndividualWrites = MinMaxPriorityQueue.orderedBy(RawUserObjectLatency.WRITE_LATENCY_COMPARATOR).maximumSize(topStatsLimit).create();
         this.topAggregatedReads = MinMaxPriorityQueue.orderedBy(AggregateUserLatency.AGGREGATE_READ_LATENCY_COMPARATOR).maximumSize(topStatsLimit).create();
         this.topAggregatedWrites = MinMaxPriorityQueue.orderedBy(AggregateUserLatency.AGGREGATE_WRITE_LATENCY_COMPARATOR).maximumSize(topStatsLimit).create();
      } else {
         this.topIndividualReads = EMPTY_RAW_USER_METRICS;
         this.topIndividualWrites = EMPTY_RAW_USER_METRICS;
         this.topAggregatedReads = EMPTY_AGGREGATE_METRICS;
         this.topAggregatedWrites = EMPTY_AGGREGATE_METRICS;
      }

      this.allByUser = this.analyse(allMetrics);
   }

   public Iterable<AggregateUserLatency> getAllUserActivity() {
      return this.allByUser;
   }

   public Queue<AggregateUserLatency> getTopUsersByRead() {
      return this.topAggregatedReads;
   }

   public Queue<AggregateUserLatency> getTopUsersByWrite() {
      return this.topAggregatedWrites;
   }

   public Queue<RawUserObjectLatency> getTopUserObjectsByRead() {
      return this.topIndividualReads;
   }

   public Queue<RawUserObjectLatency> getTopUserObjectsByWrite() {
      return this.topIndividualWrites;
   }

   public Iterable<AggregateUserLatency> analyse(Iterable<RawUserObjectLatency> all) {
      Map<ClientConnectionMetadata, AggregateUserLatency> aggregateUserMetrics = new HashMap();
      Iterator var3 = all.iterator();

      while(var3.hasNext()) {
         RawUserObjectLatency userObjectLatency = (RawUserObjectLatency)var3.next();
         AggregateUserLatency userMetrics = (AggregateUserLatency)aggregateUserMetrics.get(userObjectLatency.connectionInfo);
         if(userMetrics == null) {
            userMetrics = new AggregateUserLatency(userObjectLatency.connectionInfo);
            aggregateUserMetrics.put(userObjectLatency.connectionInfo, userMetrics);
         }

         userMetrics.addLatencyMetric(userObjectLatency.latency);
         if(this.topStatsLimit > 0) {
            this.topIndividualReads.offer(userObjectLatency);
            this.topIndividualWrites.offer(userObjectLatency);
         }
      }

      if(this.topStatsLimit > 0) {
         var3 = aggregateUserMetrics.values().iterator();

         while(var3.hasNext()) {
            AggregateUserLatency userMetrics = (AggregateUserLatency)var3.next();
            this.topAggregatedReads.offer(userMetrics);
            this.topAggregatedWrites.offer(userMetrics);
         }
      }

      if(logger.isDebugEnabled()) {
         this.debug(aggregateUserMetrics.size());
      }

      return aggregateUserMetrics.values();
   }

   private void debug(int allAggregatedRecordCount) {
      if(logger.isDebugEnabled()) {
         logger.debug("Analyzed metrics: Top User/Object read metrics {}, Top User/Object write metrics {}, Top agg. Users by read {}, Top agg. Users by write {} Total agg. User records {}", new Object[]{Integer.valueOf(this.topIndividualReads == null?0:this.topIndividualReads.size()), Integer.valueOf(this.topIndividualWrites == null?0:this.topIndividualWrites.size()), Integer.valueOf(this.topAggregatedReads == null?0:this.topAggregatedReads.size()), Integer.valueOf(this.topAggregatedWrites == null?0:this.topAggregatedWrites.size()), Integer.valueOf(allAggregatedRecordCount)});
      }

   }

   static {
      EMPTY_AGGREGATE_METRICS = MinMaxPriorityQueue.orderedBy(AggregateUserLatency.AGGREGATE_READ_LATENCY_COMPARATOR).create();
      EMPTY_RAW_USER_METRICS = MinMaxPriorityQueue.orderedBy(RawUserObjectLatency.READ_LATENCY_COMPARATOR).create();
   }
}
