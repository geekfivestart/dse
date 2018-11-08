package com.datastax.bdp.cassandra.metrics;

import com.datastax.bdp.config.DseConfig;
import com.google.common.collect.AbstractIterator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LatencyTracker implements Iterable<RawObjectLatency> {
   private static final Logger logger = LoggerFactory.getLogger(LatencyTracker.class);
   private Map<String, Map<String, LatencyValues>> interactions = new HashMap();

   public LatencyTracker() {
   }

   public void reset() {
      this.interactions.clear();
   }

   public void recordLatencyEvent(String ks, String cf, LatencyValues.EventType type, long latencyValue, TimeUnit unit) {
      Map<String, LatencyValues> keyspaceInteractions = (Map)this.interactions.get(ks);
      if(keyspaceInteractions == null) {
         keyspaceInteractions = new HashMap();
         this.interactions.put(ks, keyspaceInteractions);
      }

      LatencyValues cfMetrics = (LatencyValues)((Map)keyspaceInteractions).get(cf);
      if(cfMetrics == null) {
         cfMetrics = new LatencyValues(DseConfig.resourceLatencyTrackingQuantiles());
         ((Map)keyspaceInteractions).put(cf, cfMetrics);
      }

      cfMetrics.increment(TimeUnit.NANOSECONDS.convert(latencyValue, unit), type);
   }

   public Iterator<RawObjectLatency> iterator() {
      final Iterator<Entry<String, Map<String, LatencyValues>>> ksIter = this.interactions.entrySet().iterator();
      return new AbstractIterator<RawObjectLatency>() {
         String ks;
         Iterator<Entry<String, LatencyValues>> cfIter;

         protected RawObjectLatency computeNext() {
            if(this.cfIter != null && this.cfIter.hasNext()) {
               Entry<String, LatencyValues> entry = (Entry)this.cfIter.next();
               LatencyValues values = (LatencyValues)entry.getValue();
               return new RawObjectLatency(this.ks, (String)entry.getKey(), values.getValue(LatencyValues.EventType.READ), values.getCount(LatencyValues.EventType.READ), values.getValue(LatencyValues.EventType.WRITE), values.getCount(LatencyValues.EventType.WRITE), values.getQuantiles(LatencyValues.EventType.READ), values.getQuantiles(LatencyValues.EventType.WRITE));
            } else {
               return this.moveToNextKeyspaceOrEnd();
            }
         }

         private RawObjectLatency moveToNextKeyspaceOrEnd() {
            if(!ksIter.hasNext()) {
               return (RawObjectLatency)this.endOfData();
            } else {
               Entry<String, Map<String, LatencyValues>> entry = (Entry)ksIter.next();
               this.ks = (String)entry.getKey();
               this.cfIter = ((Map)entry.getValue()).entrySet().iterator();
               return this.computeNext();
            }
         }
      };
   }
}
