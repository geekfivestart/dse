package org.apache.cassandra.locator;

import com.codahale.metrics.Snapshot;
import com.google.common.annotations.VisibleForTesting;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.metrics.Histogram;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

public class DynamicEndpointSnitch extends AbstractEndpointSnitch implements ILatencySubscriber, DynamicEndpointSnitchMBean {
   private static final boolean USE_SEVERITY = !Boolean.getBoolean("cassandra.ignore_dynamic_snitch_severity");
   static final long MAX_LATENCY = 107964792L;
   private static final int HISTOGRAM_UPDATE_INTERVAL_MILLIS = 0;
   private volatile int dynamicUpdateInterval;
   private volatile int dynamicResetInterval;
   private volatile double dynamicBadnessThreshold;
   private static final double RANGE_MERGING_PREFERENCE = 1.5D;
   private String mbeanName;
   private boolean registered;
   private volatile Map<InetAddress, Double> scores;
   private final ConcurrentHashMap<InetAddress, Histogram> samples;
   public final IEndpointSnitch subsnitch;
   @Nullable
   private volatile ScheduledFuture<?> updateSchedular;
   @Nullable
   private volatile ScheduledFuture<?> resetSchedular;
   private static final double percentile = 0.9D;

   public DynamicEndpointSnitch(IEndpointSnitch snitch) {
      this(snitch, (String)null, true);
   }

   public DynamicEndpointSnitch(IEndpointSnitch snitch, String instance, boolean useScheduler) {
      this.dynamicUpdateInterval = DatabaseDescriptor.getDynamicUpdateInterval();
      this.dynamicResetInterval = DatabaseDescriptor.getDynamicResetInterval();
      this.dynamicBadnessThreshold = DatabaseDescriptor.getDynamicBadnessThreshold();
      this.registered = false;
      this.scores = Collections.emptyMap();
      this.samples = new ConcurrentHashMap();
      this.mbeanName = "org.apache.cassandra.db:type=DynamicEndpointSnitch";
      if(instance != null) {
         this.mbeanName = this.mbeanName + ",instance=" + instance;
      }

      this.subsnitch = snitch;
      if(DatabaseDescriptor.isDaemonInitialized()) {
         if(useScheduler) {
            this.updateSchedular = ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(this::updateScores, (long)this.dynamicUpdateInterval, (long)this.dynamicUpdateInterval, TimeUnit.MILLISECONDS);
            this.resetSchedular = ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(this::reset, (long)this.dynamicResetInterval, (long)this.dynamicResetInterval, TimeUnit.MILLISECONDS);
         }

         this.registerMBean();
      }

   }

   public void applyConfigChanges() {
      if(this.dynamicUpdateInterval != DatabaseDescriptor.getDynamicUpdateInterval()) {
         this.dynamicUpdateInterval = DatabaseDescriptor.getDynamicUpdateInterval();
         if(DatabaseDescriptor.isDaemonInitialized()) {
            this.updateSchedular.cancel(false);
            this.updateSchedular = ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(this::updateScores, (long)this.dynamicUpdateInterval, (long)this.dynamicUpdateInterval, TimeUnit.MILLISECONDS);
         }
      }

      if(this.dynamicResetInterval != DatabaseDescriptor.getDynamicResetInterval()) {
         this.dynamicResetInterval = DatabaseDescriptor.getDynamicResetInterval();
         if(DatabaseDescriptor.isDaemonInitialized()) {
            this.resetSchedular.cancel(false);
            this.resetSchedular = ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(this::reset, (long)this.dynamicResetInterval, (long)this.dynamicResetInterval, TimeUnit.MILLISECONDS);
         }
      }

      this.dynamicBadnessThreshold = DatabaseDescriptor.getDynamicBadnessThreshold();
   }

   private void registerMBean() {
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

      try {
         mbs.registerMBean(this, new ObjectName(this.mbeanName));
      } catch (Exception var3) {
         throw new RuntimeException(var3);
      }
   }

   public void close() {
      if(this.updateSchedular != null) {
         this.updateSchedular.cancel(false);
      }

      if(this.resetSchedular != null) {
         this.resetSchedular.cancel(false);
      }

      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

      try {
         mbs.unregisterMBean(new ObjectName(this.mbeanName));
      } catch (InstanceNotFoundException var3) {
         ;
      } catch (Exception var4) {
         throw new RuntimeException(var4);
      }

   }

   public void gossiperStarting() {
      this.subsnitch.gossiperStarting();
   }

   public String getRack(InetAddress endpoint) {
      return this.subsnitch.getRack(endpoint);
   }

   public String getDatacenter(InetAddress endpoint) {
      return this.subsnitch.getDatacenter(endpoint);
   }

   public String getLocalDatacenter() {
      return this.subsnitch.getLocalDatacenter();
   }

   public String getLocalRack() {
      return this.subsnitch.getLocalRack();
   }

   public boolean isInLocalDatacenter(InetAddress endpoint) {
      return this.subsnitch.isInLocalDatacenter(endpoint);
   }

   public boolean isInLocalRack(InetAddress endpoint) {
      return this.subsnitch.isInLocalRack(endpoint);
   }

   public long getCrossDcRttLatency(InetAddress endpoint) {
      return this.subsnitch.getCrossDcRttLatency(endpoint);
   }

   public List<InetAddress> getSortedListByProximity(InetAddress address, Collection<InetAddress> addresses) {
      List<InetAddress> list = new ArrayList(addresses);
      this.sortByProximity(address, list);
      return list;
   }

   public void sortByProximity(InetAddress address, List<InetAddress> addresses) {
      assert address.equals(FBUtilities.getBroadcastAddress());

      if(this.dynamicBadnessThreshold == 0.0D) {
         this.sortByProximityWithScore(address, addresses);
      } else {
         this.sortByProximityWithBadness(address, addresses);
      }

   }

   private void sortByProximityWithScore(InetAddress address, List<InetAddress> addresses) {
      Map<InetAddress, Double> scores = this.scores;
      addresses.sort((a1, a2) -> {
         return this.compareEndpoints(address, a1, a2, scores);
      });
   }

   private void sortByProximityWithBadness(InetAddress address, List<InetAddress> addresses) {
      if(addresses.size() >= 2) {
         this.subsnitch.sortByProximity(address, addresses);
         Map<InetAddress, Double> scores = this.scores;
         ArrayList<Double> subsnitchOrderedScores = new ArrayList(addresses.size());

         Double score;
         for(Iterator var5 = addresses.iterator(); var5.hasNext(); subsnitchOrderedScores.add(score)) {
            InetAddress inet = (InetAddress)var5.next();
            score = (Double)scores.get(inet);
            if(score == null) {
               score = Double.valueOf(0.0D);
            }
         }

         ArrayList<Double> sortedScores = new ArrayList(subsnitchOrderedScores);
         Collections.sort(sortedScores);
         Iterator<Double> sortedScoreIterator = sortedScores.iterator();
         Iterator var11 = subsnitchOrderedScores.iterator();

         Double subsnitchScore;
         do {
            if(!var11.hasNext()) {
               return;
            }

            subsnitchScore = (Double)var11.next();
         } while(subsnitchScore.doubleValue() <= ((Double)sortedScoreIterator.next()).doubleValue() * (1.0D + this.dynamicBadnessThreshold));

         this.sortByProximityWithScore(address, addresses);
      }
   }

   private int compareEndpoints(InetAddress target, InetAddress a1, InetAddress a2, Map<InetAddress, Double> scores) {
      double scored1 = ((Double)scores.getOrDefault(a1, Double.valueOf(0.0D))).doubleValue();
      double scored2 = ((Double)scores.getOrDefault(a2, Double.valueOf(0.0D))).doubleValue();
      int cmp = Double.compare(scored1, scored2);
      return cmp != 0?cmp:this.subsnitch.compareEndpoints(target, a1, a2);
   }

   public int compareEndpoints(InetAddress target, InetAddress a1, InetAddress a2) {
      throw new UnsupportedOperationException("You shouldn't wrap the DynamicEndpointSnitch (within itself or otherwise)");
   }

   public void receiveTiming(Verb<?, ?> verb, InetAddress host, long latency) {
      if(verb.group().equals(Verbs.READS)) {
         if(107964792L < latency) {
            latency = 107964792L;
         }

         Histogram sample = (Histogram)this.samples.get(host);
         if(sample == null) {
            sample = (Histogram)this.samples.computeIfAbsent(host, (h) -> {
               return Histogram.make(true, 107964792L, 0, false);
            });
         }

         sample.update(latency);
      }
   }

   @VisibleForTesting
   public void updateScores() {
      if (!DatabaseDescriptor.isTPCInitialized()) {
         return;
      }
      if (!StorageService.instance.isGossipActive()) {
         return;
      }
      if (!this.registered && MessagingService.instance() != null) {
         MessagingService.instance().register(this);
         this.registered = true;
      }
      double maxLatency = 1.0;
      Map<InetAddress, Snapshot> snapshots = this.samples.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> ((Histogram)entry.getValue()).getSnapshot()));
      HashMap<InetAddress, Double> newScores = new HashMap<InetAddress, Double>();
      for (Map.Entry<InetAddress, Snapshot> entry2 : snapshots.entrySet()) {
         double mean = entry2.getValue().getValue(0.9);
         if (mean <= maxLatency) continue;
         maxLatency = mean;
      }
      for (Map.Entry<InetAddress, Snapshot> entry2 : snapshots.entrySet()) {
         double score = entry2.getValue().getValue(0.9) / maxLatency;
         if (USE_SEVERITY) {
            score += this.getSeverity(entry2.getKey());
         }
         newScores.put(entry2.getKey(), score);
      }
      this.scores = newScores;
   }

   private void reset() {
      this.samples.clear();
   }

   public Map<InetAddress, Double> getScores() {
      return this.scores;
   }

   public int getUpdateInterval() {
      return this.dynamicUpdateInterval;
   }

   public int getResetInterval() {
      return this.dynamicResetInterval;
   }

   public double getBadnessThreshold() {
      return this.dynamicBadnessThreshold;
   }

   public String getSubsnitchClassName() {
      return this.subsnitch.getClass().getName();
   }

   public String getDisplayName() {
      return this.subsnitch.getDisplayName();
   }

   public List<Double> dumpTimings(String hostname) throws UnknownHostException {
      return this.dumpTimings(InetAddress.getByName(hostname));
   }

   public List<Double> dumpTimings(InetAddress host) throws UnknownHostException {
      ArrayList<Double> timings = new ArrayList();
      Histogram sample = (Histogram)this.samples.get(host);
      if(sample != null) {
         long[] values = sample.getSnapshot().getValues();
         long[] bucketOffsets = sample.getOffsets();

         for(int i = 0; i < values.length; ++i) {
            for(long l = 0L; l < values[i]; ++l) {
               timings.add(Double.valueOf((double)bucketOffsets[i]));
            }
         }
      }

      return timings;
   }

   public void setSeverity(double severity) {
      Gossiper.instance.addLocalApplicationState(ApplicationState.SEVERITY, StorageService.instance.valueFactory.severity(severity));
   }

   private double getSeverity(InetAddress endpoint) {
      EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
      if(state == null) {
         return 0.0D;
      } else {
         VersionedValue event = state.getApplicationState(ApplicationState.SEVERITY);
         return event == null?0.0D:Double.parseDouble(event.value);
      }
   }

   public double getSeverity() {
      return this.getSeverity(FBUtilities.getBroadcastAddress());
   }

   public boolean isWorthMergingForRangeQuery(List<InetAddress> merged, List<InetAddress> l1, List<InetAddress> l2) {
      if(!this.subsnitch.isWorthMergingForRangeQuery(merged, l1, l2)) {
         return false;
      } else if(l1.size() == 1 && l2.size() == 1 && ((InetAddress)l1.get(0)).equals(l2.get(0))) {
         return true;
      } else {
         double maxMerged = this.maxScore(merged);
         double maxL1 = this.maxScore(l1);
         double maxL2 = this.maxScore(l2);
         return maxMerged >= 0.0D && maxL1 >= 0.0D && maxL2 >= 0.0D?maxMerged <= (maxL1 + maxL2) * 1.5D:true;
      }
   }

   private double maxScore(List<InetAddress> endpoints) {
      double maxScore = -1.0D;
      Iterator var4 = endpoints.iterator();

      while(var4.hasNext()) {
         InetAddress endpoint = (InetAddress)var4.next();
         Double score = (Double)this.scores.get(endpoint);
         if(score != null && score.doubleValue() > maxScore) {
            maxScore = score.doubleValue();
         }
      }

      return maxScore;
   }

   public boolean isDefaultDC(String dc) {
      return this.subsnitch.isDefaultDC(dc);
   }

   public String toString() {
      return "DynamicEndpointSnitch{registered=" + this.registered + ", subsnitch=" + this.subsnitch + '}';
   }
}
