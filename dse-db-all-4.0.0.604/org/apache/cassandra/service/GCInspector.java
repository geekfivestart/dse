package org.apache.cassandra.service;

import com.sun.management.GarbageCollectionNotificationInfo;
import com.sun.management.GcInfo;
import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.Notification;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.management.QueryExp;
import javax.management.openmbean.CompositeData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.utils.StatusLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GCInspector implements NotificationListener, GCInspectorMXBean {
   public static final String MBEAN_NAME = "org.apache.cassandra.service:type=GCInspector";
   private static final Logger logger = LoggerFactory.getLogger(GCInspector.class);
   private volatile long gcLogThreshholdInMs = DatabaseDescriptor.getGCLogThreshold();
   private volatile long gcWarnThreasholdInMs = DatabaseDescriptor.getGCWarnThreshold();
   static final Field BITS_TOTAL_CAPACITY;
   final AtomicReference<GCInspector.State> state = new AtomicReference(new GCInspector.State());
   final Map<String, GCInspector.GCState> gcStates = new HashMap();

   public GCInspector() {
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

      try {
         ObjectName gcName = new ObjectName("java.lang:type=GarbageCollector,*");
         Iterator var3 = mbs.queryNames(gcName, (QueryExp)null).iterator();

         while(var3.hasNext()) {
            ObjectName name = (ObjectName)var3.next();
            GarbageCollectorMXBean gc = (GarbageCollectorMXBean)ManagementFactory.newPlatformMXBeanProxy(mbs, name.getCanonicalName(), GarbageCollectorMXBean.class);
            this.gcStates.put(gc.getName(), new GCInspector.GCState(gc, assumeGCIsPartiallyConcurrent(gc), assumeGCIsOldGen(gc)));
         }

         ObjectName me = new ObjectName("org.apache.cassandra.service:type=GCInspector");
         if(!mbs.isRegistered(me)) {
            mbs.registerMBean(this, new ObjectName("org.apache.cassandra.service:type=GCInspector"));
         }

      } catch (InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException | MalformedObjectNameException | IOException | RuntimeException var6) {
         throw new RuntimeException(var6);
      }
   }

   public static void register() throws Exception {
      GCInspector inspector = new GCInspector();
      MBeanServer server = ManagementFactory.getPlatformMBeanServer();
      ObjectName gcName = new ObjectName("java.lang:type=GarbageCollector,*");
      Iterator var3 = server.queryNames(gcName, (QueryExp)null).iterator();

      while(var3.hasNext()) {
         ObjectName name = (ObjectName)var3.next();
         server.addNotificationListener(name, inspector, (NotificationFilter)null, (Object)null);
      }

   }

   private static boolean assumeGCIsPartiallyConcurrent(GarbageCollectorMXBean gc) {
      String var1 = gc.getName();
      byte var2 = -1;
      switch(var1.hashCode()) {
      case -1911579297:
         if(var1.equals("ParNew")) {
            var2 = 5;
         }
         break;
      case -1022169080:
         if(var1.equals("ConcurrentMarkSweep")) {
            var2 = 6;
         }
         break;
      case -842066396:
         if(var1.equals("MarkSweepCompact")) {
            var2 = 1;
         }
         break;
      case 2106261:
         if(var1.equals("Copy")) {
            var2 = 0;
         }
         break;
      case 320101609:
         if(var1.equals("PS Scavenge")) {
            var2 = 3;
         }
         break;
      case 369537270:
         if(var1.equals("G1 Young Generation")) {
            var2 = 4;
         }
         break;
      case 1093844743:
         if(var1.equals("G1 Old Generation")) {
            var2 = 7;
         }
         break;
      case 1973769762:
         if(var1.equals("PS MarkSweep")) {
            var2 = 2;
         }
      }

      switch(var2) {
      case 0:
      case 1:
      case 2:
      case 3:
      case 4:
      case 5:
         return false;
      case 6:
      case 7:
         return true;
      default:
         return true;
      }
   }

   private static boolean assumeGCIsOldGen(GarbageCollectorMXBean gc) {
      String var1 = gc.getName();
      byte var2 = -1;
      switch(var1.hashCode()) {
      case -1911579297:
         if(var1.equals("ParNew")) {
            var2 = 3;
         }
         break;
      case -1022169080:
         if(var1.equals("ConcurrentMarkSweep")) {
            var2 = 6;
         }
         break;
      case -842066396:
         if(var1.equals("MarkSweepCompact")) {
            var2 = 4;
         }
         break;
      case 2106261:
         if(var1.equals("Copy")) {
            var2 = 0;
         }
         break;
      case 320101609:
         if(var1.equals("PS Scavenge")) {
            var2 = 1;
         }
         break;
      case 369537270:
         if(var1.equals("G1 Young Generation")) {
            var2 = 2;
         }
         break;
      case 1093844743:
         if(var1.equals("G1 Old Generation")) {
            var2 = 7;
         }
         break;
      case 1973769762:
         if(var1.equals("PS MarkSweep")) {
            var2 = 5;
         }
      }

      switch(var2) {
      case 0:
      case 1:
      case 2:
      case 3:
         return false;
      case 4:
      case 5:
      case 6:
      case 7:
         return true;
      default:
         return false;
      }
   }

   public void handleNotification(Notification notification, Object handback) {
      String type = notification.getType();
      if(type.equals("com.sun.management.gc.notification")) {
         CompositeData cd = (CompositeData)notification.getUserData();
         GarbageCollectionNotificationInfo info = GarbageCollectionNotificationInfo.from(cd);
         String gcName = info.getGcName();
         GcInfo gcInfo = info.getGcInfo();
         long duration = gcInfo.getDuration();
         GCInspector.GCState gcState = (GCInspector.GCState)this.gcStates.get(gcName);
         if(gcState.assumeGCIsPartiallyConcurrent) {
            long previousTotal = gcState.lastGcTotalDuration;
            long total = gcState.gcBean.getCollectionTime();
            gcState.lastGcTotalDuration = total;
            duration = total - previousTotal;
         }

         StringBuilder sb = new StringBuilder();
         sb.append(info.getGcName()).append(" GC in ").append(duration).append("ms.  ");
         long bytes = 0L;
         Map<String, MemoryUsage> beforeMemoryUsage = gcInfo.getMemoryUsageBeforeGc();
         Map<String, MemoryUsage> afterMemoryUsage = gcInfo.getMemoryUsageAfterGc();
         String[] var16 = gcState.keys(info);
         int var17 = var16.length;

         for(int var18 = 0; var18 < var17; ++var18) {
            String key = var16[var18];
            MemoryUsage before = (MemoryUsage)beforeMemoryUsage.get(key);
            MemoryUsage after = (MemoryUsage)afterMemoryUsage.get(key);
            if(after != null && after.getUsed() != before.getUsed()) {
               sb.append(key).append(": ").append(before.getUsed());
               sb.append(" -> ");
               sb.append(after.getUsed());
               if(!key.equals(gcState.keys[gcState.keys.length - 1])) {
                  sb.append("; ");
               }

               bytes += before.getUsed() - after.getUsed();
            }
         }

         GCInspector.State prev;
         do {
            prev = (GCInspector.State)this.state.get();
         } while(!this.state.compareAndSet(prev, new GCInspector.State((double)duration, (double)bytes, prev)));

         if(this.gcWarnThreasholdInMs != 0L && duration > this.gcWarnThreasholdInMs) {
            logger.warn(sb.toString());
         } else if(duration > this.gcLogThreshholdInMs) {
            logger.info(sb.toString());
         } else if(logger.isTraceEnabled()) {
            logger.trace(sb.toString());
         }

         if(duration > this.getStatusThresholdInMs()) {
            StatusLogger.log();
         }

         if(gcState.assumeGCIsOldGen) {
            LifecycleTransaction.rescheduleFailedDeletions();
         }
      }

   }

   public GCInspector.State getTotalSinceLastCheck() {
      return (GCInspector.State)this.state.getAndSet(new GCInspector.State());
   }

   public double[] getAndResetStats() {
      GCInspector.State state = this.getTotalSinceLastCheck();
      double[] r = new double[]{(double)TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - state.startNanos), state.maxRealTimeElapsed, state.totalRealTimeElapsed, state.sumSquaresRealTimeElapsed, state.totalBytesReclaimed, state.count, (double)getAllocatedDirectMemory()};
      return r;
   }

   private static long getAllocatedDirectMemory() {
      if(BITS_TOTAL_CAPACITY == null) {
         return -1L;
      } else {
         try {
            return BITS_TOTAL_CAPACITY.getLong((Object)null);
         } catch (Throwable var1) {
            logger.trace("Error accessing field of java.nio.Bits", var1);
            return -1L;
         }
      }
   }

   public void setGcWarnThresholdInMs(long threshold) {
      if(threshold < 0L) {
         throw new IllegalArgumentException("Threshold must be greater than or equal to 0");
      } else if(threshold != 0L && threshold <= this.gcLogThreshholdInMs) {
         throw new IllegalArgumentException("Threshold must be greater than gcLogTreasholdInMs which is currently " + this.gcLogThreshholdInMs);
      } else {
         this.gcWarnThreasholdInMs = threshold;
      }
   }

   public long getGcWarnThresholdInMs() {
      return this.gcWarnThreasholdInMs;
   }

   public void setGcLogThresholdInMs(long threshold) {
      if(threshold <= 0L) {
         throw new IllegalArgumentException("Threashold must be greater than 0");
      } else if(this.gcWarnThreasholdInMs != 0L && threshold > this.gcWarnThreasholdInMs) {
         throw new IllegalArgumentException("Threashold must be less than gcWarnTreasholdInMs which is currently " + this.gcWarnThreasholdInMs);
      } else {
         this.gcLogThreshholdInMs = threshold;
      }
   }

   public long getGcLogThresholdInMs() {
      return this.gcLogThreshholdInMs;
   }

   public long getStatusThresholdInMs() {
      return this.gcWarnThreasholdInMs != 0L?this.gcWarnThreasholdInMs:this.gcLogThreshholdInMs;
   }

   static {
      Field temp = null;

      try {
         Class<?> bitsClass = Class.forName("java.nio.Bits");
         Field f = bitsClass.getDeclaredField("totalCapacity");
         f.setAccessible(true);
         temp = f;
      } catch (Throwable var3) {
         logger.debug("Error accessing field of java.nio.Bits", var3);
      }

      BITS_TOTAL_CAPACITY = temp;
   }

   static final class GCState {
      final GarbageCollectorMXBean gcBean;
      final boolean assumeGCIsPartiallyConcurrent;
      final boolean assumeGCIsOldGen;
      private String[] keys;
      long lastGcTotalDuration = 0L;

      GCState(GarbageCollectorMXBean gcBean, boolean assumeGCIsPartiallyConcurrent, boolean assumeGCIsOldGen) {
         this.gcBean = gcBean;
         this.assumeGCIsPartiallyConcurrent = assumeGCIsPartiallyConcurrent;
         this.assumeGCIsOldGen = assumeGCIsOldGen;
      }

      String[] keys(GarbageCollectionNotificationInfo info) {
         if(this.keys != null) {
            return this.keys;
         } else {
            this.keys = (String[])info.getGcInfo().getMemoryUsageBeforeGc().keySet().toArray(new String[0]);
            Arrays.sort(this.keys);
            return this.keys;
         }
      }
   }

   static final class State {
      final double maxRealTimeElapsed;
      final double totalRealTimeElapsed;
      final double sumSquaresRealTimeElapsed;
      final double totalBytesReclaimed;
      final double count;
      final long startNanos;

      State(double extraElapsed, double extraBytes, GCInspector.State prev) {
         this.totalRealTimeElapsed = prev.totalRealTimeElapsed + extraElapsed;
         this.totalBytesReclaimed = prev.totalBytesReclaimed + extraBytes;
         this.sumSquaresRealTimeElapsed = prev.sumSquaresRealTimeElapsed + extraElapsed * extraElapsed;
         this.startNanos = prev.startNanos;
         this.count = prev.count + 1.0D;
         this.maxRealTimeElapsed = Math.max(prev.maxRealTimeElapsed, extraElapsed);
      }

      State() {
         this.count = this.maxRealTimeElapsed = this.sumSquaresRealTimeElapsed = this.totalRealTimeElapsed = this.totalBytesReclaimed = 0.0D;
         this.startNanos = System.nanoTime();
      }
   }
}
