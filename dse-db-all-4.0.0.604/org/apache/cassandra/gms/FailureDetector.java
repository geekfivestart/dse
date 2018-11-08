package org.apache.cassandra.gms;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FailureDetector implements IFailureDetector, FailureDetectorMBean {
   private static final Logger logger = LoggerFactory.getLogger(FailureDetector.class);
   public static final String MBEAN_NAME = "org.apache.cassandra.net:type=FailureDetector";
   private static final int SAMPLE_SIZE = 1000;
   protected static final long INITIAL_VALUE_NANOS;
   private static final int DEBUG_PERCENTAGE = 80;
   private static final long DEFAULT_MAX_PAUSE = 5000000000L;
   private static final long MAX_LOCAL_PAUSE_IN_NANOS;
   private long lastInterpret = System.nanoTime();
   private long lastPause = 0L;
   public static final IFailureDetector instance;
   private final double PHI_FACTOR = 1.0D / Math.log(10.0D);
   private final ConcurrentHashMap<InetAddress, ArrivalWindow> arrivalSamples = new ConcurrentHashMap();
   private final List<IFailureDetectionEventListener> fdEvntListeners = new CopyOnWriteArrayList();

   protected static long getMaxLocalPause() {
      if(System.getProperty("cassandra.max_local_pause_in_ms") != null) {
         long pause = Long.parseLong(System.getProperty("cassandra.max_local_pause_in_ms"));
         logger.warn("Overriding max local pause time to {}ms", Long.valueOf(pause));
         return pause * 1000000L;
      } else {
         return 5000000000L;
      }
   }

   public FailureDetector() {
      try {
         MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
         mbs.registerMBean(this, new ObjectName("org.apache.cassandra.net:type=FailureDetector"));
      } catch (Exception var2) {
         throw new RuntimeException(var2);
      }
   }

   private static long getInitialValue() {
      String newvalue = System.getProperty("cassandra.fd_initial_value_ms");
      if(newvalue == null) {
         return 2000L;
      } else {
         logger.info("Overriding FD INITIAL_VALUE to {}ms", newvalue);
         return (long)Integer.parseInt(newvalue);
      }
   }

   public String getAllEndpointStates() {
      StringBuilder sb = new StringBuilder();
      Iterator var2 = Gossiper.instance.endpointStateMap.entrySet().iterator();

      while(var2.hasNext()) {
         Entry<InetAddress, EndpointState> entry = (Entry)var2.next();
         sb.append(entry.getKey()).append("\n");
         this.appendEndpointState(sb, (EndpointState)entry.getValue());
      }

      return sb.toString();
   }

   public Map<String, String> getSimpleStates() {
      Map<String, String> nodesStatus = new HashMap(Gossiper.instance.endpointStateMap.size());
      Iterator var2 = Gossiper.instance.endpointStateMap.entrySet().iterator();

      while(var2.hasNext()) {
         Entry<InetAddress, EndpointState> entry = (Entry)var2.next();
         if(((EndpointState)entry.getValue()).isAlive()) {
            nodesStatus.put(((InetAddress)entry.getKey()).toString(), "UP");
         } else {
            nodesStatus.put(((InetAddress)entry.getKey()).toString(), "DOWN");
         }
      }

      return nodesStatus;
   }

   public int getDownEndpointCount() {
      int count = 0;
      Iterator var2 = Gossiper.instance.endpointStateMap.entrySet().iterator();

      while(var2.hasNext()) {
         Entry<InetAddress, EndpointState> entry = (Entry)var2.next();
         if(!((EndpointState)entry.getValue()).isAlive()) {
            ++count;
         }
      }

      return count;
   }

   public int getUpEndpointCount() {
      int count = 0;
      Iterator var2 = Gossiper.instance.endpointStateMap.entrySet().iterator();

      while(var2.hasNext()) {
         Entry<InetAddress, EndpointState> entry = (Entry)var2.next();
         if(((EndpointState)entry.getValue()).isAlive()) {
            ++count;
         }
      }

      return count;
   }

   public TabularData getPhiValues() throws OpenDataException {
      CompositeType ct = new CompositeType("Node", "Node", new String[]{"Endpoint", "PHI"}, new String[]{"IP of the endpoint", "PHI value"}, new OpenType[]{SimpleType.STRING, SimpleType.DOUBLE});
      TabularDataSupport results = new TabularDataSupport(new TabularType("PhiList", "PhiList", ct, new String[]{"Endpoint"}));
      Iterator var3 = this.arrivalSamples.entrySet().iterator();

      while(var3.hasNext()) {
         Entry<InetAddress, ArrivalWindow> entry = (Entry)var3.next();
         ArrivalWindow window = (ArrivalWindow)entry.getValue();
         if(window.mean() > 0.0D) {
            double phi = window.getLastReportedPhi();
            if(phi != 4.9E-324D) {
               CompositeData data = new CompositeDataSupport(ct, new String[]{"Endpoint", "PHI"}, new Object[]{((InetAddress)entry.getKey()).toString(), Double.valueOf(phi * this.PHI_FACTOR)});
               results.put(data);
            }
         }
      }

      return results;
   }

   public String getEndpointState(String address) throws UnknownHostException {
      StringBuilder sb = new StringBuilder();
      EndpointState endpointState = Gossiper.instance.getEndpointStateForEndpoint(InetAddress.getByName(address));
      this.appendEndpointState(sb, endpointState);
      return sb.toString();
   }

   private void appendEndpointState(StringBuilder sb, EndpointState endpointState) {
      sb.append("  generation:").append(endpointState.getHeartBeatState().getGeneration()).append("\n");
      sb.append("  heartbeat:").append(endpointState.getHeartBeatState().getHeartBeatVersion()).append("\n");
      Iterator var3 = endpointState.states().iterator();

      while(var3.hasNext()) {
         Entry<ApplicationState, VersionedValue> state = (Entry)var3.next();
         if(state.getKey() != ApplicationState.TOKENS) {
            sb.append("  ").append(state.getKey()).append(":").append(((VersionedValue)state.getValue()).version).append(":").append(((VersionedValue)state.getValue()).value).append("\n");
         }
      }

      VersionedValue tokens = endpointState.getApplicationState(ApplicationState.TOKENS);
      if(tokens != null) {
         sb.append("  TOKENS:").append(tokens.version).append(":<hidden>\n");
      } else {
         sb.append("  TOKENS: not present\n");
      }

   }

   public void dumpInterArrivalTimes() {
      File file = FileUtils.createTempFile("failuredetector-", ".dat");

      try {
         OutputStream os = new BufferedOutputStream(new FileOutputStream(file, true));
         Throwable var3 = null;

         try {
            os.write(this.toString().getBytes());
         } catch (Throwable var13) {
            var3 = var13;
            throw var13;
         } finally {
            if(os != null) {
               if(var3 != null) {
                  try {
                     os.close();
                  } catch (Throwable var12) {
                     var3.addSuppressed(var12);
                  }
               } else {
                  os.close();
               }
            }

         }

      } catch (IOException var15) {
         throw new FSWriteError(var15, file);
      }
   }

   public void setPhiConvictThreshold(double phi) {
      DatabaseDescriptor.setPhiConvictThreshold(phi);
   }

   public double getPhiConvictThreshold() {
      return DatabaseDescriptor.getPhiConvictThreshold();
   }

   public boolean isAlive(InetAddress ep) {
      if(ep.equals(FBUtilities.getBroadcastAddress())) {
         return true;
      } else {
         EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(ep);
         if(epState == null) {
            logger.error("unknown endpoint {}", ep);
         }

         return epState != null && epState.isAlive();
      }
   }

   public void report(InetAddress ep) {
      long now = System.nanoTime();
      ArrivalWindow heartbeatWindow = (ArrivalWindow)this.arrivalSamples.get(ep);
      if(heartbeatWindow == null) {
         heartbeatWindow = new ArrivalWindow(1000);
         heartbeatWindow.add(now, ep);
         heartbeatWindow = (ArrivalWindow)this.arrivalSamples.putIfAbsent(ep, heartbeatWindow);
         if(heartbeatWindow != null) {
            heartbeatWindow.add(now, ep);
         }
      } else {
         heartbeatWindow.add(now, ep);
      }

      if(logger.isTraceEnabled() && heartbeatWindow != null) {
         logger.trace("Average for {} is {}", ep, Double.valueOf(heartbeatWindow.mean()));
      }

   }

   public void interpret(InetAddress ep) {
      ArrivalWindow hbWnd = (ArrivalWindow)this.arrivalSamples.get(ep);
      if(hbWnd != null) {
         long now = System.nanoTime();
         long diff = now - this.lastInterpret;
         this.lastInterpret = now;
         if(diff > MAX_LOCAL_PAUSE_IN_NANOS) {
            logger.warn("Not marking nodes down due to local pause of {} > {}", Long.valueOf(diff), Long.valueOf(MAX_LOCAL_PAUSE_IN_NANOS));
            this.lastPause = now;
         } else if(System.nanoTime() - this.lastPause < MAX_LOCAL_PAUSE_IN_NANOS) {
            logger.debug("Still not marking nodes down due to local pause");
         } else {
            double phi = hbWnd.phi(now);
            if(logger.isTraceEnabled()) {
               logger.trace("PHI for {} : {}", ep, Double.valueOf(phi));
            }

            if(this.PHI_FACTOR * phi > this.getPhiConvictThreshold()) {
               if(logger.isTraceEnabled()) {
                  logger.trace("Node {} phi {} > {}; intervals: {} mean: {}", new Object[]{ep, Double.valueOf(this.PHI_FACTOR * phi), Double.valueOf(this.getPhiConvictThreshold()), hbWnd, Double.valueOf(hbWnd.mean())});
               }

               Iterator var9 = this.fdEvntListeners.iterator();

               while(var9.hasNext()) {
                  IFailureDetectionEventListener listener = (IFailureDetectionEventListener)var9.next();
                  listener.convict(ep, phi);
               }
            } else if(logger.isDebugEnabled() && this.PHI_FACTOR * phi * 80.0D / 100.0D > this.getPhiConvictThreshold()) {
               logger.debug("PHI for {} : {}", ep, Double.valueOf(phi));
            } else if(logger.isTraceEnabled()) {
               logger.trace("PHI for {} : {}", ep, Double.valueOf(phi));
               logger.trace("mean for {} : {}", ep, Double.valueOf(hbWnd.mean()));
            }

         }
      }
   }

   public void forceConviction(InetAddress ep) {
      logger.debug("Forcing conviction of {}", ep);
      Iterator var2 = this.fdEvntListeners.iterator();

      while(var2.hasNext()) {
         IFailureDetectionEventListener listener = (IFailureDetectionEventListener)var2.next();
         listener.convict(ep, this.getPhiConvictThreshold());
      }

   }

   public void remove(InetAddress ep) {
      this.arrivalSamples.remove(ep);
   }

   public void registerFailureDetectionEventListener(IFailureDetectionEventListener listener) {
      this.fdEvntListeners.add(listener);
   }

   public void unregisterFailureDetectionEventListener(IFailureDetectionEventListener listener) {
      this.fdEvntListeners.remove(listener);
   }

   public String toString() {
      StringBuilder sb = new StringBuilder();
      Set<InetAddress> eps = this.arrivalSamples.keySet();
      sb.append("-----------------------------------------------------------------------");
      Iterator var3 = eps.iterator();

      while(var3.hasNext()) {
         InetAddress ep = (InetAddress)var3.next();
         ArrivalWindow hWnd = (ArrivalWindow)this.arrivalSamples.get(ep);
         sb.append(ep + " : ");
         sb.append(hWnd);
         sb.append(System.getProperty("line.separator"));
      }

      sb.append("-----------------------------------------------------------------------");
      return sb.toString();
   }

   static {
      INITIAL_VALUE_NANOS = TimeUnit.NANOSECONDS.convert(getInitialValue(), TimeUnit.MILLISECONDS);
      MAX_LOCAL_PAUSE_IN_NANOS = getMaxLocalPause();
      instance = new FailureDetector();
   }
}
