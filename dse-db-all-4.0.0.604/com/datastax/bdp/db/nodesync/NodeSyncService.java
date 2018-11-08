package com.datastax.bdp.db.nodesync;

import com.datastax.bdp.db.utils.concurrent.CompletableFutures;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Uninterruptibles;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.NodeSyncConfig;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.exceptions.InternalRequestExecutionException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.metrics.AbstractMetricNameFactory;
import org.apache.cassandra.metrics.MetricNameFactory;
import org.apache.cassandra.metrics.NodeSyncMetrics;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MessagingVersion;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Streams;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.units.RateUnit;
import org.apache.cassandra.utils.units.RateValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeSyncService implements NodeSyncServiceMBean {
   private static final Logger logger = LoggerFactory.getLogger(NodeSyncService.class);
   public static final String MIN_VALIDATION_INTERVAL_PROP_NAME = "dse.nodesync.min_validation_interval_ms";
   public static final long MIN_VALIDATION_INTERVAL_MS;
   static final long MIN_WARN_INTERVAL_MS;
   private static final MetricNameFactory factory;
   private final NodeSyncMetrics metrics;
   final NodeSyncStatusTableProxy statusTableProxy;
   private final NodeSyncConfig config;
   private volatile NodeSyncService.Instance instance;
   private volatile NodeSyncService.DelayedUpgradeStartListener upgradeListener;
   private final NodeSyncTracing tracing;

   public NodeSyncService() {
      this(NodeSyncStatusTableProxy.DEFAULT);
   }

   @VisibleForTesting
   NodeSyncService(NodeSyncStatusTableProxy statusTableProxy) {
      this(DatabaseDescriptor.getNodeSyncConfig(), statusTableProxy);
   }

   @VisibleForTesting
   NodeSyncService(NodeSyncConfig config, NodeSyncStatusTableProxy statusTableProxy) {
      this.metrics = new NodeSyncMetrics(factory, "NodeSync");
      this.tracing = new NodeSyncTracing();
      this.config = config;
      this.statusTableProxy = statusTableProxy;
      this.registerJMX();
   }

   private static boolean supportsNodeSync(InetAddress endpoint) {
      return ((Boolean)MessagingService.instance().getVersion(endpoint).map((v) -> {
         return Boolean.valueOf(v.isDSE() && v.compareTo(MessagingVersion.DSE_60) >= 0);
      }).orElse(Boolean.valueOf(false))).booleanValue();
   }

   public NodeSyncMetrics metrics() {
      return this.metrics;
   }

   public NodeSyncConfig config() {
      return this.config;
   }

   NodeSyncTracing tracing() {
      return this.tracing;
   }

   private void registerJMX() {
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

      try {
         ObjectName jmxName = new ObjectName(MBEAN_NAME);
         mbs.registerMBean(this, jmxName);
      } catch (InstanceAlreadyExistsException var3) {
         logger.error("Cannot register NodeSync through JMX as a prior instance already exists: this shouldn't happen and should be reported to support. It won't prevent NodeSync from running, but it will prevent controlling this instance through JMX");
      } catch (Exception var4) {
         logger.error("Cannot register NodeSync through JMX due to unexpected error: this shouldn't happen and should be reported to support. It won't prevent NodeSync from running, but it will prevent controlling this instance through JMX", var4);
      }

   }

   public CompletableFuture<Boolean> enableAsync() {
      return this.enableAsync(true);
   }

   private synchronized CompletableFuture<Boolean> enableAsync(boolean doNodeVersionCheck) {
      this.config.setEnabled(true);
      if(this.isRunning()) {
         return CompletableFuture.completedFuture(Boolean.valueOf(false));
      } else {
         if(doNodeVersionCheck) {
            if(this.upgradeListener != null) {
               return CompletableFutures.exceptionallyCompletedFuture(new NodeSyncService.UpgradingClusterException(this.upgradeListener.nonUpgradedNodes));
            }

            Set<InetAddress> nonUpgradeNodes = (Set)Streams.of(Gossiper.instance.getAllEndpoints()).filter((n) -> {
               return !supportsNodeSync(n);
            }).collect(Collectors.toSet());
            if(!nonUpgradeNodes.isEmpty()) {
               this.upgradeListener = new NodeSyncService.DelayedUpgradeStartListener(nonUpgradeNodes);
               return CompletableFutures.exceptionallyCompletedFuture(new NodeSyncService.UpgradingClusterException(nonUpgradeNodes));
            }
         }

         try {
            this.instance = new NodeSyncService.Instance();
            return this.instance.start();
         } catch (RuntimeException var3) {
            if(this.instance != null) {
               this.instance.stop(true);
            }

            this.config.setEnabled(false);
            return CompletableFutures.exceptionallyCompletedFuture(var3);
         }
      }
   }

   public boolean enable() {
      try {
         return this.enable(9223372036854775807L, TimeUnit.DAYS);
      } catch (TimeoutException var2) {
         throw new AssertionError("I hope the wait wasn't too long");
      }
   }

   public boolean enable(long timeout, TimeUnit timeoutUnit) throws TimeoutException {
      try {
         Uninterruptibles.getUninterruptibly(this.enableAsync(), timeout, timeoutUnit);
         return true;
      } catch (ExecutionException var5) {
         throw Throwables.cleaned(var5);
      }
   }

   public synchronized CompletableFuture<Boolean> disableAsync(boolean force) {
      this.config.setEnabled(false);
      return !this.isRunning()?CompletableFuture.completedFuture(Boolean.valueOf(false)):this.instance.stop(force).thenRun(() -> {
         logger.info("Disabled NodeSync service");
      }).thenApply((x) -> {
         return Boolean.valueOf(true);
      });
   }

   public boolean disable() {
      try {
         return this.disable(false, 9223372036854775807L, TimeUnit.DAYS);
      } catch (TimeoutException var2) {
         throw new AssertionError("I hope the wait wasn't too long");
      }
   }

   public boolean disable(boolean force, long timeout, TimeUnit timeoutUnit) throws TimeoutException {
      if(!this.isRunning()) {
         return false;
      } else {
         try {
            Uninterruptibles.getUninterruptibly(this.disableAsync(force), timeout, timeoutUnit);
            return true;
         } catch (ExecutionException var6) {
            throw new AssertionError(var6);
         }
      }
   }

   private synchronized void finishShutdown() {
      this.instance = null;
   }

   public boolean isRunning() {
      return this.instance != null;
   }

   public boolean canPurge(TableMetadata table, int gcBefore) {
      NodeSyncService.Instance current = this.instance;
      if(current == null) {
         return true;
      } else {
         ContinuousValidationProposer proposer = current.scheduler.getContinuousProposer(table);
         return proposer == null?true:(long)gcBefore <= proposer.state.oldestSuccessfulValidation();
      }
   }

   void updateMetrics(TableMetadata table, ValidationMetrics validationMetrics) {
      validationMetrics.addTo(this.metrics);
      ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(table.id);
      if(cfs != null) {
         validationMetrics.addTo(cfs.metric.nodeSyncMetrics);
      }

   }

   public void setRate(int kbPerSecond) {
      NodeSyncService.Instance current = this.instance;
      if(current != null) {
         current.scheduler.setRate(RateValue.of((long)kbPerSecond, RateUnit.KB_S));
         current.maintenanceTasks.onRateUpdate();
      } else {
         this.config.setRate(RateValue.of((long)kbPerSecond, RateUnit.KB_S));
      }

   }

   public int getRate() {
      return (int)this.config.getRate().in(RateUnit.KB_S);
   }

   void startUserValidation(UserValidationOptions options) {
      NodeSyncService.Instance current = this.instance;
      if(current == null) {
         throw new NodeSyncService.NodeSyncNotRunningException("Cannot start user validation, NodeSync is not currently running.");
      } else {
         current.scheduler.addUserValidation(options);
      }
   }

   public void startUserValidation(Map<String, String> optionMap) {
      this.startUserValidation(UserValidationOptions.fromMap(optionMap));
   }

   public void startUserValidation(String id, String keyspace, String table, String ranges, Integer rateInKB) {
      HashMap<String, String> m = new HashMap();
      m.put("id", id);
      m.put("keyspace", keyspace);
      m.put("table", table);
      if(ranges != null && !ranges.isEmpty()) {
         m.put("ranges", ranges);
      }

      if(rateInKB != null) {
         m.put("rate", rateInKB.toString());
      }

      this.startUserValidation((Map)m);
   }

   public void cancelUserValidation(String id) {
      this.cancelUserValidation(UserValidationID.from(id));
   }

   void cancelUserValidation(UserValidationID id) {
      NodeSyncService.Instance current = this.instance;
      if(current == null) {
         throw new NodeSyncService.NodeSyncNotRunningException("Cannot cancel user validation, NodeSync is not currently running.");
      } else {
         UserValidationProposer proposer = current.scheduler.getUserValidation(id);
         if(proposer == null) {
            throw new NodeSyncService.NotFoundValidationException(id);
         } else if(!proposer.cancel()) {
            throw new NodeSyncService.CancelledValidationException(id);
         }
      }
   }

   public List<Map<String, String>> getRateSimulatorInfo(boolean includeAllTables) {
      return RateSimulator.Info.compute(includeAllTables).toJMX();
   }

   public UUID enableTracing() {
      return this.enableTracing(Collections.emptyMap());
   }

   public UUID enableTracing(Map<String, String> optionMap) {
      return this.enableTracing(TracingOptions.fromMap(optionMap));
   }

   UUID enableTracing(TracingOptions options) {
      return this.tracing.enable(options);
   }

   public UUID currentTracingSession() {
      return (UUID)this.currentTracingSessionIfEnabled().orElse((Object)null);
   }

   Optional<UUID> currentTracingSessionIfEnabled() {
      return this.tracing.currentTracingSession();
   }

   public boolean isTracingEnabled() {
      return this.tracing.isEnabled();
   }

   public void disableTracing() {
      this.tracing.disable();
   }

   static {
      MIN_VALIDATION_INTERVAL_MS = Long.getLong("dse.nodesync.min_validation_interval_ms", TimeUnit.MINUTES.toMillis(5L)).longValue();
      MIN_WARN_INTERVAL_MS = TimeUnit.SECONDS.toMillis(Long.getLong("dse.nodesync.min_warn_interval_sec", TimeUnit.HOURS.toSeconds(10L)).longValue());
      factory = new AbstractMetricNameFactory("com.datastax.nodesync", "NodeSyncMetrics");
   }

   public static final class CancelledValidationException extends NodeSyncService.NodeSyncServiceException {
      CancelledValidationException(UserValidationID id) {
         super(RequestFailureReason.CANCELLED_NODESYNC_USER_VALIDATION, "User validation #" + id + " is already completed", null);
      }
   }

   public static final class NotFoundValidationException extends NodeSyncService.NodeSyncServiceException {
      NotFoundValidationException(UserValidationID id) {
         super(RequestFailureReason.UNKNOWN_NODESYNC_USER_VALIDATION, "Cannot find user validation #" + id, null);
      }
   }

   public static final class NodeSyncNotRunningException extends NodeSyncService.NodeSyncServiceException {
      NodeSyncNotRunningException(String message) {
         super(RequestFailureReason.NODESYNC_NOT_RUNNING, message, null);
      }
   }

   public static final class UpgradingClusterException extends RuntimeException {
      private UpgradingClusterException(Set<InetAddress> nonUpgradedNodes) {
         super(msg(nonUpgradedNodes));
      }

      private static String msg(Set<InetAddress> nonUpgradedNodes) {
         String nodesStr = (String)nonUpgradedNodes.stream().map(InetAddress::toString).collect(Collectors.joining(", "));
         return String.format("The NodeSync service cannot start at this time as some nodes in the cluster (%s) are not DSE 6+ nodes yet. The service will start automatically once those nodes are upgraded.", new Object[]{nodesStr});
      }
   }

   static class NodeSyncServiceException extends InternalRequestExecutionException {
      private NodeSyncServiceException(RequestFailureReason reason, String message) {
         super(reason, message);
      }
   }

   private class DelayedUpgradeStartListener implements IEndpointStateChangeSubscriber {
      private final Set<InetAddress> nonUpgradedNodes;

      private DelayedUpgradeStartListener(Set<InetAddress> var1) {
         this.nonUpgradedNodes = ConcurrentHashMap.newKeySet();

         assert !nonUpgradedNodes.isEmpty();

         this.nonUpgradedNodes.addAll(nonUpgradedNodes);
         Gossiper.instance.register(this);
      }

      private void maybeStartService(InetAddress changedEndpoint) {
         if(NodeSyncService.supportsNodeSync(changedEndpoint)) {
            this.nonUpgradedNodes.remove(changedEndpoint);
            if(this.nonUpgradedNodes.isEmpty()) {
               Gossiper.instance.unregister(this);
               NodeSyncService var2 = NodeSyncService.this;
               synchronized(NodeSyncService.this) {
                  NodeSyncService.this.upgradeListener = null;
               }

               if(NodeSyncService.this.config.isEnabled()) {
                  NodeSyncService.logger.info("All nodes have been upgraded to DSE 6+, the NodeSync service can now be started");
                  NodeSyncService.this.enableAsync(false);
               }

            }
         }
      }

      public void onJoin(InetAddress endpoint, EndpointState epState) {
         this.maybeStartService(endpoint);
      }

      public void beforeChange(InetAddress endpoint, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue) {
      }

      public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value) {
      }

      public void onAlive(InetAddress endpoint, EndpointState state) {
         this.maybeStartService(endpoint);
      }

      public void onDead(InetAddress endpoint, EndpointState state) {
      }

      public void onRemove(InetAddress endpoint) {
      }

      public void onRestart(InetAddress endpoint, EndpointState state) {
         this.maybeStartService(endpoint);
      }
   }

   class Instance {
      final NodeSyncState state;
      final ValidationScheduler scheduler;
      private final ValidationExecutor executor;
      private final NodeSyncMaintenanceTasks maintenanceTasks;

      private Instance() {
         this.state = new NodeSyncState(NodeSyncService.this);
         this.scheduler = new ValidationScheduler(this.state);
         this.executor = new ValidationExecutor(this.scheduler, NodeSyncService.this.config);
         this.maintenanceTasks = new NodeSyncMaintenanceTasks(this);
      }

      NodeSyncService service() {
         return NodeSyncService.this;
      }

      private CompletableFuture<Boolean> start() {
         Schema.instance.registerListener(this.scheduler);
         StorageService.instance.register(this.scheduler);
         this.executor.start();
         this.maintenanceTasks.start();
         if(StorageService.instance.getTokenMetadata().getAllEndpoints().size() == 1) {
            NodeSyncService.logger.info("Enabled NodeSync service (currently inactive as this is the only node in the cluster; will activate automatically once more nodes join)");
            return CompletableFuture.completedFuture(Boolean.valueOf(true));
         } else {
            return this.scheduler.addAllContinuous(NodeSyncHelpers.nodeSyncEnabledTables()).handle((tables, exc) -> {
               if(exc != null) {
                  this.stop(true);
                  throw Throwables.cleaned(exc);
               } else {
                  String details = tables.isEmpty()?"currently inactive as no replicated table has NodeSync enabled; will activate automatically once this change":(tables.size() == 1?"1 table has NodeSync enabled":tables.size() + " tables have NodeSync enabled");
                  NodeSyncService.logger.info("Enabled NodeSync service ({})", details);
                  return Boolean.valueOf(true);
               }
            });
         }
      }

      private CompletableFuture<Void> stop(boolean force) {
         Schema.instance.unregisterListener(this.scheduler);
         StorageService.instance.unregister(this.scheduler);
         CompletableFuture var10000 = this.executor.shutdown(force);
         NodeSyncMaintenanceTasks var10001 = this.maintenanceTasks;
         this.maintenanceTasks.getClass();
         return var10000.thenRun(var10001::stop).thenRun(() -> {
            NodeSyncService.this.finishShutdown();
         });
      }
   }
}
