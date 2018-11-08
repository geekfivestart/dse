package com.datastax.bdp;

import com.datastax.bdp.cassandra.auth.AuthenticationSchemeResource;
import com.datastax.bdp.cassandra.auth.CassandraDelegationTokenSecretManager;
import com.datastax.bdp.cassandra.auth.DigestTokensManager;
import com.datastax.bdp.cassandra.auth.DseResourceFactory;
import com.datastax.bdp.cassandra.auth.DseRowResource;
import com.datastax.bdp.cassandra.auth.ResourceManagerSubmissionResource;
import com.datastax.bdp.cassandra.auth.ResourceManagerWorkPoolResource;
import com.datastax.bdp.cassandra.auth.RpcResource;
import com.datastax.bdp.cassandra.auth.SaslServerDigestCallbackHandler;
import com.datastax.bdp.cassandra.cql3.CqlSlowLogPlugin;
import com.datastax.bdp.cassandra.cql3.DseQueryHandler;
import com.datastax.bdp.cassandra.metrics.LeaseMetricsPlugin;
import com.datastax.bdp.cassandra.metrics.NodeObjectLatencyPlugin;
import com.datastax.bdp.cassandra.metrics.PercentileFilter;
import com.datastax.bdp.cassandra.metrics.PerformanceObjectsPlugin;
import com.datastax.bdp.cassandra.metrics.UserLatencyMetricsWriter;
import com.datastax.bdp.cassandra.metrics.UserObjectLatencyPlugin;
import com.datastax.bdp.config.DseConfig;
import com.datastax.bdp.ioc.DseInjector;
import com.datastax.bdp.ioc.UserLatencyMetricsWriterProvider;
import com.datastax.bdp.leasemanager.LeasePlugin;
import com.datastax.bdp.leasemanager.LeaseProtocol;
import com.datastax.bdp.leasemanager.SmallExclusiveTasksPlugin;
import com.datastax.bdp.node.transport.internode.InternodeClient;
import com.datastax.bdp.node.transport.internode.InternodeMessaging;
import com.datastax.bdp.node.transport.internode.InternodeProtocolRegistry;
import com.datastax.bdp.plugin.DseClientToolPlugin;
import com.datastax.bdp.plugin.DseSystemPlugin;
import com.datastax.bdp.plugin.InternalQueryRouterPlugin;
import com.datastax.bdp.plugin.PerformanceObjectsController;
import com.datastax.bdp.plugin.PluginManager;
import com.datastax.bdp.plugin.ThreadPoolPlugin;
import com.datastax.bdp.plugin.ThreadPoolPluginBean;
import com.datastax.bdp.plugin.bean.HistogramDataTablesBean;
import com.datastax.bdp.plugin.bean.LeaseMetricsBean;
import com.datastax.bdp.plugin.bean.PluginBean;
import com.datastax.bdp.plugin.bean.UserLatencyTrackingBean;
import com.datastax.bdp.plugin.health.NodeHealthPlugin;
import com.datastax.bdp.plugin.health.NodeHealthPluginUpdater;
import com.datastax.bdp.reporting.CqlSystemInfoPlugin;
import com.datastax.bdp.reporting.snapshots.db.DbInfoRollupPlugin;
import com.datastax.bdp.reporting.snapshots.db.TableSnapshotPlugin;
import com.datastax.bdp.reporting.snapshots.histograms.HistogramInfoPlugin;
import com.datastax.bdp.reporting.snapshots.node.ClusterInfoRollupPlugin;
import com.datastax.bdp.reporting.snapshots.node.NodeSnapshotPlugin;
import com.datastax.bdp.router.InternalQueryRouter;
import com.datastax.bdp.router.InternalQueryRouterProtocol;
import com.datastax.bdp.server.DseDaemon;
import com.datastax.bdp.server.LifecycleAware;
import com.datastax.bdp.system.PerformanceObjectsKeyspace;
import com.datastax.bdp.system.SystemTimeSource;
import com.datastax.bdp.system.TimeSource;
import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Provider;
import com.google.inject.multibindings.Multibinder;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.config.DatabaseDescriptor;

public class DseCoreModule extends AbstractModule {
   public DseCoreModule() {
      DatabaseDescriptor.daemonInitialization();
      DseConfig.init();
   }

   protected void configure() {
      Binder binder = this.binder();
      binder.requireExplicitBindings();
      Multibinder<LifecycleAware> lifecycleListeners = Multibinder.newSetBinder(binder, LifecycleAware.class);
      Class[] var3 = this.getLifecycleListeners();
      int var4 = var3.length;

      int var5;
      for(var5 = 0; var5 < var4; ++var5) {
         Class<? extends LifecycleAware> beanClass = var3[var5];
         lifecycleListeners.addBinding().to(beanClass);
      }

      Multibinder<PluginBean> plugins = Multibinder.newSetBinder(binder, PluginBean.class);
      Class[] var10 = this.getPluginBeanClasses();
      var5 = var10.length;

      int var13;
      for(var13 = 0; var13 < var5; ++var13) {
         Class<? extends PluginBean> beanClass = var10[var13];
         plugins.addBinding().to(beanClass);
      }

      Multibinder<DseResourceFactory.Factory> resourceFactories = Multibinder.newSetBinder(binder, DseResourceFactory.Factory.class);
      Class[] var12 = this.getResourceFactories();
      var13 = var12.length;

      for(int var14 = 0; var14 < var13; ++var14) {
         Class<? extends DseResourceFactory.Factory> resourceFactory = var12[var14];
         resourceFactories.addBinding().to(resourceFactory);
      }

      this.bind(ClusterInfoRollupPlugin.class);
      this.bind(PerformanceObjectsController.ClusterSummaryStatsBean.class);
      this.bind(PerformanceObjectsController.CqlSlowLogBean.class);
      this.bind(CqlSlowLogPlugin.class);
      this.bind(PerformanceObjectsController.CqlSystemInfoBean.class);
      this.bind(CqlSystemInfoPlugin.class);
      this.bind(DbInfoRollupPlugin.class);
      this.bind(PerformanceObjectsController.DbSummaryStatsBean.class);
      this.bindDseDaemon();
      this.bind(DseClientToolPlugin.class);
      this.bind(DseSystemPlugin.class);
      this.bind(HistogramDataTablesBean.class);
      this.bind(HistogramInfoPlugin.class);
      this.bind(LeasePlugin.class);
      this.bind(LeaseMetricsPlugin.class);
      this.bind(LeaseMetricsBean.class);
      this.bind(NodeHealthPlugin.class);
      this.bind(NodeHealthPluginUpdater.class);
      this.bind(NodeObjectLatencyPlugin.class);
      this.bind(NodeSnapshotPlugin.class);
      this.bind(PercentileFilter.class);
      this.bind(PerformanceObjectsPlugin.class);
      this.bind(PluginManager.class);
      this.bind(PerformanceObjectsController.ResourceLatencyTrackingBean.class);
      this.bind(SmallExclusiveTasksPlugin.class);
      this.bind(TableSnapshotPlugin.class);
      this.bind(ThreadPoolPlugin.class);
      this.bind(ThreadPoolPluginBean.class);
      this.bind(UserLatencyTrackingBean.class);
      this.bind(UserObjectLatencyPlugin.class);
      this.bind(LeaseProtocol.class);
      this.bind(InternodeMessaging.class);
      this.bind(InternalQueryRouterProtocol.class);
      this.bind(InternodeProtocolRegistry.class).to(InternodeMessaging.class);
      this.bind(InternalQueryRouterPlugin.class);
      this.bind(InternalQueryRouter.class).toProvider(InternalQueryRouterPlugin.class);
      this.bind(DigestTokensManager.class);
      this.bindSecretManager();
      this.bind(SaslServerDigestCallbackHandler.class);
      Multibinder.newSetBinder(this.binder(), PerformanceObjectsKeyspace.TableDef.class);
      this.bind(InternodeClient.class).toProvider(InternodeMessaging.class);
      this.requestStaticInjection(new Class[]{DseQueryHandler.class, DseDaemon.class, DseResourceFactory.class});
      this.bind(UserLatencyMetricsWriter.class).toProvider(UserLatencyMetricsWriterProvider.class);
      this.bind(TimeSource.class).to(SystemTimeSource.class);
      this.bind(UserLatencyMetricsWriterProvider.class);
   }

   protected void bindDseDaemon() {
      this.bind(DseDaemon.class);
      this.requestStaticInjection(new Class[]{DseDaemon.class});
   }

   private void bindSecretManager() {
      this.bind(CassandraDelegationTokenSecretManager.class).toProvider(new Provider<CassandraDelegationTokenSecretManager>() {
         private CassandraDelegationTokenSecretManager tokenSecretManager;

         public synchronized CassandraDelegationTokenSecretManager get() {
            if(this.tokenSecretManager == null) {
               long tokenMaxLifetime = TimeUnit.DAYS.toMillis(7L);
               long tokenRenewInterval = TimeUnit.HOURS.toMillis(1L);

               try {
                  Class<?> jobConfClass = Class.forName("com.datastax.bdp.hadoop.mapred.CassandraJobConf");
                  Object jobConf = jobConfClass.getConstructor(new Class[0]).newInstance(new Object[0]);
                  Method getLong = jobConfClass.getMethod("getLong", new Class[]{String.class, Long.TYPE});
                  tokenMaxLifetime = ((Long)getLong.invoke(jobConf, new Object[]{"delegationTokenMaxLifetime", Long.valueOf(tokenMaxLifetime)})).longValue();
                  tokenRenewInterval = ((Long)getLong.invoke(jobConf, new Object[]{"delegationTokenRenewInterval", Long.valueOf(tokenRenewInterval)})).longValue();
               } catch (IllegalAccessException | NoSuchMethodException | InvocationTargetException | InstantiationException var8) {
                  throw new RuntimeException(var8);
               } catch (ClassNotFoundException var9) {
                  ;
               }

               DigestTokensManager dtm = (DigestTokensManager)DseInjector.get().getInstance(DigestTokensManager.class);
               TimeSource timeSource = (TimeSource)DseInjector.get().getInstance(TimeSource.class);
               this.tokenSecretManager = new CassandraDelegationTokenSecretManager(tokenMaxLifetime, tokenRenewInterval, dtm, timeSource);
            }

            return this.tokenSecretManager;
         }
      });
   }

   protected Class<? extends LifecycleAware>[] getLifecycleListeners() {
      return new Class[]{PerformanceObjectsController.class, PluginManager.class, LeasePlugin.class, InternalQueryRouterPlugin.class};
   }

   protected Class<? extends PluginBean>[] getPluginBeanClasses() {
      return new Class[]{PerformanceObjectsController.CqlSlowLogBean.class, PerformanceObjectsController.CqlSystemInfoBean.class, PerformanceObjectsController.ClusterSummaryStatsBean.class, PerformanceObjectsController.DbSummaryStatsBean.class, HistogramDataTablesBean.class, PerformanceObjectsController.ResourceLatencyTrackingBean.class, UserLatencyTrackingBean.class, ThreadPoolPluginBean.class};
   }

   protected Class<? extends DseResourceFactory.Factory>[] getResourceFactories() {
      return new Class[]{AuthenticationSchemeResource.Factory.class, DseRowResource.Factory.class, RpcResource.Factory.class, ResourceManagerWorkPoolResource.Factory.class, ResourceManagerSubmissionResource.Factory.class};
   }
}
