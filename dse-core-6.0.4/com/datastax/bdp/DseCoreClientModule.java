package com.datastax.bdp;

import com.datastax.bdp.config.ClientConfiguration;
import com.datastax.bdp.config.ClientConfigurationFactory;
import com.datastax.bdp.config.DseConfig;
import com.datastax.bdp.config.DseConfigurationLoader;
import com.datastax.bdp.plugin.PerformanceObjectsController;
import com.datastax.bdp.plugin.ThreadPoolPluginBean;
import com.datastax.bdp.plugin.bean.HistogramDataTablesBean;
import com.datastax.bdp.plugin.bean.PluginBean;
import com.datastax.bdp.plugin.bean.UserLatencyTrackingBean;
import com.datastax.bdp.server.DseDaemon;
import com.datastax.bdp.system.SystemTimeSource;
import com.datastax.bdp.system.TimeSource;
import com.datastax.bdp.tools.DseTool;
import com.datastax.bdp.tools.DseToolArgumentParser;
import com.datastax.bdp.tools.DseToolCommands;
import com.datastax.bdp.tools.NodeJmxProxyPoolFactory;
import com.datastax.bdp.tools.ProxySource;
import com.google.inject.AbstractModule;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import java.security.ProviderException;
import org.apache.cassandra.exceptions.ConfigurationException;

public class DseCoreClientModule extends AbstractModule {
   public DseCoreClientModule() {
      DseDaemon.initDseClientMode();
      DseConfig.init();
   }

   protected void configure() {
      this.binder().requireExplicitBindings();
      Multibinder<PluginBean> plugins = Multibinder.newSetBinder(this.binder(), PluginBean.class);
      Class[] var2 = this.getPluginBeanClasses();
      int var3 = var2.length;

      for(int var4 = 0; var4 < var3; ++var4) {
         Class<? extends PluginBean> beanClass = var2[var4];
         plugins.addBinding().to(beanClass);
      }

      Multibinder.newSetBinder(this.binder(), ProxySource.class).addBinding().to(DseToolCommands.class);
      this.bind(ClientConfiguration.class).toProvider(() -> {
         try {
            return ClientConfigurationFactory.getClientConfiguration();
         } catch (ConfigurationException var1) {
            throw new ProviderException(var1);
         }
      }).in(Scopes.SINGLETON);
      this.bind(NodeJmxProxyPoolFactory.class);
      this.bind(DseConfigurationLoader.class);
      this.bind(DseToolArgumentParser.class).in(Scopes.SINGLETON);
      this.bind(DseTool.class);
      this.bind(DseToolCommands.class);
      this.bind(TimeSource.class).to(SystemTimeSource.class);
   }

   protected Class<? extends PluginBean>[] getPluginBeanClasses() {
      return new Class[]{PerformanceObjectsController.CqlSlowLogBean.class, PerformanceObjectsController.CqlSystemInfoBean.class, PerformanceObjectsController.ClusterSummaryStatsBean.class, PerformanceObjectsController.DbSummaryStatsBean.class, HistogramDataTablesBean.class, PerformanceObjectsController.ResourceLatencyTrackingBean.class, UserLatencyTrackingBean.class, ThreadPoolPluginBean.class};
   }
}
